#!/usr/bin/env python3
"""
MITS Free Fundamental Scanner - EOD Fundamental & Relative Strength (RS) Ingestion Pipeline
Strictly Self-Contained & Isolated within mits-free-fundamental-scanner

Ingests daily EOD fundamental metrics and 6-month (123 trading days) price history
for the Nifty 500 universe and Nifty 50 index (^NSEI). Calculates Relative Strength (RS)
ratios, normalizes them into 1-99 percentile RS ratings, categorizes RS status,
and outputs a clean, flattened quantitative data payload to data.json.
"""

import sys
import os
import json
import time
import datetime
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import csv
import io

try:
    import yfinance as yf
except ImportError:
    print("[ERROR] yfinance is required. Install via: pip install yfinance")
    sys.exit(1)

# NSE Official Nifty 500 CSV URL & Benchmark Symbol
NSE_NIFTY500_URL = "https://archives.nseindia.com/content/indices/ind_nifty500list.csv"
BENCHMARK_INDEX_SYMBOL = "^NSEI"  # Nifty 50 Index
WORKSPACE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DATA_PATH = os.path.join(WORKSPACE_DIR, "data.json")

# Fallback top Nifty constituents if NSE network endpoint is unreachable
FALLBACK_SYMBOLS = [
    {"symbol": "RELIANCE", "name": "Reliance Industries Ltd", "industry": "Energy"},
    {"symbol": "TCS", "name": "Tata Consultancy Services Ltd", "industry": "Information Technology"},
    {"symbol": "HDFCBANK", "name": "HDFC Bank Ltd", "industry": "Financial Services"},
    {"symbol": "BHARTIARTL", "name": "Bharti Airtel Ltd", "industry": "Telecommunication"},
    {"symbol": "ICICIBANK", "name": "ICICI Bank Ltd", "industry": "Financial Services"},
    {"symbol": "INFY", "name": "Infosys Ltd", "industry": "Information Technology"},
    {"symbol": "SBIN", "name": "State Bank of India", "industry": "Financial Services"},
    {"symbol": "ITC", "name": "ITC Ltd", "industry": "Consumer Goods"},
    {"symbol": "HINDUNILVR", "name": "Hindustan Unilever Ltd", "industry": "Consumer Goods"},
    {"symbol": "LT", "name": "Larsen & Toubro Ltd", "industry": "Capital Goods"},
    {"symbol": "BAJFINANCE", "name": "Bajaj Finance Ltd", "industry": "Financial Services"},
    {"symbol": "HCLTECH", "name": "HCL Technologies Ltd", "industry": "Information Technology"},
    {"symbol": "MARUTI", "name": "Maruti Suzuki India Ltd", "industry": "Automobile"},
    {"symbol": "SUNPHARMA", "name": "Sun Pharmaceutical Industries Ltd", "industry": "Healthcare"},
    {"symbol": "TITAN", "name": "Titan Company Ltd", "industry": "Consumer Goods"},
    {"symbol": "TATAMOTORS", "name": "Tata Motors Ltd", "industry": "Automobile"},
    {"symbol": "KOTAKBANK", "name": "Kotak Mahindra Bank Ltd", "industry": "Financial Services"},
    {"symbol": "ONGC", "name": "Oil & Natural Gas Corporation Ltd", "industry": "Energy"},
    {"symbol": "NTPC", "name": "NTPC Ltd", "industry": "Utilities"},
    {"symbol": "AXISBANK", "name": "Axis Bank Ltd", "industry": "Financial Services"},
    {"symbol": "ADANIENT", "name": "Adani Enterprises Ltd", "industry": "Metals & Mining"},
    {"symbol": "COALINDIA", "name": "Coal India Ltd", "industry": "Metals & Mining"},
    {"symbol": "POWERGRID", "name": "Power Grid Corporation of India Ltd", "industry": "Utilities"},
    {"symbol": "TRENT", "name": "Trent Ltd", "industry": "Retail"},
    {"symbol": "BAJAJFINSV", "name": "Bajaj Finserv Ltd", "industry": "Financial Services"},
    {"symbol": "ASIANPAINT", "name": "Asian Paints Ltd", "industry": "Consumer Goods"},
    {"symbol": "DIVISLAB", "name": "Divi's Laboratories Ltd", "industry": "Healthcare"},
    {"symbol": "BAJAJ-AUTO", "name": "Bajaj Auto Ltd", "industry": "Automobile"},
    {"symbol": "POLYCAB", "name": "Polycab India Ltd", "industry": "Capital Goods"},
    {"symbol": "PERSISTENT", "name": "Persistent Systems Ltd", "industry": "Information Technology"},
    {"symbol": "VBL", "name": "Varun Beverages Ltd", "industry": "Consumer Goods"},
    {"symbol": "SOLARINDS", "name": "Solar Industries India Ltd", "industry": "Chemicals"},
    {"symbol": "ABBOTTINDIA", "name": "Abbott India Ltd", "industry": "Healthcare"},
    {"symbol": "LTTS", "name": "L&T Technology Services Ltd", "industry": "Information Technology"},
    {"symbol": "TATAELXSI", "name": "Tata Elxsi Ltd", "industry": "Information Technology"}
]


def fetch_nifty500_universe():
    """Fetch the full list of Nifty 500 constituents from NSE, falling back gracefully."""
    print("[MITS EOD] Fetching Nifty 500 constituent universe from NSE...")
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    try:
        response = requests.get(NSE_NIFTY500_URL, headers=headers, timeout=12)
        if response.status_code == 200 and "Symbol" in response.text:
            reader = csv.DictReader(io.StringIO(response.text))
            symbols_list = []
            for row in reader:
                sym = row.get("Symbol", "").strip()
                name = row.get("Company Name", "").strip()
                ind = row.get("Industry", "").strip()
                if sym:
                    symbols_list.append({"symbol": sym, "name": name, "industry": ind})
            if symbols_list:
                print(f"[MITS EOD] Successfully loaded {len(symbols_list)} constituents from NSE.")
                return symbols_list
    except Exception as e:
        print(f"[WARN] Failed to fetch live Nifty 500 CSV from NSE: {e}")

    print(f"[MITS EOD] Falling back to embedded core universe ({len(FALLBACK_SYMBOLS)} symbols).")
    return FALLBACK_SYMBOLS


def fetch_nifty50_benchmark_performance():
    """
    Download 6-month (123 trading days) history for Nifty 50 (^NSEI) and
    compute benchmark compounding ratio: (Latest Close / 123D Ago Close).
    """
    print("[MITS RS] Downloading benchmark Nifty 50 (^NSEI) 6-month history...")
    try:
        nifty = yf.Ticker(BENCHMARK_INDEX_SYMBOL)
        hist = nifty.history(period="6mo")
        if hist is not None and not hist.empty and len(hist) >= 2:
            offset = min(123, len(hist))
            latest_close = float(hist["Close"].iloc[-1])
            ago_close = float(hist["Close"].iloc[-offset])
            perf_ratio = latest_close / ago_close if ago_close > 0 else 1.0
            print(f"[MITS RS] Nifty 50 Benchmark: Latest=₹{latest_close:.2f} | 123D Ago=₹{ago_close:.2f} | Ratio={perf_ratio:.4f} ({((perf_ratio - 1) * 100):+.2f}%)")
            return perf_ratio
    except Exception as e:
        print(f"[WARN] Failed to fetch Nifty 50 history: {e}")
    return 1.0


def compute_mits_score(metrics):
    """
    Compute 0-100 institutional composite ranking score based on 4 pillars:
    1. Capital Efficiency & Profitability (max 35 pts)
    2. Solvency & Balance Sheet (max 25 pts)
    3. Growth & Cash Generation (max 25 pts)
    4. Valuation & Safety (max 15 pts)
    """
    score = 0

    roce = metrics.get("roce", 0.0)
    roe = metrics.get("roe", 0.0)
    ebitda_margin = metrics.get("ebitda_margin", 0.0)
    roa = metrics.get("roa", 0.0)
    de = metrics.get("debt_to_equity", 0.0)
    cr = metrics.get("current_ratio", 0.0)
    sales_growth = metrics.get("sales_3yr_cagr", 0.0)
    pat_growth = metrics.get("pat_3yr_cagr", 0.0)
    fcf_yield = metrics.get("fcf_yield", 0.0)
    peg = metrics.get("peg", 0.0)

    # 1. Capital Efficiency (max 35)
    if roce >= 30.0:
        score += 15
    elif roce >= 20.0:
        score += 11
    elif roce >= 14.0:
        score += 7
    elif roce >= 8.0:
        score += 3

    if roe >= 25.0:
        score += 10
    elif roe >= 18.0:
        score += 7
    elif roe >= 12.0:
        score += 4
    elif roe >= 6.0:
        score += 2

    if ebitda_margin >= 25.0:
        score += 10
    elif ebitda_margin >= 18.0:
        score += 7
    elif ebitda_margin >= 12.0:
        score += 4
    elif ebitda_margin >= 6.0:
        score += 2

    # 2. Solvency & Balance Sheet (max 25)
    if de <= 0.05:
        score += 15
    elif de <= 0.25:
        score += 12
    elif de <= 0.50:
        score += 8
    elif de <= 1.0:
        score += 4

    if cr >= 1.8:
        score += 10
    elif cr >= 1.3:
        score += 7
    elif cr >= 1.0:
        score += 4
    elif cr > 0.0:
        score += 2

    # 3. Growth & Cash Flow (max 25)
    if sales_growth >= 20.0:
        score += 9
    elif sales_growth >= 12.0:
        score += 6
    elif sales_growth >= 6.0:
        score += 3

    if pat_growth >= 20.0:
        score += 8
    elif pat_growth >= 12.0:
        score += 5
    elif pat_growth >= 5.0:
        score += 2

    if fcf_yield >= 3.0:
        score += 8
    elif fcf_yield >= 1.5:
        score += 5
    elif fcf_yield > 0.0:
        score += 2

    # 4. Valuation / Multiples (max 15)
    if 0.1 <= peg <= 1.5:
        score += 8
    elif 1.5 < peg <= 2.2:
        score += 5
    elif peg > 2.2:
        score += 2

    if roa >= 15.0:
        score += 7
    elif roa >= 8.0:
        score += 4
    elif roa >= 4.0:
        score += 2

    # Normalize between 25 and 99
    return max(25, min(99, score))


def assign_tier(mits_score):
    if mits_score >= 85:
        return "Tier-1 Alpha"
    elif mits_score >= 70:
        return "Prime Quality"
    else:
        return "Stable Core"


def generate_thesis(metrics):
    roce = metrics.get("roce", 0.0)
    de = metrics.get("debt_to_equity", 0.0)
    sales = metrics.get("sales_3yr_cagr", 0.0)
    pat = metrics.get("pat_3yr_cagr", 0.0)
    fcf_yld = metrics.get("fcf_yield", 0.0)
    div_yld = metrics.get("dividend_yield", 0.0)
    ebitda = metrics.get("ebitda_margin", 0.0)
    peg = metrics.get("peg", 0.0)

    if de <= 0.05 and roce >= 30.0:
        return f"Elite capital efficiency ({roce}% ROCE) combined with a fortress zero-debt balance sheet and robust operating cash flow."
    elif sales >= 20.0 and pat >= 20.0:
        return f"High-velocity compounding engine with {sales}% top-line revenue growth and operating leverage."
    elif fcf_yld >= 3.5 and div_yld >= 2.0:
        return f"Cash flow fortress generating {fcf_yld}% FCF yield with disciplined shareholder dividend payout."
    elif de <= 0.1 and roce >= 22.0:
        return f"Institutional-grade balance sheet with low leverage and sustained return on capital ({roce}% ROCE)."
    elif 0.1 <= peg <= 1.5 and roce >= 18.0:
        return f"Attractive Growth-At-Reasonable-Price (GARP) valuation (PEG {peg}) supported by disciplined compounding."
    elif div_yld >= 3.0:
        return f"Defensive yield stalwart with strong cash surplus and recession-resilient distribution power."
    elif ebitda >= 25.0:
        return f"High pricing power franchise delivering industry-leading EBITDA margins ({ebitda}%)."
    else:
        return "Core institutional holding with stable operating metrics, disciplined solvency, and resilient market position."


def process_ticker(item, nifty_perf):
    """
    Fetch fundamental data and 6-month historical prices from yfinance for a single stock.
    Calculates standardized Indian consolidated TTM P/E, P/B, ROCE, ROE and raw RS ratio against Nifty 50.
    """
    symbol = item["symbol"]
    name_hint = item.get("name", "")
    industry_hint = item.get("industry", "")
    yf_symbol = f"{symbol}.NS"

    try:
        t = yf.Ticker(yf_symbol)
        inf = t.info
        if not inf or not isinstance(inf, dict):
            return None

        # Price and market cap
        price = inf.get("currentPrice") or inf.get("regularMarketPrice") or 0.0
        if not price or price <= 0:
            return None

        prev_close = inf.get("previousClose") or inf.get("regularMarketPreviousClose") or price
        change_pct = inf.get("regularMarketChangePercent")
        if change_pct is None:
            change_pct = ((price - prev_close) / prev_close) * 100.0 if prev_close > 0 else 0.0

        market_cap = inf.get("marketCap") or 0.0
        mcap_cr = market_cap / 1e7

        if mcap_cr >= 100000:
            mcap_cat = "Mega Cap"
        elif mcap_cr >= 25000:
            mcap_cat = "Large Cap"
        elif mcap_cr >= 7000:
            mcap_cat = "Mid Cap"
        else:
            mcap_cat = "Small Cap"

        company_name = inf.get("longName") or inf.get("shortName") or name_hint or symbol
        sector = industry_hint or inf.get("sector") or inf.get("industry") or "Diversified"

        # Check financial currencies (e.g. ADR dual-listed Indian firms report statements in USD)
        fin_curr = inf.get("financialCurrency", "INR")
        curr = inf.get("currency", "INR")

        # 1. Statements extraction
        try:
            qi = t.quarterly_income_stmt
            if qi is None or qi.empty:
                qi = t.income_stmt
        except Exception:
            qi = None

        try:
            qb = t.quarterly_balance_sheet
            if qb is None or qb.empty:
                qb = t.balance_sheet
        except Exception:
            qb = None

        # 2. Shares Outstanding
        shares = float(inf.get("sharesOutstanding") or 0.0)
        if shares <= 0 and qb is not None and not qb.empty:
            for r in ["Ordinary Shares Number", "Share Issued"]:
                if r in qb.index:
                    s_val = qb.loc[r].dropna()
                    if not s_val.empty and float(s_val.iloc[0]) > 0:
                        shares = float(s_val.iloc[0])
                        break
        if shares <= 0 and qi is not None and not qi.empty:
            for r in ["Basic Average Shares", "Diluted Average Shares"]:
                if r in qi.index:
                    s_val = qi.loc[r].dropna()
                    if not s_val.empty and float(s_val.iloc[0]) > 0:
                        shares = float(s_val.iloc[0])
                        break

        # 3. TTM Consolidated Net Profit, Revenue, EBIT & Quarterly Growth
        ttm_net_profit = 0.0
        ttm_revenue = 0.0
        ttm_ebit = 0.0
        qtr_pat_growth = 0.0

        cols_inc = list(qi.columns)[:4] if qi is not None and not qi.empty else []
        if qi is not None and not qi.empty:
            # Net profit (Consolidated TTM)
            for r in [
                "Net Income Common Stockholders",
                "Net Income Continuous Operations",
                "Net Income From Continuing Operation Net Minority Interest",
                "Net Income",
                "Diluted NI Availto Com Stockholders"
            ]:
                if r in qi.index:
                    val = qi.loc[r].dropna()
                    avail_cols = [c for c in cols_inc if c in val.index]
                    if avail_cols:
                        s = float(val[avail_cols].sum())
                        if len(avail_cols) < 4 and len(avail_cols) > 0:
                            s = s * (4.0 / len(avail_cols))
                        ttm_net_profit = s
                        break

            # Revenue (TTM)
            for r in ["Total Revenue", "Operating Revenue"]:
                if r in qi.index:
                    val = qi.loc[r].dropna()
                    avail_cols = [c for c in cols_inc if c in val.index]
                    if avail_cols:
                        s = float(val[avail_cols].sum())
                        if len(avail_cols) < 4 and len(avail_cols) > 0:
                            s = s * (4.0 / len(avail_cols))
                        ttm_revenue = s
                        break

            # EBIT / Operating Income (TTM)
            for r in ["EBIT", "Operating Income", "Pretax Income"]:
                if r in qi.index:
                    val = qi.loc[r].dropna()
                    avail_cols = [c for c in cols_inc if c in val.index]
                    if avail_cols:
                        s = float(val[avail_cols].sum())
                        if len(avail_cols) < 4 and len(avail_cols) > 0:
                            s = s * (4.0 / len(avail_cols))
                        if s > 0:
                            ttm_ebit = s
                            break

            # Quarterly PAT Growth YoY
            for r in ["Net Income Common Stockholders", "Net Income Continuous Operations", "Net Income"]:
                if r in qi.index:
                    val = qi.loc[r].dropna()
                    if len(val) >= 5:
                        q0 = float(val.iloc[0])
                        q4 = float(val.iloc[4])
                        if abs(q4) > 0:
                            qtr_pat_growth = round(((q0 - q4) / abs(q4)) * 100.0, 2)
                            break
                    elif len(val) >= 2:
                        q0 = float(val.iloc[0])
                        q1 = float(val.iloc[1])
                        if abs(q1) > 0:
                            qtr_pat_growth = round(((q0 - q1) / abs(q1)) * 100.0, 2)
                            break

        if qtr_pat_growth == 0.0:
            qg_raw = inf.get("earningsQuarterlyGrowth") or inf.get("earningsGrowth")
            qtr_pat_growth = round(float(qg_raw) * 100.0, 2) if qg_raw is not None else 0.0

        if ttm_revenue <= 0:
            ttm_revenue = float(inf.get("totalRevenue") or 0.0)

        if ttm_ebit <= 0:
            op_m = inf.get("operatingMargins") or 0.0
            ttm_ebit = float(op_m * ttm_revenue)

        # 4. Balance Sheet: Equity, Total Assets, Current Liabilities, Total Debt
        latest_equity = 0.0
        tot_assets = 0.0
        curr_liab = 0.0
        tot_debt = 0.0

        if qb is not None and not qb.empty:
            for r in ["Stockholders Equity", "Common Stock Equity", "Total Equity Gross Minority Interest", "Invested Capital"]:
                if r in qb.index:
                    val = qb.loc[r].dropna()
                    if not val.empty and float(val.iloc[0]) > 0:
                        latest_equity = float(val.iloc[0])
                        break
            if "Total Assets" in qb.index:
                val = qb.loc["Total Assets"].dropna()
                if not val.empty:
                    tot_assets = float(val.iloc[0])
            if "Current Liabilities" in qb.index:
                val = qb.loc["Current Liabilities"].dropna()
                if not val.empty:
                    curr_liab = float(val.iloc[0])
            if "Total Debt" in qb.index:
                val = qb.loc["Total Debt"].dropna()
                if not val.empty:
                    tot_debt = float(val.iloc[0])

        if tot_debt <= 0:
            tot_debt = float(inf.get("totalDebt") or 0.0)

        if latest_equity <= 0:
            bv = inf.get("bookValue") or 0.0
            if bv > 0 and shares > 0:
                latest_equity = bv * shares

        # 5. Core Valuation Ratios
        # P/E & P/B
        if fin_curr != curr and inf.get("trailingEps"):
            ttm_eps = float(inf.get("trailingEps"))
            bvps = float(inf.get("bookValue") or 0.0)
        else:
            ttm_eps = (ttm_net_profit / shares) if (shares > 0 and ttm_net_profit > 0) else float(inf.get("trailingEps") or 0.0)
            bvps = (latest_equity / shares) if (shares > 0 and latest_equity > 0) else float(inf.get("bookValue") or 0.0)

        pe = round(price / ttm_eps, 2) if ttm_eps > 0 else round(float(inf.get("trailingPE") or 0.0), 2)
        pb = round(price / bvps, 2) if bvps > 0 else round(float(inf.get("priceToBook") or 0.0), 2)

        # ROCE: EBIT / Capital Employed
        ce_assets = (tot_assets - curr_liab) if (tot_assets > 0 and curr_liab > 0) else 0.0
        ce_eq_debt = latest_equity + tot_debt

        # If provisions/clearing non-debt items inflate assets-liabilities, use equity + debt
        if ce_assets > 0 and ce_eq_debt > 0 and (ce_assets / ce_eq_debt) > 1.35:
            cap_employed = ce_eq_debt
        elif ce_assets > 0:
            cap_employed = ce_assets
        else:
            cap_employed = ce_eq_debt

        if cap_employed > 0 and ttm_ebit > 0:
            roce = round((ttm_ebit / cap_employed) * 100.0, 2)
        else:
            roce = round((inf.get("returnOnEquity") or 0.0) * 115.0, 2)

        # ROE: TTM Consolidated Net Profit / Latest Shareholders' Equity
        if latest_equity > 0 and ttm_net_profit > 0:
            roe = round((ttm_net_profit / latest_equity) * 100.0, 2)
        else:
            roe = round((inf.get("returnOnEquity") or 0.0) * 100.0, 2)

        # ROA: TTM Consolidated Net Profit / Total Assets
        if tot_assets > 0 and ttm_net_profit > 0:
            roa = round((ttm_net_profit / tot_assets) * 100.0, 2)
        else:
            roa = round((inf.get("returnOnAssets") or 0.0) * 100.0, 2)

        # Net Margin: TTM Net Profit / TTM Revenue
        if ttm_revenue > 0 and ttm_net_profit > 0:
            net_margin = round((ttm_net_profit / ttm_revenue) * 100.0, 2)
        else:
            npm_raw = inf.get("profitMargins")
            net_margin = round(float(npm_raw) * 100.0, 2) if npm_raw is not None else 0.0

        # P/S Ratio
        if ttm_revenue > 0 and market_cap > 0:
            ps_ratio = round(market_cap / ttm_revenue, 2)
        else:
            ps_raw = inf.get("priceToSalesTrailing12Months")
            if ps_raw and float(ps_raw) > 0:
                ps_ratio = round(float(ps_raw), 2)
            elif pe > 0 and net_margin > 0:
                ps_ratio = round(pe * (net_margin / 100.0), 2)
            else:
                ps_ratio = 0.0

        # Asset Turnover
        if tot_assets > 0 and ttm_revenue > 0:
            asset_turnover = round(ttm_revenue / tot_assets, 2)
        elif net_margin > 0 and roa > 0:
            asset_turnover = round(roa / net_margin, 2)
        else:
            asset_turnover = 0.0

        # Debt to Equity
        if latest_equity > 0 and tot_debt >= 0:
            debt_to_equity = round(tot_debt / latest_equity, 2)
        else:
            de_raw = inf.get("debtToEquity")
            debt_to_equity = round(de_raw / 100.0, 2) if de_raw is not None else 0.0

        # Cash Flow & Additional Multiples
        fcf = inf.get("freeCashflow") or 0.0
        fcf_cr = round(fcf / 1e7, 1)
        fcf_yield = round(((fcf / market_cap) * 100.0), 2) if (market_cap > 0 and fcf > 0) else 0.0

        current_ratio = round(float(inf.get("currentRatio") or 0.0), 2)
        peg = round(float(inf.get("pegRatio") or 0.0), 2)

        sales_growth = round(float(inf.get("revenueGrowth") or 0.0) * 100.0, 2)
        pat_growth = round(float(inf.get("earningsGrowth") or 0.0) * 100.0, 2)

        div_raw = inf.get("dividendYield") or 0.0
        dividend_yield = round(float(div_raw), 2)
        ebitda_margin = round(float(inf.get("ebitdaMargins") or 0.0) * 100.0, 2)

        net_worth_cr = round(latest_equity / 1e7, 2) if latest_equity > 0 else 1.0

        # Relative Strength (RS vs Nifty 50) calculation (123 trading days)
        try:
            hist = t.history(period="6mo")
            if hist is not None and not hist.empty and len(hist) >= 2:
                offset = min(123, len(hist))
                st_close = float(hist["Close"].iloc[-1])
                st_123_ago = float(hist["Close"].iloc[-offset])
                stock_perf = st_close / st_123_ago if st_123_ago > 0 else 1.0
                rs_ratio = stock_perf / nifty_perf if nifty_perf > 0 else 1.0
            else:
                rs_ratio = 1.0
        except Exception:
            rs_ratio = 1.0

        metrics_dict = {
            "roce": round(roce, 2),
            "roe": round(roe, 2),
            "ebitda_margin": round(ebitda_margin, 2),
            "roa": round(roa, 2),
            "debt_to_equity": round(debt_to_equity, 2),
            "current_ratio": round(current_ratio, 2),
            "sales_3yr_cagr": round(sales_growth, 2),
            "pat_3yr_cagr": round(pat_growth, 2),
            "fcf_yield": round(fcf_yield, 2),
            "dividend_yield": round(dividend_yield, 2),
            "peg": round(peg, 2),
        }

        score = compute_mits_score(metrics_dict)
        tier = assign_tier(score)
        thesis = generate_thesis(metrics_dict)

        record = {
            "ticker": symbol,
            "name": company_name,
            "sector": sector,
            "market_cap_category": mcap_cat,
            "price": round(float(price), 2),
            "change_pct": round(float(change_pct), 2),
            "pe": round(float(pe), 2),
            "pb": round(float(pb), 2),
            "roa": round(float(roa), 2),
            "roce": round(float(roce), 2),
            "roe": round(float(roe), 2),
            "ebitda_margin": round(float(ebitda_margin), 2),
            "debt_to_equity": round(float(debt_to_equity), 2),
            "fcf_cr": round(float(fcf_cr), 1),
            "current_ratio": round(float(current_ratio), 2),
            "peg": round(float(peg), 2),
            "sales_3yr_cagr": round(float(sales_growth), 2),
            "pat_3yr_cagr": round(float(pat_growth), 2),
            "dividend_yield": round(float(dividend_yield), 2),
            "fcf_yield": round(float(fcf_yield), 2),
            "net_margin": round(float(net_margin), 2),
            "ps_ratio": round(float(ps_ratio), 2),
            "asset_turnover": round(float(asset_turnover), 2),
            "qtr_pat_growth": round(float(qtr_pat_growth), 2),
            "mits_score": int(score),
            "tier": tier,
            "rs_ratio": float(rs_ratio),
            "thesis": thesis,
            "pat_growth": round(float(pat_growth), 2),
            "pe_ratio": round(float(pe), 2),
            "pb_ratio": round(float(pb), 2),
            "div_yield": round(float(dividend_yield), 2),
            "roic": round(float(roce), 2),
            "net_worth": round(float(net_worth_cr), 2)
        }
        return record

    except Exception:
        return None


def main():
    parser = argparse.ArgumentParser(description="MITS Free Fundamental Scanner - EOD & RS Ingestion Pipeline")
    parser.add_argument("--workers", type=int, default=25, help="Parallel worker threads (default: 25)")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of stocks (for rapid debugging, 0 = all)")
    parser.add_argument("--output", type=str, default=OUTPUT_DATA_PATH, help="Output JSON path")
    args = parser.parse_args()

    print("=" * 70)
    print("⚡ MITS 360 QUANT INTELLIGENCE ENGINE - EOD & RELATIVE STRENGTH PIPELINE")
    print(f"Target Output: {args.output}")
    print(f"Parallel Workers: {args.workers}")
    print("=" * 70)

    # 1. Fetch benchmark Nifty 50 performance (123 trading days)
    nifty_perf = fetch_nifty50_benchmark_performance()

    # 2. Fetch constituent universe
    universe = fetch_nifty500_universe()
    if args.limit > 0:
        universe = universe[:args.limit]
        print(f"[MITS EOD] Limited run: processing first {args.limit} stocks.")

    start_time = time.time()
    successful_records = []

    print(f"[MITS EOD] Starting parallel extraction for {len(universe)} symbols...")
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        future_to_sym = {executor.submit(process_ticker, item, nifty_perf): item["symbol"] for item in universe}
        completed = 0
        total = len(future_to_sym)

        for future in as_completed(future_to_sym):
            completed += 1
            record = future.result()
            if record is not None:
                successful_records.append(record)
            if completed % 50 == 0 or completed == total:
                pct = (completed / total) * 100
                print(f"  [Progress] {completed}/{total} ({pct:.1f}%) | Valid Stocks Ingested: {len(successful_records)}")

    # 3. Calculate Percentile RS Rating (1 to 99) and categorize RS Status
    n = len(successful_records)
    print(f"[MITS RS] Normalizing Relative Strength across {n} constituents...")
    if n > 0:
        # Sort indices by rs_ratio ascending
        sorted_indices = sorted(range(n), key=lambda i: successful_records[i]["rs_ratio"])
        for rank_pos, idx in enumerate(sorted_indices):
            # Scale rank to 1 - 99 percentile
            percentile = int(round(1 + (rank_pos / (n - 1 if n > 1 else 1)) * 98))
            percentile = max(1, min(99, percentile))

            # Categorize RS Status
            if percentile >= 80:
                status = "Super Outperformer"
            elif percentile >= 55:
                status = "Outperforming"
            elif percentile >= 45:
                status = "Neutral"
            else:
                status = "Underperforming"

            successful_records[idx]["rs_rating"] = percentile
            successful_records[idx]["rs_score"] = percentile
            successful_records[idx]["rs_status"] = status
            successful_records[idx]["rs_ratio"] = round(float(successful_records[idx]["rs_ratio"]), 4)

    # 4. Sort descending by MITS Score, then RS Rating
    successful_records.sort(key=lambda s: (s["mits_score"], s["rs_rating"]), reverse=True)

    # Current IST Timestamp
    now_ist = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=5, minutes=30))).isoformat()

    payload = {
        "metadata": {
            "engine": "MITS 360 Quant Intelligence Engine",
            "version": "3.2.0",
            "last_updated": now_ist,
            "feed_status": "EOD Automated Sync Completed",
            "universe_size": len(successful_records),
            "currency": "INR (₹)",
            "benchmark_index": "Nifty 50 (^NSEI)",
            "benchmark_perf_123d": round(nifty_perf, 4)
        },
        "stocks": successful_records
    }

    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    duration = round(time.time() - start_time, 2)
    print("=" * 70)
    print(f"✅ Ingestion Complete in {duration}s!")
    print(f"📊 Valid Screened Stocks Ingested: {len(successful_records)}")
    print(f"⚡ Relative Strength Metric (`rs_rating` & `rs_status`) Injected Successfully.")
    print(f"💾 File Written: {args.output}")
    print("=" * 70)


if __name__ == "__main__":
    main()
