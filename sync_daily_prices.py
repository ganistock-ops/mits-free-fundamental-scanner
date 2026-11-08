#!/usr/bin/env python3
"""
MITS Free Fundamental Scanner - Fast Daily EOD Price & Dynamic Multiples Sync
Strictly Self-Contained within mits-free-fundamental-scanner

Objective:
Reads existing data.json, performs a fast single batch download for all 500 Nifty
constituents to get today's Close, Change %, and recomputes dynamic valuation
multiples (P/E, P/B, P/S) while retaining all deep fundamental metrics (ROCE, ROA,
Margins, Cash Flow, RS Rating) intact.
"""

import os
import sys
import json
import time
import datetime
import argparse
import numpy as np

try:
    import yfinance as yf
except ImportError:
    print("[ERROR] yfinance is required. Install via: pip install yfinance")
    sys.exit(1)

WORKSPACE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_JSON_PATH = os.path.join(WORKSPACE_DIR, "data.json")


def sync_daily_prices(input_path=DATA_JSON_PATH, output_path=DATA_JSON_PATH, limit=0):
    start_time = time.time()
    print("=" * 70)
    print("⚡ MITS 360 QUANT ENGINE - DAILY EOD PRICE & VALUATION SYNC")
    print(f"Target Source & Output: {output_path}")
    print("=" * 70)

    if not os.path.exists(input_path):
        print(f"[ERROR] Input dataset not found: {input_path}")
        sys.exit(1)

    with open(input_path, "r", encoding="utf-8") as f:
        payload = json.load(f)

    stocks = payload.get("stocks", [])
    total_stocks = len(stocks)
    print(f"[DATA] Loaded {total_stocks} constituents from baseline dataset.")

    if total_stocks == 0:
        print("[ERROR] No stocks found in dataset.")
        sys.exit(1)

    target_stocks = stocks[:limit] if limit > 0 else stocks

    # Prepare ticker symbol mapping (TICKER -> TICKER.NS)
    symbol_map = {f"{s['ticker']}.NS": s for s in target_stocks}
    download_symbols = list(symbol_map.keys())

    print(f"[FETCH] Initiating parallel batch download for {len(download_symbols)} tickers...")
    try:
        df = yf.download(
            tickers=download_symbols,
            period="5d",
            interval="1d",
            progress=False,
            threads=True
        )
    except Exception as e:
        print(f"[WARN] Batch download encountered error: {e}. Falling back to individual retention.")
        df = None

    updated_count = 0
    retained_count = 0

    if df is not None and "Close" in df:
        close_df = df["Close"]

        for sym, stock in symbol_map.items():
            try:
                # Check if symbol exists in columns
                if sym in close_df.columns:
                    series = close_df[sym].dropna()
                elif (sym,) in close_df.columns:
                    series = close_df[(sym,)].dropna()
                else:
                    series = None

                if series is not None and len(series) >= 1:
                    latest_close = float(series.iloc[-1])
                    if np.isnan(latest_close) or latest_close <= 0:
                        retained_count += 1
                        continue

                    prev_close = float(series.iloc[-2]) if len(series) >= 2 else latest_close
                    if np.isnan(prev_close) or prev_close <= 0:
                        prev_close = latest_close

                    change_pct = round(((latest_close - prev_close) / prev_close) * 100.0, 2)

                    old_price = float(stock.get("price") or 0.0)
                    old_pe = float(stock.get("pe") or 0.0)
                    old_pb = float(stock.get("pb") or 0.0)
                    old_ps = float(stock.get("ps_ratio") or 0.0)

                    # Dynamic P/E recomputation: new_pe = latest_price / (old_price / old_pe)
                    if old_price > 0 and old_pe > 0:
                        eps = old_price / old_pe
                        new_pe = round(latest_close / eps, 2) if eps > 0 else old_pe
                    else:
                        new_pe = old_pe

                    # Dynamic P/B recomputation
                    if old_price > 0 and old_pb > 0:
                        bvps = old_price / old_pb
                        new_pb = round(latest_close / bvps, 2) if bvps > 0 else old_pb
                    else:
                        new_pb = old_pb

                    # Dynamic P/S recomputation
                    if old_price > 0 and old_ps > 0:
                        sps = old_price / old_ps
                        new_ps = round(latest_close / sps, 2) if sps > 0 else old_ps
                    else:
                        new_ps = old_ps

                    # Update price and valuation multiples
                    stock["price"] = round(latest_close, 2)
                    stock["change_pct"] = change_pct
                    stock["pe"] = new_pe
                    stock["pe_ratio"] = new_pe
                    stock["pb"] = new_pb
                    stock["pb_ratio"] = new_pb
                    stock["ps_ratio"] = new_ps

                    updated_count += 1
                else:
                    retained_count += 1
            except Exception:
                retained_count += 1
    else:
        print("[WARN] No Close data retrieved. Retaining baseline prices.")
        retained_count = len(target_stocks)

    # Update metadata
    now_ist = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=5, minutes=30))).isoformat()
    metadata = payload.get("metadata", {})
    metadata["last_updated"] = now_ist
    metadata["feed_status"] = "Daily EOD Price & P/E Sync Completed"
    metadata["universe_size"] = len(stocks)
    payload["metadata"] = metadata
    payload["stocks"] = stocks

    # Atomic write to data.json
    temp_output = output_path + ".tmp"
    with open(temp_output, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
    os.replace(temp_output, output_path)

    elapsed = round(time.time() - start_time, 2)
    print("=" * 70)
    print(f"✅ Daily Price Sync Completed in {elapsed}s!")
    print(f"📊 Updated Prices: {updated_count} stocks | Retained Baseline: {retained_count} stocks")
    print(f"🕒 Timestamp: {now_ist}")
    print(f"💾 File Written: {output_path}")
    print("=" * 70)


def main():
    parser = argparse.ArgumentParser(description="MITS Free Fundamental Scanner - Fast Daily Price Sync")
    parser.add_argument("--input", type=str, default=DATA_JSON_PATH, help="Path to input data.json")
    parser.add_argument("--output", type=str, default=DATA_JSON_PATH, help="Path to output data.json")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of stocks for testing (0 = all)")
    args = parser.parse_args()

    sync_daily_prices(input_path=args.input, output_path=args.output, limit=args.limit)


if __name__ == "__main__":
    main()
