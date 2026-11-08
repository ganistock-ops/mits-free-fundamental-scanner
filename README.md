# MITS Free Fundamental Scanner

A self-contained, single-file institutional stock screening engine component for Indian equities (NSE/BSE) featuring a **2-Tier Strategy Navigation hierarchy**, engineered for:
- Direct embedding inside **WordPress** (via Custom HTML block or widget) with zero style bleed.
- **GitHub Actions** daily automated EOD data ingestion (`data.json`).
- **100% Mobile & Touch Responsiveness** with a sticky first column (Ticker/Company).
- **Ultra-premium Institutional Dark Mode** with glassmorphism and electric accents.

---

## ⚡ 2-Tier Strategy Navigation Hierarchy

### Tier-1: 3 Core Strategy Macro Tabs
1. ⚡ **Profit Powerhouse:** High ROCE (>25%), Super Operating Margins (>20%) & Strong Positive Free Cash Flow.
2. 🚀 **Growth Engines:** Compounding Sales (>15% CAGR), High EPS Growth & Balanced PEG (<1.8).
3. 🛡️ **Defensive Shields:** Zero/Low Debt (<0.10 D/E), Fortress Solvency & Deep Economic Moats.

### Tier-2: 18 Dynamic Sub-Strategy Filters
Selecting any Tier-1 tab dynamically populates the **"Select Institutional Filter"** dropdown with 6 specialized sub-strategies:

| Main Strategy (Tier-1) | 6 Dynamic Sub-Strategies (Tier-2) |
|---|---|
| **Profit Powerhouse** (⚡) | 1. Alpha Asset Compounders<br>2. Cash Flow Fortress<br>3. Deep Value Moat<br>4. Margin Expansion Leaders<br>5. Capital Efficiency Titans<br>6. Solvency Shield Value |
| **Growth Engines** (🚀) | 1. Alpha Return Compounders<br>2. EBITDA Surge Leaders<br>3. Twin-Engine Growth Titans<br>4. Midcap Velocity Stars<br>5. GARP Champions<br>6. Compounding Revenue Moat |
| **Defensive Shields** (🛡️) | 1. Dividend Fortress Elite<br>2. Deep Value Solvency Shield<br>3. Resilient Long-Term Stalwarts<br>4. Zero-Debt Profit Shields<br>5. Liquid Cash Vaults<br>6. Undervalued EPS Compounding |

---

## 📊 Metrics Table Columns

The institutional data table features 10 quantitative financial columns:
1. **Ticker / Company:** Sticky on mobile horizontal scroll with market-cap pill.
2. **Sector:** Industry categorization.
3. **Price (₹):** Real-time EOD price with color-coded day change %.
4. **P/E:** Trailing Price to Earnings ratio.
5. **P/B:** Price to Book ratio.
6. **ROA (%):** Return on Assets.
7. **ROCE (%):** Return on Capital Employed (highlighted cyan pill).
8. **EBITDA Margin (%):** Operating cash profitability margin.
9. **D/E:** Debt to Equity ratio (highlighted in emerald when debt-free $\le 0.05$).
10. **MITS Quality Score:** Composite quantitative ranking (0–100) with progress meter and Tier rating (Tier-1 Alpha, Prime Quality, Stable Core).

---

## 🚀 WordPress Embedding Instructions

### Option A: Gutenberg Block Editor (Recommended)
1. In your WordPress Admin, edit any Page or Post.
2. Add a **Custom HTML** block.
3. Copy the entire contents of [`index.html`](./index.html) (or just the `<div class="mits-scanner-root">...</div>` wrapper).
4. Paste it into the block and click **Preview** or **Update**.

### Option B: Elementor Page Builder
1. Drag an **HTML** widget onto your section.
2. Paste the contents of [`index.html`](./index.html) into the **HTML Code** field and click **Update**.

---

## 💻 Local Testing & Preview

Open `index.html` directly in your browser or run:
```bash
python -m http.server 8085
```
Then visit: `http://localhost:8085/index.html`
