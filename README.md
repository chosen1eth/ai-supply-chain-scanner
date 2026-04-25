# AI Supply Chain Alpha Scanner

Python MVP for detecting early AI infrastructure bottleneck signals before they become mainstream market narratives.

## What it tracks

- HBM
- Advanced packaging / CoWoS
- Optical modules / transceivers
- Data center power
- AI servers
- Cooling
- Networking chips

## Data sources

- RSS feeds
- Yahoo Finance through `yfinance`
- Manual company watchlist in `data_sources.py`

If RSS or market APIs fail, the app falls back to mock articles and neutral market data so the dashboard still runs locally. Market data uses the neutral fallback by default to keep local scans fast and reliable. In the app, check `Use live market data` to query Yahoo Finance.

## Setup

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
python -m streamlit run app.py
```

On Windows, prefer `python -m streamlit run app.py` because the `streamlit.exe` script directory may not be on PATH.

## Files

- `data_sources.py`: RSS, Yahoo Finance, watchlist, and mock fallback data
- `signal_extractor.py`: theme and bottleneck keyword extraction
- `scoring.py`: signal scoring and theme aggregation
- `database.py`: SQLite schema and persistence helpers
- `app.py`: Streamlit dashboard

## Scoring

The MVP creates:

- `demand_score`
- `supply_constraint_score`
- `price_power_score`
- `market_attention_score`
- `total_alpha_score`

Scores are intentionally simple and explainable. The goal is to surface candidate bottlenecks for review, not to produce a production-grade trading model.

## Output

The Streamlit dashboard shows:

- Top 10 emerging bottleneck themes
- Related companies
- Latest source articles
- Signal keywords
- A short explanation of why each theme is flagged

Results are stored in `alpha_scanner.sqlite`.
