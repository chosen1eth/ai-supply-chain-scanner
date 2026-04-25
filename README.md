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
- Public institutional insights pages in `research_sources.py`
- Local research files in `research_reports/`
- YouTube channel RSS feeds and public captions through `youtube_sources.py`

If RSS or market APIs fail, the app falls back to mock articles and neutral market data so the dashboard still runs locally. Market data uses the neutral fallback by default to keep local scans fast and reliable. In the app, check `Use live market data` to query Yahoo Finance.

## Setup

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
python -m streamlit run app.py
```

On Windows, prefer `python -m streamlit run app.py` because the `streamlit.exe` script directory may not be on PATH.

## Institutional Research

The app has a separate institutional research scanner. It can:

- Fetch configured public insights pages from firms such as Goldman Sachs, Morgan Stanley, and J.P. Morgan
- Read legally obtained local `.pdf`, `.txt`, `.md`, and `.html` reports from `research_reports/`
- Extract repeated themes, institutions, source documents, and recurring phrases

Full sell-side research from bank portals is usually access-controlled. Put files you are allowed to use into `research_reports/`; those files are ignored by Git.

## YouTube Research Digests

The app can monitor YouTube channels and automatically try to extract public captions without downloading video files.

1. Add channels to `youtube_channels.csv`:

```csv
channel_name,channel_id
Example Channel,UCxxxxxxxxxxxxxxxxxxxxxx
```

2. Click `Scan YouTube` in the app.
3. If a video has no accessible captions, put a NotebookLM or manual transcript into `youtube_transcripts/`. Include the 11-character video ID in the filename when possible.

YouTube's official caption download API generally requires edit permission on the video, so this app uses public channel RSS plus best-effort public captions. It does not bypass private, paid, or access-controlled content.

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
