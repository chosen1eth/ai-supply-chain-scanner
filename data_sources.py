from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Iterable

import feedparser
import pandas as pd
import requests
import yfinance as yf


RSS_FEEDS = [
    "https://www.semianalysis.com/feed",
    "https://www.datacenterdynamics.com/en/rss/",
    "https://www.tomshardware.com/feeds/all",
    "https://www.anandtech.com/rss/",
]

COMPANY_WATCHLIST = [
    {"ticker": "NVDA", "company": "NVIDIA", "themes": ["AI servers", "networking chips"]},
    {"ticker": "AMD", "company": "AMD", "themes": ["AI servers", "networking chips"]},
    {"ticker": "AVGO", "company": "Broadcom", "themes": ["networking chips", "optical modules / transceivers"]},
    {"ticker": "TSM", "company": "TSMC", "themes": ["advanced packaging / CoWoS"]},
    {"ticker": "MU", "company": "Micron", "themes": ["HBM"]},
    {"ticker": "SMCI", "company": "Super Micro Computer", "themes": ["AI servers", "cooling"]},
    {"ticker": "VRT", "company": "Vertiv", "themes": ["data center power", "cooling"]},
    {"ticker": "ETN", "company": "Eaton", "themes": ["data center power"]},
    {"ticker": "ANET", "company": "Arista Networks", "themes": ["networking chips"]},
    {"ticker": "COHR", "company": "Coherent", "themes": ["optical modules / transceivers"]},
]

MOCK_ARTICLES = [
    {
        "title": "HBM supply tightness persists as AI accelerator demand expands",
        "summary": "Memory vendors describe longer lead time, backlog growth, and price increase for HBM capacity.",
        "link": "mock://hbm-tightness",
        "source": "Mock Semiconductor News",
        "published": "2026-04-24",
    },
    {
        "title": "CoWoS capacity constraint drives new packaging capex increase",
        "summary": "Advanced packaging suppliers are expanding capacity, but customers still face allocation and shortage risk.",
        "link": "mock://cowos-capacity",
        "source": "Mock Foundry Brief",
        "published": "2026-04-23",
    },
    {
        "title": "Data center power equipment backlog rises on AI buildout",
        "summary": "Power and cooling vendors report margin expansion as large cloud buyers pull in orders.",
        "link": "mock://power-backlog",
        "source": "Mock Infrastructure Daily",
        "published": "2026-04-22",
    },
]


def load_watchlist() -> pd.DataFrame:
    return pd.DataFrame(COMPANY_WATCHLIST)


def fetch_rss_articles(feed_urls: Iterable[str] | None = None, timeout: int = 8) -> pd.DataFrame:
    rows = []
    for url in feed_urls or RSS_FEEDS:
        try:
            response = requests.get(url, timeout=timeout, headers={"User-Agent": "ai-alpha-scanner/0.1"})
            response.raise_for_status()
            parsed = feedparser.parse(response.content)
        except Exception:
            continue

        for entry in parsed.entries[:25]:
            rows.append(
                {
                    "title": entry.get("title", ""),
                    "summary": entry.get("summary", "") or entry.get("description", ""),
                    "link": entry.get("link", ""),
                    "source": parsed.feed.get("title", url),
                    "published": entry.get("published", "") or entry.get("updated", ""),
                }
            )

    if not rows:
        rows = MOCK_ARTICLES
    return pd.DataFrame(rows)


def _neutral_market_data(symbols: Iterable[str]) -> pd.DataFrame:
    fetched_at = datetime.now(timezone.utc).isoformat()
    return pd.DataFrame(
        [
            {
                "ticker": ticker,
                "last_price": 0.0,
                "daily_change_pct": 0.0,
                "market_cap": 0.0,
                "fetched_at": fetched_at,
            }
            for ticker in symbols
        ]
    )


def fetch_market_data(tickers: Iterable[str] | None = None, live: bool | None = None) -> pd.DataFrame:
    symbols = list(load_watchlist()["ticker"] if tickers is None else tickers)
    if live is None:
        live = os.getenv("AI_SCANNER_LIVE_MARKET", "").lower() in {"1", "true", "yes"}
    if not live:
        return _neutral_market_data(symbols)

    rows = []
    for ticker in symbols:
        try:
            stock = yf.Ticker(ticker)
            info = stock.fast_info
            history = stock.history(period="5d")
            last_close = float(history["Close"].iloc[-1]) if not history.empty else None
            previous_close = float(history["Close"].iloc[-2]) if len(history) > 1 else None
            daily_change_pct = (
                ((last_close - previous_close) / previous_close) * 100
                if last_close is not None and previous_close
                else 0.0
            )
            rows.append(
                {
                    "ticker": ticker,
                    "last_price": last_close or float(info.get("last_price", 0) or 0),
                    "daily_change_pct": daily_change_pct,
                    "market_cap": float(info.get("market_cap", 0) or 0),
                    "fetched_at": datetime.now(timezone.utc).isoformat(),
                }
            )
        except Exception:
            rows.append(
                {
                    "ticker": ticker,
                    "last_price": 0.0,
                    "daily_change_pct": 0.0,
                    "market_cap": 0.0,
                    "fetched_at": datetime.now(timezone.utc).isoformat(),
                }
            )
    return pd.DataFrame(rows)


def collect_inputs(live_market_data: bool | None = None) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    watchlist = load_watchlist()
    articles = fetch_rss_articles()
    market_data = fetch_market_data(watchlist["ticker"], live=live_market_data)
    return articles, watchlist, market_data
