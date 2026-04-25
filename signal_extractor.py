from __future__ import annotations

import re
from collections import defaultdict

import pandas as pd


THEMES = {
    "HBM": ["hbm", "high bandwidth memory", "dram"],
    "advanced packaging / CoWoS": ["cowos", "advanced packaging", "chip-on-wafer", "packaging capacity"],
    "optical modules / transceivers": ["optical module", "transceiver", "800g", "1.6t", "coherent optics"],
    "data center power": ["power", "grid", "transformer", "switchgear", "ups", "substation"],
    "AI servers": ["ai server", "gpu server", "rack-scale", "accelerator server"],
    "cooling": ["liquid cooling", "cooling", "thermal", "immersion"],
    "networking chips": ["networking chip", "ethernet", "switch chip", "infiniband", "nic", "dpu"],
}

SIGNAL_KEYWORDS = {
    "demand": ["demand", "orders", "pull in", "buildout", "adoption", "growth"],
    "supply_constraint": [
        "shortage",
        "capacity constraint",
        "lead time",
        "backlog",
        "supply tightness",
        "allocation",
        "bottleneck",
    ],
    "price_power": ["price increase", "pricing", "margin expansion", "premium", "asp", "gross margin"],
    "capex": ["capex increase", "capacity expansion", "new fab", "new facility", "investment"],
}


def _normalize(text: str) -> str:
    return re.sub(r"\s+", " ", text.lower()).strip()


def _matches(text: str, terms: list[str]) -> list[str]:
    found = []
    for term in terms:
        pattern = r"\b" + re.escape(term.lower()) + r"\b"
        if re.search(pattern, text):
            found.append(term)
    return found


def extract_signals(articles: pd.DataFrame, watchlist: pd.DataFrame) -> pd.DataFrame:
    rows = []
    company_terms = {
        row["company"]: [row["company"].lower(), row["ticker"].lower()]
        for _, row in watchlist.iterrows()
    }

    for _, article in articles.iterrows():
        text = _normalize(f"{article.get('title', '')} {article.get('summary', '')}")
        theme_hits = {theme: _matches(text, terms) for theme, terms in THEMES.items()}
        theme_hits = {theme: hits for theme, hits in theme_hits.items() if hits}

        signal_hits = defaultdict(list)
        for category, keywords in SIGNAL_KEYWORDS.items():
            signal_hits[category].extend(_matches(text, keywords))

        related_companies = []
        for company, terms in company_terms.items():
            if any(term in text for term in terms):
                related_companies.append(company)

        if not theme_hits and not any(signal_hits.values()):
            continue

        candidate_themes = theme_hits.keys() or ["Unclassified AI infrastructure"]
        for theme in candidate_themes:
            rows.append(
                {
                    "theme": theme,
                    "theme_keywords": ", ".join(theme_hits.get(theme, [])),
                    "signal_keywords": ", ".join(sorted({k for values in signal_hits.values() for k in values})),
                    "demand_hits": len(signal_hits["demand"]),
                    "supply_constraint_hits": len(signal_hits["supply_constraint"]),
                    "price_power_hits": len(signal_hits["price_power"]),
                    "capex_hits": len(signal_hits["capex"]),
                    "related_companies": ", ".join(related_companies),
                    "article_title": article.get("title", ""),
                    "article_summary": article.get("summary", ""),
                    "article_link": article.get("link", ""),
                    "article_source": article.get("source", ""),
                    "published": article.get("published", ""),
                }
            )

    return pd.DataFrame(rows)
