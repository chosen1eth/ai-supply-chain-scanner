from __future__ import annotations

import re
from collections import Counter

import pandas as pd

from signal_extractor import SIGNAL_KEYWORDS, THEMES


REPEATED_PHRASES = [
    "ai infrastructure",
    "data center capacity",
    "data center power",
    "power demand",
    "power constraints",
    "grid constraints",
    "energy infrastructure",
    "hyperscaler capex",
    "capital expenditure",
    "compute demand",
    "compute supply",
    "advanced packaging",
    "hbm",
    "high bandwidth memory",
    "liquid cooling",
    "power density",
    "rack-scale",
    "optical transceivers",
    "ethernet switching",
    "inference demand",
    "debt financing",
    "credit markets",
    "supply chain",
    "bottlenecks",
]


def extract_research_trends(documents: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    if documents.empty:
        return pd.DataFrame(), pd.DataFrame()

    signal_rows = []
    for _, doc in documents.iterrows():
        text = _normalize(str(doc.get("text", "")))
        theme_hits = {theme: _matches(text, terms) for theme, terms in THEMES.items()}
        theme_hits = {theme: hits for theme, hits in theme_hits.items() if hits}
        phrase_hits = _matches(text, REPEATED_PHRASES)
        signal_hits = {
            category: _matches(text, keywords)
            for category, keywords in SIGNAL_KEYWORDS.items()
        }

        candidate_themes = theme_hits.keys() or ["Unclassified AI infrastructure"]
        for theme in candidate_themes:
            signal_rows.append(
                {
                    "theme": theme,
                    "institution": doc.get("institution", ""),
                    "title": doc.get("title", ""),
                    "url": doc.get("url", ""),
                    "published": doc.get("published", ""),
                    "source_type": doc.get("source_type", ""),
                    "theme_keywords": ", ".join(theme_hits.get(theme, [])),
                    "repeated_phrases": ", ".join(phrase_hits),
                    "demand_hits": len(signal_hits["demand"]),
                    "supply_constraint_hits": len(signal_hits["supply_constraint"]),
                    "price_power_hits": len(signal_hits["price_power"]),
                    "capex_hits": len(signal_hits["capex"]),
                    "snippet": _snippet(doc.get("text", ""), theme_hits.get(theme, []) + phrase_hits),
                }
            )

    signals = pd.DataFrame(signal_rows)
    if signals.empty:
        return pd.DataFrame(), signals

    trends = _aggregate_trends(signals)
    return trends, signals


def _aggregate_trends(signals: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for theme, group in signals.groupby("theme"):
        institutions = _join_unique(group["institution"])
        repeated_phrases = _top_items(group["repeated_phrases"], limit=8)
        keyword_score = (
            group["demand_hits"].sum()
            + group["supply_constraint_hits"].sum() * 1.5
            + group["price_power_hits"].sum() * 1.25
            + group["capex_hits"].sum()
        )
        institution_count = len([item for item in institutions.split(", ") if item])
        document_count = len(group)
        trend_score = round(document_count * 10 + institution_count * 15 + keyword_score, 2)

        rows.append(
            {
                "theme": theme,
                "trend_score": trend_score,
                "documents": document_count,
                "institution_count": institution_count,
                "institutions": institutions,
                "repeated_phrases": repeated_phrases,
                "latest_published": _latest_text(group["published"]),
                "sample_titles": _join_unique(group["title"], limit=4),
                "summary": _summarize_theme(theme, institutions, repeated_phrases, document_count),
            }
        )
    return pd.DataFrame(rows).sort_values("trend_score", ascending=False)


def _normalize(text: str) -> str:
    return re.sub(r"\s+", " ", text.lower()).strip()


def _matches(text: str, terms: list[str]) -> list[str]:
    found = []
    for term in terms:
        pattern = r"\b" + re.escape(term.lower()) + r"\b"
        if re.search(pattern, text):
            found.append(term)
    return found


def _snippet(text: str, terms: list[str], window: int = 220) -> str:
    clean = re.sub(r"\s+", " ", str(text)).strip()
    lower = clean.lower()
    positions = [lower.find(term.lower()) for term in terms if lower.find(term.lower()) >= 0]
    if not positions:
        return clean[:window]
    start = max(min(positions) - 80, 0)
    return clean[start : start + window].strip()


def _join_unique(values: pd.Series, limit: int | None = None) -> str:
    seen = []
    for value in values.dropna():
        for item in str(value).split(","):
            item = item.strip()
            if item and item not in seen:
                seen.append(item)
                if limit and len(seen) >= limit:
                    return ", ".join(seen)
    return ", ".join(seen)


def _top_items(values: pd.Series, limit: int = 8) -> str:
    counter: Counter[str] = Counter()
    for _, value in values.dropna().items():
        for item in str(value).split(","):
            item = item.strip()
            if item:
                counter[item] += 1
    return ", ".join(item for item, _ in counter.most_common(limit))


def _latest_text(values: pd.Series) -> str:
    items = [str(value).strip() for value in values.dropna() if str(value).strip()]
    return sorted(items)[-1] if items else ""


def _summarize_theme(theme: str, institutions: str, phrases: str, documents: int) -> str:
    institution_text = institutions or "the collected sources"
    phrase_text = phrases or "AI infrastructure language"
    return (
        f"{theme} appears across {documents} research document(s). "
        f"Repeated signals include {phrase_text}. Sources include {institution_text}."
    )
