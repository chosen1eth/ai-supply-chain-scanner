from __future__ import annotations

import pandas as pd


def score_signals(signals: pd.DataFrame, market_data: pd.DataFrame, watchlist: pd.DataFrame) -> pd.DataFrame:
    if signals.empty:
        return signals

    company_to_ticker = dict(zip(watchlist["company"], watchlist["ticker"]))
    ticker_change = dict(zip(market_data["ticker"], market_data["daily_change_pct"]))

    scored = signals.copy()
    scored["demand_score"] = (scored["demand_hits"] * 15 + scored["capex_hits"] * 10).clip(upper=100)
    scored["supply_constraint_score"] = (scored["supply_constraint_hits"] * 25).clip(upper=100)
    scored["price_power_score"] = (scored["price_power_hits"] * 25).clip(upper=100)

    attention_scores = []
    for companies in scored["related_companies"].fillna(""):
        changes = []
        for company in [item.strip() for item in companies.split(",") if item.strip()]:
            ticker = company_to_ticker.get(company)
            if ticker:
                changes.append(abs(float(ticker_change.get(ticker, 0.0))))
        attention_scores.append(min(sum(changes) * 4, 100) if changes else 5)

    scored["market_attention_score"] = attention_scores
    scored["total_alpha_score"] = (
        scored["demand_score"] * 0.25
        + scored["supply_constraint_score"] * 0.35
        + scored["price_power_score"] * 0.25
        + scored["market_attention_score"] * 0.15
    ).round(2)

    scored["explanation"] = scored.apply(_explain_signal, axis=1)
    return scored.sort_values("total_alpha_score", ascending=False)


def aggregate_themes(scored_signals: pd.DataFrame) -> pd.DataFrame:
    if scored_signals.empty:
        return pd.DataFrame()

    grouped = (
        scored_signals.groupby("theme")
        .agg(
            total_alpha_score=("total_alpha_score", "max"),
            demand_score=("demand_score", "max"),
            supply_constraint_score=("supply_constraint_score", "max"),
            price_power_score=("price_power_score", "max"),
            market_attention_score=("market_attention_score", "max"),
            articles=("article_title", "count"),
            related_companies=("related_companies", _join_unique),
            signal_keywords=("signal_keywords", _join_unique),
            explanation=("explanation", "first"),
        )
        .reset_index()
        .sort_values("total_alpha_score", ascending=False)
    )
    return grouped


def _join_unique(values: pd.Series) -> str:
    seen = []
    for value in values.dropna():
        for item in str(value).split(","):
            item = item.strip()
            if item and item not in seen:
                seen.append(item)
    return ", ".join(seen)


def _explain_signal(row: pd.Series) -> str:
    reasons = []
    if row["supply_constraint_score"] > 0:
        reasons.append("supply constraint language")
    if row["price_power_score"] > 0:
        reasons.append("pricing or margin language")
    if row["demand_score"] > 0:
        reasons.append("demand or capex language")
    if row["market_attention_score"] > 5:
        reasons.append("watchlist market movement")
    if not reasons:
        reasons.append("AI infrastructure keyword activity")
    return f"Flagged because the article combines {', '.join(reasons)} for {row['theme']}."
