from __future__ import annotations

import pandas as pd
import streamlit as st

from data_sources import collect_inputs
from database import get_connection, init_db, load_latest_signals, load_theme_scores, save_results
from scoring import aggregate_themes, score_signals
from signal_extractor import extract_signals


st.set_page_config(page_title="AI Supply Chain Alpha Scanner", layout="wide")


@st.cache_data(ttl=900)
def run_scan(live_market_data: bool = False) -> tuple[pd.DataFrame, pd.DataFrame]:
    articles, watchlist, market_data = collect_inputs(live_market_data=live_market_data)
    signals = extract_signals(articles, watchlist)
    scored = score_signals(signals, market_data, watchlist)
    themes = aggregate_themes(scored)

    conn = get_connection()
    init_db(conn)
    save_results(conn, scored, themes)
    conn.close()
    return themes, scored


def load_cached_results() -> tuple[pd.DataFrame, pd.DataFrame]:
    conn = get_connection()
    init_db(conn)
    themes = load_theme_scores(conn)
    signals = load_latest_signals(conn)
    conn.close()
    return themes, signals


st.title("AI Supply Chain Alpha Scanner")
st.caption("Early signal scanner for AI infrastructure bottlenecks.")

left, right = st.columns([1, 4])
with left:
    live_market_data = st.checkbox("Use live market data", value=False)
    if st.button("Run scan", use_container_width=True):
        with st.spinner("Collecting feeds, market data, and bottleneck signals..."):
            themes_df, signals_df = run_scan(live_market_data=live_market_data)
            st.session_state["themes_df"] = themes_df
            st.session_state["signals_df"] = signals_df

if "themes_df" not in st.session_state:
    st.session_state["themes_df"], st.session_state["signals_df"] = load_cached_results()

themes_df = st.session_state["themes_df"]
signals_df = st.session_state["signals_df"]

if themes_df.empty:
    st.info("No stored scan yet. Click Run scan to collect live data or use mock fallback data.")
    st.stop()

top_themes = themes_df.head(10)

st.subheader("Top 10 Emerging Bottleneck Themes")
st.dataframe(
    top_themes[
        [
            "theme",
            "total_alpha_score",
            "demand_score",
            "supply_constraint_score",
            "price_power_score",
            "market_attention_score",
            "related_companies",
            "signal_keywords",
        ]
    ],
    use_container_width=True,
    hide_index=True,
)

selected_theme = st.selectbox("Inspect theme", top_themes["theme"].tolist())
theme_row = top_themes[top_themes["theme"] == selected_theme].iloc[0]

score_cols = st.columns(5)
score_cols[0].metric("Total alpha", f"{theme_row['total_alpha_score']:.1f}")
score_cols[1].metric("Demand", f"{theme_row['demand_score']:.0f}")
score_cols[2].metric("Supply constraint", f"{theme_row['supply_constraint_score']:.0f}")
score_cols[3].metric("Price power", f"{theme_row['price_power_score']:.0f}")
score_cols[4].metric("Market attention", f"{theme_row['market_attention_score']:.0f}")

st.write(theme_row["explanation"])
st.write(f"Related companies: {theme_row['related_companies'] or 'None found in source text'}")
st.write(f"Signal keywords: {theme_row['signal_keywords'] or 'Theme-only signal'}")

st.subheader("Latest Source Articles")
theme_signals = signals_df[signals_df["theme"] == selected_theme].sort_values(
    "total_alpha_score", ascending=False
)

for _, signal in theme_signals.head(10).iterrows():
    with st.container(border=True):
        st.markdown(f"**{signal['article_title']}**")
        st.caption(f"{signal['article_source']} | {signal['published']}")
        st.write(signal["article_summary"])
        if str(signal["article_link"]).startswith("http"):
            st.link_button("Open source", signal["article_link"])

st.subheader("Raw Signals")
st.dataframe(signals_df.head(50), use_container_width=True, hide_index=True)
