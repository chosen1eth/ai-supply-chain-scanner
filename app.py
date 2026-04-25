from __future__ import annotations

import pandas as pd
import streamlit as st

from data_sources import collect_inputs
from database import (
    get_connection,
    init_db,
    load_latest_signals,
    load_research_documents,
    load_research_signals,
    load_research_trends,
    load_theme_scores,
    load_youtube_documents,
    load_youtube_signals,
    load_youtube_trends,
    save_research_results,
    save_results,
    save_youtube_results,
)
from research_sources import RESEARCH_REPORTS_DIR, collect_research_inputs
from scoring import aggregate_themes, score_signals
from signal_extractor import extract_signals
from trend_extractor import extract_research_trends
from youtube_sources import YOUTUBE_CHANNELS_PATH, YOUTUBE_TRANSCRIPTS_DIR, collect_youtube_transcripts


st.set_page_config(page_title="AI Supply Chain Alpha Scanner", layout="wide")


TEXT = {
    "zh": {
        "title": "AI 供应链 Alpha 扫描器",
        "caption": "用于发现 AI 基础设施瓶颈的早期信号。",
        "language": "界面语言",
        "live_market_data": "使用实时市场数据",
        "run_scan": "开始扫描",
        "spinner": "正在收集资讯、市场数据和瓶颈信号...",
        "empty": "还没有已保存的扫描结果。点击“开始扫描”收集实时资讯，或使用本地模拟数据。",
        "top_themes": "Top 10 新兴瓶颈主题",
        "inspect_theme": "查看主题",
        "total_alpha": "Alpha 总分",
        "demand": "需求",
        "supply_constraint": "供应约束",
        "price_power": "定价能力",
        "market_attention": "市场关注度",
        "related_companies": "相关公司",
        "signal_keywords": "信号关键词",
        "none_found": "原文中未识别到相关公司",
        "theme_only": "仅主题信号",
        "latest_articles": "最新来源文章",
        "open_source": "打开来源",
        "raw_signals": "原始信号",
        "research_section": "机构研究信号",
        "run_research": "扫描机构研究",
        "include_public_research": "抓取公开机构页面",
        "include_local_research": "读取本地研报文件夹",
        "research_spinner": "正在收集机构研究并提取重复趋势...",
        "research_empty": "还没有机构研究结果。点击“扫描机构研究”，或把合法获取的 PDF/TXT/HTML 放入 research_reports 文件夹。",
        "research_trends": "重复出现的机构主题",
        "research_docs": "来源文档",
        "research_signals": "机构研究原始信号",
        "inspect_research_theme": "查看机构主题",
        "trend_score": "趋势分",
        "documents": "文档数",
        "institution_count": "机构数",
        "institutions": "机构",
        "repeated_phrases": "重复表述",
        "sample_titles": "样本文档",
        "source_folder": "本地研报文件夹",
        "youtube_section": "YouTube 研报解读信号",
        "run_youtube": "扫描 YouTube",
        "youtube_spinner": "正在监控频道、抓取字幕并提取趋势...",
        "youtube_empty": "还没有 YouTube 结果。先在 youtube_channels.csv 填入频道 ID；如果自动字幕失败，可把文字稿放进 youtube_transcripts 文件夹。",
        "youtube_trends": "YouTube 重复主题",
        "youtube_signals": "YouTube 原始信号",
        "youtube_docs": "来源视频",
        "youtube_channels": "频道配置",
        "youtube_transcripts": "本地文字稿文件夹",
        "max_videos": "每个频道扫描视频数",
        "inspect_youtube_theme": "查看 YouTube 主题",
        "explanation_prefix": "该主题被标记，是因为文章出现了",
        "explanation_suffix": "等信号。",
        "reason_supply": "供应约束表述",
        "reason_price": "定价或利润率表述",
        "reason_demand": "需求或资本开支表述",
        "reason_attention": "关注公司股价波动",
        "reason_default": "AI 基础设施关键词活动",
    },
    "en": {
        "title": "AI Supply Chain Alpha Scanner",
        "caption": "Early signal scanner for AI infrastructure bottlenecks.",
        "language": "Language",
        "live_market_data": "Use live market data",
        "run_scan": "Run scan",
        "spinner": "Collecting feeds, market data, and bottleneck signals...",
        "empty": "No stored scan yet. Click Run scan to collect live data or use mock fallback data.",
        "top_themes": "Top 10 Emerging Bottleneck Themes",
        "inspect_theme": "Inspect theme",
        "total_alpha": "Total alpha",
        "demand": "Demand",
        "supply_constraint": "Supply constraint",
        "price_power": "Price power",
        "market_attention": "Market attention",
        "related_companies": "Related companies",
        "signal_keywords": "Signal keywords",
        "none_found": "None found in source text",
        "theme_only": "Theme-only signal",
        "latest_articles": "Latest Source Articles",
        "open_source": "Open source",
        "raw_signals": "Raw Signals",
        "research_section": "Institutional Research Signals",
        "run_research": "Scan research",
        "include_public_research": "Fetch public institutional pages",
        "include_local_research": "Read local research folder",
        "research_spinner": "Collecting institutional research and extracting repeated trends...",
        "research_empty": "No institutional research results yet. Click Scan research, or put legally obtained PDF/TXT/HTML files in the research_reports folder.",
        "research_trends": "Repeated Institutional Themes",
        "research_docs": "Source Documents",
        "research_signals": "Raw Research Signals",
        "inspect_research_theme": "Inspect research theme",
        "trend_score": "Trend score",
        "documents": "Documents",
        "institution_count": "Institutions",
        "institutions": "Institutions",
        "repeated_phrases": "Repeated phrases",
        "sample_titles": "Sample titles",
        "source_folder": "Local research folder",
        "youtube_section": "YouTube Research-Digest Signals",
        "run_youtube": "Scan YouTube",
        "youtube_spinner": "Monitoring channels, fetching captions, and extracting trends...",
        "youtube_empty": "No YouTube results yet. Add channel IDs to youtube_channels.csv; if automated captions fail, put transcripts in youtube_transcripts.",
        "youtube_trends": "Repeated YouTube Themes",
        "youtube_signals": "Raw YouTube Signals",
        "youtube_docs": "Source Videos",
        "youtube_channels": "Channel config",
        "youtube_transcripts": "Local transcript folder",
        "max_videos": "Videos per channel",
        "inspect_youtube_theme": "Inspect YouTube theme",
    },
}

TABLE_COLUMNS = {
    "zh": {
        "theme": "主题",
        "total_alpha_score": "Alpha 总分",
        "demand_score": "需求分",
        "supply_constraint_score": "供应约束分",
        "price_power_score": "定价能力分",
        "market_attention_score": "市场关注分",
        "related_companies": "相关公司",
        "signal_keywords": "信号关键词",
    },
    "en": {},
}

RESEARCH_TABLE_COLUMNS = {
    "zh": {
        "theme": "主题",
        "trend_score": "趋势分",
        "documents": "文档数",
        "institution_count": "机构数",
        "institutions": "机构",
        "repeated_phrases": "重复表述",
        "latest_published": "最新日期",
        "sample_titles": "样本文档",
    },
    "en": {},
}


def explain_signal(row: pd.Series, labels: dict[str, str], language: str) -> str:
    if language == "en":
        return str(row["explanation"])

    reasons = []
    if row["supply_constraint_score"] > 0:
        reasons.append(labels["reason_supply"])
    if row["price_power_score"] > 0:
        reasons.append(labels["reason_price"])
    if row["demand_score"] > 0:
        reasons.append(labels["reason_demand"])
    if row["market_attention_score"] > 5:
        reasons.append(labels["reason_attention"])
    if not reasons:
        reasons.append(labels["reason_default"])
    return f"{labels['explanation_prefix']}{'、'.join(reasons)}，指向 {row['theme']} {labels['explanation_suffix']}"


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


@st.cache_data(ttl=900)
def run_research_scan(include_public: bool = True, include_local: bool = True) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    documents = collect_research_inputs(include_public=include_public, include_local=include_local)
    trends, signals = extract_research_trends(documents)

    conn = get_connection()
    init_db(conn)
    save_research_results(conn, documents, trends, signals)
    conn.close()
    return trends, signals, documents


def load_cached_research_results() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    conn = get_connection()
    init_db(conn)
    trends = load_research_trends(conn)
    signals = load_research_signals(conn)
    documents = load_research_documents(conn)
    conn.close()
    return trends, signals, documents


@st.cache_data(ttl=900)
def run_youtube_scan(max_videos_per_channel: int = 3) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    documents = collect_youtube_transcripts(max_videos_per_channel=max_videos_per_channel)
    trends, signals = extract_research_trends(documents)

    conn = get_connection()
    init_db(conn)
    save_youtube_results(conn, documents, trends, signals)
    conn.close()
    return trends, signals, documents


def load_cached_youtube_results() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    conn = get_connection()
    init_db(conn)
    trends = load_youtube_trends(conn)
    signals = load_youtube_signals(conn)
    documents = load_youtube_documents(conn)
    conn.close()
    return trends, signals, documents


language_label = st.sidebar.radio("Language / 界面语言", ["中文", "English"], horizontal=True)
language = "zh" if language_label == "中文" else "en"
text = TEXT[language]

st.title(text["title"])
st.caption(text["caption"])

left, right = st.columns([1, 4])
with left:
    live_market_data = st.checkbox(text["live_market_data"], value=False)
    if st.button(text["run_scan"], width="stretch"):
        with st.spinner(text["spinner"]):
            themes_df, signals_df = run_scan(live_market_data=live_market_data)
            st.session_state["themes_df"] = themes_df
            st.session_state["signals_df"] = signals_df

    st.divider()
    include_public_research = st.checkbox(text["include_public_research"], value=True)
    include_local_research = st.checkbox(text["include_local_research"], value=True)
    if st.button(text["run_research"], width="stretch"):
        with st.spinner(text["research_spinner"]):
            research_trends_df, research_signals_df, research_documents_df = run_research_scan(
                include_public=include_public_research,
                include_local=include_local_research,
            )
            st.session_state["research_trends_df"] = research_trends_df
            st.session_state["research_signals_df"] = research_signals_df
            st.session_state["research_documents_df"] = research_documents_df

    st.divider()
    max_youtube_videos = st.number_input(text["max_videos"], min_value=1, max_value=10, value=3, step=1)
    if st.button(text["run_youtube"], width="stretch"):
        with st.spinner(text["youtube_spinner"]):
            youtube_trends_df, youtube_signals_df, youtube_documents_df = run_youtube_scan(
                max_videos_per_channel=int(max_youtube_videos)
            )
            st.session_state["youtube_trends_df"] = youtube_trends_df
            st.session_state["youtube_signals_df"] = youtube_signals_df
            st.session_state["youtube_documents_df"] = youtube_documents_df

if "themes_df" not in st.session_state:
    st.session_state["themes_df"], st.session_state["signals_df"] = load_cached_results()

if "research_trends_df" not in st.session_state:
    (
        st.session_state["research_trends_df"],
        st.session_state["research_signals_df"],
        st.session_state["research_documents_df"],
    ) = load_cached_research_results()

if "youtube_trends_df" not in st.session_state:
    (
        st.session_state["youtube_trends_df"],
        st.session_state["youtube_signals_df"],
        st.session_state["youtube_documents_df"],
    ) = load_cached_youtube_results()

themes_df = st.session_state["themes_df"]
signals_df = st.session_state["signals_df"]
research_trends_df = st.session_state["research_trends_df"]
research_signals_df = st.session_state["research_signals_df"]
research_documents_df = st.session_state["research_documents_df"]
youtube_trends_df = st.session_state["youtube_trends_df"]
youtube_signals_df = st.session_state["youtube_signals_df"]
youtube_documents_df = st.session_state["youtube_documents_df"]

if themes_df.empty:
    st.info(text["empty"])
    st.stop()

top_themes = themes_df.head(10)

st.subheader(text["top_themes"])
display_columns = [
    "theme",
    "total_alpha_score",
    "demand_score",
    "supply_constraint_score",
    "price_power_score",
    "market_attention_score",
    "related_companies",
    "signal_keywords",
]
top_theme_display = top_themes[display_columns].rename(columns=TABLE_COLUMNS[language])
st.dataframe(
    top_theme_display,
    width="stretch",
    hide_index=True,
)

selected_theme = st.selectbox(text["inspect_theme"], top_themes["theme"].tolist())
theme_row = top_themes[top_themes["theme"] == selected_theme].iloc[0]

score_cols = st.columns(5)
score_cols[0].metric(text["total_alpha"], f"{theme_row['total_alpha_score']:.1f}")
score_cols[1].metric(text["demand"], f"{theme_row['demand_score']:.0f}")
score_cols[2].metric(text["supply_constraint"], f"{theme_row['supply_constraint_score']:.0f}")
score_cols[3].metric(text["price_power"], f"{theme_row['price_power_score']:.0f}")
score_cols[4].metric(text["market_attention"], f"{theme_row['market_attention_score']:.0f}")

st.write(explain_signal(theme_row, text, language))
st.write(f"{text['related_companies']}: {theme_row['related_companies'] or text['none_found']}")
st.write(f"{text['signal_keywords']}: {theme_row['signal_keywords'] or text['theme_only']}")

st.subheader(text["latest_articles"])
theme_signals = signals_df[signals_df["theme"] == selected_theme].sort_values(
    "total_alpha_score", ascending=False
)

for _, signal in theme_signals.head(10).iterrows():
    with st.container(border=True):
        st.markdown(f"**{signal['article_title']}**")
        st.caption(f"{signal['article_source']} | {signal['published']}")
        st.write(signal["article_summary"])
        if str(signal["article_link"]).startswith("http"):
            st.link_button(text["open_source"], signal["article_link"])

st.subheader(text["raw_signals"])
st.dataframe(signals_df.head(50), width="stretch", hide_index=True)

st.divider()
st.header(text["research_section"])
st.caption(f"{text['source_folder']}: {RESEARCH_REPORTS_DIR.resolve()}")

if research_trends_df.empty:
    st.info(text["research_empty"])
else:
    st.subheader(text["research_trends"])
    research_display_columns = [
        "theme",
        "trend_score",
        "documents",
        "institution_count",
        "institutions",
        "repeated_phrases",
        "latest_published",
        "sample_titles",
    ]
    st.dataframe(
        research_trends_df[research_display_columns].rename(columns=RESEARCH_TABLE_COLUMNS[language]),
        width="stretch",
        hide_index=True,
    )

    selected_research_theme = st.selectbox(
        text["inspect_research_theme"],
        research_trends_df["theme"].tolist(),
    )
    research_row = research_trends_df[research_trends_df["theme"] == selected_research_theme].iloc[0]

    research_cols = st.columns(3)
    research_cols[0].metric(text["trend_score"], f"{research_row['trend_score']:.1f}")
    research_cols[1].metric(text["documents"], f"{research_row['documents']:.0f}")
    research_cols[2].metric(text["institution_count"], f"{research_row['institution_count']:.0f}")
    st.write(research_row["summary"])
    st.write(f"{text['institutions']}: {research_row['institutions']}")
    st.write(f"{text['repeated_phrases']}: {research_row['repeated_phrases']}")

    st.subheader(text["research_signals"])
    theme_research_signals = research_signals_df[
        research_signals_df["theme"] == selected_research_theme
    ]
    for _, signal in theme_research_signals.head(10).iterrows():
        with st.container(border=True):
            st.markdown(f"**{signal['title']}**")
            st.caption(f"{signal['institution']} | {signal['published']} | {signal['source_type']}")
            st.write(signal["snippet"])
            if str(signal["url"]).startswith("http"):
                st.link_button(text["open_source"], signal["url"])

    st.subheader(text["research_docs"])
    st.dataframe(research_documents_df, width="stretch", hide_index=True)

st.divider()
st.header(text["youtube_section"])
st.caption(f"{text['youtube_channels']}: {YOUTUBE_CHANNELS_PATH.resolve()}")
st.caption(f"{text['youtube_transcripts']}: {YOUTUBE_TRANSCRIPTS_DIR.resolve()}")

if youtube_trends_df.empty:
    st.info(text["youtube_empty"])
else:
    st.subheader(text["youtube_trends"])
    youtube_display_columns = [
        "theme",
        "trend_score",
        "documents",
        "institution_count",
        "institutions",
        "repeated_phrases",
        "latest_published",
        "sample_titles",
    ]
    st.dataframe(
        youtube_trends_df[youtube_display_columns].rename(columns=RESEARCH_TABLE_COLUMNS[language]),
        width="stretch",
        hide_index=True,
    )

    selected_youtube_theme = st.selectbox(
        text["inspect_youtube_theme"],
        youtube_trends_df["theme"].tolist(),
    )
    youtube_row = youtube_trends_df[youtube_trends_df["theme"] == selected_youtube_theme].iloc[0]

    youtube_cols = st.columns(3)
    youtube_cols[0].metric(text["trend_score"], f"{youtube_row['trend_score']:.1f}")
    youtube_cols[1].metric(text["documents"], f"{youtube_row['documents']:.0f}")
    youtube_cols[2].metric(text["institution_count"], f"{youtube_row['institution_count']:.0f}")
    st.write(youtube_row["summary"])
    st.write(f"{text['institutions']}: {youtube_row['institutions']}")
    st.write(f"{text['repeated_phrases']}: {youtube_row['repeated_phrases']}")

    st.subheader(text["youtube_signals"])
    theme_youtube_signals = youtube_signals_df[
        youtube_signals_df["theme"] == selected_youtube_theme
    ]
    for _, signal in theme_youtube_signals.head(10).iterrows():
        with st.container(border=True):
            st.markdown(f"**{signal['title']}**")
            st.caption(f"{signal['institution']} | {signal['published']} | {signal['source_type']}")
            st.write(signal["snippet"])
            if str(signal["url"]).startswith("http"):
                st.link_button(text["open_source"], signal["url"])

    st.subheader(text["youtube_docs"])
    st.dataframe(youtube_documents_df, width="stretch", hide_index=True)
