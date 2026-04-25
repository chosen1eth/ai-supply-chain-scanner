from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd


DB_PATH = Path("alpha_scanner.sqlite")


def get_connection(db_path: Path | str = DB_PATH) -> sqlite3.Connection:
    return sqlite3.connect(db_path)


def init_db(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS scored_signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            theme TEXT,
            theme_keywords TEXT,
            signal_keywords TEXT,
            demand_score REAL,
            supply_constraint_score REAL,
            price_power_score REAL,
            market_attention_score REAL,
            total_alpha_score REAL,
            related_companies TEXT,
            article_title TEXT,
            article_summary TEXT,
            article_link TEXT,
            article_source TEXT,
            published TEXT,
            explanation TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS theme_scores (
            theme TEXT PRIMARY KEY,
            total_alpha_score REAL,
            demand_score REAL,
            supply_constraint_score REAL,
            price_power_score REAL,
            market_attention_score REAL,
            articles INTEGER,
            related_companies TEXT,
            signal_keywords TEXT,
            explanation TEXT,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS research_documents (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            institution TEXT,
            title TEXT,
            url TEXT,
            published TEXT,
            text TEXT,
            source_type TEXT,
            fetched_at TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS research_signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            theme TEXT,
            institution TEXT,
            title TEXT,
            url TEXT,
            published TEXT,
            source_type TEXT,
            theme_keywords TEXT,
            repeated_phrases TEXT,
            demand_hits INTEGER,
            supply_constraint_hits INTEGER,
            price_power_hits INTEGER,
            capex_hits INTEGER,
            snippet TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS research_trends (
            theme TEXT PRIMARY KEY,
            trend_score REAL,
            documents INTEGER,
            institution_count INTEGER,
            institutions TEXT,
            repeated_phrases TEXT,
            latest_published TEXT,
            sample_titles TEXT,
            summary TEXT,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS youtube_documents (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_name TEXT,
            video_id TEXT,
            title TEXT,
            url TEXT,
            published TEXT,
            text TEXT,
            source_type TEXT,
            fetched_at TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS youtube_signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            theme TEXT,
            institution TEXT,
            title TEXT,
            url TEXT,
            published TEXT,
            source_type TEXT,
            theme_keywords TEXT,
            repeated_phrases TEXT,
            demand_hits INTEGER,
            supply_constraint_hits INTEGER,
            price_power_hits INTEGER,
            capex_hits INTEGER,
            snippet TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS youtube_trends (
            theme TEXT PRIMARY KEY,
            trend_score REAL,
            documents INTEGER,
            institution_count INTEGER,
            institutions TEXT,
            repeated_phrases TEXT,
            latest_published TEXT,
            sample_titles TEXT,
            summary TEXT,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.commit()


def save_results(conn: sqlite3.Connection, scored_signals: pd.DataFrame, theme_scores: pd.DataFrame) -> None:
    if not scored_signals.empty:
        columns = [
            "theme",
            "theme_keywords",
            "signal_keywords",
            "demand_score",
            "supply_constraint_score",
            "price_power_score",
            "market_attention_score",
            "total_alpha_score",
            "related_companies",
            "article_title",
            "article_summary",
            "article_link",
            "article_source",
            "published",
            "explanation",
        ]
        scored_signals[columns].to_sql("scored_signals", conn, if_exists="append", index=False)

    if not theme_scores.empty:
        conn.execute("DELETE FROM theme_scores")
        theme_scores.to_sql("theme_scores", conn, if_exists="append", index=False)
    conn.commit()


def load_theme_scores(conn: sqlite3.Connection) -> pd.DataFrame:
    return pd.read_sql_query("SELECT * FROM theme_scores ORDER BY total_alpha_score DESC", conn)


def load_latest_signals(conn: sqlite3.Connection, limit: int = 50) -> pd.DataFrame:
    return pd.read_sql_query(
        """
        SELECT *
        FROM scored_signals
        ORDER BY created_at DESC, total_alpha_score DESC
        LIMIT ?
        """,
        conn,
        params=(limit,),
    )


def save_research_results(
    conn: sqlite3.Connection,
    documents: pd.DataFrame,
    trends: pd.DataFrame,
    signals: pd.DataFrame,
) -> None:
    if not documents.empty:
        conn.execute("DELETE FROM research_documents")
        documents[
            ["institution", "title", "url", "published", "text", "source_type", "fetched_at"]
        ].to_sql("research_documents", conn, if_exists="append", index=False)

    if not signals.empty:
        conn.execute("DELETE FROM research_signals")
        signals[
            [
                "theme",
                "institution",
                "title",
                "url",
                "published",
                "source_type",
                "theme_keywords",
                "repeated_phrases",
                "demand_hits",
                "supply_constraint_hits",
                "price_power_hits",
                "capex_hits",
                "snippet",
            ]
        ].to_sql("research_signals", conn, if_exists="append", index=False)

    if not trends.empty:
        conn.execute("DELETE FROM research_trends")
        trends[
            [
                "theme",
                "trend_score",
                "documents",
                "institution_count",
                "institutions",
                "repeated_phrases",
                "latest_published",
                "sample_titles",
                "summary",
            ]
        ].to_sql("research_trends", conn, if_exists="append", index=False)
    conn.commit()


def load_research_trends(conn: sqlite3.Connection) -> pd.DataFrame:
    return pd.read_sql_query("SELECT * FROM research_trends ORDER BY trend_score DESC", conn)


def load_research_signals(conn: sqlite3.Connection, limit: int = 100) -> pd.DataFrame:
    return pd.read_sql_query(
        """
        SELECT *
        FROM research_signals
        ORDER BY created_at DESC, theme
        LIMIT ?
        """,
        conn,
        params=(limit,),
    )


def load_research_documents(conn: sqlite3.Connection) -> pd.DataFrame:
    return pd.read_sql_query(
        """
        SELECT institution, title, url, published, source_type, fetched_at
        FROM research_documents
        ORDER BY fetched_at DESC, institution, title
        """,
        conn,
    )


def save_youtube_results(
    conn: sqlite3.Connection,
    documents: pd.DataFrame,
    trends: pd.DataFrame,
    signals: pd.DataFrame,
) -> None:
    if not documents.empty:
        conn.execute("DELETE FROM youtube_documents")
        documents[
            ["channel_name", "video_id", "title", "url", "published", "text", "source_type", "fetched_at"]
        ].to_sql("youtube_documents", conn, if_exists="append", index=False)

    if not signals.empty:
        conn.execute("DELETE FROM youtube_signals")
        signals[
            [
                "theme",
                "institution",
                "title",
                "url",
                "published",
                "source_type",
                "theme_keywords",
                "repeated_phrases",
                "demand_hits",
                "supply_constraint_hits",
                "price_power_hits",
                "capex_hits",
                "snippet",
            ]
        ].to_sql("youtube_signals", conn, if_exists="append", index=False)

    if not trends.empty:
        conn.execute("DELETE FROM youtube_trends")
        trends[
            [
                "theme",
                "trend_score",
                "documents",
                "institution_count",
                "institutions",
                "repeated_phrases",
                "latest_published",
                "sample_titles",
                "summary",
            ]
        ].to_sql("youtube_trends", conn, if_exists="append", index=False)
    conn.commit()


def load_youtube_trends(conn: sqlite3.Connection) -> pd.DataFrame:
    return pd.read_sql_query("SELECT * FROM youtube_trends ORDER BY trend_score DESC", conn)


def load_youtube_signals(conn: sqlite3.Connection, limit: int = 100) -> pd.DataFrame:
    return pd.read_sql_query(
        """
        SELECT *
        FROM youtube_signals
        ORDER BY created_at DESC, theme
        LIMIT ?
        """,
        conn,
        params=(limit,),
    )


def load_youtube_documents(conn: sqlite3.Connection) -> pd.DataFrame:
    return pd.read_sql_query(
        """
        SELECT channel_name, video_id, title, url, published, source_type, fetched_at
        FROM youtube_documents
        ORDER BY fetched_at DESC, channel_name, title
        """,
        conn,
    )
