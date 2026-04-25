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
