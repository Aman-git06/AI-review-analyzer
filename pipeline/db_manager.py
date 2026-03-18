"""
pipeline/db_manager.py
======================
Handles all SQLite operations:
  - Schema creation
  - Inserting raw reviews + LLM analysis results
  - Query helpers used by Power BI export and EDA notebook
"""

import sqlite3
import os
from datetime import datetime


DB_PATH = os.getenv("DB_PATH", "data/reviews_analysed.db")


# ─────────────────────────────────────────────
#  SCHEMA
# ─────────────────────────────────────────────

SCHEMA = """
CREATE TABLE IF NOT EXISTS raw_reviews (
    review_id       TEXT PRIMARY KEY,
    business_name   TEXT NOT NULL,
    author          TEXT,
    rating          INTEGER,
    text            TEXT,
    review_date     TEXT,
    source          TEXT,
    ingested_at     TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS analysed_reviews (
    review_id           TEXT PRIMARY KEY,
    business_name       TEXT NOT NULL,
    rating              INTEGER,
    review_date         TEXT,

    -- LLM outputs
    sentiment           TEXT,          -- 'positive' | 'negative' | 'neutral' | 'mixed'
    sentiment_score     REAL,          -- -1.0 to 1.0
    one_line_summary    TEXT,
    main_topic          TEXT,          -- e.g. 'food quality', 'wait time', 'staff'
    top_praise          TEXT,
    top_complaint       TEXT,
    tags                TEXT,          -- comma-separated: 'cleanliness,service,value'
    recommend           INTEGER,       -- 1 = would recommend, 0 = would not

    analysed_at         TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (review_id) REFERENCES raw_reviews(review_id)
);

CREATE TABLE IF NOT EXISTS business_summary (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    business_name       TEXT NOT NULL,
    report_date         TEXT,
    total_reviews       INTEGER,
    avg_rating          REAL,
    pct_positive        REAL,
    pct_negative        REAL,
    pct_neutral         REAL,
    top_praise_themes   TEXT,
    top_complaint_themes TEXT,
    overall_sentiment   TEXT,
    created_at          TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_analysed_business  ON analysed_reviews(business_name);
CREATE INDEX IF NOT EXISTS idx_analysed_sentiment ON analysed_reviews(sentiment);
CREATE INDEX IF NOT EXISTS idx_analysed_date      ON analysed_reviews(review_date);
"""


# ─────────────────────────────────────────────
#  CONNECTION HELPER
# ─────────────────────────────────────────────

def get_connection(db_path: str = DB_PATH) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row  # lets you access columns by name
    return conn


def init_db(db_path: str = DB_PATH) -> None:
    """Create all tables if they don't exist."""
    with get_connection(db_path) as conn:
        conn.executescript(SCHEMA)
    print(f"[db] Initialised database at {db_path}")


# ─────────────────────────────────────────────
#  INSERT OPERATIONS
# ─────────────────────────────────────────────

def insert_raw_reviews(reviews: list[dict], db_path: str = DB_PATH) -> int:
    """
    Insert raw scraped reviews.
    Skips duplicates (INSERT OR IGNORE).
    Returns number of newly inserted rows.
    """
    sql = """
        INSERT OR IGNORE INTO raw_reviews
            (review_id, business_name, author, rating, text, review_date, source)
        VALUES
            (:review_id, :business_name, :author, :rating, :text, :date, :source)
    """
    with get_connection(db_path) as conn:
        before = conn.execute("SELECT COUNT(*) FROM raw_reviews").fetchone()[0]
        conn.executemany(sql, reviews)
        after  = conn.execute("SELECT COUNT(*) FROM raw_reviews").fetchone()[0]
    inserted = after - before
    print(f"[db] Inserted {inserted} new raw reviews ({len(reviews) - inserted} duplicates skipped)")
    return inserted


def insert_analysed_review(analysis: dict, db_path: str = DB_PATH) -> None:
    """
    Insert a single LLM analysis result.
    analysis dict must match the analysed_reviews columns.
    """
    sql = """
        INSERT OR REPLACE INTO analysed_reviews
            (review_id, business_name, rating, review_date,
             sentiment, sentiment_score, one_line_summary,
             main_topic, top_praise, top_complaint, tags, recommend)
        VALUES
            (:review_id, :business_name, :rating, :review_date,
             :sentiment, :sentiment_score, :one_line_summary,
             :main_topic, :top_praise, :top_complaint, :tags, :recommend)
    """
    with get_connection(db_path) as conn:
        conn.execute(sql, analysis)


def upsert_business_summary(summary: dict, db_path: str = DB_PATH) -> None:
    sql = """
        INSERT OR REPLACE INTO business_summary
            (business_name, report_date, total_reviews, avg_rating,
             pct_positive, pct_negative, pct_neutral,
             top_praise_themes, top_complaint_themes, overall_sentiment)
        VALUES
            (:business_name, :report_date, :total_reviews, :avg_rating,
             :pct_positive, :pct_negative, :pct_neutral,
             :top_praise_themes, :top_complaint_themes, :overall_sentiment)
    """
    with get_connection(db_path) as conn:
        conn.execute(sql, summary)


# ─────────────────────────────────────────────
#  QUERY HELPERS
# ─────────────────────────────────────────────

def get_unanalysed_reviews(db_path: str = DB_PATH) -> list[dict]:
    """Return raw reviews that haven't been through the LLM yet."""
    sql = """
        SELECT r.*
        FROM   raw_reviews r
        LEFT JOIN analysed_reviews a ON r.review_id = a.review_id
        WHERE  a.review_id IS NULL
        ORDER  BY r.review_date DESC
    """
    with get_connection(db_path) as conn:
        rows = conn.execute(sql).fetchall()
    return [dict(row) for row in rows]


def get_all_analysed(business_name: str = None,
                     db_path: str = DB_PATH) -> list[dict]:
    sql = "SELECT * FROM analysed_reviews"
    params = []
    if business_name:
        sql   += " WHERE business_name = ?"
        params = [business_name]
    sql += " ORDER BY review_date DESC"
    with get_connection(db_path) as conn:
        rows = conn.execute(sql, params).fetchall()
    return [dict(row) for row in rows]


def get_sentiment_breakdown(business_name: str = None,
                             db_path: str = DB_PATH) -> list[dict]:
    sql = """
        SELECT
            business_name,
            sentiment,
            COUNT(*)                              AS count,
            ROUND(AVG(sentiment_score), 3)        AS avg_score,
            ROUND(AVG(rating), 2)                 AS avg_star_rating
        FROM analysed_reviews
        {where}
        GROUP BY business_name, sentiment
        ORDER BY count DESC
    """
    where  = "WHERE business_name = ?" if business_name else ""
    params = [business_name] if business_name else []
    with get_connection(db_path) as conn:
        rows = conn.execute(sql.format(where=where), params).fetchall()
    return [dict(row) for row in rows]


def get_top_themes(sentiment_filter: str = None,
                   limit: int = 10,
                   db_path: str = DB_PATH) -> list[dict]:
    """Return top complained-about or praised topics."""
    col = "top_complaint" if sentiment_filter == "negative" else "top_praise"
    sql = f"""
        SELECT
            {col}               AS theme,
            COUNT(*)            AS mentions,
            AVG(rating)         AS avg_rating
        FROM analysed_reviews
        WHERE {col} IS NOT NULL AND {col} != ''
        {"AND sentiment = ?" if sentiment_filter else ""}
        GROUP BY {col}
        ORDER BY mentions DESC
        LIMIT ?
    """
    params = ([sentiment_filter] if sentiment_filter else []) + [limit]
    with get_connection(db_path) as conn:
        rows = conn.execute(sql, params).fetchall()
    return [dict(row) for row in rows]


def get_monthly_sentiment_trend(business_name: str = None,
                                 db_path: str = DB_PATH) -> list[dict]:
    sql = """
        SELECT
            SUBSTR(review_date, 1, 7)             AS month,
            business_name,
            COUNT(*)                              AS total_reviews,
            ROUND(AVG(sentiment_score), 3)        AS avg_sentiment,
            ROUND(AVG(rating), 2)                 AS avg_rating,
            SUM(CASE WHEN sentiment='positive' THEN 1 ELSE 0 END) AS positive_count,
            SUM(CASE WHEN sentiment='negative' THEN 1 ELSE 0 END) AS negative_count
        FROM analysed_reviews
        {where}
        GROUP BY month, business_name
        ORDER BY month
    """
    where  = "WHERE business_name = ?" if business_name else ""
    params = [business_name] if business_name else []
    with get_connection(db_path) as conn:
        rows = conn.execute(sql.format(where=where), params).fetchall()
    return [dict(row) for row in rows]


# ─────────────────────────────────────────────
#  QUICK TEST
# ─────────────────────────────────────────────
if __name__ == "__main__":
    init_db()
    print("[db] Schema OK")
    print("[db] Sentiment breakdown:", get_sentiment_breakdown())
