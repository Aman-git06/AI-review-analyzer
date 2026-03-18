-- ============================================================
-- powerbi_queries.sql
-- Ready-to-use SQL queries for Power BI Desktop
-- Connect via: Get Data → ODBC → SQLite ODBC Driver
-- Or use Python script connector with sqlite3
-- ============================================================


-- ─────────────────────────────────────────────
-- TABLE 1: Main fact table for all visuals
-- Use as your primary dataset in Power BI
-- ─────────────────────────────────────────────
SELECT
    a.review_id,
    a.business_name,
    a.rating,
    a.review_date,
    SUBSTR(a.review_date, 1, 7)         AS year_month,
    SUBSTR(a.review_date, 1, 4)         AS year,
    a.sentiment,
    a.sentiment_score,
    a.one_line_summary,
    a.main_topic,
    a.top_praise,
    a.top_complaint,
    a.tags,
    a.recommend,
    CASE
        WHEN a.rating >= 4 THEN 'High (4-5★)'
        WHEN a.rating = 3  THEN 'Mid (3★)'
        ELSE                    'Low (1-2★)'
    END AS rating_band
FROM analysed_reviews a
ORDER BY a.review_date DESC;


-- ─────────────────────────────────────────────
-- TABLE 2: KPI Summary Card
-- For the top-row metric cards in your dashboard
-- ─────────────────────────────────────────────
SELECT
    business_name,
    COUNT(*)                                            AS total_reviews,
    ROUND(AVG(rating), 2)                               AS avg_star_rating,
    ROUND(AVG(sentiment_score), 3)                      AS avg_sentiment_score,
    SUM(CASE WHEN sentiment = 'positive' THEN 1 ELSE 0 END) AS positive_count,
    SUM(CASE WHEN sentiment = 'negative' THEN 1 ELSE 0 END) AS negative_count,
    ROUND(
        100.0 * SUM(CASE WHEN sentiment = 'positive' THEN 1 ELSE 0 END) / COUNT(*),
    1)                                                  AS pct_positive,
    ROUND(
        100.0 * SUM(CASE WHEN recommend = 1 THEN 1 ELSE 0 END) / COUNT(*),
    1)                                                  AS pct_recommend
FROM analysed_reviews
GROUP BY business_name;


-- ─────────────────────────────────────────────
-- TABLE 3: Monthly trend (line chart)
-- ─────────────────────────────────────────────
SELECT
    SUBSTR(review_date, 1, 7)                           AS month,
    business_name,
    COUNT(*)                                            AS review_count,
    ROUND(AVG(rating), 2)                               AS avg_rating,
    ROUND(AVG(sentiment_score), 3)                      AS avg_sentiment,
    SUM(CASE WHEN sentiment = 'positive' THEN 1 ELSE 0 END) AS positive,
    SUM(CASE WHEN sentiment = 'negative' THEN 1 ELSE 0 END) AS negative,
    SUM(CASE WHEN sentiment = 'neutral'  THEN 1 ELSE 0 END) AS neutral
FROM analysed_reviews
GROUP BY month, business_name
ORDER BY month;


-- ─────────────────────────────────────────────
-- TABLE 4: Top complaint topics (bar chart)
-- ─────────────────────────────────────────────
SELECT
    top_complaint                                       AS topic,
    COUNT(*)                                            AS mentions,
    ROUND(AVG(rating), 2)                               AS avg_rating_when_complained,
    ROUND(AVG(sentiment_score), 3)                      AS avg_sentiment
FROM analysed_reviews
WHERE top_complaint IS NOT NULL
  AND top_complaint != ''
  AND sentiment IN ('negative', 'mixed')
GROUP BY top_complaint
ORDER BY mentions DESC
LIMIT 10;


-- ─────────────────────────────────────────────
-- TABLE 5: Top praise topics (bar chart)
-- ─────────────────────────────────────────────
SELECT
    top_praise                                          AS topic,
    COUNT(*)                                            AS mentions,
    ROUND(AVG(rating), 2)                               AS avg_rating_when_praised
FROM analysed_reviews
WHERE top_praise IS NOT NULL
  AND top_praise != ''
  AND sentiment IN ('positive', 'mixed')
GROUP BY top_praise
ORDER BY mentions DESC
LIMIT 10;


-- ─────────────────────────────────────────────
-- TABLE 6: Sentiment donut chart data
-- ─────────────────────────────────────────────
SELECT
    sentiment,
    COUNT(*)                                            AS count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS percentage
FROM analysed_reviews
GROUP BY sentiment
ORDER BY count DESC;


-- ─────────────────────────────────────────────
-- TABLE 7: Star rating distribution (histogram)
-- ─────────────────────────────────────────────
SELECT
    rating                                              AS stars,
    COUNT(*)                                            AS count,
    ROUND(AVG(sentiment_score), 3)                      AS avg_sentiment_score
FROM analysed_reviews
GROUP BY rating
ORDER BY rating DESC;


-- ─────────────────────────────────────────────
-- TABLE 8: Tag frequency (word-cloud source)
-- Split comma-separated tags into individual rows
-- ─────────────────────────────────────────────
WITH RECURSIVE split(tag, rest, review_id, sentiment) AS (
    SELECT
        TRIM(SUBSTR(tags, 1, CASE WHEN INSTR(tags,',')>0
                                  THEN INSTR(tags,',')-1
                                  ELSE LENGTH(tags) END)),
        CASE WHEN INSTR(tags,',')>0
             THEN SUBSTR(tags, INSTR(tags,',')+1)
             ELSE '' END,
        review_id,
        sentiment
    FROM analysed_reviews
    WHERE tags IS NOT NULL AND tags != ''

    UNION ALL

    SELECT
        TRIM(SUBSTR(rest, 1, CASE WHEN INSTR(rest,',')>0
                                  THEN INSTR(rest,',')-1
                                  ELSE LENGTH(rest) END)),
        CASE WHEN INSTR(rest,',')>0
             THEN SUBSTR(rest, INSTR(rest,',')+1)
             ELSE '' END,
        review_id,
        sentiment
    FROM split
    WHERE rest != ''
)
SELECT
    tag,
    COUNT(*)                                                    AS frequency,
    SUM(CASE WHEN sentiment='positive' THEN 1 ELSE 0 END)       AS positive_mentions,
    SUM(CASE WHEN sentiment='negative' THEN 1 ELSE 0 END)       AS negative_mentions
FROM split
WHERE tag != ''
GROUP BY tag
ORDER BY frequency DESC;


-- ─────────────────────────────────────────────
-- TABLE 9: Executive Summary (card visual)
-- ─────────────────────────────────────────────
SELECT
    business_name,
    report_date,
    total_reviews,
    avg_rating,
    pct_positive,
    pct_negative,
    top_praise_themes,
    top_complaint_themes,
    overall_sentiment
FROM business_summary
ORDER BY created_at DESC
LIMIT 1;
