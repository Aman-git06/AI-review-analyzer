"""
pipeline/run_pipeline.py
========================
Orchestrator — ties scraper + LLM analyser + DB together.

Usage:
  python pipeline/run_pipeline.py --business "Starbucks London" --reviews 50
  python pipeline/run_pipeline.py --input data/reviews_raw.json
  python pipeline/run_pipeline.py --report
"""

import argparse
import json
import os
import sys

# Make sure imports work from project root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scraper.scrape_reviews  import scrape_reviews, save_raw, load_raw
from pipeline.db_manager     import (
    init_db, insert_raw_reviews, insert_analysed_review,
    upsert_business_summary, get_unanalysed_reviews,
    get_sentiment_breakdown, get_top_themes, get_monthly_sentiment_trend
)
from pipeline.llm_analyser   import analyse_all_reviews, generate_business_summary


# ─────────────────────────────────────────────
#  STEP 1 — SCRAPE
# ─────────────────────────────────────────────

def step_scrape(business_name: str, count: int) -> list[dict]:
    print(f"\n{'='*55}")
    print(f"  STEP 1 — Scraping reviews for: {business_name}")
    print(f"{'='*55}")
    reviews = scrape_reviews(business_name, count)
    save_raw(reviews)
    return reviews


# ─────────────────────────────────────────────
#  STEP 2 — STORE RAW
# ─────────────────────────────────────────────

def step_store_raw(reviews: list[dict]) -> None:
    print(f"\n{'='*55}")
    print(f"  STEP 2 — Storing raw reviews in SQLite")
    print(f"{'='*55}")
    init_db()
    insert_raw_reviews(reviews)


# ─────────────────────────────────────────────
#  STEP 3 — LLM ANALYSIS
# ─────────────────────────────────────────────

def step_analyse() -> list[dict]:
    print(f"\n{'='*55}")
    print(f"  STEP 3 — LLM Analysis via Claude API")
    print(f"{'='*55}")

    pending = get_unanalysed_reviews()
    if not pending:
        print("[pipeline] No new reviews to analyse.")
        return []

    print(f"[pipeline] {len(pending)} reviews queued for analysis...")
    analysed = analyse_all_reviews(pending)

    # Store each result
    for result in analysed:
        insert_analysed_review(result)

    print(f"[pipeline] Stored {len(analysed)} analysis results")
    return analysed


# ─────────────────────────────────────────────
#  STEP 4 — BUSINESS SUMMARY
# ─────────────────────────────────────────────

def step_summarise(business_name: str, analysed: list[dict]) -> None:
    print(f"\n{'='*55}")
    print(f"  STEP 4 — Generating Business Summary")
    print(f"{'='*55}")

    if not analysed:
        print("[pipeline] No data to summarise.")
        return

    summary = generate_business_summary(business_name, analysed)
    upsert_business_summary(summary)

    print(f"\n  Business: {summary['business_name']}")
    print(f"  Overall sentiment:  {summary['overall_sentiment'].upper()}")
    print(f"  Avg rating:         {summary['avg_rating']} / 5.0")
    print(f"  Positive:           {summary['pct_positive']}%")
    print(f"  Negative:           {summary['pct_negative']}%")
    print(f"  Top praises:        {summary['top_praise_themes']}")
    print(f"  Top complaints:     {summary['top_complaint_themes']}")
    if summary.get("executive_summary"):
        print(f"\n  Executive summary:\n  {summary['executive_summary']}")


# ─────────────────────────────────────────────
#  STEP 5 — CONSOLE REPORT
# ─────────────────────────────────────────────

def step_report(business_name: str = None) -> None:
    print(f"\n{'='*55}")
    print(f"  REPORT — Sentiment Breakdown")
    print(f"{'='*55}")

    breakdown = get_sentiment_breakdown(business_name)
    if not breakdown:
        print("  No analysed data found. Run the pipeline first.")
        return

    for row in breakdown:
        bar = "█" * int(row["count"] / max(b["count"] for b in breakdown) * 20)
        print(f"  {row['sentiment']:<10} {row['count']:>4} reviews  {bar}  avg⭐{row['avg_star_rating']}")

    print(f"\n  Top complaint topics:")
    for t in get_top_themes("negative", limit=5):
        print(f"    • {t['theme']:<25} {t['mentions']} mentions")

    print(f"\n  Top praise topics:")
    for t in get_top_themes("positive", limit=5):
        print(f"    + {t['theme']:<25} {t['mentions']} mentions")

    print(f"\n  Monthly sentiment trend:")
    for m in get_monthly_sentiment_trend(business_name):
        trend_icon = "↑" if m["avg_sentiment"] > 0.2 else ("↓" if m["avg_sentiment"] < -0.2 else "→")
        print(f"    {m['month']}  {trend_icon}  score={m['avg_sentiment']:+.2f}  reviews={m['total_reviews']}")


# ─────────────────────────────────────────────
#  CLI
# ─────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="AI Review Analyser — full pipeline"
    )
    parser.add_argument("--business", type=str, default="Demo Coffee Shop",
                        help="Business name to scrape and analyse")
    parser.add_argument("--reviews",  type=int, default=30,
                        help="Number of reviews to fetch (default: 30)")
    parser.add_argument("--input",    type=str, default=None,
                        help="Path to existing raw JSON file (skips scraping)")
    parser.add_argument("--report",   action="store_true",
                        help="Print report from existing DB and exit")
    parser.add_argument("--skip-scrape", action="store_true",
                        help="Skip scraping and only analyse existing unanalysed reviews")
    args = parser.parse_args()

    # Report-only mode
    if args.report:
        step_report(args.business if args.business != "Demo Coffee Shop" else None)
        return

    # Load from file OR scrape
    if args.input:
        print(f"[pipeline] Loading reviews from {args.input}")
        reviews = load_raw(args.input)
        args.business = reviews[0]["business_name"] if reviews else args.business
    elif not args.skip_scrape:
        reviews = step_scrape(args.business, args.reviews)
    else:
        reviews = []

    # Store raw in DB
    if reviews:
        step_store_raw(reviews)

    # LLM analysis
    analysed = step_analyse()

    # Business summary
    if analysed:
        step_summarise(args.business, analysed)

    # Final report
    step_report(args.business)

    print(f"\n[pipeline] All done! Database saved to: data/reviews_analysed.db")
    print(f"[pipeline] Connect Power BI to this file for your dashboard.\n")


if __name__ == "__main__":
    main()
