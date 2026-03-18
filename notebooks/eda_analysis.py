"""
notebooks/eda_analysis.py
=========================
Exploratory Data Analysis — generates 6 charts for your GitHub README.
Run after the pipeline has populated the database.

Charts saved to: notebooks/charts/
"""

import os
import sys
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import seaborn as sns

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

DB_PATH    = "data/reviews_analysed.db"
CHARTS_DIR = "notebooks/charts"
os.makedirs(CHARTS_DIR, exist_ok=True)


# ── Colour palette ──────────────────────────
PALETTE = {
    "positive": "#2E8B57",
    "negative": "#C0392B",
    "neutral":  "#7F8C8D",
    "mixed":    "#E67E22",
    "accent":   "#2C3E7A",
    "bg":       "#FAFAFA",
}

plt.rcParams.update({
    "figure.facecolor": PALETTE["bg"],
    "axes.facecolor":   PALETTE["bg"],
    "font.family":      "DejaVu Sans",
    "axes.spines.top":  False,
    "axes.spines.right":False,
    "axes.titlesize":   13,
    "axes.titleweight": "bold",
})


# ── Load data ───────────────────────────────
def load_data() -> pd.DataFrame:
    conn = sqlite3.connect(DB_PATH)
    df   = pd.read_sql("SELECT * FROM analysed_reviews", conn,
                       parse_dates=["review_date"])
    conn.close()
    print(f"Loaded {len(df)} analysed reviews")
    return df


# ── Chart 1: Sentiment Distribution (donut) ─
def chart_sentiment_donut(df: pd.DataFrame) -> None:
    counts = df["sentiment"].value_counts()
    colors = [PALETTE.get(s, "#BDC3C7") for s in counts.index]

    fig, ax = plt.subplots(figsize=(6, 5))
    wedges, texts, autotexts = ax.pie(
        counts, labels=counts.index, autopct="%1.1f%%",
        colors=colors, startangle=90,
        wedgeprops={"width": 0.55, "edgecolor": "white", "linewidth": 2},
    )
    for at in autotexts:
        at.set_fontsize(11)
        at.set_fontweight("bold")
    ax.set_title("Overall Sentiment Distribution", pad=20)
    plt.tight_layout()
    plt.savefig(f"{CHARTS_DIR}/01_sentiment_donut.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("  Chart 1 saved: sentiment donut")


# ── Chart 2: Monthly Sentiment Trend ────────
def chart_monthly_trend(df: pd.DataFrame) -> None:
    df["month"] = df["review_date"].dt.to_period("M").astype(str)
    monthly = (
        df.groupby("month")
          .agg(avg_sentiment=("sentiment_score", "mean"),
               avg_rating=("rating", "mean"),
               review_count=("review_id", "count"))
          .reset_index()
    )

    fig, ax1 = plt.subplots(figsize=(10, 4))
    ax2 = ax1.twinx()

    ax1.bar(monthly["month"], monthly["review_count"],
            color=PALETTE["accent"], alpha=0.25, label="Review count")
    ax2.plot(monthly["month"], monthly["avg_sentiment"],
             color=PALETTE["positive"], marker="o", linewidth=2.5, label="Avg sentiment")
    ax2.axhline(0, color="grey", linewidth=0.8, linestyle="--", alpha=0.5)

    ax1.set_ylabel("Review count", color=PALETTE["accent"])
    ax2.set_ylabel("Avg sentiment score", color=PALETTE["positive"])
    ax1.tick_params(axis="x", rotation=45)
    ax1.set_title("Monthly Review Volume & Sentiment Trend")

    lines = [
        mpatches.Patch(color=PALETTE["accent"], alpha=0.4, label="Review count"),
        plt.Line2D([0], [0], color=PALETTE["positive"], linewidth=2, label="Avg sentiment"),
    ]
    ax1.legend(handles=lines, loc="upper left", fontsize=9)
    plt.tight_layout()
    plt.savefig(f"{CHARTS_DIR}/02_monthly_trend.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("  Chart 2 saved: monthly trend")


# ── Chart 3: Star Rating Distribution ───────
def chart_star_distribution(df: pd.DataFrame) -> None:
    counts = df["rating"].value_counts().sort_index()
    colors = ["#C0392B","#E67E22","#F1C40F","#2ECC71","#27AE60"]

    fig, ax = plt.subplots(figsize=(7, 4))
    bars = ax.bar(counts.index.astype(str) + "★",
                  counts.values, color=colors, width=0.6,
                  edgecolor="white", linewidth=1.5)

    for bar, val in zip(bars, counts.values):
        ax.text(bar.get_x() + bar.get_width()/2,
                bar.get_height() + 0.5,
                str(val), ha="center", va="bottom",
                fontweight="bold", fontsize=11)

    ax.set_xlabel("Star Rating")
    ax.set_ylabel("Number of Reviews")
    ax.set_title("Star Rating Distribution")
    ax.set_ylim(0, counts.max() * 1.18)
    plt.tight_layout()
    plt.savefig(f"{CHARTS_DIR}/03_star_distribution.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("  Chart 3 saved: star distribution")


# ── Chart 4: Top Complaint Topics ───────────
def chart_top_complaints(df: pd.DataFrame) -> None:
    complaints = (
        df[df["top_complaint"].notna() & (df["top_complaint"] != "")]
        ["top_complaint"].value_counts().head(8)
    )

    fig, ax = plt.subplots(figsize=(8, 4))
    bars = ax.barh(complaints.index[::-1], complaints.values[::-1],
                   color=PALETTE["negative"], alpha=0.85, height=0.6)

    for bar, val in zip(bars, complaints.values[::-1]):
        ax.text(bar.get_width() + 0.2, bar.get_y() + bar.get_height()/2,
                str(val), va="center", fontsize=10, fontweight="bold")

    ax.set_xlabel("Number of Mentions")
    ax.set_title("Top Complaint Topics (from LLM Analysis)")
    ax.set_xlim(0, complaints.max() * 1.2)
    plt.tight_layout()
    plt.savefig(f"{CHARTS_DIR}/04_top_complaints.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("  Chart 4 saved: top complaints")


# ── Chart 5: Top Praise Topics ──────────────
def chart_top_praises(df: pd.DataFrame) -> None:
    praises = (
        df[df["top_praise"].notna() & (df["top_praise"] != "")]
        ["top_praise"].value_counts().head(8)
    )

    fig, ax = plt.subplots(figsize=(8, 4))
    ax.barh(praises.index[::-1], praises.values[::-1],
            color=PALETTE["positive"], alpha=0.85, height=0.6)

    for i, (bar, val) in enumerate(zip(ax.patches, praises.values[::-1])):
        ax.text(bar.get_width() + 0.2, bar.get_y() + bar.get_height()/2,
                str(val), va="center", fontsize=10, fontweight="bold")

    ax.set_xlabel("Number of Mentions")
    ax.set_title("Top Praise Topics (from LLM Analysis)")
    ax.set_xlim(0, praises.max() * 1.2)
    plt.tight_layout()
    plt.savefig(f"{CHARTS_DIR}/05_top_praises.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("  Chart 5 saved: top praises")


# ── Chart 6: Sentiment Score vs Star Rating ─
def chart_sentiment_vs_rating(df: pd.DataFrame) -> None:
    fig, ax = plt.subplots(figsize=(7, 4))
    color_map = {s: PALETTE.get(s, "grey") for s in df["sentiment"].unique()}

    for sentiment, group in df.groupby("sentiment"):
        ax.scatter(group["rating"], group["sentiment_score"],
                   alpha=0.45, s=40,
                   color=color_map[sentiment],
                   label=sentiment)

    ax.set_xlabel("Star Rating (1–5)")
    ax.set_ylabel("Claude Sentiment Score (–1 to +1)")
    ax.set_title("Star Rating vs LLM Sentiment Score")
    ax.axhline(0, color="grey", linewidth=0.8, linestyle="--", alpha=0.5)
    ax.legend(title="Sentiment", fontsize=9)
    plt.tight_layout()
    plt.savefig(f"{CHARTS_DIR}/06_sentiment_vs_rating.png",
                dpi=150, bbox_inches="tight")
    plt.close()
    print("  Chart 6 saved: sentiment vs rating scatter")


# ── Run all ─────────────────────────────────
if __name__ == "__main__":
    print("Running EDA — generating 6 charts...\n")
    df = load_data()

    if df.empty:
        print("No data in DB yet. Run the pipeline first:\n"
              "  python pipeline/run_pipeline.py --business 'YourBusiness' --reviews 50")
        sys.exit(0)

    chart_sentiment_donut(df)
    chart_monthly_trend(df)
    chart_star_distribution(df)
    chart_top_complaints(df)
    chart_top_praises(df)
    chart_sentiment_vs_rating(df)

    print(f"\nAll charts saved to: {CHARTS_DIR}/")
    print("Add them to your GitHub README for maximum impact!")
