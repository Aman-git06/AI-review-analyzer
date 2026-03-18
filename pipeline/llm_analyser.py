"""
pipeline/llm_analyser.py
========================
Sends reviews to the Claude API in batches.
Returns structured JSON for each review:
  sentiment, score, summary, topic, praise, complaint, tags, recommend.

Uses batch processing to minimise API calls — sends 5 reviews per call.
"""

import os
import json
import time
import anthropic
from dotenv import load_dotenv

load_dotenv()

client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

MODEL   = "claude-sonnet-4-20250514"
BATCH_SIZE = 5   # reviews per API call — sweet spot for cost vs context


# ─────────────────────────────────────────────
#  PROMPT
# ─────────────────────────────────────────────

SYSTEM_PROMPT = """You are a business intelligence analyst specialising in customer feedback analysis.
Your job is to extract structured insights from customer reviews.
Always respond with ONLY valid JSON — no markdown fences, no explanation, just the JSON array."""


def build_user_prompt(reviews_batch: list[dict]) -> str:
    """
    Build a prompt that asks Claude to analyse multiple reviews at once.
    Returns a JSON array — one object per review.
    """
    reviews_text = ""
    for i, r in enumerate(reviews_batch):
        reviews_text += f"""
Review {i+1}:
  ID: {r['review_id']}
  Star rating: {r['rating']}/5
  Date: {r.get('review_date') or r.get('date', 'unknown')}
  Text: "{r['text']}"
"""

    return f"""Analyse the following {len(reviews_batch)} customer review(s) and return a JSON array.

{reviews_text}

Return ONLY a JSON array with exactly {len(reviews_batch)} objects. Each object must have these keys:

{{
  "review_id": "<same id as input>",
  "sentiment": "<one of: positive | negative | neutral | mixed>",
  "sentiment_score": <float from -1.0 (very negative) to 1.0 (very positive)>,
  "one_line_summary": "<max 15 words capturing the customer's main point>",
  "main_topic": "<the single most important topic, e.g. food quality | wait time | staff attitude | cleanliness | value | app/online | atmosphere>",
  "top_praise": "<the single best thing mentioned, or empty string if none>",
  "top_complaint": "<the single worst thing mentioned, or empty string if none>",
  "tags": "<comma-separated relevant tags from: food,service,speed,cleanliness,value,staff,app,parking,atmosphere,quality>",
  "recommend": <1 if customer would recommend the business, 0 if not, based on tone>
}}

Rules:
- sentiment_score must reflect both the text tone AND the star rating together
- main_topic must be one of the exact categories listed above
- tags must only use tags from the provided list
- Return only the JSON array, nothing else"""


# ─────────────────────────────────────────────
#  CORE ANALYSIS FUNCTION
# ─────────────────────────────────────────────

def analyse_batch(reviews_batch: list[dict],
                  retries: int = 3) -> list[dict]:
    """
    Send a batch of reviews to Claude and parse the JSON response.
    Retries up to `retries` times on failure.
    """
    prompt = build_user_prompt(reviews_batch)

    for attempt in range(retries):
        try:
            response = client.messages.create(
                model=MODEL,
                max_tokens=1500,
                system=SYSTEM_PROMPT,
                messages=[{"role": "user", "content": prompt}],
            )

            raw_text = response.content[0].text.strip()

            # Defensive: strip accidental markdown fences
            if raw_text.startswith("```"):
                raw_text = raw_text.split("```")[1]
                if raw_text.startswith("json"):
                    raw_text = raw_text[4:]

            results = json.loads(raw_text)

            # Validate we got back the right number of results
            if len(results) != len(reviews_batch):
                raise ValueError(
                    f"Expected {len(reviews_batch)} results, got {len(results)}"
                )

            return results

        except (json.JSONDecodeError, ValueError, KeyError) as e:
            print(f"[llm] Parse error on attempt {attempt+1}: {e}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)  # exponential backoff

        except anthropic.RateLimitError:
            wait = 30 * (attempt + 1)
            print(f"[llm] Rate limited — waiting {wait}s before retry...")
            time.sleep(wait)

        except anthropic.APIError as e:
            print(f"[llm] API error: {e}")
            if attempt < retries - 1:
                time.sleep(5)

    # Return empty stubs on complete failure so pipeline continues
    print(f"[llm] Failed after {retries} attempts — returning empty stubs")
    return [_empty_stub(r) for r in reviews_batch]


def _empty_stub(review: dict) -> dict:
    return {
        "review_id":        review["review_id"],
        "sentiment":        "neutral",
        "sentiment_score":  0.0,
        "one_line_summary": "Analysis failed",
        "main_topic":       "unknown",
        "top_praise":       "",
        "top_complaint":    "",
        "tags":             "",
        "recommend":        0,
    }


# ─────────────────────────────────────────────
#  BATCH ORCHESTRATOR
# ─────────────────────────────────────────────

def analyse_all_reviews(reviews: list[dict],
                        delay_between_batches: float = 1.0) -> list[dict]:
    """
    Analyse all reviews in batches of BATCH_SIZE.
    Returns flat list of analysis dicts ready to be inserted into SQLite.
    """
    all_results = []
    total_batches = (len(reviews) + BATCH_SIZE - 1) // BATCH_SIZE

    print(f"[llm] Analysing {len(reviews)} reviews in {total_batches} batches of {BATCH_SIZE}...")

    for i in range(0, len(reviews), BATCH_SIZE):
        batch     = reviews[i : i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        print(f"[llm] Batch {batch_num}/{total_batches} ({len(batch)} reviews)...", end=" ")

        results = analyse_batch(batch)

        # Enrich with fields needed for DB insert
        for raw, result in zip(batch, results):
            result["business_name"] = raw["business_name"]
            result["rating"]        = raw["rating"]
            result["review_date"]   = raw.get("review_date") or raw.get("date", "")

        all_results.extend(results)
        print(f"done. Sentiments: {[r['sentiment'] for r in results]}")

        # Be polite to the API
        if i + BATCH_SIZE < len(reviews):
            time.sleep(delay_between_batches)

    print(f"[llm] Analysis complete — {len(all_results)} reviews processed")
    return all_results


# ─────────────────────────────────────────────
#  BUSINESS SUMMARY  (single Claude call)
# ─────────────────────────────────────────────

def generate_business_summary(business_name: str,
                               analysed_reviews: list[dict]) -> dict:
    """
    Ask Claude to synthesise all analysis into a concise executive summary.
    """
    stats = {
        "total":    len(analysed_reviews),
        "avg_rating": round(
            sum(r["rating"] for r in analysed_reviews) / len(analysed_reviews), 2
        ) if analysed_reviews else 0,
        "positive": sum(1 for r in analysed_reviews if r["sentiment"] == "positive"),
        "negative": sum(1 for r in analysed_reviews if r["sentiment"] == "negative"),
        "neutral":  sum(1 for r in analysed_reviews if r["sentiment"] == "neutral"),
    }

    summaries = [r["one_line_summary"] for r in analysed_reviews if r.get("one_line_summary")]
    praises   = [r["top_praise"]    for r in analysed_reviews if r.get("top_praise")]
    complaints= [r["top_complaint"] for r in analysed_reviews if r.get("top_complaint")]

    prompt = f"""You are summarising customer review analytics for '{business_name}'.

Stats:
- Total reviews: {stats['total']}
- Average star rating: {stats['avg_rating']}/5
- Positive: {stats['positive']} | Negative: {stats['negative']} | Neutral: {stats['neutral']}

Top praises mentioned: {', '.join(praises[:20])}
Top complaints mentioned: {', '.join(complaints[:20])}
Sample summaries: {'; '.join(summaries[:10])}

Return ONLY a JSON object (no markdown) with:
{{
  "overall_sentiment": "<positive|negative|neutral|mixed>",
  "top_praise_themes": "<top 3 praise themes as comma-separated string>",
  "top_complaint_themes": "<top 3 complaint themes as comma-separated string>",
  "executive_summary": "<2-3 sentence plain English summary for a business owner>"
}}"""

    try:
        response = client.messages.create(
            model=MODEL,
            max_tokens=500,
            messages=[{"role": "user", "content": prompt}],
        )
        result = json.loads(response.content[0].text.strip())
    except Exception as e:
        print(f"[llm] Summary generation failed: {e}")
        result = {
            "overall_sentiment":     "mixed",
            "top_praise_themes":     "",
            "top_complaint_themes":  "",
            "executive_summary":     "Summary unavailable.",
        }

    result.update({
        "business_name":  business_name,
        "report_date":    __import__("datetime").date.today().isoformat(),
        "total_reviews":  stats["total"],
        "avg_rating":     stats["avg_rating"],
        "pct_positive":   round(stats["positive"] / stats["total"] * 100, 1) if stats["total"] else 0,
        "pct_negative":   round(stats["negative"] / stats["total"] * 100, 1) if stats["total"] else 0,
        "pct_neutral":    round(stats["neutral"]  / stats["total"] * 100, 1) if stats["total"] else 0,
    })
    return result


# ─────────────────────────────────────────────
#  QUICK TEST  (single review)
# ─────────────────────────────────────────────
if __name__ == "__main__":
    test_review = [{
        "review_id":    "test_001",
        "business_name":"Test Cafe",
        "rating":       2,
        "review_date":  "2024-11-15",
        "text": "The coffee was cold and the staff were rude when I asked for it to be remade. Won't be back.",
    }]
    results = analyse_batch(test_review)
    print(json.dumps(results, indent=2))
