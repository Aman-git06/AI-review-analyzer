"""
scraper/scrape_reviews.py
=========================
Fetches business reviews from Google Places API.
Falls back to realistic mock data if no API key is provided —
perfect for testing and GitHub demos.
"""

import os
import json
import random
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

GOOGLE_API_KEY = os.getenv("GOOGLE_PLACES_API_KEY", "")


# ─────────────────────────────────────────────
#  MOCK DATA GENERATOR  (no API key needed)
# ─────────────────────────────────────────────

MOCK_REVIEWS = {
    "positive": [
        "The staff were incredibly friendly and the food came out hot and fresh. Will definitely be back!",
        "Absolutely loved the atmosphere. The manager even came over to check on us — rare these days.",
        "Best coffee I've had in months. The barista remembered my order the second time I visited.",
        "Clean, fast, and the portion sizes are generous. Great value for money.",
        "Parking was easy and the service was lightning fast. Very impressed.",
        "The new menu items are fantastic. The seasonal special was a highlight.",
        "Ordered online and the food was ready exactly when promised. Seamless experience.",
        "Kids loved it and the staff were patient with our chaos. Family-friendly at its best.",
    ],
    "negative": [
        "Waited 25 minutes for a simple order. The place was barely half full — no excuse for that.",
        "The toilets were not clean at all. Puts you off your food honestly.",
        "Got my order completely wrong twice in a row. Staff didn't seem too bothered about fixing it.",
        "Way too loud inside. Couldn't hear my friend sitting across from me.",
        "Prices have gone up significantly but the portion sizes have shrunk. Not happy.",
        "The app kept crashing when I tried to order. Eventually gave up and went somewhere else.",
        "Cold chips. I asked for them to be replaced and was told that's just how they are. Unbelievable.",
        "Tables were sticky and hadn't been wiped down. First impression matters.",
    ],
    "mixed": [
        "Food was great but the wait was too long. Would come back at a quieter time.",
        "Service varies wildly depending on who's working. Hit or miss.",
        "Love the product but the loyalty app needs serious work — keeps logging me out.",
        "The new location looks amazing but the quality isn't quite as good as the old one.",
        "Decent enough, not outstanding. Does the job when you're in a hurry.",
        "The manager sorted out our complaint really well but it shouldn't have happened in the first place.",
    ]
}

BUSINESSES = {
    "cafe":       ["Coffee Haven", "The Daily Grind", "Brew & Co"],
    "restaurant": ["The Local Kitchen", "Spice Route", "Garden Bistro"],
    "retail":     ["StyleHub", "Tech Corner", "HomeGoods Plus"],
}


def generate_mock_reviews(business_name: str, count: int = 50) -> list[dict]:
    """Generate realistic mock reviews for demo / testing purposes."""
    reviews = []
    base_date = datetime.now()

    all_texts = (
        MOCK_REVIEWS["positive"] * 5 +
        MOCK_REVIEWS["negative"] * 3 +
        MOCK_REVIEWS["mixed"]   * 2
    )
    random.shuffle(all_texts)

    star_weights = {5: 0.35, 4: 0.25, 3: 0.15, 2: 0.13, 1: 0.12}
    stars_pool   = [s for s, w in star_weights.items()
                    for _ in range(int(w * 100))]

    for i in range(count):
        days_ago = random.randint(0, 365)
        review_date = base_date - timedelta(days=days_ago)
        stars = random.choice(stars_pool)

        reviews.append({
            "review_id":     f"mock_{i+1:04d}",
            "business_name": business_name,
            "author":        f"User_{random.randint(1000, 9999)}",
            "rating":        stars,
            "text":          random.choice(all_texts),
            "date":          review_date.strftime("%Y-%m-%d"),
            "source":        "mock_data",
        })

    print(f"[scraper] Generated {count} mock reviews for '{business_name}'")
    return reviews


# ─────────────────────────────────────────────
#  GOOGLE PLACES API  (real data)
# ─────────────────────────────────────────────

def search_place_id(business_name: str) -> str | None:
    """Find a Google Place ID for the given business name."""
    url = "https://maps.googleapis.com/maps/api/place/findplacefromtext/json"
    params = {
        "input":      business_name,
        "inputtype":  "textquery",
        "fields":     "place_id,name",
        "key":        GOOGLE_API_KEY,
    }
    resp = requests.get(url, params=params, timeout=10)
    data = resp.json()
    candidates = data.get("candidates", [])
    if candidates:
        place_id = candidates[0]["place_id"]
        print(f"[scraper] Found place_id: {place_id}")
        return place_id
    print("[scraper] No place found. Check the business name.")
    return None


def fetch_google_reviews(place_id: str, business_name: str) -> list[dict]:
    """
    Fetch reviews from Google Places Details API.
    Note: free tier returns max 5 reviews per call.
    For 50+ reviews you need the Places API (new) with a paid plan.
    """
    url = "https://maps.googleapis.com/maps/api/place/details/json"
    params = {
        "place_id": place_id,
        "fields":   "name,rating,reviews",
        "key":      GOOGLE_API_KEY,
    }
    resp = requests.get(url, params=params, timeout=10)
    data = resp.json().get("result", {})
    raw  = data.get("reviews", [])

    reviews = []
    for i, r in enumerate(raw):
        reviews.append({
            "review_id":     f"google_{place_id[:8]}_{i}",
            "business_name": business_name,
            "author":        r.get("author_name", "Anonymous"),
            "rating":        r.get("rating", 0),
            "text":          r.get("text", ""),
            "date":          datetime.fromtimestamp(
                                 r.get("time", 0)
                             ).strftime("%Y-%m-%d"),
            "source":        "google_places",
        })

    print(f"[scraper] Fetched {len(reviews)} Google reviews")
    return reviews


# ─────────────────────────────────────────────
#  PUBLIC INTERFACE
# ─────────────────────────────────────────────

def scrape_reviews(business_name: str, count: int = 50) -> list[dict]:
    """
    Main entry point.
    Uses Google Places if API key is set, otherwise generates mock data.
    """
    if GOOGLE_API_KEY:
        place_id = search_place_id(business_name)
        if place_id:
            reviews = fetch_google_reviews(place_id, business_name)
            if reviews:
                return reviews
        print("[scraper] Falling back to mock data...")

    return generate_mock_reviews(business_name, count)


def save_raw(reviews: list[dict], path: str = "data/reviews_raw.json") -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(reviews, f, indent=2)
    print(f"[scraper] Saved {len(reviews)} raw reviews → {path}")


def load_raw(path: str = "data/reviews_raw.json") -> list[dict]:
    with open(path) as f:
        return json.load(f)


# ─────────────────────────────────────────────
#  QUICK TEST
# ─────────────────────────────────────────────
if __name__ == "__main__":
    reviews = scrape_reviews("Starbucks Oxford Street", count=20)
    save_raw(reviews, "data/reviews_raw.json")
    print(f"\nSample review:\n{json.dumps(reviews[0], indent=2)}")
