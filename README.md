# AI Business Review Analyser
### End-to-end pipeline: Scrape → LLM Analysis → SQLite → Power BI Dashboard

---

## Project Structure

```
ai_review_analyser/
│
├── scraper/
│   └── scrape_reviews.py        # Scrapes reviews from Google Places API or mock data
│
├── pipeline/
│   ├── llm_analyser.py          # Sends reviews to Claude API, gets structured JSON back
│   ├── db_manager.py            # SQLite schema creation & insert/query helpers
│   └── run_pipeline.py          # Orchestrator: scrape → analyse → store
│
├── data/
│   ├── reviews_raw.json         # Raw scraped reviews
│   └── reviews_analysed.db      # SQLite database (auto-created)
│
├── notebooks/
│   └── explore.ipynb            # EDA + charts for GitHub showcase
│
├── dashboard/
│   └── powerbi_queries.sql      # Ready-to-use SQL queries for Power BI import
│
├── requirements.txt
└── README.md
```

## Stack
| Layer | Tool |
|---|---|
| Scraping | Python + `requests` + Google Places API (or mock data) |
| LLM Analysis | Claude API (`claude-sonnet-4-6`) |
| Storage | SQLite via `sqlite3` (built-in Python) |
| EDA | `pandas`, `matplotlib`, `seaborn` |
| Dashboard | Power BI (connects to SQLite via ODBC) |

## Setup

```bash
pip install -r requirements.txt
```

Add your API keys to a `.env` file:
```
ANTHROPIC_API_KEY=sk-ant-...
GOOGLE_PLACES_API_KEY=...   # optional - mock data works without this
```

## Run

```bash
# Full pipeline (scrape → analyse → store)
python pipeline/run_pipeline.py --business "Starbucks London" --reviews 50

# Analyse only (if you already have raw JSON)
python pipeline/run_pipeline.py --input data/reviews_raw.json

# Export SQL report
python pipeline/run_pipeline.py --report
```
