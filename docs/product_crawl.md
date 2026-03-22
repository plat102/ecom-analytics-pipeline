# Product Crawler

## Quick Usage Guide

### Prerequisites

```bash
poetry add playwright beautifulsoup4
poetry run playwright install chromium
```


### Step 1: Extract Product URLs

```bash
# rom GCS (in VM, if already processed & uploaded)
gcloud storage cp gs://raw_glamira/processed/product_url_map.csv data/exports/

# Or regenerate from MongoDB
poetry run python ingestion/product_crawler/extract_product_urls.py
```

### Step 2: Crawl Product Names

```bash
# Test with 50 products
head -51 data/exports/product_url_map.csv > data/exports/test.csv
poetry run python ingestion/product_crawler/crawl_products_parallel.py \
  --input data/exports/test.csv --workers 2

# Full run in tmux (~5-6 hours, 5 workers)
tmux new -s crawler
poetry run python ingestion/product_crawler/crawl_products_parallel.py --workers 5
# Detach: Ctrl+B D | Reattach: tmux attach -t crawler
```

### Resume After Crash

Re-run the same command — workers auto-resume from checkpoints.

### Output

| File | Description |
|---|---|
| `data/exports/product_names.csv` | Final merged results (~19K rows) |
| `data/exports/workers/worker_N_errors.jsonl` | Per-worker error log |

**Expected:** 85-90% success rate. Common errors: 404 (deleted products), 403 (blocked).
