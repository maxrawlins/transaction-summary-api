# Suade Graduate Challenge

This is a small API built with FastAPI and DuckDB.  
It ingests a CSV of transactions and returns per-user summary stats (count, min, max, mean) over an optional date range.

---

## Setup

### 1. Clone the repo
```bash
git clone <YOUR_REPO_URL>
cd "Suade graduate challenge"
```

### 2. Create and activate a virtual environment
```bash
python3 -m venv .venv
source .venv/bin/activate   # macOS/Linux
# .venv\Scripts\activate    # Windows
```

### 3. Install dependencies
```bash
pip install -r requirements.txt
```

---

## Running the server

```bash
uvicorn app.main:app --reload
```

- API root: http://127.0.0.1:8000  
- Docs: http://127.0.0.1:8000/docs  

---

## Endpoints

### POST /upload

Upload a CSV of transactions.  
The file must have these columns:

- transaction_id  
- user_id  
- product_id  
- timestamp (ISO datetime)  
- transaction_amount  

Example:
```bash
curl -X POST -F "file=@sample_transactions.csv" http://127.0.0.1:8000/upload
```

Response:
```json
{"status": "ok", "rows_inserted": 1000000}
```

---

### GET /summary/{user_id}

Get summary stats for a user’s transactions.  
Optional query params:  
- start (YYYY-MM-DD)  
- end (YYYY-MM-DD, inclusive)

Example:
```bash
curl "http://127.0.0.1:8000/summary/42?start=2024-01-01&end=2024-06-30"
```

Response:
```json
{
  "user_id": 42,
  "start_date": "2024-01-01",
  "end_date": "2024-06-30",
  "count": 32,
  "min": 15.79,
  "max": 496.53,
  "mean": 270.76
}
```

---

### GET /health

Simple health check:
```bash
curl http://127.0.0.1:8000/health
```

---

## Tests

Run the test suite:
```bash
python -m pytest -v
```

With coverage:
```bash
python -m pytest --cov=app --cov-report=term-missing
```

---

## Notes

- FastAPI for quick REST API development.  
- DuckDB for handling large CSV ingestion efficiently.  
- Error handling returns 400 for bad input, 404 if no data, 500 on server errors.  
- Tests cover both happy paths and edge cases.  

---

## Limitations / future improvements

- No authentication: all endpoints are public.  

---

## Example flow

```bash
# generate test data
python data_generator.py

# upload it
curl -X POST -F "file=@dummy_transactions.csv" http://127.0.0.1:8000/upload

# query a summary
curl "http://127.0.0.1:8000/summary/1?start=2024-01-01&end=2024-01-31"
```
