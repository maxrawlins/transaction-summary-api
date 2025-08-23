import io
import csv
import os
from fastapi.testclient import TestClient
from app.main import app, DB_PATH

client = TestClient(app)




# helpers

def setup_module(module):
    # reset the DB before tests run
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)

def make_csv(rows):
    # build a CSV string from a list of dict rows
    out = io.StringIO()
    writer = csv.DictWriter(
        out,
        fieldnames=["transaction_id","user_id","product_id","timestamp","transaction_amount"]
    )
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    out.seek(0)
    return out




# tests

def test_upload_rejects_non_csv():
    # uploading a .txt file should be rejected with 400
    r = client.post("/upload", files={"file": ("bad.txt", b"oops", "text/plain")})
    assert r.status_code == 400
    assert "Only CSV files" in r.text


def test_upload_rejects_missing_columns():
    # uploading a CSV without all required columns should be rejected with 400
    bad_csv = io.StringIO()
    writer = csv.DictWriter(bad_csv, fieldnames=["wrong","columns"])
    writer.writeheader()
    writer.writerow({"wrong": "x", "columns": "y"})
    bad_csv.seek(0)

    r = client.post("/upload", files={"file": ("bad.csv", bad_csv.read().encode(), "text/csv")})
    assert r.status_code == 400
    assert "Missing columns" in r.text


def test_upload_and_summary_basic():
    # upload a valid CSV with two users, then query summaries
    rows = [
        {"transaction_id":"t1","user_id":1,"product_id":10,"timestamp":"2024-01-01 10:00:00","transaction_amount":100.0},
        {"transaction_id":"t2","user_id":1,"product_id":11,"timestamp":"2024-01-02 10:00:00","transaction_amount":50.5},
        {"transaction_id":"t3","user_id":2,"product_id":12,"timestamp":"2024-01-03 10:00:00","transaction_amount":75.0},
    ]
    data = make_csv(rows).read().encode()
    r = client.post("/upload", files={"file": ("sample.csv", data, "text/csv")})
    assert r.status_code == 200
    assert r.json()["rows_inserted"] == 3

    # query user 1 summary (should have 2 transactions)
    r2 = client.get("/summary/1")
    assert r2.status_code == 200
    body = r2.json()
    assert body["count"] == 2
    assert body["min"] == 50.5
    assert body["max"] == 100.0
    assert round(body["mean"],2) == 75.25

    # query user 2 summary (should have 1 transaction)
    r3 = client.get("/summary/2")
    assert r3.status_code == 200
    body = r3.json()
    assert body["count"] == 1
    assert body["min"] == body["max"] == body["mean"] == 75.0


def test_summary_date_filtering():
    # user 1 had two transactions, one on 2024-01-01 and one on 2024-01-02
    # filtering to only 2024-01-02 should return the single smaller transaction
    r = client.get("/summary/1?start=2024-01-02&end=2024-01-02")
    assert r.status_code == 200
    body = r.json()
    assert body["count"] == 1
    assert body["min"] == body["max"] == body["mean"] == 50.5


def test_summary_invalid_date_range():
    # start date after end date should return 400
    r = client.get("/summary/1?start=2024-02-01&end=2024-01-01")
    assert r.status_code == 400
    assert "end date must be on/after start date" in r.text


def test_summary_no_data():
    # querying a user_id with no transactions should return 404
    r = client.get("/summary/999")
    assert r.status_code == 404
    assert "No transactions found" in r.text