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
    
def test_upload_header_only_inserts_zero_rows():
    # CSV with only the header (no data rows) should succeed with 0 inserts
    out = io.StringIO()
    writer = csv.DictWriter(
        out,
        fieldnames=["transaction_id","user_id","product_id","timestamp","transaction_amount"]
    )
    writer.writeheader()
    out.seek(0)
    r = client.post("/upload", files={"file": ("empty.csv", out.read().encode(), "text/csv")})
    assert r.status_code == 200
    assert r.json()["rows_inserted"] == 0


def test_upload_rejects_invalid_user_id_type():
    # Non-integer user_id should cause DuckDB CAST() to fail -> 400
    rows = [
        {"transaction_id":"bad_u1","user_id":"not-an-int","product_id":1,"timestamp":"2024-05-01 12:00:00","transaction_amount":10.0},
    ]
    data = make_csv(rows).read().encode()
    r = client.post("/upload", files={"file": ("bad_user_id.csv", data, "text/csv")})
    assert r.status_code == 400


def test_upload_rejects_invalid_amount_type():
    # Non-numeric transaction_amount should fail -> 400
    rows = [
        {"transaction_id":"bad_amt1","user_id":50,"product_id":1,"timestamp":"2024-05-02 12:00:00","transaction_amount":"ten"},
    ]
    data = make_csv(rows).read().encode()
    r = client.post("/upload", files={"file": ("bad_amount.csv", data, "text/csv")})
    assert r.status_code == 400


def test_upload_accepts_extra_columns_and_casts_strings():
    # Extra, irrelevant columns should be ignored; numeric strings should CAST correctly
    out = io.StringIO()
    writer = csv.DictWriter(
        out,
        fieldnames=["transaction_id","user_id","product_id","timestamp","transaction_amount","note"]
    )
    writer.writeheader()
    writer.writerow({"transaction_id":"ex1","user_id":"60","product_id":"7",
                    "timestamp":"2024-06-01 08:00:00","transaction_amount":" 42.0 ",
                    "note":"hello"})
    writer.writerow({"transaction_id":"ex2","user_id":"60","product_id":"7",
                    "timestamp":"2024-06-02 09:00:00","transaction_amount":"58.0",
                    "note":"world"})
    out.seek(0)

    r = client.post("/upload", files={"file": ("extra_cols.csv", out.read().encode(), "text/csv")})
    assert r.status_code == 200
    assert r.json()["rows_inserted"] == 2

    r2 = client.get("/summary/60")
    assert r2.status_code == 200
    body = r2.json()
    assert body["count"] == 2
    assert body["min"] == 42.0
    assert body["max"] == 58.0
    assert round(body["mean"], 2) == 50.0


def test_summary_start_only_filters_from_mid_range():
    # user 70 has two rows, start date should include only the later one
    rows = [
        {"transaction_id":"s70a","user_id":70,"product_id":1,"timestamp":"2024-07-01 00:00:00","transaction_amount":10.0},
        {"transaction_id":"s70b","user_id":70,"product_id":1,"timestamp":"2024-07-10 00:00:00","transaction_amount":30.0},
    ]
    data = make_csv(rows).read().encode()
    r = client.post("/upload", files={"file": ("start_only_70.csv", data, "text/csv")})
    assert r.status_code == 200

    r2 = client.get("/summary/70?start=2024-07-05")
    assert r2.status_code == 200
    body = r2.json()
    assert body["count"] == 1
    assert body["min"] == body["max"] == body["mean"] == 30.0


def test_summary_end_only_is_inclusive_by_day():
    # end=YYYY-MM-DD should include all times on that day but exclude the next day
    rows = [
        {"transaction_id":"e80a","user_id":80,"product_id":1,"timestamp":"2024-08-01 23:59:59","transaction_amount":5.0},
        {"transaction_id":"e80b","user_id":80,"product_id":1,"timestamp":"2024-08-02 00:00:00","transaction_amount":50.0},
    ]
    data = make_csv(rows).read().encode()
    r = client.post("/upload", files={"file": ("end_only_80.csv", data, "text/csv")})
    assert r.status_code == 200

    r2 = client.get("/summary/80?end=2024-08-01")
    assert r2.status_code == 200
    body = r2.json()
    assert body["count"] == 1
    assert body["min"] == body["max"] == body["mean"] == 5.0


def test_summary_handles_null_amounts():
    # empty transaction_amount values should become NULL; count counts rows, stats become nulls if all are NULL
    out = io.StringIO()
    writer = csv.DictWriter(
        out,
        fieldnames=["transaction_id","user_id","product_id","timestamp","transaction_amount"]
    )
    writer.writeheader()
    writer.writerow({"transaction_id":"n100a","user_id":100,"product_id":1,"timestamp":"2024-10-01 12:00:00","transaction_amount":""})
    writer.writerow({"transaction_id":"n100b","user_id":100,"product_id":1,"timestamp":"2024-10-02 12:00:00","transaction_amount":""})
    out.seek(0)

    r = client.post("/upload", files={"file": ("null_amounts_100.csv", out.read().encode(), "text/csv")})
    assert r.status_code == 200
    assert r.json()["rows_inserted"] == 2

    r2 = client.get("/summary/100")
    assert r2.status_code == 200
    body = r2.json()
    # COUNT(*) counts 2 rows even if amounts are NULL
    assert body["count"] == 2
    # MIN/MAX/AVG over all-NULL column should be null/None
    assert body["min"] is None
    assert body["max"] is None
    assert body["mean"] is None
    
def test_summary_validation_errors_from_fastapi():
    assert client.get("/summary/not-an-int").status_code == 422
    assert client.get("/summary/1?start=2024-13-99&end=nope").status_code == 422