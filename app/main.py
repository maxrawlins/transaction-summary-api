"""
Transactions Summary API

FastAPI service that:
1. Lets you upload CSV file of transactions, which then gets stored in a DuckDB database.
2. Lets you query per-user stats (count, min, max, mean) over a date range (which is optional).

Expected CSV columns: transaction_id, user_id, product_id, timestamp, transaction_amount
"""

from fastapi import FastAPI, UploadFile, File, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, field_validator
from typing import Optional
from datetime import datetime, date, timedelta
import duckdb
import os
import tempfile




# database setup

# location of the DuckDB database file
DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "transactions.duckdb")
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

def get_conn():
    """" Opens a DuckDB connection and ensures the transactions table exists. 
    If it doesnt yet exist it will create it"""
    conn = duckdb.connect(DB_PATH)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS transactions (
            transaction_id VARCHAR,
            user_id INTEGER,
            product_id INTEGER,
            timestamp TIMESTAMP,
            transaction_amount DOUBLE
        );
        """
    )
    return conn




# FastAPI app

app = FastAPI(
    title="Transactions Summary API",
    description="Upload CSV of transactions and query per-user summary stats over a date range.",
    version="1.0.0",
)




# response models

class SummaryResponse(BaseModel):
    """
    Response that's returned by the /summary endpoint

    Gives back basic stats for a user's transactions over an optional time window including: 
    how many transactions, min, max, and average amount
    """
    user_id: int
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    count: int
    min: Optional[float] = None
    max: Optional[float] = None
    mean: Optional[float] = None




# endpoints

@app.post("/upload")
async def upload_csv(file: UploadFile = File(...)):
    """
    Upload CSV file containing transactions and store in the DuckDB database

    The CSV file must contian the columns: transaction_id, user_id, product_id, timestamp, transaction_amount

    Returns:
        JSON with the status and the number of rows inserted into the DuckDB database

    Raises:
        - 400 if the file recieved isn’t a CSV or is missing any required columns
        - 500 if saving or loading the file goes wrong
    """
    if not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files are accepted.")

    # save upload to temp file
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
            tmp.write(await file.read())
            tmp_path = tmp.name
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to save uploaded file: {e}")

    try:
        conn = get_conn()

        # load CSV into temp view
        safe_path = tmp_path.replace("'", "''")
        conn.execute(
            f"""
            CREATE TEMP VIEW tmp_csv AS
            SELECT * FROM read_csv_auto('{safe_path}', HEADER=TRUE, SAMPLE_SIZE=-1);
            """
        )

        # check the CSV has all required columns before inserting
        required = {"transaction_id", "user_id", "product_id", "timestamp", "transaction_amount"}
        cols = {row[0] for row in conn.execute("DESCRIBE tmp_csv").fetchall()}
        if missing := (required - cols):
            raise HTTPException(status_code=400, detail=f"Missing columns: {', '.join(sorted(missing))}")

                # insert rows into transactions table with explicit type casting
        try:
            inserted = conn.execute(
                """
                INSERT INTO transactions
                SELECT
                    CAST(transaction_id AS VARCHAR),
                    CAST(user_id AS INTEGER),
                    CAST(product_id AS INTEGER),
                    CAST(timestamp AS TIMESTAMP),
                    CAST(transaction_amount AS DOUBLE)
                FROM tmp_csv
                RETURNING 1;
                """
            ).fetchall()
        except duckdb.ConversionException as e:
            # bad types in CSV (e.g., 'not-an-int' for user_id, 'ten' for amount)
            raise HTTPException(status_code=400, detail=f"Invalid data format: {e}")
        except duckdb.Error as e:
            # any other DuckDB ingestion error
            raise HTTPException(status_code=400, detail=f"Ingestion error: {e}")

        return JSONResponse({"status": "ok", "rows_inserted": len(inserted)})
    finally:
        # clean up temp file
        try:
            os.remove(tmp_path)
        except Exception:
            pass


@app.get("/summary/{user_id}", response_model=SummaryResponse)
def summary_user(
    user_id: int,
    start: Optional[date] = Query(default=None, description="Start date (inclusive, YYYY-MM-DD)"),
    end: Optional[date] = Query(default=None, description="End date (inclusive, YYYY-MM-DD)"),
):
    """
    Get the summary statistics for a user’s transactions: 
    number of transactions, min, max, and average transaction amounts

    You can optionally pass a start and/or end date to filter the range to a more specific time range
    
    If no transactions found it returns a 404
    """
    
    # reject invalid date ranges
    if start and end and end < start:
        raise HTTPException(status_code=400, detail="end date must be on/after start date.")

    conn = get_conn()

    # build WHERE clause dynamically based on filters
    conditions, params = ["user_id = ?"], [user_id]
    if start:
        conditions.append("timestamp >= ?")
        params.append(datetime.combine(start, datetime.min.time()))
    if end:
        end_next = datetime.combine(end, datetime.min.time()) + timedelta(days=1)
        conditions.append("timestamp < ?")
        params.append(end_next)

    query = f"""
        SELECT COUNT(*) AS count,
            MIN(transaction_amount) AS min,
            MAX(transaction_amount) AS max,
            AVG(transaction_amount) AS mean
        FROM transactions
        WHERE {' AND '.join(conditions)};
    """

    # run query, return 404 if no transactions found
    count, min_v, max_v, mean_v = conn.execute(query, params).fetchone()
    if count == 0:
        raise HTTPException(status_code=404, detail="No transactions found for the given criteria.")

    return SummaryResponse(
        user_id=user_id,
        start_date=start,
        end_date=end,
        count=count,
        min=min_v,
        max=max_v,
        mean=mean_v,
    )