from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from typing import Optional
from dateutil.parser import parse as parse_date
from datetime import datetime
import pandas as pd
import os

app = FastAPI(
    title="E-commerce Item Interactions API",
    description="Serves clickstream-like data from fact_item_interactions with pagination",
    version="1.1.0",
)

# ---------- CONFIG ----------
DATA_PATH = os.path.join("data", "fact_item_interactions.csv")
# ----------------------------

# Load data into memory at startup
df_interactions = pd.read_csv(DATA_PATH)
df_interactions["event_timestamp"] = pd.to_datetime(df_interactions["event_timestamp"])

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/interactions")
def get_interactions(
    from_date: Optional[str] = Query(
        None, description="Filter events from this date (YYYY-MM-DD or ISO datetime)"
    ),
    to_date: Optional[str] = Query(
        None, description="Filter events up to this date (YYYY-MM-DD or ISO datetime)"
    ),
    customer_id: Optional[int] = Query(
        None, description="Filter by customer_id"
    ),
    product_id: Optional[int] = Query(
        None, description="Filter by product_id"
    ),
    event_type: Optional[str] = Query(
        None, description="view, click, add_to_cart, review"
    ),
    page: int = Query(
        1, ge=1, description="Page number for pagination (1-based)"
    ),
    limit: int = Query(
        100, ge=1, le=1000, description="Number of records per page"
    )
):
    df = df_interactions

    # -------- Date Filters --------
    if from_date:
        try:
            dt_from = parse_date(from_date)
            df = df[df["event_timestamp"] >= dt_from]
        except Exception:
            return JSONResponse(
                status_code=400,
                content={"error": "Invalid from_date format."}
            )

    if to_date:
        try:
            dt_to = parse_date(to_date)
            df = df[df["event_timestamp"] <= dt_to]
        except Exception:
            return JSONResponse(
                status_code=400,
                content={"error": "Invalid to_date format."}
            )

    # -------- Other Filters --------
    if customer_id is not None:
        df = df[df["customer_id"] == customer_id]

    if product_id is not None:
        df = df[df["product_id"] == product_id]

    if event_type is not None:
        df = df[df["event_type"].str.lower() == event_type.lower()]

    # -------- Sort & Pagination --------
    df = df.sort_values("event_timestamp", ascending=False)

    total_records = len(df)
    total_pages = (total_records + limit - 1) // limit  # Ceiling division

    start = (page - 1) * limit
    end = start + limit

    page_df = df.iloc[start:end]

    # -------- Format & Return JSON --------
    result = page_df.copy()
    result["event_timestamp"] = result["event_timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S")

    return {
        "page": page,
        "limit": limit,
        "total_records": int(total_records),
        "total_pages": int(total_pages),
        "data": result.to_dict(orient="records"),
    }
