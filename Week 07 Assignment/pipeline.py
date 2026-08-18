"""
Python Data Pipeline Engineering - Lab Assignment
ETL Pipeline: Omnichannel Retail Sales -> Star Schema (SQLite)

Extract -> Transform (Data Quality) -> Load (Star Schema, Idempotent/Incremental)
"""

from __future__ import annotations

import logging
import sqlite3
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import pandas as pd

# --------------------------------------------------------------------------
# Logging setup
# --------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("pipeline")


# --------------------------------------------------------------------------
# Task 1: Pipeline Configuration
# --------------------------------------------------------------------------
@dataclass
class PipelineConfig:
    input_path: str
    output_db: str
    batches: list[int] = field(default_factory=lambda: [1, 2, 3])
    error_mode: str = "quarantine"  # "quarantine" = isolate bad rows and continue
    quarantine_csv: str = "output/quarantine.csv"
    run_log_csv: str = "output/pipeline_run_log.csv"

    # approved / normalized reference values
    approved_payment_methods: tuple = ("Cash", "Credit Card", "Bank Transfer", "PromptPay")
    approved_channels: tuple = ("Store", "Online", "Marketplace")
    channel_map: dict = field(default_factory=lambda: {"E-Commerce": "Online"})


# --------------------------------------------------------------------------
# Task 1: Extract
# --------------------------------------------------------------------------
def extract_dimension(config: PipelineConfig, sheet_name: str) -> pd.DataFrame:
    """Read a dimension sheet (customers / products) with error handling."""
    start = time.time()
    try:
        df = pd.read_excel(config.input_path, sheet_name=sheet_name)
        elapsed = time.time() - start
        log.info(
            "EXTRACT dim=%s rows=%d start=%.3fs end=%.3fs",
            sheet_name, len(df), start, start + elapsed,
        )
        return df
    except Exception as exc:
        log.error("EXTRACT FAILED dim=%s error=%s", sheet_name, exc)
        raise


def extract_batch(config: PipelineConfig, batch_no: int) -> pd.DataFrame:
    """Read one orders batch sheet with error handling + logging."""
    sheet_name = f"orders_batch_{batch_no}"
    start = time.time()
    try:
        df = pd.read_excel(config.input_path, sheet_name=sheet_name)
        elapsed = time.time() - start
        log.info(
            "EXTRACT batch=%d rows=%d start=%.3fs end=%.3fs",
            batch_no, len(df), start, start + elapsed,
        )
        return df
    except Exception as exc:
        log.error("EXTRACT FAILED batch=%d error=%s", batch_no, exc)
        raise


# --------------------------------------------------------------------------
# Task 2: Transform + Data Quality
# --------------------------------------------------------------------------
def normalize_payment_method(value) -> str | None:
    if pd.isna(value):
        return None
    v = str(value).strip().title()
    # "Credit Card" / "credit card" -> Title() handles case; PromptPay special
    if v.lower() == "promptpay":
        return "PromptPay"
    return v


def normalize_sales_channel(value, channel_map: dict) -> str | None:
    if pd.isna(value):
        return None
    v = str(value).strip()
    return channel_map.get(v, v)


def clean_numeric_price(value):
    """Handle values like 'THB 979.4' -> 979.4 ; non-numeric -> NaN (coerce)."""
    if pd.isna(value):
        return pd.NA
    if isinstance(value, (int, float)):
        return value
    s = str(value)
    s = s.replace("THB", "").replace("thb", "").replace(",", "").strip()
    return pd.to_numeric(s, errors="coerce")


def clean_quantity(value):
    if pd.isna(value):
        return pd.NA
    if isinstance(value, (int, float)):
        return value
    s = str(value).strip()
    return pd.to_numeric(s, errors="coerce")


def transform_batch(
    df: pd.DataFrame,
    batch_no: int,
    valid_customers: set,
    valid_products: set,
    config: PipelineConfig,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Clean + validate one batch. Returns (clean_df, quarantine_df).
    quarantine_df has all original columns + reason_code + source_batch.
    """
    df = df.copy()
    rows_read = len(df)

    # --- safe type coercion ---
    df["order_datetime"] = pd.to_datetime(df["order_datetime"], errors="coerce")
    df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce")
    df["quantity"] = df["quantity"].apply(clean_quantity)
    df["unit_price"] = df["unit_price"].apply(clean_numeric_price)
    df["discount_pct"] = pd.to_numeric(df["discount_pct"], errors="coerce")

    # --- normalize categoricals ---
    df["payment_method"] = df["payment_method"].apply(normalize_payment_method)
    df["sales_channel"] = df["sales_channel"].apply(
        lambda v: normalize_sales_channel(v, config.channel_map)
    )

    # --- deduplicate by order_id, keep latest updated_at ---
    dup_mask = df.duplicated(subset=["order_id"], keep=False)
    n_dupes_total = dup_mask.sum()
    df = df.sort_values("updated_at").drop_duplicates(subset=["order_id"], keep="last")
    n_deduped = rows_read - len(df)

    # --- build reason codes for validation ---
    reasons = pd.Series([[] for _ in range(len(df))], index=df.index)

    def add_reason(mask, code):
        for idx in df.index[mask]:
            reasons.at[idx].append(code)

    add_reason(df["order_id"].isna() | (df["order_id"].astype(str).str.strip() == ""), "MISSING_ORDER_ID")
    add_reason(df["order_datetime"].isna(), "INVALID_DATETIME")
    add_reason(df["customer_id"].isna(), "MISSING_CUSTOMER_ID")
    add_reason(~df["customer_id"].isin(valid_customers) & df["customer_id"].notna(), "CUSTOMER_NOT_FOUND")
    add_reason(df["product_id"].isna(), "MISSING_PRODUCT_ID")
    add_reason(~df["product_id"].isin(valid_products) & df["product_id"].notna(), "PRODUCT_NOT_FOUND")
    add_reason(df["quantity"].isna() | (df["quantity"] <= 0) | (df["quantity"] > 20), "INVALID_QUANTITY")
    add_reason(df["unit_price"].isna() | (df["unit_price"] <= 0), "INVALID_UNIT_PRICE")
    add_reason(df["discount_pct"].isna() | (df["discount_pct"] < 0) | (df["discount_pct"] > 100), "INVALID_DISCOUNT")
    add_reason(
        ~df["payment_method"].isin(config.approved_payment_methods) & df["payment_method"].notna(),
        "INVALID_PAYMENT_METHOD",
    )
    add_reason(df["payment_method"].isna(), "MISSING_PAYMENT_METHOD")
    add_reason(
        ~df["sales_channel"].isin(config.approved_channels) & df["sales_channel"].notna(),
        "INVALID_SALES_CHANNEL",
    )
    add_reason(df["sales_channel"].isna(), "MISSING_SALES_CHANNEL")

    df["reason_codes"] = reasons
    is_bad = df["reason_codes"].apply(len) > 0

    quarantine_df = df[is_bad].copy()
    quarantine_df["reason_code"] = quarantine_df["reason_codes"].apply(lambda lst: ";".join(lst))
    quarantine_df["source_batch"] = batch_no
    quarantine_df = quarantine_df.drop(columns=["reason_codes"])

    clean_df = df[~is_bad].copy().drop(columns=["reason_codes"])

    # --- derived fields (only on clean rows) ---
    clean_df["gross_amount"] = clean_df["quantity"] * clean_df["unit_price"]
    clean_df["net_amount"] = clean_df["gross_amount"] * (1 - clean_df["discount_pct"] / 100)

    log.info(
        "TRANSFORM batch=%d read=%d duplicates_found=%d deduped_out=%d valid=%d quarantined=%d",
        batch_no, rows_read, n_dupes_total, n_deduped, len(clean_df), len(quarantine_df),
    )

    return clean_df, quarantine_df


def transform_dimensions(
    customers: pd.DataFrame, products: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    customers = customers.copy()
    products = products.copy()
    customers["signup_date"] = pd.to_datetime(customers["signup_date"], errors="coerce")
    products["unit_price"] = pd.to_numeric(products["unit_price"], errors="coerce")
    return customers, products


# --------------------------------------------------------------------------
# Task 3: Star Schema DDL
# --------------------------------------------------------------------------
DDL = """
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id TEXT UNIQUE NOT NULL,
    customer_name TEXT,
    province TEXT,
    segment TEXT
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_key INTEGER PRIMARY KEY AUTOINCREMENT,
    product_id TEXT UNIQUE NOT NULL,
    product_name TEXT,
    category TEXT
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date TEXT UNIQUE NOT NULL,
    day INTEGER,
    month INTEGER,
    quarter INTEGER,
    year INTEGER
);

CREATE TABLE IF NOT EXISTS fact_sales (
    fact_key INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id TEXT UNIQUE NOT NULL,
    date_key INTEGER NOT NULL,
    customer_key INTEGER NOT NULL,
    product_key INTEGER NOT NULL,
    quantity INTEGER,
    unit_price REAL,
    discount_pct REAL,
    gross_amount REAL,
    net_amount REAL,
    payment_method TEXT,
    sales_channel TEXT,
    updated_at TEXT,
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (customer_key) REFERENCES dim_customer(customer_key),
    FOREIGN KEY (product_key) REFERENCES dim_product(product_key)
);

CREATE TABLE IF NOT EXISTS quarantine_records (
    quarantine_key INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id TEXT,
    order_datetime TEXT,
    customer_id TEXT,
    product_id TEXT,
    quantity TEXT,
    unit_price TEXT,
    discount_pct TEXT,
    payment_method TEXT,
    sales_channel TEXT,
    updated_at TEXT,
    source_batch INTEGER,
    reason_code TEXT
);

CREATE TABLE IF NOT EXISTS pipeline_run_log (
    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch INTEGER,
    started_at TEXT,
    ended_at TEXT,
    rows_read INTEGER,
    rows_valid INTEGER,
    rows_loaded INTEGER,
    rows_rejected INTEGER,
    status TEXT
);
"""


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(DDL)
    conn.commit()


# --------------------------------------------------------------------------
# Task 3: Load dimensions (upsert / insert-or-ignore)
# --------------------------------------------------------------------------
def load_dim_customer(conn: sqlite3.Connection, customers: pd.DataFrame) -> None:
    rows = [
        (r.customer_id, r.customer_name, r.province, r.segment)
        for r in customers.itertuples(index=False)
    ]
    conn.executemany(
        """
        INSERT INTO dim_customer (customer_id, customer_name, province, segment)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(customer_id) DO UPDATE SET
            customer_name=excluded.customer_name,
            province=excluded.province,
            segment=excluded.segment
        """,
        rows,
    )
    conn.commit()


def load_dim_product(conn: sqlite3.Connection, products: pd.DataFrame) -> None:
    rows = [
        (r.product_id, r.product_name, r.category)
        for r in products.itertuples(index=False)
    ]
    conn.executemany(
        """
        INSERT INTO dim_product (product_id, product_name, category)
        VALUES (?, ?, ?)
        ON CONFLICT(product_id) DO UPDATE SET
            product_name=excluded.product_name,
            category=excluded.category
        """,
        rows,
    )
    conn.commit()


def load_dim_date(conn: sqlite3.Connection, dates: pd.Series) -> None:
    unique_dates = pd.to_datetime(dates.dropna().unique())
    rows = []
    for d in unique_dates:
        date_key = int(d.strftime("%Y%m%d"))
        rows.append(
            (date_key, d.strftime("%Y-%m-%d"), d.day, d.month, (d.month - 1) // 3 + 1, d.year)
        )
    conn.executemany(
        "INSERT OR IGNORE INTO dim_date (date_key, full_date, day, month, quarter, year) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        rows,
    )
    conn.commit()


# --------------------------------------------------------------------------
# Task 3 & 4: Load fact_sales (idempotent upsert / incremental)
# --------------------------------------------------------------------------
def load_fact_sales(conn: sqlite3.Connection, clean_df: pd.DataFrame) -> int:
    """
    Upsert clean rows into fact_sales. Idempotent: re-running the same
    batch with unchanged updated_at values results in zero actual DB
    writes (the WHERE clause on the upsert blocks no-op updates), so
    fact row count never grows on repeated runs.
    Returns the number of rows actually inserted/updated (DB changes).
    """
    if clean_df.empty:
        return 0

    cust_map = dict(conn.execute("SELECT customer_id, customer_key FROM dim_customer").fetchall())
    prod_map = dict(conn.execute("SELECT product_id, product_key FROM dim_product").fetchall())
    changes_before = conn.total_changes

    rows = []
    for r in clean_df.itertuples(index=False):
        date_key = int(pd.Timestamp(r.order_datetime).strftime("%Y%m%d"))
        customer_key = cust_map.get(r.customer_id)
        product_key = prod_map.get(r.product_id)
        if customer_key is None or product_key is None:
            continue  # safety net; should already be filtered upstream
        rows.append(
            (
                r.order_id,
                date_key,
                customer_key,
                product_key,
                int(r.quantity),
                float(r.unit_price),
                float(r.discount_pct),
                float(r.gross_amount),
                float(r.net_amount),
                r.payment_method,
                r.sales_channel,
                str(r.updated_at),
            )
        )

    cur = conn.executemany(
        """
        INSERT INTO fact_sales
            (order_id, date_key, customer_key, product_key, quantity, unit_price,
             discount_pct, gross_amount, net_amount, payment_method, sales_channel, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(order_id) DO UPDATE SET
            date_key=excluded.date_key,
            customer_key=excluded.customer_key,
            product_key=excluded.product_key,
            quantity=excluded.quantity,
            unit_price=excluded.unit_price,
            discount_pct=excluded.discount_pct,
            gross_amount=excluded.gross_amount,
            net_amount=excluded.net_amount,
            payment_method=excluded.payment_method,
            sales_channel=excluded.sales_channel,
            updated_at=excluded.updated_at
        WHERE excluded.updated_at > fact_sales.updated_at
        """,
        rows,
    )
    conn.commit()
    return conn.total_changes - changes_before


def load_quarantine(conn: sqlite3.Connection, quarantine_df: pd.DataFrame) -> None:
    if quarantine_df.empty:
        return
    cols = [
        "order_id", "order_datetime", "customer_id", "product_id", "quantity",
        "unit_price", "discount_pct", "payment_method", "sales_channel",
        "updated_at", "source_batch", "reason_code",
    ]
    rows = [
        tuple(str(v) if pd.notna(v) else None for v in r)
        for r in quarantine_df[cols].itertuples(index=False)
    ]
    conn.executemany(
        f"INSERT INTO quarantine_records ({', '.join(cols)}) VALUES ({', '.join(['?'] * len(cols))})",
        rows,
    )
    conn.commit()


def log_run(
    conn: sqlite3.Connection,
    batch: int,
    started_at: datetime,
    ended_at: datetime,
    rows_read: int,
    rows_valid: int,
    rows_loaded: int,
    rows_rejected: int,
    status: str,
) -> None:
    conn.execute(
        """
        INSERT INTO pipeline_run_log
            (batch, started_at, ended_at, rows_read, rows_valid, rows_loaded, rows_rejected, status)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            batch,
            started_at.isoformat(),
            ended_at.isoformat(),
            rows_read,
            rows_valid,
            rows_loaded,
            rows_rejected,
            status,
        ),
    )
    conn.commit()


# --------------------------------------------------------------------------
# Task 5: Orchestration
# --------------------------------------------------------------------------
def run_pipeline(config: PipelineConfig) -> dict:
    Path("output").mkdir(exist_ok=True)
    conn = sqlite3.connect(config.output_db)
    init_db(conn)

    # --- Extract + load dimensions once ---
    customers_raw = extract_dimension(config, "customers")
    products_raw = extract_dimension(config, "products")
    customers, products = transform_dimensions(customers_raw, products_raw)
    load_dim_customer(conn, customers)
    load_dim_product(conn, products)

    valid_customers = set(customers["customer_id"])
    valid_products = set(products["product_id"])

    kpi = {
        "rows_read": 0,
        "rows_valid": 0,
        "rows_rejected": 0,
        "rows_duplicated": 0,
        "rows_loaded": 0,
        "net_sales_total": 0.0,
    }

    for batch_no in config.batches:
        started_at = datetime.now()
        try:
            raw = extract_batch(config, batch_no)
            rows_read = len(raw)

            clean_df, quarantine_df = transform_batch(
                raw, batch_no, valid_customers, valid_products, config
            )

            load_dim_date(conn, clean_df["order_datetime"]) if not clean_df.empty else None
            rows_loaded = load_fact_sales(conn, clean_df)

            ended_at = datetime.now()
            log_run(
                conn, batch_no, started_at, ended_at,
                rows_read=rows_read,
                rows_valid=len(clean_df),
                rows_loaded=rows_loaded,
                rows_rejected=len(quarantine_df),
                status="SUCCESS",
            )

            load_quarantine(conn, quarantine_df)
            dup_count = rows_read - len(clean_df) - len(quarantine_df)
            kpi["rows_read"] += rows_read
            kpi["rows_valid"] += len(clean_df)
            kpi["rows_rejected"] += len(quarantine_df)
            kpi["rows_duplicated"] += max(dup_count, 0)
            kpi["rows_loaded"] += rows_loaded
            kpi["net_sales_total"] += clean_df["net_amount"].sum() if not clean_df.empty else 0.0

            log.info(
                "LOAD batch=%d rows_loaded=%d (fact upserted) status=SUCCESS", batch_no, rows_loaded
            )

        except Exception as exc:
            ended_at = datetime.now()
            log.error("BATCH FAILED batch=%d error=%s", batch_no, exc)
            log_run(
                conn, batch_no, started_at, ended_at,
                rows_read=0, rows_valid=0, rows_loaded=0, rows_rejected=0,
                status=f"FAILED: {exc}",
            )
            # do not destroy previously loaded data; continue to next batch
            continue

    # --- export cumulative quarantine.csv (from quarantine_records table) ---
    quarantine_all = pd.read_sql("SELECT * FROM quarantine_records", conn)
    quarantine_all.to_csv(config.quarantine_csv, index=False)
    log.info("WROTE quarantine file rows=%d path=%s", len(quarantine_all), config.quarantine_csv)

    # --- export run log ---
    run_log_df = pd.read_sql("SELECT * FROM pipeline_run_log", conn)
    run_log_df.to_csv(config.run_log_csv, index=False)
    log.info("WROTE run log rows=%d path=%s", len(run_log_df), config.run_log_csv)

    conn.close()

    log.info(
        "PIPELINE SUMMARY read=%d valid=%d rejected=%d duplicated=%d loaded=%d net_sales_total=%.2f",
        kpi["rows_read"], kpi["rows_valid"], kpi["rows_rejected"],
        kpi["rows_duplicated"], kpi["rows_loaded"], kpi["net_sales_total"],
    )
    return kpi


def fact_count(db_path: str) -> int:
    conn = sqlite3.connect(db_path)
    n = conn.execute("SELECT COUNT(*) FROM fact_sales").fetchone()[0]
    conn.close()
    return n


if __name__ == "__main__":
    db_path = "output/retail_dw.db"
    # start clean so the demo below is reproducible on every run of this script
    Path(db_path).unlink(missing_ok=True)

    cfg = PipelineConfig(
        input_path="data/Python_Data_Pipeline_Lab_Dataset__1_.xlsx",
        output_db=db_path,
    )

    print("\n=== ROUND 1: load batch_1 (first load) ===")
    cfg.batches = [1]
    run_pipeline(cfg)
    print("fact_sales rows after round 1:", fact_count(db_path))

    print("\n=== ROUND 2: load batch_1 again (idempotency check) ===")
    cfg.batches = [1]
    run_pipeline(cfg)
    print("fact_sales rows after round 2 (must equal round 1):", fact_count(db_path))

    print("\n=== ROUND 3: load batch_2 (incremental - new batch only) ===")
    cfg.batches = [2]
    run_pipeline(cfg)
    print("fact_sales rows after round 3:", fact_count(db_path))

    print("\n=== ROUND 4: load batch_3 (incremental - new batch only) ===")
    cfg.batches = [3]
    final_kpi = run_pipeline(cfg)
    print("fact_sales rows after round 4:", fact_count(db_path))

    print("\nFinal fact_sales row count:", fact_count(db_path))
