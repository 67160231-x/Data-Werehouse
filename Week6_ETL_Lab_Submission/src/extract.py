import json
import logging
import sqlite3

import pandas as pd

from .config import RAW_DIR, SOURCE_DB

logger = logging.getLogger(__name__)


def extract_data():
    """
    Extract data from:
      - customers.csv
      - orders.csv
      - products.json
      - stores table in store.db
    Return a dictionary of DataFrames.
    """
    customers = pd.read_csv(RAW_DIR / "customers.csv", dtype=str)
    logger.info("Extracted customers: %s rows", len(customers))

    orders = pd.read_csv(RAW_DIR / "orders.csv", dtype=str)
    logger.info("Extracted orders: %s rows", len(orders))

    with open(RAW_DIR / "products.json", "r", encoding="utf-8") as f:
        raw_products = json.load(f)
    products = pd.json_normalize(raw_products)
    logger.info("Extracted products: %s rows", len(products))

    with sqlite3.connect(SOURCE_DB) as con:
        stores = pd.read_sql_query("SELECT * FROM stores", con)
    logger.info("Extracted stores: %s rows", len(stores))

    raw = {
        "customers": customers,
        "orders": orders,
        "products": products,
        "stores": stores,
    }

    for name, df in raw.items():
        logger.info("Checkpoint | %s | shape=%s | columns=%s", name, df.shape, list(df.columns))

    return raw
