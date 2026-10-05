import logging
import sqlite3

from .config import WAREHOUSE_DB

logger = logging.getLogger(__name__)

DDL = """
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id TEXT PRIMARY KEY,
    name        TEXT,
    province    TEXT,
    email       TEXT
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_id   TEXT PRIMARY KEY,
    product_name TEXT,
    category     TEXT,
    price        REAL
);

CREATE TABLE IF NOT EXISTS fact_sales (
    order_id      TEXT PRIMARY KEY,
    customer_id   TEXT,
    product_id    TEXT,
    order_date    TEXT,
    qty           REAL,
    unit_price    REAL,
    discount_pct  REAL,
    sales_amount  REAL
);
"""


def load_data(customers, products, sales):
    """
    Load dim_customer, dim_product, fact_sales into the SQLite warehouse.

    Uses UNIQUE/PRIMARY KEY constraints plus INSERT OR REPLACE (dims) /
    INSERT OR IGNORE (fact) so running the pipeline twice does not
    duplicate fact_sales rows.
    """
    WAREHOUSE_DB.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(WAREHOUSE_DB) as con:
        con.executescript(DDL)

        cur = con.cursor()

        customer_rows = list(
            customers[["customer_id", "name", "province", "email"]].itertuples(index=False, name=None)
        )
        cur.executemany(
            "INSERT OR REPLACE INTO dim_customer (customer_id, name, province, email) "
            "VALUES (?, ?, ?, ?)",
            customer_rows,
        )
        logger.info("Loaded %s rows into dim_customer", len(customer_rows))

        product_rows = list(
            products[["product_id", "product_name", "category", "price"]].itertuples(index=False, name=None)
        )
        cur.executemany(
            "INSERT OR REPLACE INTO dim_product (product_id, product_name, category, price) "
            "VALUES (?, ?, ?, ?)",
            product_rows,
        )
        logger.info("Loaded %s rows into dim_product", len(product_rows))

        sales_rows = list(
            sales[[
                "order_id", "customer_id", "product_id", "order_date",
                "qty", "unit_price", "discount_pct", "sales_amount",
            ]].itertuples(index=False, name=None)
        )
        cur.executemany(
            "INSERT OR IGNORE INTO fact_sales "
            "(order_id, customer_id, product_id, order_date, qty, unit_price, discount_pct, sales_amount) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            sales_rows,
        )
        con.commit()

        cur.execute("SELECT COUNT(*) FROM fact_sales")
        total = cur.fetchone()[0]
        logger.info("fact_sales now has %s rows total (attempted to insert %s)", total, len(sales_rows))
