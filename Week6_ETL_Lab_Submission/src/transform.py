import logging

import numpy as np
import pandas as pd

from .config import PROVINCE_MAP

logger = logging.getLogger(__name__)

# order_date arrives in several known formats; each row must match exactly
# one of these (tried in order) or it is treated as invalid.
DATE_FORMATS = ["%Y/%m/%d", "%d/%m/%Y", "%Y-%m-%d", "%d-%b-%Y"]

VALID_STATUSES = {"paid", "completed"}


def _parse_mixed_date(value):
    """Try each known date format in turn; return NaT if none match."""
    if pd.isna(value):
        return pd.NaT
    text = str(value).strip()
    for fmt in DATE_FORMATS:
        try:
            return pd.to_datetime(text, format=fmt)
        except (ValueError, TypeError):
            continue
    return pd.NaT


def _clean_customers(customers):
    df = customers.copy()

    before = len(df)
    df = df.drop_duplicates(subset="customer_id", keep="first")
    logger.info("Customers: removed %s duplicate customer_id rows", before - len(df))

    def standardize_province(value):
        if pd.isna(value) or str(value).strip() == "":
            return "Unknown"
        key = str(value).strip().lower()
        return PROVINCE_MAP.get(key, "Unknown")

    df["province"] = df["province"].apply(standardize_province)
    df["email"] = df["email"].fillna("unknown@example.com")
    df.loc[df["email"].str.strip() == "", "email"] = "unknown@example.com"

    return df.reset_index(drop=True)


def _clean_products(products):
    df = products.copy()

    df = df.rename(columns={
        "category.name": "category",
        "pricing.price": "price",
    })

    df["category"] = df["category"].fillna("Unknown")
    df.loc[df["category"].astype(str).str.strip() == "", "category"] = "Unknown"

    def to_numeric_price(value):
        if pd.isna(value):
            return np.nan
        if isinstance(value, str):
            value = value.replace(",", "").strip()
        try:
            return float(value)
        except (ValueError, TypeError):
            return np.nan

    df["price"] = df["price"].apply(to_numeric_price)

    df = df[["product_id", "product_name", "category", "price"]]
    return df.reset_index(drop=True)


def _clean_orders(orders):
    """Dedupe, parse dates, normalize status. Returns (clean_orders, reject_rows)."""
    df = orders.copy()
    rejects = []

    before = len(df)
    dupe_mask = df.duplicated(subset="order_id", keep="first")
    if dupe_mask.any():
        dupe_rows = df[dupe_mask].copy()
        dupe_rows["reject_reason"] = "duplicate_order_id"
        rejects.append(dupe_rows)
    df = df[~dupe_mask]
    logger.info("Orders: removed %s duplicate order_id rows", before - len(df))

    df["qty"] = pd.to_numeric(df["qty"], errors="coerce")
    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")
    df["discount_pct"] = pd.to_numeric(df["discount_pct"], errors="coerce")
    df["order_date_parsed"] = df["order_date"].apply(_parse_mixed_date)
    df["status"] = df["status"].str.strip().str.lower()

    invalid_mask = (
        df["qty"].isna() | (df["qty"] <= 0)
        | df["unit_price"].isna() | (df["unit_price"] <= 0)
        | df["discount_pct"].isna() | (df["discount_pct"] < 0) | (df["discount_pct"] > 100)
        | df["order_date_parsed"].isna()
    )

    def reason_for(row):
        reasons = []
        if pd.isna(row["qty"]) or row["qty"] <= 0:
            reasons.append("invalid_qty")
        if pd.isna(row["unit_price"]) or row["unit_price"] <= 0:
            reasons.append("invalid_unit_price")
        if pd.isna(row["discount_pct"]) or row["discount_pct"] < 0 or row["discount_pct"] > 100:
            reasons.append("invalid_discount_pct")
        if pd.isna(row["order_date_parsed"]):
            reasons.append("invalid_order_date")
        return ";".join(reasons)

    if invalid_mask.any():
        invalid_rows = df[invalid_mask].copy()
        invalid_rows["reject_reason"] = invalid_rows.apply(reason_for, axis=1)
        invalid_rows = invalid_rows.drop(columns=["order_date_parsed"])
        rejects.append(invalid_rows)
    logger.info("Orders: rejected %s rows for invalid qty/price/discount/date", invalid_mask.sum())

    df = df[~invalid_mask].copy()
    df["order_date"] = df["order_date_parsed"].dt.strftime("%Y-%m-%d")
    df = df.drop(columns=["order_date_parsed"])

    return df.reset_index(drop=True), rejects


def transform_data(raw):
    """
    Clean customers/products/orders, merge into sales fact rows, and
    collect rejected records.

    Returns: clean_customers, clean_products, sales, rejects
    """
    clean_customers = _clean_customers(raw["customers"])
    clean_products = _clean_products(raw["products"])
    clean_orders, reject_frames = _clean_orders(raw["orders"])

    # Keep only paid/completed orders for the sales fact.
    business_orders = clean_orders[clean_orders["status"].isin(VALID_STATUSES)].copy()
    logger.info(
        "Orders: %s paid/completed of %s clean orders (rest are pending/cancelled, excluded from sales)",
        len(business_orders), len(clean_orders),
    )

    known_customer_ids = set(clean_customers["customer_id"])
    known_product_ids = set(clean_products["product_id"])

    unknown_customer_mask = ~business_orders["customer_id"].isin(known_customer_ids)
    unknown_product_mask = ~business_orders["product_id"].isin(known_product_ids)
    unknown_mask = unknown_customer_mask | unknown_product_mask

    if unknown_mask.any():
        unknown_rows = business_orders[unknown_mask].copy()

        def unknown_reason(row):
            reasons = []
            if row["customer_id"] not in known_customer_ids:
                reasons.append("unknown_customer")
            if row["product_id"] not in known_product_ids:
                reasons.append("unknown_product")
            return ";".join(reasons)

        unknown_rows["reject_reason"] = unknown_rows.apply(unknown_reason, axis=1)
        reject_frames.append(unknown_rows)
    logger.info("Orders: rejected %s rows for unknown customer/product", unknown_mask.sum())

    matched = business_orders[~unknown_mask].copy()

    sales = matched.merge(
        clean_customers[["customer_id", "name", "province", "email"]],
        on="customer_id", how="left",
    ).merge(
        clean_products[["product_id", "product_name", "category", "price"]],
        on="product_id", how="left",
    )

    sales["gross_amount"] = sales["qty"] * sales["unit_price"]
    sales["discount_amount"] = sales["gross_amount"] * sales["discount_pct"] / 100
    sales["sales_amount"] = sales["gross_amount"] - sales["discount_amount"]

    sales_cols = [
        "order_id", "customer_id", "product_id", "order_date",
        "qty", "unit_price", "discount_pct",
        "gross_amount", "discount_amount", "sales_amount", "status",
    ]
    sales = sales[sales_cols].reset_index(drop=True)

    if reject_frames:
        rejects = pd.concat(reject_frames, ignore_index=True, sort=False)
    else:
        rejects = pd.DataFrame(columns=list(raw["orders"].columns) + ["reject_reason"])

    logger.info("Transform complete: %s sales rows, %s reject rows", len(sales), len(rejects))

    return clean_customers, clean_products, sales, rejects
