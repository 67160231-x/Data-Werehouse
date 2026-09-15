"""Bonus B: copy warehouse.db to challenge.db, add Oct 1 orders, verify before/after JOIN."""
import shutil
import sqlite3
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "data" / "warehouse.db"
DST = ROOT / "data" / "challenge.db"

shutil.copyfile(SRC, DST)

with sqlite3.connect(DST) as con:
    con.execute("PRAGMA foreign_keys=ON;")

    # New date dimension row
    con.execute(
        "INSERT INTO dim_date VALUES (20261001, '2026-10-01', 2026, '2026-10')"
    )

    # New fact rows: product_key 1=Tea, 2=Cookie; store_key 1=Bangsaen, 2=Siam
    con.executemany(
        "INSERT INTO fact_sales VALUES (?,?,?,?,?,?,?)",
        [
            ("O1007", 1, 20261001, 1, 1, 3, 50),   # Tea x3 @ Bangsaen (Chonburi)
            ("O1007", 2, 20261001, 2, 1, 2, 80),   # Cookie x2 @ Bangsaen (Chonburi)
            ("O1008", 1, 20261001, 1, 2, 4, 50),   # Tea x4 @ Siam (Bangkok)
        ],
    )

    fk_errors = con.execute("PRAGMA foreign_key_check").fetchall()
    assert not fk_errors, f"Foreign key errors: {fk_errors}"
    con.commit()

    print("Before/after check (fact_sales vs sales view):")
    fact_rows, fact_rev = con.execute(
        "SELECT COUNT(*), SUM(quantity*unit_price) FROM fact_sales"
    ).fetchone()
    view_rows, view_rev, view_orders = con.execute(
        "SELECT COUNT(*), SUM(amount), COUNT(DISTINCT order_id) FROM sales"
    ).fetchone()
    print(f"  fact_sales: rows={fact_rows}, revenue={fact_rev}")
    print(f"  sales view: rows={view_rows}, revenue={view_rev}, orders={view_orders}")
    assert fact_rows == view_rows and fact_rev == view_rev, "Mismatch after JOIN!"
    print("  OK: row count and revenue match before and after JOIN.")

    print("\nNew pivot (province x month, sum):")
    import pandas as pd
    df = pd.read_sql_query("SELECT * FROM sales", con)
    pivot = pd.pivot_table(
        df, index="province", columns="month", values="amount",
        aggfunc="sum", fill_value=0, margins=True, margins_name="Total",
    )
    print(pivot)
