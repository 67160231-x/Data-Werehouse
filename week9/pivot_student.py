from pathlib import Path
import sqlite3
import pandas as pd

ROOT = Path(__file__).resolve().parent
OUT = ROOT / "answers"
OUT.mkdir(exist_ok=True)

with sqlite3.connect((ROOT / "data" / "warehouse.db").as_uri() + "?mode=ro", uri=True) as con:
    df = pd.read_sql_query("SELECT * FROM sales", con)
print(df.head())

# P1: province x month, sum(amount), fill_value=0, margins=True
p1 = pd.pivot_table(
    df,
    index="province",
    columns="month",
    values="amount",
    aggfunc="sum",
    fill_value=0,
    margins=True,
    margins_name="Total",
)
print("\n--- P1: province x month (sum) ---")
print(p1)

# P2: filter September, then category x province
sep = df[df["month"] == "2026-09"]
p2 = pd.pivot_table(
    sep,
    index="category",
    columns="province",
    values="amount",
    aggfunc="sum",
    fill_value=0,
)
print("\n--- P2: September only, category x province (sum) ---")
print(p2)

# P3: assert Grand Total of P1 equals df["amount"].sum()
# Select ONLY the Total x Total cell -- not every cell that happens to be
# labelled "Total" (that would double count the margin row/column).
grand_total = p1.loc["Total", "Total"]
assert grand_total == df["amount"].sum(), (
    f"Grand total mismatch: pivot={grand_total}, raw sum={df['amount'].sum()}"
)
print(f"\nP3 OK: pivot grand total ({grand_total}) == df['amount'].sum() ({df['amount'].sum()})")

# P4: export each result to CSV
p1.to_csv(OUT / "pivot_province_month.csv")
p2.to_csv(OUT / "pivot_september.csv")
print(f"\nSaved: {OUT / 'pivot_province_month.csv'}")
print(f"Saved: {OUT / 'pivot_september.csv'}")

# --- Error experiment: drop aggfunc and see what happens ---
# pivot_table's default aggfunc is "mean", not "sum". Without aggfunc="sum",
# each cell becomes the AVERAGE amount per line for that province/month,
# not the total revenue. That's why Bangkok/September shows 270: the mean of
# that cell's two line amounts (300 tea + 240 cookie)/2 = 270, not their sum (540).
p1_wrong = pd.pivot_table(
    df, index="province", columns="month", values="amount", fill_value=0
)
print("\n--- Buggy version (no aggfunc, defaults to mean) ---")
print(p1_wrong)
print("Bangkok/2026-09 mean cell:", p1_wrong.loc["Bangkok", "2026-09"])
print("Bangkok/2026-09 correct sum cell:", p1.loc["Bangkok", "2026-09"])

# --- Excel PivotTable fallback in pandas: filter category == "Drink" ---
drink = df[df["category"] == "Drink"]
p_drink = pd.pivot_table(
    drink, index="province", columns="month", values="amount", aggfunc="sum", fill_value=0
)
print("\n--- Drink only: province x month (sum) ---")
print(p_drink)
print("Drink total revenue:", drink["amount"].sum())
p_drink.to_csv(OUT / "pivot_drink.csv")
print(f"Saved: {OUT / 'pivot_drink.csv'}")
