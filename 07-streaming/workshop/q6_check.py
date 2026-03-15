import pandas as pd

FILE = "green_tripdata_2025-10.parquet"

df = pd.read_parquet(FILE, columns=["lpep_pickup_datetime", "tip_amount"])
df = df.dropna(subset=["lpep_pickup_datetime", "tip_amount"]).copy()

df["lpep_pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])
df["tip_amount"] = pd.to_numeric(df["tip_amount"], errors="coerce").fillna(0)

df["window_start"] = df["lpep_pickup_datetime"].dt.floor("h")

result = (
    df.groupby("window_start")["tip_amount"]
      .sum()
      .reset_index(name="total_tip")
      .sort_values("total_tip", ascending=False)
)

print(result.head(10))
print("Top hour:", result.iloc[0]["window_start"])
print("Top total tip:", float(result.iloc[0]["total_tip"]))