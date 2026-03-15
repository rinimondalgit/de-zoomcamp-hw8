import pandas as pd

FILE = "green_tripdata_2025-10.parquet"

df = pd.read_parquet(FILE, columns=["lpep_pickup_datetime", "PULocationID"])
df = df.dropna(subset=["lpep_pickup_datetime", "PULocationID"]).copy()

df["lpep_pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])
df["PULocationID"] = df["PULocationID"].astype(int)

# Sort by location and pickup time
df = df.sort_values(["PULocationID", "lpep_pickup_datetime"]).reset_index(drop=True)

# New session when gap > 5 minutes within same location
df["prev_time"] = df.groupby("PULocationID")["lpep_pickup_datetime"].shift(1)
df["gap_minutes"] = (
    (df["lpep_pickup_datetime"] - df["prev_time"]).dt.total_seconds() / 60
)

df["new_session"] = df["gap_minutes"].isna() | (df["gap_minutes"] > 5)
df["session_id"] = df.groupby("PULocationID")["new_session"].cumsum()

result = (
    df.groupby(["PULocationID", "session_id"])
      .size()
      .reset_index(name="num_trips")
      .sort_values("num_trips", ascending=False)
)

print(result.head(10))
print("Longest session trips:", int(result.iloc[0]["num_trips"]))
print("Top PULocationID:", int(result.iloc[0]["PULocationID"]))