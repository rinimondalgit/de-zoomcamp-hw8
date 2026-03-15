import pandas as pd

FILE = "green_tripdata_2025-10.parquet"

df = pd.read_parquet(FILE, columns=["lpep_pickup_datetime", "PULocationID"])
df = df.dropna(subset=["lpep_pickup_datetime", "PULocationID"]).copy()

df["lpep_pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])
df["PULocationID"] = df["PULocationID"].astype(int)

# create 5-minute tumbling window start
df["window_start"] = df["lpep_pickup_datetime"].dt.floor("5min")

result = (
    df.groupby(["window_start", "PULocationID"])
      .size()
      .reset_index(name="num_trips")
      .sort_values("num_trips", ascending=False)
)

print(result.head(10))
print("Top PULocationID:", int(result.iloc[0]["PULocationID"]))
print("Top num_trips:", int(result.iloc[0]["num_trips"]))
print("Top window_start:", result.iloc[0]["window_start"])