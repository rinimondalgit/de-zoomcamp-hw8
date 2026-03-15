# Data Engineering Zoomcamp 2026 – Module 7: Stream Processing

This project implements a **real-time streaming data pipeline** using:

- **Redpanda (Kafka-compatible broker)**
- **Python Producer / Consumer**
- **Apache Flink (PyFlink)**
- **PostgreSQL Sink**

The pipeline processes **NYC Green Taxi Trip Data (October 2025)** and performs real-time aggregations using **windowed streaming queries**.

---

# Architecture

---

# Dataset

NYC TLC Green Taxi Data (October 2025)

Columns used:

- `lpep_pickup_datetime`
- `lpep_dropoff_datetime`
- `PULocationID`
- `DOLocationID`
- `passenger_count`
- `trip_distance`
- `tip_amount`
- `total_amount`

---

# Environment Setup

Services are started using Docker.

```bash
cd 07-streaming/workshop

docker compose build
docker compose up -d

This starts:

Redpanda (Kafka) – localhost:9092

Flink Job Manager – http://localhost:8081

Flink Task Manager

PostgreSQL – localhost:5432

This starts:

Redpanda (Kafka) – localhost:9092

Flink Job Manager – http://localhost:8081

Flink Task Manager

PostgreSQL – localhost:5432
docker exec -it workshop-redpanda-1 rpk topic create green-trips

Producer

The producer reads the parquet dataset and publishes messages to Kafka.

python src/producer_green.py

Consumer

Reads messages from Kafka and computes statistics
python src/consumer_green.py

Example output:
messages read: 49416
trip_distance > 5: 8506

Flink Streaming Jobs

Streaming jobs are executed inside the Flink container.

Submit job
docker exec -it workshop-jobmanager-1 \
flink run -py /opt/src/job/<job_file>.py -pyfs /opt/src
Question 4 – Tumbling Window

5-minute tumbling window to count trips per pickup location.

File:
Question 4 – Tumbling Window

src/job/q4_tumbling_pu.py
SQL logic:
TUMBLE(TABLE green_trips, DESCRIPTOR(event_timestamp), INTERVAL '5' MINUTES)
Result:
Top PULocationID = 74
Question 5 – Session Window

Session window with 5-minute gap per pickup location.

File:
src/job/q5_session_window.py

SQL logic:
SESSION(TABLE green_trips, DESCRIPTOR(event_timestamp), INTERVAL '5' MINUTES)
Result:
Longest session streak = 81 trips
PULocationID = 74

Question 6 – Hourly Tip Aggregation

1-hour tumbling window calculating total tip amount.

File:
src/job/q6_hourly_tips.py
Result:
Highest total tips hour:
2025-10-16 18:00:00
Homework Answers
Question	Answer
Q1	Redpanda version from rpk version
Q2	~10 seconds
Q3	8506
Q4	74
Q5	81
Q6	2025-10-16 18:00:00
Technologies Used

Python

Apache Flink / PyFlink

Redpanda (Kafka API)

Docker

PostgreSQL

Pandas

Kafka Python Client

Key Streaming Concepts Demonstrated

Kafka topic streaming

Windowed aggregations

Event-time processing

Watermarks

Tumbling windows

Session windows

Streaming sinks

Real-time analytics

Author: Rini Mondal

Project completed as part of:

Data Engineering Zoomcamp 2026
