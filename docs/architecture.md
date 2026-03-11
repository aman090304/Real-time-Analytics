Event Generator
       │
       ▼
Ingestion Service
       │
       ▼
Kafka Event Bus
       │
 ┌─────┴─────┐
 ▼           ▼
Stream Workers
 ▼
Redis Cache
 ▼
Analytics Database
 ▼
Query API
 ▼
Dashboard


Backend:

Python (FastAPI)

Streaming:

Kafka

Cache:

Redis

Analytics DB:

ClickHouse

Monitoring:

Prometheus + Grafana

Frontend:

React (optional)