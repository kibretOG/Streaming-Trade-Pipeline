# Event-Driven Data Pipeline (WIP)

In-progress data pipeline built for streaming and processing trade datamusing Apache Airflow, Kafka, Spark, and a Flask API to enable scalable, real-time data movement and processing.

---

## ⚙️ Architecture Overview


### 1. **Orchestration: Apache Airflow**
- Coordinates and schedules data ingestion tasks.
- Uses `CeleryExecutor` for distributed task execution.
- PostgreSQL for metadata storage
- Redis for Celery's message broker

---

### 2. **Ingestion: Kafka**
- **Purpose**: Ingests trade data from live financial APIs (e.g., `yliveticker`) and streams it to Kafka.
- **Broker Setup**: Confluent Kafka with a local Zookeeper.

---

### 3. **Processing: Apache Spark**
- Stream and process Kafka data to clean, enrich, and prepare it for serving.
- **Integration**:
  - Read from Kafka topic.
  - Apply transformations.
  - **Write the processed output into Apache Cassandra** 

---

### 4. **Serving: Flask API (Planned)**
- **Purpose**: Expose processed data stored in Cassandra via RESTful endpoints for downstream consumers (e.g., dashboards, analytics apps).

---
