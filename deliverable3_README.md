# Nelo Data Engineering Take-Home Case Study

## Description

> Nelo recently launched an e-commerce store that allows users
to browse and purchase products directly within our app. We
have started collecting detailed user activity logs to better
understand how customers engage with product listings and
which products convert best.
The Growth Team has requested the Data Team’s support to
create a dataset that will power a dashboard answering
questions such as:
● Which item lists (e.g. Trending, Recommended for You,
Deals of the Day) are driving the most engagement?
● Which products are performing best in terms of
engagement and conversions?

---

## Table of Contents

- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Execution](#execution)
- [Project Structure](#project-structure)
- [Data Models](#data-models)
- [Exploratory Analysis](#exploratory-analysis)

---

## Architecture

This project implements a **Medallion Architecture** for data processing:

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   AWS SQS   │ --> │   Bronze    │ --> │   Silver    │ --> │    Gold     │
│  (Messages) │     │  (Raw JSON) │     │  (Cleaned)  │     │ (Modeled)   │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
```

### Data Layers

| Layer | Description | Format |
|-------|-------------|--------|
| **Bronze** | Raw unprocessed data, exactly as received from SQS | JSON (partitioned by date) |
| **Silver** | Clean and parsed data, normalized | DuckDB / Parquet |
| **Gold** | Dimensional model for analytics (fact tables + dimensions) | DuckDB / Parquet |

---

## Prerequisites

- **Python 3.10+**
- **AWS Credentials** with access to the SQS queue
- **pip** or **conda** for package management

---

## Installation

### 1. Clone the repository

```bash
git clone <repository-url>
cd test-sqs-pipeline
```

### 2. Create virtual environment (recommended)

```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
# or
.\venv\Scripts\activate   # Windows
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

## Configuration

### 1. Create configuration file

Create a `config.py` file in the project root with the required variables.

### 2. Configure environment variables (alternative)

Create a `.env` file in the project root.


## Execution

### Run the Complete Pipeline

```bash
python deliverable2_pipeline.py
```

By default, the pipeline processes data up to the **Gold** layer.


## Project Structure

```
nelo-de-case-study/
├── deliverable1_analytics_notebook.ipynb  # Exploratory analysis notebook
├── deliverable2_pipeline.py               # Main pipeline (orchestrator)
├── deliverable3_README.md                 # This documentation
├── config.py                              # Project configuration
├── requirements.txt                       # Python dependencies
├── pipeline.log                           # Pipeline logs
│
├── src/                                   # Modular source code
│   ├── __init__.py
│   ├── consumer.py                        # SQS message consumer
│   ├── transformer.py                     # Bronze transformations / Unnest JSONs
│   ├── loader.py                          # DuckDB loader and Silver/Gold transformations
│   └── models.py                          # Pydantic models and DuckDB schemas
│
├── sql/
│   └── models/                            # SQL models for transformations
│       ├── silver_events.sql              # Bronze → Silver events
│       ├── silver_items.sql               # Bronze → Silver items
│       ├── gold_fact_events.sql           # Silver → Gold fact events
│       ├── gold_fact_items.sql            # Silver → Gold fact items
│       ├── gold_dim_items.sql             # Products dimension
│       ├── gold_dim_users.sql             # Users dimension
│       └── master_metrics_tables.sql      # Views for metrics
│
└── data/                                  # Generated data
    ├── analytics.duckdb                   # DuckDB database
    ├── bronze/                            # Raw JSON messages (partitioned by date)
    │   └── YYYY/MM/DD/*.json
    ├── silver/                            # Silver tables in Parquet
    └── gold/                              # Gold tables in Parquet
```

---

## Data Models

### Silver Layer

#### `silver_events`
Clean and parsed user events.

| Column | Type | Description |
|--------|------|-------------|
| `event_id` | VARCHAR | Unique event identifier (PK) |
| `message_id` | VARCHAR | Original SQS message ID |
| `event_timestamp` | TIMESTAMP | Event timestamp |
| `user_id` | VARCHAR | User identifier |
| `event_name` | VARCHAR | Event type (`view_item_list`, `view_item`, `begin_checkout`, `purchase`) |
| `platform` | VARCHAR | Platform (`IOS`, `ANDROID`) |

#### `silver_items`
Items associated with each event, with flattened parameters.

| Column | Type | Description |
|--------|------|-------------|
| `item_record_id` | VARCHAR | Unique record identifier (PK) |
| `event_id` | VARCHAR | FK to event |
| `item_id` | VARCHAR | Product identifier |
| `item_name` | VARCHAR | Product name |
| `price` | DECIMAL | Item price |
| `item_list_name` | VARCHAR | Name of the list where it was displayed |
| `in_stock` | INTEGER | Availability (1/0) |
| `discounts` | VARCHAR | Applied discounts |
| `discount_amount` | DECIMAL | Discount amount |

### Gold Layer

#### `fact_events`
Event fact table with session logic.

| Column | Type | Description |
|--------|------|-------------|
| `event_id` | VARCHAR | PK |
| `user_id` | VARCHAR | User |
| `session_id` | VARCHAR | Session ID (derived: new if >30 min without activity) |
| `event_name` | VARCHAR | Event type |
| `event_date` | VARCHAR | Event date |
| `event_hour` | INTEGER | Event hour |

#### `fact_event_items`
Item fact table per event.

| Column | Type | Description |
|--------|------|-------------|
| `event_item_id` | VARCHAR | PK |
| `event_id` | VARCHAR | FK to fact_events |
| `item_id` | VARCHAR | Product |
| `item_list_name` | VARCHAR | Source list |
| `has_discount` | BOOLEAN | Discount flag |
| `position_in_list` | INTEGER | Position in list |

#### `dim_items`
Products dimension (SCD Type 2).

| Column | Type | Description |
|--------|------|-------------|
| `item_sk` | VARCHAR | Surrogate key |
| `item_id` | VARCHAR | Natural key |
| `item_name` | VARCHAR | Name |
| `first_seen_at` | TIMESTAMP | First appearance |
| `last_seen_at` | TIMESTAMP | Last appearance |
| `is_current` | BOOLEAN | Current record |

#### `dim_users`
Users dimension.

| Column | Type | Description |
|--------|------|-------------|
| `user_sk` | VARCHAR | Surrogate key |
| `user_id` | VARCHAR | Natural key |
| `first_platform` | VARCHAR | First platform used |
| `last_platform` | VARCHAR | Last platform used |
| `total_sessions` | INTEGER | Total sessions |

---

#### `master_list_items_engagement`
Master view built with the following definition:

```sql
CREATE OR REPLACE VIEW master_list_items_engagement AS
SELECT 
    fi.event_item_id,
    fi.event_id,
    fi.item_id,
    fi.item_name,
    fi.item_list_name,
    fi.item_list_id,
    fi.item_category,
    fi.item_brand,
    fi.price,
    fi.total_price,
    fi.quantity,
    fi.position_in_list,
    fi.has_discount,
    fi.discount_amount,
    fi.in_stock,
    fe.event_timestamp,
    fe.user_id,
    fe.session_id,
    fe.event_date,
    fe.event_hour,
    fe.processed_at as processed_at_fact_events
FROM fact_event_items fi
INNER JOIN fact_events fe ON fi.event_id = fe.event_id
WHERE fi.item_list_name IS NOT NULL
```

## Exploratory Analysis

The notebook `deliverable1_analytics_notebook.ipynb` contains exploratory analysis of the processed data.


# Solution Description/Discussion

- Vanilla Python and boto3 for data ingestion from the source, payloads are saved as JSON files that will be used as the **Bronze** layer in our Architecture
- DuckDB because it's an OLAP engine that can power any data flow due to its optimization level compared to other OLAP engines, it's also open source and can be hosted on practically any instance. If migration is needed, DuckDB's SQL usage for transformations and table/view creation means this architecture can be migrated without complication to Snowflake, BigQuery, Databricks, PostgreSQL, etc. along with a dbt project (core)
- The architecture can easily follow a lambda architecture pattern
- The architecture, in addition to persisting data in a DB, also persists it in JSON and PARQUET files, enabling the possibility of migrating our architecture to a Lakehouse or having redundancy for any fallback
- The architecture along with its codebase provides good observability of the complete pipeline metadata, giving the opportunity to measure the volume of data moving through the platform
- Consistency can be implemented with Data Quality jobs without issues within DuckDB or any OLAP engine present
- The biggest tradeoff is perhaps latency when the volume of generated data increases; in that case, compute would need to be migrated to tools like PySpark along with Kinesis/Kafka/Flink for streaming processing (or microbatches). This migration would be straightforward since the transformations presented here use SQL, making it very quick to replicate in PySpark
