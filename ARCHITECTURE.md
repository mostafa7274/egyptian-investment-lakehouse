# Egyptian Investment Lakehouse — Architecture Deep Dive

## Table of Contents

1. [System Overview](#1-system-overview)
2. [Storage Layer — AWS S3](#2-storage-layer--aws-s3)
3. [Table Format — Apache Iceberg](#3-table-format--apache-iceberg)
4. [Catalog — Project Nessie](#4-catalog--project-nessie)
5. [Compute — Apache Spark](#5-compute--apache-spark)
6. [Medallion Architecture](#6-medallion-architecture)
7. [Query Layer — Dremio](#7-query-layer--dremio)
8. [JAR Dependency Strategy](#8-jar-dependency-strategy)
9. [S3 Path Convention](#9-s3-path-convention)
10. [Key Design Decisions](#10-key-design-decisions)
11. [Known Constraints & Trade-offs](#11-known-constraints--trade-offs)

---

## 1. System Overview

The lakehouse is built on the **open lakehouse architecture** pattern: object storage + open table format + a decoupled catalog + a decoupled query engine. No vendor lock-in at any layer.

```
┌─────────────────────────────────────────────────────────────┐
│                        Data Sources                         │
│  EGX Stocks │ EGX30 Index │ Fundamentals │ Gold/Silver      │
│  Currency Rates │ Spot Prices │ Real Estate (PropertyFinder) │
└────────────────────────┬────────────────────────────────────┘
                         │  CSV / JSON
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                    AWS S3 (raw/)                             │
│              my-icebergdatalake, eu-north-1                  │
└────────────────────────┬────────────────────────────────────┘
                         │  s3a:// (Spark reads raw files)
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                   Apache Spark 3.3                           │
│          PySpark notebooks (01_bronze, 02_silver)            │
│    Iceberg 1.3.0 + Nessie 0.67.0 + S3FileIO extensions      │
└──────────┬───────────────────────────┬──────────────────────┘
           │ writes Iceberg tables     │ registers in catalog
           ▼                           ▼
┌──────────────────────┐    ┌──────────────────────────────────┐
│      AWS S3           │    │         Project Nessie           │
│  bronze/ silver/ gold/│    │   Iceberg catalog, port 19120    │
│  iceberg-warehouse/  │    │   Git-like branch: main/gold-dev │
│  (Iceberg data files) │    │   (table metadata + history)     │
└──────────────────────┘    └─────────────────┬────────────────┘
                                               │
                                               ▼
                                  ┌────────────────────────┐
                                  │         Dremio          │
                                  │  SQL query engine       │
                                  │  Nessie source → main   │
                                  │  port 9047              │
                                  └────────┬───────────────┘
                                           │
                              ┌────────────┼────────────┐
                              ▼            ▼            ▼
                           Power BI    Grafana     RAG Pipeline
                                                  (FAISS + LLM)
```

---

## 2. Storage Layer — AWS S3

**Bucket:** `my-icebergdatalake`  
**Region:** `eu-north-1` (Stockholm)  
**Access:** IAM user with S3 read/write permissions; credentials injected via `.env`

### Bucket Layout

```
s3://my-icebergdatalake/
├── raw/                        # Source CSV/JSON files (uploaded manually before pipeline)
│   ├── batch_eod_all_stocks.csv
│   ├── EGX30_index.csv
│   ├── fundamentals_all.csv
│   ├── live_quotes_all.csv
│   ├── authority_prices.csv
│   ├── currency_rates.csv
│   ├── spot_prices.csv
│   └── data_enriched.json
│
├── bronze/                     # Iceberg table data files — raw ingestion
│   ├── stocks_eod/
│   ├── egx30_index/
│   ├── fundamentals/
│   ├── live_quotes/
│   ├── gold_silver_prices/
│   ├── currency_rates/
│   ├── spot_prices/
│   ├── real_estate/
│   └── real_estate_enriched/
│
├── silver/                     # Iceberg table data files — cleaned & typed
│   ├── stocks_eod/
│   ├── egx30_index/
│   ├── fundamentals/
│   ├── live_quotes/
│   ├── gold_silver_prices/
│   ├── currency_rates/
│   ├── spot_prices/
│   ├── real_estate_propertyfinder/
│   └── real_estate_unified/
│
├── gold/                       # Iceberg table data files — aggregations
│
└── iceberg-warehouse/          # Nessie internal metadata (do not modify manually)
```

S3 is the single source of truth for all Iceberg data files. Nessie stores only the metadata (table schemas, snapshots, branch pointers) — the actual Parquet data files live in S3.

---

## 3. Table Format — Apache Iceberg

**Version:** 1.3.0  
**Integration:** `org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.3.0`

Iceberg provides:

- **Schema evolution** without rewriting data
- **Time travel** via snapshot history
- **ACID transactions** on object storage
- **Partition evolution** — change partition strategy without rewriting tables
- **Hidden partitioning** — partition metadata tracked in the catalog, invisible to queries

### Iceberg Table Creation Pattern

All tables in this project are created using the DataFrame API's `createOrReplace()` method, not SQL `CREATE TABLE` statements:

```python
df.writeTo("nessie.bronze.stocks_eod") \
  .tableProperty("write.format.default", "parquet") \
  .tableProperty("location", "s3://my-icebergdatalake/bronze/stocks_eod") \
  .createOrReplace()
```

**Why not SQL DDL?** SQL `CREATE TABLE` against Nessie can encounter stale hash references when the catalog branch has been updated since the Spark session started. `createOrReplace()` via the DataFrame API resolves this reliably.

### Table Locations

Every Iceberg table has an explicit `location` property pointing to its S3 prefix under `s3://` (not `s3a://`). This is because Iceberg uses its own `S3FileIO` implementation for data file I/O, separate from Spark's Hadoop S3A filesystem.

---

## 4. Catalog — Project Nessie

**Version:** 0.67.0  
**Port:** 19120  
**API:** REST (`http://nessie:19120/api/v1`)  
**Authentication:** None (internal Docker network)

Nessie acts as the Iceberg catalog — it tracks every table's metadata: schema, current snapshot, partition spec, and history. It also provides **Git-like branching** for the catalog itself.

### Branch Strategy

```
main
 └── gold-dev     (active during Gold layer development)
      └── merge → main  (when Gold tables are verified)
```

- **Bronze and Silver layers** write directly to `main`
- **Gold layer** writes to `gold-dev` branch first, then merges to `main` after verification
- This prevents incomplete or broken Gold tables from being visible in Dremio/Power BI during development

### Creating a Branch via API

```python
import requests

NESSIE = 'http://nessie:19120/api/v1'
hash_ = requests.get(f'{NESSIE}/trees/tree/main').json()['hash']

requests.post(f'{NESSIE}/trees/branch', json={
    'name': 'gold-dev',
    'hash': hash_,
    'sourceRefName': 'main'
})
```

### Pointing Spark to a Branch

```python
spark = SparkSession.builder \
    .config('spark.sql.catalog.nessie', 'org.apache.iceberg.spark.SparkCatalog') \
    .config('spark.sql.catalog.nessie.catalog-impl', 'org.apache.iceberg.nessie.NessieCatalog') \
    .config('spark.sql.catalog.nessie.uri', 'http://nessie:19120/api/v1') \
    .config('spark.sql.catalog.nessie.ref', 'gold-dev')   # ← switch branch here
    .config('spark.sql.catalog.nessie.warehouse', 's3://my-icebergdatalake/iceberg-warehouse') \
    .getOrCreate()
```

### Namespace Registration

Nessie requires namespaces to be registered explicitly before tables can be created in them. This is done once per branch:

```python
import requests
NESSIE = 'http://nessie:19120/api/v1'
requests.post(f'{NESSIE}/namespaces/namespace/main/bronze')
requests.post(f'{NESSIE}/namespaces/namespace/main/silver')
```

---

## 5. Compute — Apache Spark

**Version:** 3.3  
**Interface:** PySpark notebooks (Jupyter, port 8888)

### SparkSession Configuration

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("EgyptianInvestmentLakehouse") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
    .config("spark.sql.catalog.nessie.uri", "http://nessie:19120/api/v1") \
    .config("spark.sql.catalog.nessie.ref", "main") \
    .config("spark.sql.catalog.nessie.warehouse", "s3://my-icebergdatalake/iceberg-warehouse") \
    .config("spark.sql.catalog.nessie.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID) \
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.eu-north-1.amazonaws.com") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()
```

### Reading Raw Files from S3

Raw files in `s3://my-icebergdatalake/raw/` are read using the `s3a://` prefix (Hadoop filesystem):

```python
df = spark.read.csv("s3a://my-icebergdatalake/raw/batch_eod_all_stocks.csv", header=True)
df_json = spark.read.json("s3a://my-icebergdatalake/raw/data_enriched.json")
```

---

## 6. Medallion Architecture

### Bronze Layer

**Purpose:** Faithful copy of source data. No business logic, no transformations.  
**Pattern:** Read raw file → add `_ingested_at` (current timestamp) and `_source_file` (filename string) → write Iceberg table via `createOrReplace()`

```python
from pyspark.sql.functions import current_timestamp, lit

df = spark.read.csv("s3a://my-icebergdatalake/raw/batch_eod_all_stocks.csv", header=True)
df = df.withColumn("_ingested_at", current_timestamp()) \
       .withColumn("_source_file", lit("batch_eod_all_stocks.csv"))

df.writeTo("nessie.bronze.stocks_eod") \
  .tableProperty("location", "s3://my-icebergdatalake/bronze/stocks_eod") \
  .createOrReplace()
```

### Silver Layer

**Purpose:** Cleaned, typed, standardized data. Ready for analytical queries and Gold layer aggregations.

**Transformation patterns applied across tables:**

| Pattern | Tables Applied |
|---|---|
| Snake_case column rename | `stocks_eod`, `egx30_index` |
| Date casting (`StringType` → `DateType`) | `stocks_eod`, `egx30_index`, `gold_silver_prices` |
| Deduplication (`dropDuplicates` on business keys) | `stocks_eod`, `egx30_index`, `gold_silver_prices` |
| Drop constant-value columns | `fundamentals`, `live_quotes` |
| Float rounding | `live_quotes` (4dp), `spot_prices` (6dp) |
| String uppercasing | `gold_silver_prices`, `spot_prices` (metal name) |
| Row filtering | `currency_rates` (348 → 10 currencies) |
| String-to-numeric parsing | `real_estate_propertyfinder` (area, price) |
| Array-to-string conversion | `real_estate_propertyfinder` (amenities) |
| Source consolidation | `real_estate_unified` (PropertyFinder only) |

**Why PropertyFinder only for real estate?**  
Bayut data (`data.json`) had null values across all key columns after inspection. The decision was made to drop it entirely rather than carry sparse data into the Silver layer and propagate nulls downstream.

### Gold Layer

Business aggregations, time-series summaries, and enriched tables built on top of Silver. Developed on the `gold-dev` Nessie branch and merged to `main` after validation. Consumed by Power BI (via Dremio), Grafana (via Dremio), and the RAG pipeline.

---

## 7. Query Layer — Dremio

**Version:** OSS (latest)  
**Port:** 9047  

Dremio connects to Nessie as a source and exposes all Iceberg tables via ANSI SQL. It acts as the intermediary between the lakehouse and BI tools.

**Why Dremio?**
- Grafana cannot read Parquet/Iceberg directly from S3 — it needs a SQL endpoint
- Power BI needs a JDBC/ODBC connection — Dremio provides this
- Dremio's Reflections (materialized views) can accelerate repeated BI queries
- Native Nessie + Iceberg support with no extra configuration

### Connecting Dremio to Nessie

1. Open `http://localhost:9047`
2. **Sources → Add Source → Nessie**
3. Configuration:
   - **Endpoint URL:** `http://nessie:19120/api/v1`
   - **Authentication:** None
   - **Default Branch:** `main`
4. **Storage settings:** add AWS access key, secret key, region `eu-north-1`

Once connected, all tables under `nessie.bronze.*`, `nessie.silver.*`, and `nessie.gold.*` are immediately queryable in Dremio's SQL editor.

---

## 8. JAR Dependency Strategy

Three JARs are required to connect Spark to both S3 and Nessie:

| JAR | Purpose |
|---|---|
| `bundle-2.20.18.jar` | AWS SDK v2 — required by Iceberg's `S3FileIO` |
| `hadoop-aws-3.3.2.jar` | Hadoop `S3AFileSystem` — for reading raw files via `s3a://` |
| `aws-java-sdk-bundle-1.12.262.jar` | AWS SDK v1 — required by `hadoop-aws` |

### Placement: `pyspark/jars/` directory

All three JARs must be placed directly in PySpark's `jars/` directory inside the notebook container — **not** passed via `spark.jars` or `spark.driver.extraClassPath`.

**Why?** `bundle-2.20.18.jar` is ~500MB. Passing it via `spark.jars` causes Spark to broadcast it over the internal network between driver and executor, which times out. Direct placement in `jars/` makes it available on the classpath at JVM startup without broadcasting.

```python
import os, pyspark
spark_jars_dir = os.path.join(os.path.dirname(pyspark.__file__), 'jars')
# Place all three JARs here
```

---

## 9. S3 Path Convention

Two different path prefixes are used depending on the access pattern:

| Prefix | Used for | Implementation |
|---|---|---|
| `s3a://` | Reading raw CSV/JSON files from `raw/` | Hadoop `S3AFileSystem` (via `hadoop-aws`) |
| `s3://` | Iceberg table `location` properties | Iceberg `S3FileIO` (via `bundle-2.20.18.jar`) |

**Never mix them for the same resource.** Using `s3a://` for Iceberg table locations causes `S3FileIO` to fail because it does not resolve that prefix. Using `s3://` for raw file reads fails because Spark's Hadoop filesystem layer does not resolve `s3://` without AWS SDK v1.

---

## 10. Key Design Decisions

### `createOrReplace()` over SQL DDL

Using `df.writeTo(...).createOrReplace()` instead of `spark.sql("CREATE TABLE IF NOT EXISTS ...")` avoids a class of Nessie catalog errors. When a Spark session runs SQL DDL, it reads the current catalog hash at parse time and submits the DDL with that hash. If the catalog has been modified since the session started (e.g. a previous cell wrote a table), the hash is stale and Nessie rejects the operation. The DataFrame API resolves the hash at execution time, not parse time.

### S3 over MinIO

The team migrated from a local MinIO container to AWS S3 so all teammates can access shared data from anywhere without running a local storage stack. The trade-off is AWS costs (minimal at this data volume) and the need for IAM credentials, but it eliminates the constraint of co-location.

### Bayut Real Estate Data Dropped

After Bronze ingestion, Bayut data (`data.json`, 107 rows) was inspected and found to have null values across all key columns (price, area, location, listing details). Carrying null-heavy data into Silver would produce a useless table and pollute the unified real estate dataset. PropertyFinder (`data_enriched.json`, 300 rows) had complete, parseable data and was used exclusively.

### No AWS Glue

AWS Glue uses the Glue Data Catalog as its Iceberg catalog. This project uses Nessie. The two catalogs are incompatible — a table registered in Nessie is invisible to Glue and vice versa. All Spark jobs run inside Docker containers against the Nessie catalog to keep the stack consistent.

---

## 11. Known Constraints & Trade-offs

| Constraint | Detail |
|---|---|
| Spark runs in single-node mode | All notebooks run on a local Spark session inside Docker. No cluster. Suitable for the current data volumes (< 100K rows per table). |
| No incremental ingestion | Bronze layer uses `createOrReplace()` — each run rewrites the full table. Suitable for daily batch at this scale. |
| Nessie has no authentication | Running on an internal Docker network. Not suitable for a production multi-user environment without adding auth. |
| Gold layer on branch | `gold-dev` must be manually merged to `main`. No automated merge CI. |
| Real estate data is static | Both PropertyFinder and Bayut sources are static JSON files, not live API feeds. |
