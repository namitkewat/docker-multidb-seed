# docker-multidb-seed

A **one-command**, Docker-based setup that spins up **PostgreSQL, MySQL, SQL Server, and Oracle** — pre-loaded with realistic, schema-rich seed data designed for **full Parquet data-type coverage**.

Built for engineers who test **ETL pipelines, CDC connectors, data lake exports, and schema-mapping logic** against real-world column diversity across all four major RDBMS engines.

---

## Why This Exists

Enterprise ETL pipelines — whether built on Spark, AWS Glue, dbt, Airbyte, Fivetran, or custom extractors — must handle the full spectrum of source database types and faithfully map them to target formats like **Apache Parquet**. Most test setups use a handful of `VARCHAR` and `INT` columns, which means type-mapping bugs (precision loss in `DECIMAL`, timezone stripping in `TIMESTAMP WITH TIME ZONE`, `ARRAY`→`LIST` conversion, `MONEY`→`DECIMAL` drift, etc.) only surface in production.

**docker-multidb-seed** solves this by giving you 4 tables × 4 databases, each with **30–45+ columns** specifically chosen to exercise every Parquet logical type from every major source engine.

### Typical Use Cases

- **ETL Export Testing** — Validate that your pipeline correctly exports `DECIMAL(15,2)`, `JSONB`, `BYTEA`, `INTERVAL`, `UUID`, `ARRAY[]`, `TIMESTAMP WITH TIME ZONE` etc. to Parquet without precision loss or silent coercion.
- **CDC / Change Data Capture** — Test Debezium, AWS DMS, or custom CDC connectors against rich schemas with NULLable columns, binary data, and nested JSON.
- **Schema Evolution Testing** — Verify how your tools handle varied column types when source schemas change.
- **Data Lake Ingestion** — Ensure your S3/ADLS/GCS landing zone Parquet files have correct logical types for downstream query engines (Athena, Trino, Databricks, BigQuery).
- **Connector & Driver Validation** — Stress-test DB drivers (`psycopg`, `pymysql`, `pymssql`, `oracledb`, JDBC/ODBC) against edge-case types.
- **Local Development** — Quick multi-DB sandbox with realistic data for application development.

---

## Tables & Data Model

| # | Table              | Domain                           | Records | Columns | Key Types Exercised                                      |
|---|--------------------|----------------------------------|---------|---------|----------------------------------------------------------|
| 1 | `invoices`         | Financial / transactional        | 5,000   | 41      | DECIMAL, JSON/JSONB, BLOB/BYTEA, DATE, TIMESTAMP(TZ)    |
| 2 | `employees`        | HR / person data                 | 5,000   | 40+     | UUID, ENUM, BOOLEAN, ARRAY[], INTERVAL, INET, TIME       |
| 3 | `sensor_readings`  | IoT / time-series                | 5,000   | 31+     | BIGINT, FLOAT/REAL/DOUBLE, high-precision DECIMAL, BINARY|
| 4 | `product_catalog`  | Product catalog / e-commerce     | 5,000   | 46      | Nested JSON, large TEXT/CLOB, MONEY, all numeric widths  |

Record counts are configurable (default 5,000 per table, per database).

---

## Parquet Data-Type Coverage Matrix

This is the core value of the project. Every row below is covered by at least one column across the four tables, in all four databases:

| Parquet Logical Type             | PostgreSQL                | MySQL                  | SQL Server                  | Oracle                      |
|----------------------------------|---------------------------|------------------------|-----------------------------|-----------------------------|
| **BOOLEAN**                      | `BOOLEAN`                 | `BOOLEAN` (TINYINT(1)) | `BIT`                       | `NUMBER(1)` + CHECK         |
| **INT8 / INT16 / INT32**         | `SMALLINT`, `INTEGER`     | `SMALLINT`, `INT`      | `TINYINT`, `SMALLINT`, `INT`| `NUMBER(3)`, `NUMBER(10)`   |
| **INT64**                        | `BIGINT`, `BIGSERIAL`     | `BIGINT`               | `BIGINT`                    | `NUMBER(18)`                |
| **FLOAT (IEEE 32-bit)**          | `REAL`                    | `FLOAT`                | `REAL`                      | `BINARY_FLOAT`              |
| **DOUBLE (IEEE 64-bit)**         | `DOUBLE PRECISION`        | `DOUBLE`               | `FLOAT`                     | `BINARY_DOUBLE`             |
| **DECIMAL (fixed-point)**        | `DECIMAL(p,s)`, `NUMERIC` | `DECIMAL(p,s)`         | `DECIMAL(p,s)`, `NUMERIC`   | `NUMBER(p,s)`               |
| **DECIMAL (currency)**           | `DECIMAL(15,2)`           | `DECIMAL(15,2)`        | `MONEY`, `SMALLMONEY`       | `NUMBER(15,2)`              |
| **BYTE_ARRAY (UTF-8 string)**    | `VARCHAR`, `TEXT`         | `VARCHAR`, `TEXT`      | `NVARCHAR`, `NVARCHAR(MAX)` | `VARCHAR2`, `CLOB`          |
| **BYTE_ARRAY (fixed-len str)**   | `CHAR(n)` (via serial)    | `CHAR(16)`             | `NCHAR(16)`                 | `CHAR(16)`                  |
| **BYTE_ARRAY (binary)**          | `BYTEA`                   | `BLOB`                 | `VARBINARY(MAX)`            | `BLOB`                      |
| **FIXED_LEN_BYTE_ARRAY (binary)**| `BYTEA` (fixed-size gen)  | `BLOB` (fixed-size)    | `BINARY(32)`                | `RAW(32)`, `RAW(16)`        |
| **UUID**                         | `UUID` (native)           | `CHAR(36)`             | `UNIQUEIDENTIFIER`          | `VARCHAR2(36)` + `RAW(16)`  |
| **DATE**                         | `DATE`                    | `DATE`                 | `DATE`                      | `DATE`                      |
| **TIME_MILLIS / TIME_MICROS**    | `TIME`, `TIME WITH TZ`    | `TIME`                 | `TIME`                      | `TIMESTAMP` (as time)       |
| **TIMESTAMP (no tz)**            | `TIMESTAMP`               | `DATETIME`             | `DATETIME2`                 | `TIMESTAMP`                 |
| **TIMESTAMP (with tz)**          | `TIMESTAMP WITH TIME ZONE`| `DATETIME(6)`          | `DATETIMEOFFSET`            | `TIMESTAMP WITH TIME ZONE`  |
| **LIST (repeated group)**        | `TEXT[]`, `INTEGER[]`, `DOUBLE PRECISION[]` | `JSON` (array) | `NVARCHAR(MAX)` (JSON array) | `CLOB` (JSON array) |
| **MAP / STRUCT (nested)**        | `JSONB`                   | `JSON`                 | `NVARCHAR(MAX)` (JSON obj)  | `CLOB` (JSON obj)           |
| **ENUM (string)**                | `VARCHAR` + CHECK         | `ENUM` (native)        | `NVARCHAR` + CHECK          | `VARCHAR2` + CHECK          |
| **INTERVAL**                     | `INTERVAL`                | `VARCHAR` (repr)       | `NVARCHAR` (repr)           | `INTERVAL YEAR TO MONTH`, `INTERVAL DAY TO SECOND` |
| **Network types (as string)**    | `INET`, `MACADDR`         | `VARCHAR`              | `NVARCHAR`                  | `VARCHAR2`                  |

### Why This Matters for ETL

Common production bugs this matrix catches:

| Bug                                         | Source Type                      | What Goes Wrong                                      |
|---------------------------------------------|----------------------------------|------------------------------------------------------|
| Precision loss                              | `DECIMAL(15,2)` / `MONEY`       | Exported as `DOUBLE` → rounding errors in financials |
| Timezone stripping                          | `TIMESTAMP WITH TIME ZONE`      | Exported as naive timestamp → wrong times downstream |
| Boolean coercion                            | `BIT` / `NUMBER(1)` / `TINYINT` | Exported as `INT` instead of `BOOLEAN`               |
| Array flattening                            | `TEXT[]` / `INTEGER[]`           | Exported as comma-separated string instead of `LIST` |
| UUID as plain string                        | `UUID` / `UNIQUEIDENTIFIER`     | Loses fixed-length optimization in Parquet           |
| Binary truncation                           | `BYTEA` / `BLOB` / `RAW`        | Silently truncated or base64-encoded as string       |
| JSON nesting loss                           | `JSONB` / `JSON`                | Flattened to string instead of Parquet nested struct |
| Interval dropped                            | `INTERVAL`                      | Exported as NULL or garbage string                   |
| Fixed-length binary mishandled              | `BINARY(32)` / `RAW(16)`        | Variable-length instead of `FIXED_LEN_BYTE_ARRAY`   |
| SMALLMONEY / MONEY type drift              | `MONEY`, `SMALLMONEY`           | Mapped to `FLOAT` instead of `DECIMAL(19,4)`        |

---

## Quick Start

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (includes Docker Compose)
- Python 3.10+

### 1. Clone & Start

```bash
git clone https://github.com/<your-username>/docker-multidb-seed.git
cd docker-multidb-seed

docker-compose up -d
```

Wait for all containers to become healthy:

```bash
docker-compose ps
```

> **First run** may take several minutes to pull images (~3–5 GB total). Subsequent starts are instant.

### 2. Install Python Dependencies

```bash
pip install psycopg[binary] pymysql pymssql oracledb
```

### 3. Generate Data

```bash
# PostgreSQL
python pg_data_generator.py

# MySQL
python mysql_data_generator.py

# SQL Server (auto-creates 'citadel' database)
python mssql_data_generator.py

# Oracle (uses thin mode by default — no Oracle Client needed)
python oracle_data_generator.py
```

Each script creates all 4 tables and inserts **5,000 records per table** by default.

---

## Connection Details

| Database       | Host        | Port   | DB / Service | User       | Password            |
|----------------|-------------|--------|--------------|------------|---------------------|
| **PostgreSQL** | `localhost` | `5432` | `citadel`    | `sentinel` | `Test_123_Password` |
| **MySQL**      | `localhost` | `3306` | `citadel`    | `sentinel` | `Test_123_Password` |
| **Oracle**     | `localhost` | `1521` | `FREEPDB1`   | `sentinel` | `Test_123_Password` |
| **SQL Server** | `localhost` | `1433` | `citadel`    | `sa`       | `Test_123_Password` |

### Connection Strings (SQLAlchemy / Generic)

```bash
# PostgreSQL
postgresql://sentinel:Test_123_Password@localhost:5432/citadel

# MySQL
mysql+pymysql://sentinel:Test_123_Password@localhost:3306/citadel

# SQL Server
mssql+pymssql://sa:Test_123_Password@localhost:1433/citadel

# Oracle
oracle+oracledb://sentinel:Test_123_Password@localhost:1521/FREEPDB1
```

---

## Database-Specific Notes

### Idempotent Data Generation (`ensure_table`)
All four Python generator scripts are designed to be fully idempotent. They utilize an internal `ensure_table` helper that checks if the target tables (`invoices`, `employees`, etc.) already exist. 

If they do, the scripts automatically issue a `DROP TABLE IF EXISTS` (or equivalent `CASCADE` logic) before recreating them. This ensures you always start with a clean slate and perfectly synchronized DDL constraints on every run, eliminating the need for manual `.sql` initialization scripts.

### SQL Server Auto-Initialization
SQL Server requires the target database to exist before connecting to it. The `mssql_data_generator.py` script automatically handles this by first connecting to the `master` database with `autocommit=True` and issuing a `CREATE DATABASE citadel` command if it does not already exist. No manual setup is needed.

### SQL Server — Insert Performance & Driver Choice
You may notice that the `mssql_data_generator.py` script runs noticeably slower than the PostgreSQL or MySQL generators. 

This is an intentional trade-off. We use the **`pymssql`** driver because it is incredibly lightweight and self-contained. The alternative—`pyodbc` with `fast_executemany=True`—is much faster at bulk inserts, but it requires developers to install system-level Microsoft ODBC drivers on their host machine or inside their Python container. 

Because `pymssql`'s `executemany` function sends statements row-by-row over the network rather than as a true bulk binary payload, heavy tables with large JSON blocks and binary blobs take a bit longer to transmit. For a local seeding script generating a few thousand rows, the simplicity and "it-just-works" nature of `pymssql` far outweighs the headache of managing ODBC driver dependencies!

### Oracle — Thin vs. Thick Mode

| Mode  | Setup Required              | When to Use                              |
|-------|-----------------------------|------------------------------------------|
| Thin  | None (default)              | Works with `oracledb` >= 2.0 out of the box |
| Thick | Oracle Instant Client + env | Required for advanced features (e.g., Oracle Advanced Security) |

**Thin mode is the default.** No Oracle Client installation is needed.

To use **thick mode**, download [Oracle Instant Client](https://www.oracle.com/database/technologies/instant-client.html), extract it, and update `oracle_data_generator.py`:

```python
# In main(), uncomment and set your path:
oracledb.init_oracle_client(lib_dir="/path/to/instantclient")
```

Then add the Instant Client directory to your system `PATH`:

```bash
# Linux / macOS
export LD_LIBRARY_PATH="/path/to/instantclient:$LD_LIBRARY_PATH"

# Windows (PowerShell)
$env:PATH = "C:\path\to\instantclient;$env:PATH"
```

---

## Project Structure

```
docker-multidb-seed/
├── docker-compose.yaml          # All 4 databases with health checks
├── pg_data_generator.py         # PostgreSQL: native ARRAY/JSONB/UUID/INTERVAL
├── mysql_data_generator.py      # MySQL: native ENUM/JSON, DATETIME(6)
├── mssql_data_generator.py      # SQL Server: MONEY/UNIQUEIDENTIFIER/DATETIMEOFFSET
├── oracle_data_generator.py     # Oracle: RAW/BINARY_FLOAT/INTERVAL/CLOB
└── README.md
```

---

## Customization

### Change Record Count

In each generator's `main()` function:

```python
run(
    ...,
    total_records=10000,    # records per table (default: 5,000)
    batch_size=1000         # insert batch size (default: 500)
)
```

### Add New Tables

Every generator follows the same extensible pattern:

1. Define `TABLE_DDL` — the `CREATE TABLE` statement with DB-specific types
2. Define `TABLE_INSERT` — parameterized `INSERT` statement
3. Define `gen_row(idx)` — function returning a tuple of randomized values
4. Append to the `TABLE_CONFIG` list

### Modify Port Mappings

Edit `docker-compose.yaml` if defaults conflict with existing services:

```yaml
ports:
  - "15432:5432"   # PostgreSQL on 15432 instead of 5432
```

Then update the corresponding `main()` in the generator script.

---

## Cleanup

```bash
# Stop containers (data preserved)
docker-compose down

# Stop and destroy all data volumes
docker-compose down -v
```

---

## Compatibility

| Component       | Tested With                    |
|-----------------|--------------------------------|
| Docker          | Docker Desktop 4.x, Podman 4.x|
| PostgreSQL      | 17 (Bitnami)                   |
| MySQL           | 9.4.0 (Bitnami)               |
| SQL Server      | 2022 (Developer Edition)       |
| Oracle          | 23 Free (gvenzl/oracle-free)   |
| Python          | 3.10, 3.11, 3.12, 3.13        |
| OS              | Windows, macOS, Linux          |

---

## Contributing

Contributions welcome! Some ideas:

- Add more tables for edge-case types (spatial/geometry, XML, ARRAY of JSON, etc.)
- Add a unified `run_all.py` script
- Add Parquet export validation scripts (read back + assert types)
- Add MariaDB / CockroachDB / SQLite support
- Docker health-check gating before auto-running generators

---

## License

MIT License
