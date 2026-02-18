# pg_data_generator.py
# =====================================================================================
# PostgreSQL Data Generator — Full Parquet Data-Type Coverage
# =====================================================================================
# Tables: invoices, employees, sensor_readings, product_catalog
#
# Parquet type mapping target:
#   BOOLEAN, INT32, INT64, FLOAT, DOUBLE, DECIMAL, BYTE_ARRAY (string/binary),
#   FIXED_LEN_BYTE_ARRAY (UUID), DATE, TIME, TIMESTAMP (millis/micros, tz-aware),
#   LIST (repeated), MAP, JSON (struct), ENUM (string), INTERVAL
# =====================================================================================

import psycopg
import random
import string
from decimal import Decimal
from datetime import datetime, date, timedelta, time, timezone
import uuid
import json
from typing import List, Dict, Any
import sys
import ipaddress

# -------------------------------------------------------------------------------------
# Shared helpers
# -------------------------------------------------------------------------------------
COUNTRIES = ['USA', 'Germany', 'Japan', 'China', 'Brazil', 'India', 'UK', 'France',
             'Australia', 'Canada', 'South Korea', 'Singapore']
CURRENCIES = ['USD', 'EUR', 'JPY', 'CNY', 'BRL', 'INR', 'GBP', 'CHF', 'AUD', 'CAD']
PLANTS = ['US-PLANT-01', 'GER-PLANT-02', 'JP-PLANT-03', 'CN-PLANT-04', 'IN-PLANT-05']
TIMEZONES = ['UTC', 'US/Eastern', 'Europe/Berlin', 'Asia/Tokyo', 'Asia/Shanghai',
             'Asia/Kolkata', 'Australia/Sydney']

def rand_str(length: int = 8) -> str:
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def rand_ipv4() -> str:
    return str(ipaddress.IPv4Address(random.randint(0x0A000001, 0x0AFFFFFF)))

def rand_ipv6() -> str:
    return str(ipaddress.IPv6Address(random.getrandbits(128)))

def rand_mac() -> str:
    return ':'.join(f'{random.randint(0,255):02x}' for _ in range(6))

def rand_semver() -> str:
    return f"{random.randint(0,9)}.{random.randint(0,99)}.{random.randint(0,999)}"


# =====================================================================================
# Database Helper: Ensure a clean table exists for data generation
# =====================================================================================
def ensure_table(conn, table_name: str, ddl: str):
    """
    Ensures that a specified table exists in the database with the latest schema.
    
    If the table already exists, it is dropped (along with any dependent 
    constraints) to prevent schema conflicts or stale data during the seeding 
    process. After ensuring a clean slate, the table is created fresh.

    Args:
        conn (psycopg.Connection): The active PostgreSQL database connection object.
        table_name (str): The name of the table to check and create.
        ddl (str): The SQL Data Definition Language (CREATE TABLE & INDEXES) statement.
    """
    with conn.cursor() as cur:
        # Check if table exists in the public schema. 
        # Psycopg uses %s for safe parameter binding.
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' AND table_name = %s
            )
        """, (table_name,))
        
        exists = cur.fetchone()[0]
        
        # If the table is found, perform a clean teardown before rebuilding it.
        if exists:
            print(f"  Table {table_name} already exists. Dropping old table...")
            # CASCADE drops the table even if other objects depend on it
            cur.execute(f"DROP TABLE {table_name} CASCADE")
            
        # Execute the provided DDL string to create the table and its indexes from scratch
        print(f"  Creating table {table_name}...")
        cur.execute(ddl)
        
        # Commit the transaction so the drop/create operations are saved
        conn.commit()
        print(f"  Table {table_name} ready.")

# =====================================================================================
# TABLE 1: invoices  (carried over from original, with minor tweaks)
# =====================================================================================
INVOICES_DDL = """
CREATE TABLE IF NOT EXISTS invoices (
    invoice_id          SERIAL PRIMARY KEY,
    invoice_number      VARCHAR(50) UNIQUE NOT NULL,
    customer_id         INTEGER NOT NULL,
    customer_code       VARCHAR(20) NOT NULL,
    customer_name       VARCHAR(255) NOT NULL,
    customer_address    TEXT NOT NULL,
    billing_country     VARCHAR(100) NOT NULL,
    shipping_country    VARCHAR(100),
    currency_code       VARCHAR(3) NOT NULL,
    payment_terms       VARCHAR(50),
    sales_representative VARCHAR(100),

    total_amount        DECIMAL(15,2) NOT NULL,
    tax_amount          DECIMAL(12,2) NOT NULL,
    discount_amount     DECIMAL(10,2) DEFAULT 0.00,
    shipping_cost       DECIMAL(10,2) DEFAULT 0.00,
    subtotal_amount     DECIMAL(15,2) NOT NULL,

    items_count         SMALLINT NOT NULL,
    revision_number     SMALLINT DEFAULT 0,
    processing_days     SMALLINT,

    exchange_rate       DOUBLE PRECISION DEFAULT 1.0,
    tax_rate            DOUBLE PRECISION NOT NULL,

    is_paid             BOOLEAN DEFAULT FALSE,
    is_shipped          BOOLEAN DEFAULT FALSE,
    is_recurring        BOOLEAN DEFAULT FALSE,
    requires_approval   BOOLEAN DEFAULT FALSE,
    is_international    BOOLEAN DEFAULT FALSE,

    invoice_date        DATE NOT NULL,
    due_date            DATE NOT NULL,
    created_datetime    TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_datetime    TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    payment_date        TIMESTAMP WITH TIME ZONE,
    shipment_date       TIMESTAMP WITH TIME ZONE,
    approval_date       TIMESTAMP WITH TIME ZONE,

    invoice_timezone    VARCHAR(50) DEFAULT 'UTC',
    invoice_items       JSONB NOT NULL,
    digital_signature   BYTEA,
    metadata_json       JSONB,
    status              VARCHAR(20) DEFAULT 'DRAFT',

    project_code        VARCHAR(30),
    cost_center         VARCHAR(20),
    manufacturing_plant VARCHAR(50),
    quality_check_passed BOOLEAN,
    compliance_verified  BOOLEAN,

    CONSTRAINT chk_inv_total_positive    CHECK (total_amount >= 0),
    CONSTRAINT chk_inv_tax_positive      CHECK (tax_amount >= 0),
    CONSTRAINT chk_inv_discount_positive CHECK (discount_amount >= 0)
);
CREATE INDEX IF NOT EXISTS idx_inv_number    ON invoices(invoice_number);
CREATE INDEX IF NOT EXISTS idx_inv_cust_code ON invoices(customer_code);
CREATE INDEX IF NOT EXISTS idx_inv_date      ON invoices(invoice_date);
CREATE INDEX IF NOT EXISTS idx_inv_paid      ON invoices(is_paid);
CREATE INDEX IF NOT EXISTS idx_inv_status    ON invoices(status);
"""

INVOICES_INSERT = """
INSERT INTO invoices (
    invoice_number, customer_id, customer_code, customer_name, customer_address,
    billing_country, shipping_country, currency_code, payment_terms, sales_representative,
    total_amount, tax_amount, discount_amount, shipping_cost, subtotal_amount,
    items_count, revision_number, processing_days, exchange_rate, tax_rate,
    is_paid, is_shipped, is_recurring, requires_approval, is_international,
    invoice_date, due_date, created_datetime, payment_date, shipment_date, approval_date,
    invoice_timezone, invoice_items, digital_signature, metadata_json, status,
    project_code, cost_center, manufacturing_plant, quality_check_passed, compliance_verified
) VALUES (
    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
)
"""

def gen_invoice(idx: int) -> tuple:
    billing = random.choice(COUNTRIES)
    shipping = random.choice(COUNTRIES)
    currency = random.choice(CURRENCIES)
    items_count = random.randint(1, 15)
    items, subtotal = [], Decimal('0.00')
    for j in range(items_count):
        up = Decimal(str(round(random.uniform(10, 1000), 2)))
        qty = random.randint(1, 50)
        total = up * qty
        items.append({
            'item_id': j + 1,
            'product_code': f"PROD-{random.randint(1000,9999)}",
            'description': f'Manufactured Part {random.randint(100,999)}',
            'quantity': qty,
            'unit_price': float(up),
            'total_amount': float(total),
            'uom': random.choice(['PCS','KG','M','L']),
            'hs_code': f"{random.randint(1000,9999)}.{random.randint(10,99)}"
        })
        subtotal += total

    discount = Decimal(str(round(random.uniform(0, min(float(subtotal * Decimal('0.2')), 500)), 2)))
    ship_cost = Decimal(str(round(random.uniform(0, 200), 2)))
    tax_rate = round(random.uniform(0.05, 0.25), 3)
    tax_amt = (subtotal - discount) * Decimal(str(tax_rate))
    total_amt = subtotal - discount + tax_amt + ship_cost

    base_dt = date.today() - timedelta(days=random.randint(0, 365))
    created = datetime.now(timezone.utc) - timedelta(days=random.randint(0, 30))
    pay_dt = created - timedelta(days=random.randint(1, 15)) if random.random() > 0.5 else None
    ship_dt = created - timedelta(days=random.randint(1, 10)) if random.random() > 0.5 else None
    appr_dt = created - timedelta(days=random.randint(1, 5)) if random.random() > 0.5 else None

    return (
        f"INV-{date.today().strftime('%Y%m')}-{idx+1:06d}",
        random.randint(1000, 9999),
        f"CUST-{random.randint(10000,99999)}",
        f"Customer {random.randint(1,1000)} Corp.",
        f"{random.randint(1,9999)} Main St, City {random.randint(1,100)}, {billing}",
        billing, shipping, currency,
        random.choice(['NET30','NET60','DUE_ON_RECEIPT','NET15']),
        f"SalesRep-{random.randint(1,50)}",
        total_amt, tax_amt, discount, ship_cost, subtotal,
        items_count, random.randint(0, 5), random.randint(1, 10),
        round(random.uniform(0.8, 1.2), 4), tax_rate,
        random.choice([True, False]), random.choice([True, False]),
        random.choice([True, False]), random.choice([True, False]),
        billing != shipping,
        base_dt, base_dt + timedelta(days=random.randint(15, 90)),
        created, pay_dt, ship_dt, appr_dt,
        random.choice(TIMEZONES),
        json.dumps(items),
        bytes(random.getrandbits(8) for _ in range(64)),
        json.dumps({'version': '1.0', 'batch_id': str(uuid.uuid4()),
                    'quality_metrics': {'score': round(random.uniform(85, 99.9), 1)}}),
        random.choice(['DRAFT','SENT','PAID','OVERDUE','CANCELLED']),
        f"PROJ-{random.randint(1000,9999)}",
        f"CC-{random.randint(100,999)}",
        random.choice(PLANTS),
        random.choice([True, False]),
        random.choice([True, False])
    )


# =====================================================================================
# TABLE 2: employees  (HR data — UUIDs, enums, dates, intervals, arrays, INET)
# =====================================================================================
EMPLOYEES_DDL = """
CREATE TABLE IF NOT EXISTS employees (
    employee_id         SERIAL PRIMARY KEY,
    employee_uuid       UUID NOT NULL DEFAULT gen_random_uuid(),
    employee_code       VARCHAR(20) UNIQUE NOT NULL,
    first_name          VARCHAR(100) NOT NULL,
    last_name           VARCHAR(100) NOT NULL,
    full_name           TEXT GENERATED ALWAYS AS (first_name || ' ' || last_name) STORED,
    email               VARCHAR(255) NOT NULL,
    phone_number        VARCHAR(30),

    -- Enum-like via VARCHAR + CHECK
    gender              VARCHAR(20),
    employment_type     VARCHAR(30) NOT NULL,
    department          VARCHAR(100) NOT NULL,
    job_title           VARCHAR(150) NOT NULL,
    job_level           SMALLINT NOT NULL,

    -- Numeric types
    base_salary         DECIMAL(12,2) NOT NULL,
    bonus_pct           REAL,                          -- 32-bit float  → Parquet FLOAT
    stock_options        INTEGER DEFAULT 0,
    years_experience    SMALLINT,
    employee_rating     DOUBLE PRECISION,              -- 64-bit float  → Parquet DOUBLE
    badge_number        BIGINT,                        -- 64-bit int    → Parquet INT64

    -- Boolean
    is_active           BOOLEAN DEFAULT TRUE,
    is_manager          BOOLEAN DEFAULT FALSE,
    has_remote_access   BOOLEAN DEFAULT FALSE,
    background_check_ok BOOLEAN,

    -- Date / Time / Timestamp / Interval  (Parquet DATE, TIME, TIMESTAMP, INTERVAL)
    date_of_birth       DATE,
    hire_date           DATE NOT NULL,
    termination_date    DATE,
    last_login_time     TIME WITHOUT TIME ZONE,        -- Parquet TIME_MILLIS
    shift_start         TIME WITH TIME ZONE,
    created_at          TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    probation_period    INTERVAL,                      -- Parquet INTERVAL (fixed 12-byte)

    -- Network / Specialty PG types  (exported as strings to Parquet)
    office_ip           INET,
    vpn_mac             MACADDR,

    -- Array types  (Parquet LIST)
    skills              TEXT[],
    certifications      VARCHAR(100)[],
    project_ids         INTEGER[],

    -- JSON / JSONB  (Parquet JSON / struct)
    address_json        JSONB,
    emergency_contact   JSONB,
    preferences         JSONB,

    -- Binary
    profile_photo_thumb BYTEA,

    -- Large text
    bio                 TEXT,
    notes               TEXT,

    CONSTRAINT chk_emp_gender CHECK (gender IN ('Male','Female','Non-Binary','Prefer Not to Say')),
    CONSTRAINT chk_emp_type   CHECK (employment_type IN ('Full-Time','Part-Time','Contract','Intern')),
    CONSTRAINT chk_emp_salary CHECK (base_salary >= 0)
);
CREATE INDEX IF NOT EXISTS idx_emp_uuid   ON employees(employee_uuid);
CREATE INDEX IF NOT EXISTS idx_emp_code   ON employees(employee_code);
CREATE INDEX IF NOT EXISTS idx_emp_dept   ON employees(department);
CREATE INDEX IF NOT EXISTS idx_emp_active ON employees(is_active);
"""

EMPLOYEES_INSERT = """
INSERT INTO employees (
    employee_uuid, employee_code, first_name, last_name, email, phone_number,
    gender, employment_type, department, job_title, job_level,
    base_salary, bonus_pct, stock_options, years_experience, employee_rating, badge_number,
    is_active, is_manager, has_remote_access, background_check_ok,
    date_of_birth, hire_date, termination_date, last_login_time, shift_start,
    created_at, updated_at, probation_period,
    office_ip, vpn_mac,
    skills, certifications, project_ids,
    address_json, emergency_contact, preferences,
    profile_photo_thumb, bio, notes
) VALUES (
    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
)
"""

FIRST_NAMES = ['Aarav','Wei','Sakura','Priya','Liam','Emma','Hiroshi','Fatima','Carlos','Ingrid',
               'Raj','Mei','Yuki','Olga','James','Sofia','Kenji','Anika','Diego','Freya']
LAST_NAMES = ['Patel','Wang','Tanaka','Sharma','Smith','Mueller','Suzuki','Khan','Garcia','Johansson',
              'Kumar','Chen','Sato','Petrov','Johnson','Schmidt','Kim','Singh','Lopez','Andersson']
DEPARTMENTS = ['Engineering','Finance','HR','Sales','Marketing','Operations','Legal','R&D','Support','QA']
JOB_TITLES = ['Software Engineer','Analyst','Manager','Director','VP','Intern','Consultant',
              'Architect','Lead','Specialist','Administrator','Coordinator']
SKILLS_POOL = ['Python','Java','SQL','AWS','Docker','Kubernetes','React','TypeScript','Go','Rust',
               'Terraform','CI/CD','Machine Learning','Data Engineering','Kafka','Spark']
CERTS_POOL = ['AWS-SAA','AWS-SAP','CKA','PMP','CISSP','TOGAF','AZ-900','GCP-ACE','CKAD','OCP']

def gen_employee(idx: int) -> tuple:
    fn = random.choice(FIRST_NAMES)
    ln = random.choice(LAST_NAMES)
    emp_uuid = str(uuid.uuid4())
    dob = date(random.randint(1960, 2002), random.randint(1, 12), random.randint(1, 28))
    hire = date(random.randint(2010, 2025), random.randint(1, 12), random.randint(1, 28))
    term = hire + timedelta(days=random.randint(180, 1800)) if random.random() < 0.15 else None

    skills = random.sample(SKILLS_POOL, k=random.randint(2, 7))
    certs = random.sample(CERTS_POOL, k=random.randint(0, 4))
    proj_ids = [random.randint(1000, 9999) for _ in range(random.randint(1, 5))]

    return (
        emp_uuid,
        f"EMP-{idx+1:06d}",
        fn, ln,
        f"{fn.lower()}.{ln.lower()}{random.randint(1,99)}@example.com",
        f"+{random.randint(1,99)}-{random.randint(100,999)}-{random.randint(1000,9999)}",
        random.choice(['Male','Female','Non-Binary','Prefer Not to Say']),
        random.choice(['Full-Time','Part-Time','Contract','Intern']),
        random.choice(DEPARTMENTS),
        random.choice(JOB_TITLES),
        random.randint(1, 10),
        Decimal(str(round(random.uniform(30000, 250000), 2))),
        round(random.uniform(0, 0.30), 4),           # REAL / float32
        random.randint(0, 50000),
        random.randint(0, 35),
        round(random.uniform(1.0, 5.0), 6),          # DOUBLE
        random.randint(100000, 999999),               # BIGINT
        random.choice([True, False]),
        random.choice([True, False]),
        random.choice([True, False]),
        random.choice([True, False, None]),
        dob, hire, term,
        time(random.randint(0, 23), random.randint(0, 59), random.randint(0, 59)),
        time(random.randint(6, 10), 0, 0),
        datetime.now(timezone.utc) - timedelta(days=random.randint(0, 30)),
        datetime.now() - timedelta(days=random.randint(0, 10)),
        f"{random.choice([3,6])} months",             # INTERVAL
        rand_ipv4(),
        rand_mac(),
        skills,          # TEXT[]
        certs,           # VARCHAR[]
        proj_ids,        # INTEGER[]
        json.dumps({
            'street': f"{random.randint(1,9999)} {random.choice(['Oak','Elm','Main','Maple'])} St",
            'city': random.choice(['Tokyo','Berlin','Mumbai','NYC','London','Singapore']),
            'zip': f"{random.randint(10000,99999)}",
            'country': random.choice(COUNTRIES)
        }),
        json.dumps({
            'name': f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}",
            'phone': f"+{random.randint(1,99)}-{random.randint(100,999)}-{random.randint(1000,9999)}",
            'relation': random.choice(['Spouse','Parent','Sibling','Friend'])
        }),
        json.dumps({
            'theme': random.choice(['dark','light','system']),
            'notifications': random.choice([True, False]),
            'language': random.choice(['en','de','ja','hi','es'])
        }),
        bytes(random.getrandbits(8) for _ in range(128)),  # small thumbnail stub
        f"Experienced {random.choice(JOB_TITLES).lower()} with {random.randint(1,20)} years in {random.choice(DEPARTMENTS).lower()}.",
        f"Note: {rand_str(50)}" if random.random() > 0.5 else None
    )


# =====================================================================================
# TABLE 3: sensor_readings  (IoT / time-series — high-precision, NUMERIC(38), arrays)
# =====================================================================================
SENSOR_DDL = """
CREATE TABLE IF NOT EXISTS sensor_readings (
    reading_id          BIGSERIAL PRIMARY KEY,          -- INT64
    reading_uuid        UUID NOT NULL DEFAULT gen_random_uuid(),
    device_id           VARCHAR(50) NOT NULL,
    device_serial       CHAR(16) NOT NULL,              -- fixed-length string
    firmware_version    VARCHAR(20),

    -- High-precision numerics  → Parquet DECIMAL / INT96
    temperature_c       DECIMAL(8,4),
    humidity_pct        DECIMAL(6,3),
    pressure_hpa        DECIMAL(10,4),
    voltage             REAL,                           -- FLOAT32
    current_amps        DOUBLE PRECISION,               -- FLOAT64
    power_watts         NUMERIC(12,6),
    latitude            DECIMAL(10,7),
    longitude           DECIMAL(11,7),
    altitude_m          REAL,
    signal_strength_dbm SMALLINT,
    error_code          INTEGER DEFAULT 0,
    uptime_seconds      BIGINT,

    -- Booleans
    is_anomaly          BOOLEAN DEFAULT FALSE,
    is_calibrated       BOOLEAN DEFAULT TRUE,
    battery_low         BOOLEAN DEFAULT FALSE,

    -- Timestamps
    reading_timestamp   TIMESTAMP WITH TIME ZONE NOT NULL,
    server_received_at  TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    reading_date        DATE NOT NULL,
    reading_time        TIME WITHOUT TIME ZONE NOT NULL,

    -- Array / List types  → Parquet LIST
    tag_ids             INTEGER[],
    sensor_labels       TEXT[],
    raw_samples         DOUBLE PRECISION[],             -- LIST<DOUBLE>

    -- JSON
    device_metadata     JSONB,
    alert_config        JSONB,

    -- Binary
    raw_payload         BYTEA,

    -- Text
    location_name       VARCHAR(200),
    notes               TEXT
);
CREATE INDEX IF NOT EXISTS idx_sr_device    ON sensor_readings(device_id);
CREATE INDEX IF NOT EXISTS idx_sr_ts        ON sensor_readings(reading_timestamp);
CREATE INDEX IF NOT EXISTS idx_sr_date      ON sensor_readings(reading_date);
CREATE INDEX IF NOT EXISTS idx_sr_anomaly   ON sensor_readings(is_anomaly);
"""

SENSOR_INSERT = """
INSERT INTO sensor_readings (
    reading_uuid, device_id, device_serial, firmware_version,
    temperature_c, humidity_pct, pressure_hpa, voltage, current_amps, power_watts,
    latitude, longitude, altitude_m, signal_strength_dbm, error_code, uptime_seconds,
    is_anomaly, is_calibrated, battery_low,
    reading_timestamp, server_received_at, reading_date, reading_time,
    tag_ids, sensor_labels, raw_samples,
    device_metadata, alert_config,
    raw_payload, location_name, notes
) VALUES (
    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
)
"""

SENSOR_LOCATIONS = ['Factory Floor A','Warehouse B','Server Room C','Outdoor Station D',
                    'Cold Storage E','Lab Room F','Rooftop G','Basement H']

def gen_sensor_reading(idx: int) -> tuple:
    ts = datetime.now(timezone.utc) - timedelta(
        days=random.randint(0, 90),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
        seconds=random.randint(0, 59)
    )
    lat = Decimal(str(round(random.uniform(-90, 90), 7)))
    lon = Decimal(str(round(random.uniform(-180, 180), 7)))

    raw_samples = [round(random.uniform(-50, 150), 6) for _ in range(random.randint(5, 20))]

    return (
        str(uuid.uuid4()),
        f"DEV-{random.randint(1,200):04d}",
        rand_str(16).upper(),
        rand_semver(),
        Decimal(str(round(random.uniform(-40, 85), 4))),
        Decimal(str(round(random.uniform(0, 100), 3))),
        Decimal(str(round(random.uniform(900, 1100), 4))),
        round(random.uniform(0, 48), 3),               # REAL
        round(random.uniform(0, 10), 8),                # DOUBLE
        Decimal(str(round(random.uniform(0, 5000), 6))),
        lat, lon,
        round(random.uniform(-50, 5000), 2),            # altitude REAL
        random.randint(-120, 0),                         # SMALLINT
        random.choice([0, 0, 0, 0, 1, 2, 3, 99]),       # mostly 0
        random.randint(0, 10_000_000),                   # BIGINT
        random.random() < 0.05,                          # 5% anomaly rate
        random.random() > 0.02,
        random.random() < 0.1,
        ts,
        ts + timedelta(milliseconds=random.randint(50, 2000)),
        ts.date(),
        ts.time(),
        [random.randint(1, 500) for _ in range(random.randint(1, 5))],
        random.sample(['temp','humidity','pressure','vibration','acoustic','light'], k=random.randint(1,4)),
        raw_samples,
        json.dumps({
            'manufacturer': random.choice(['Bosch','Siemens','Honeywell','ABB']),
            'model': f"M{random.randint(100,999)}",
            'install_date': str(date.today() - timedelta(days=random.randint(30, 1000)))
        }),
        json.dumps({
            'temp_high': round(random.uniform(50, 80), 1),
            'temp_low': round(random.uniform(-20, 10), 1),
            'notify': random.choice(['email','sms','slack'])
        }),
        bytes(random.getrandbits(8) for _ in range(random.randint(32, 256))),
        random.choice(SENSOR_LOCATIONS),
        f"Auto-generated reading #{idx+1}" if random.random() > 0.7 else None
    )


# =====================================================================================
# TABLE 4: product_catalog  (catalog — nested JSON, large text, numeric variety)
# =====================================================================================
CATALOG_DDL = """
CREATE TABLE IF NOT EXISTS product_catalog (
    product_id          SERIAL PRIMARY KEY,
    product_uuid        UUID NOT NULL DEFAULT gen_random_uuid(),
    sku                 VARCHAR(40) UNIQUE NOT NULL,
    product_name        VARCHAR(300) NOT NULL,
    category            VARCHAR(100) NOT NULL,
    subcategory         VARCHAR(100),
    brand               VARCHAR(100),
    description_short   VARCHAR(500),
    description_long    TEXT,

    -- Numeric variety
    unit_price          DECIMAL(12,4) NOT NULL,
    wholesale_price     DECIMAL(12,4),
    cost_price          DECIMAL(12,4),
    weight_kg           REAL,
    length_cm           REAL,
    width_cm            REAL,
    height_cm           REAL,
    volume_cm3          DOUBLE PRECISION,
    stock_quantity       INTEGER NOT NULL DEFAULT 0,
    reorder_level       SMALLINT DEFAULT 10,
    max_order_qty       INTEGER,
    popularity_score    DOUBLE PRECISION,
    avg_rating          DECIMAL(3,2),
    total_reviews       INTEGER DEFAULT 0,
    view_count          BIGINT DEFAULT 0,

    -- Boolean
    is_active           BOOLEAN DEFAULT TRUE,
    is_digital          BOOLEAN DEFAULT FALSE,
    is_fragile          BOOLEAN DEFAULT FALSE,
    is_hazardous        BOOLEAN DEFAULT FALSE,
    requires_assembly   BOOLEAN DEFAULT FALSE,
    tax_exempt          BOOLEAN DEFAULT FALSE,

    -- Dates / Timestamps
    launch_date         DATE,
    discontinue_date    DATE,
    last_restock_date   DATE,
    created_at          TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,

    -- Arrays  → Parquet LIST
    tags                TEXT[],
    color_options       VARCHAR(50)[],
    compatible_skus     VARCHAR(40)[],
    warehouse_ids       INTEGER[],

    -- JSON / JSONB  → Parquet JSON / nested structs
    specifications      JSONB,
    shipping_info       JSONB,
    supplier_info       JSONB,
    seo_metadata        JSONB,

    -- Binary
    thumbnail_blob      BYTEA,

    -- Extra text
    internal_notes      TEXT,
    country_of_origin   VARCHAR(100),
    hs_tariff_code      VARCHAR(20),

    CONSTRAINT chk_cat_price CHECK (unit_price >= 0),
    CONSTRAINT chk_cat_stock CHECK (stock_quantity >= 0)
);
CREATE INDEX IF NOT EXISTS idx_cat_sku      ON product_catalog(sku);
CREATE INDEX IF NOT EXISTS idx_cat_category ON product_catalog(category);
CREATE INDEX IF NOT EXISTS idx_cat_brand    ON product_catalog(brand);
CREATE INDEX IF NOT EXISTS idx_cat_active   ON product_catalog(is_active);
"""

CATALOG_INSERT = """
INSERT INTO product_catalog (
    product_uuid, sku, product_name, category, subcategory, brand,
    description_short, description_long,
    unit_price, wholesale_price, cost_price,
    weight_kg, length_cm, width_cm, height_cm, volume_cm3,
    stock_quantity, reorder_level, max_order_qty,
    popularity_score, avg_rating, total_reviews, view_count,
    is_active, is_digital, is_fragile, is_hazardous, requires_assembly, tax_exempt,
    launch_date, discontinue_date, last_restock_date, created_at, updated_at,
    tags, color_options, compatible_skus, warehouse_ids,
    specifications, shipping_info, supplier_info, seo_metadata,
    thumbnail_blob, internal_notes, country_of_origin, hs_tariff_code
) VALUES (
    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
    %s,%s,%s,%s,%s,%s
)
"""

CATEGORIES = {
    'Electronics': ['Sensors','Displays','Controllers','Cables','Batteries'],
    'Mechanical': ['Bearings','Gears','Shafts','Springs','Fasteners'],
    'Safety': ['Helmets','Gloves','Goggles','Vests','Harnesses'],
    'Chemicals': ['Lubricants','Solvents','Adhesives','Coatings','Cleaners'],
    'Tools': ['Drills','Wrenches','Cutters','Meters','Probes'],
}
BRANDS = ['Bosch','3M','Siemens','ABB','Schneider','Honeywell','TE Connectivity','Parker']
COLORS = ['Red','Blue','Green','Black','White','Silver','Yellow','Orange','Gray']

def gen_product(idx: int) -> tuple:
    cat = random.choice(list(CATEGORIES.keys()))
    subcat = random.choice(CATEGORIES[cat])
    brand = random.choice(BRANDS)
    unit_p = Decimal(str(round(random.uniform(1, 5000), 4)))
    ws_p = unit_p * Decimal(str(round(random.uniform(0.5, 0.9), 2)))
    cost_p = ws_p * Decimal(str(round(random.uniform(0.4, 0.8), 2)))
    w = round(random.uniform(0.01, 100), 3)
    l = round(random.uniform(1, 200), 2)
    wd = round(random.uniform(1, 200), 2)
    h = round(random.uniform(1, 200), 2)

    launch = date(random.randint(2015, 2025), random.randint(1, 12), random.randint(1, 28))
    disc = launch + timedelta(days=random.randint(365, 3650)) if random.random() < 0.1 else None
    restock = date.today() - timedelta(days=random.randint(0, 90))

    return (
        str(uuid.uuid4()),
        f"SKU-{cat[:3].upper()}-{idx+1:06d}",
        f"{brand} {subcat} {random.choice(['Pro','Standard','Elite','Eco','Max'])} {random.randint(100,999)}",
        cat, subcat, brand,
        f"High-quality {subcat.lower()} for industrial use. Model #{random.randint(100,999)}.",
        f"Detailed description for {subcat} product by {brand}. " * random.randint(3, 10),
        unit_p, ws_p, cost_p,
        w, l, wd, h, round(l * wd * h, 4),
        random.randint(0, 10000), random.randint(5, 100), random.randint(50, 5000),
        round(random.uniform(0, 100), 8),
        Decimal(str(round(random.uniform(1, 5), 2))),
        random.randint(0, 5000),
        random.randint(0, 1_000_000),
        random.choice([True, True, True, False]),
        random.random() < 0.1,
        random.random() < 0.2,
        cat == 'Chemicals',
        random.random() < 0.3,
        random.random() < 0.05,
        launch, disc, restock,
        datetime.now(timezone.utc) - timedelta(days=random.randint(0, 60)),
        datetime.now(),
        random.sample(['industrial','premium','sale','new','eco','certified','OEM'], k=random.randint(1, 4)),
        random.sample(COLORS, k=random.randint(1, 4)),
        [f"SKU-{random.choice(list(CATEGORIES.keys()))[:3].upper()}-{random.randint(1,9999):06d}"
         for _ in range(random.randint(0, 3))],
        [random.randint(1, 20) for _ in range(random.randint(1, 4))],
        json.dumps({
            'material': random.choice(['Steel','Aluminum','Plastic','Composite','Copper']),
            'operating_temp': f"{random.randint(-40,0)}C to {random.randint(50,150)}C",
            'ip_rating': f"IP{random.choice([54,65,67,68])}",
            'certifications': random.sample(['CE','UL','ISO9001','RoHS','REACH'], k=random.randint(1, 3))
        }),
        json.dumps({
            'ship_class': random.choice(['Standard','Oversize','Hazmat','Fragile']),
            'est_days': random.randint(1, 14),
            'free_shipping_above': round(random.uniform(50, 500), 2)
        }),
        json.dumps({
            'supplier_id': f"SUP-{random.randint(100,999)}",
            'lead_time_days': random.randint(7, 90),
            'moq': random.randint(1, 100),
            'country': random.choice(COUNTRIES)
        }),
        json.dumps({
            'title': f"Buy {subcat} from {brand}",
            'keywords': random.sample(['industrial','tools','safety','electronics','parts'], k=3),
            'og_description': f"Shop {brand} {subcat} at competitive prices."
        }),
        bytes(random.getrandbits(8) for _ in range(64)),
        f"Internal: margin tier {random.choice(['A','B','C'])}, review pending" if random.random() > 0.6 else None,
        random.choice(COUNTRIES),
        f"{random.randint(1000,9999)}.{random.randint(10,99)}.{random.randint(10,99)}"
    )


# =====================================================================================
# Runner
# =====================================================================================
TABLE_CONFIG = [
    ('invoices',        INVOICES_DDL,  INVOICES_INSERT,  gen_invoice),
    ('employees',       EMPLOYEES_DDL, EMPLOYEES_INSERT, gen_employee),
    ('sensor_readings', SENSOR_DDL,    SENSOR_INSERT,    gen_sensor_reading),
    ('product_catalog', CATALOG_DDL,   CATALOG_INSERT,   gen_product),
]

def run(connection_string: str, total_records: int = 1000, batch_size: int = 100):
    with psycopg.connect(connection_string) as conn:
        for table_name, ddl, insert_sql, gen_fn in TABLE_CONFIG:
            print(f"\n{'='*60}")
            print(f"  Processing table: {table_name}")
            print(f"{'='*60}")

            # Ensure a clean table exists
            ensure_table(conn, table_name, ddl)

            # Insert data
            inserted = 0
            for batch_start in range(0, total_records, batch_size):
                batch_end = min(batch_start + batch_size, total_records)
                batch = [gen_fn(i) for i in range(batch_start, batch_end)]

                with conn.cursor() as cur:
                    cur.executemany(insert_sql, batch) # type: ignore
                conn.commit()

                inserted += len(batch)
                print(f"  [{table_name}] Inserted {inserted}/{total_records}")

            print(f"  Completed {table_name}: {inserted} records total.")

    print(f"\nAll tables populated successfully.")


def main():
    # PostgreSQL connection string
    connection_string = "postgresql://sentinel:Test_123_Password@localhost:5432/citadel"

    run(
        connection_string=connection_string,
        total_records=5000,   # per table
        batch_size=500
    )

if __name__ == "__main__":
    main()
