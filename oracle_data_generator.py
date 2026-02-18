# oracle_data_generator.py
# =====================================================================================
# Oracle Data Generator - Full Parquet Data-Type Coverage
# =====================================================================================
# Tables: invoices, employees, sensor_readings, product_catalog
#
# Oracle-specific notes:
#   NUMBER(1) for booleans, CLOB for JSON/text, BLOB for binary,
#   BINARY_DOUBLE/BINARY_FLOAT for IEEE floats, RAW for small binary,
#   TIMESTAMP WITH TIME ZONE, INTERVAL YEAR TO MONTH / DAY TO SECOND
# =====================================================================================

import oracledb
import random
import string
from decimal import Decimal
from datetime import datetime, date, timedelta, time
import uuid
import json
import ipaddress
import os
import sys

# -------------------------------------------------------------------------------------
COUNTRIES = ['USA','Germany','Japan','China','Brazil','India','UK','France',
             'Australia','Canada','South Korea','Singapore']
CURRENCIES = ['USD','EUR','JPY','CNY','BRL','INR','GBP','CHF','AUD','CAD']
PLANTS = ['US-PLANT-01','GER-PLANT-02','JP-PLANT-03','CN-PLANT-04','IN-PLANT-05']
TIMEZONES_LIST = ['UTC','US/Eastern','Europe/Berlin','Asia/Tokyo','Asia/Shanghai',
                  'Asia/Kolkata','Australia/Sydney']
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
SENSOR_LOCATIONS = ['Factory Floor A','Warehouse B','Server Room C','Outdoor Station D',
                    'Cold Storage E','Lab Room F','Rooftop G','Basement H']
CATEGORIES = {
    'Electronics': ['Sensors','Displays','Controllers','Cables','Batteries'],
    'Mechanical': ['Bearings','Gears','Shafts','Springs','Fasteners'],
    'Safety': ['Helmets','Gloves','Goggles','Vests','Harnesses'],
    'Chemicals': ['Lubricants','Solvents','Adhesives','Coatings','Cleaners'],
    'Tools': ['Drills','Wrenches','Cutters','Meters','Probes'],
}
BRANDS = ['Bosch','3M','Siemens','ABB','Schneider','Honeywell','TE Connectivity','Parker']
COLORS = ['Red','Blue','Green','Black','White','Silver','Yellow','Orange','Gray']

def rand_str(n=8):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=n))

def rand_ipv4():
    return str(ipaddress.IPv4Address(random.randint(0x0A000001, 0x0AFFFFFF)))

def rand_mac():
    return ':'.join(f'{random.randint(0,255):02x}' for _ in range(6))

def rand_semver():
    return f"{random.randint(0,9)}.{random.randint(0,99)}.{random.randint(0,999)}"


# =====================================================================================
# Database Helper: Ensure a clean table exists for data generation
# =====================================================================================
def ensure_table(conn, table_name, ddl, indexes=None):
    """
    Ensures that a specified table exists in the database with the latest schema.
    
    If the table already exists, it is dropped (along with any dependent 
    constraints) to prevent schema conflicts or stale data during the seeding 
    process. After ensuring a clean slate, the table is created fresh and any 
    specified indexes are applied.

    Args:
        conn (oracledb.Connection): The active Oracle database connection object.
        table_name (str): The name of the table to check and create.
        ddl (str): The SQL Data Definition Language (CREATE TABLE) statement.
        indexes (list of str, optional): A list of SQL statements to create indexes.
    """
    # Open a cursor to interact with the database context safely
    with conn.cursor() as cur:
        
        # Query Oracle's data dictionary (user_tables) to check if the table exists.
        # We use a bind variable (:tn) for safety and UPPER() since Oracle 
        # stores table names in uppercase by default.
        cur.execute("SELECT COUNT(*) FROM user_tables WHERE table_name = UPPER(:tn)", tn=table_name)
        
        # fetchone() returns a tuple. Grab the first element (the count) to evaluate existence.
        exists = cur.fetchone()[0] > 0
        
        # If the table is found, perform a clean teardown before rebuilding it.
        if exists:
            print(f"  Table {table_name} already exists. Dropping old table...")
            # CASCADE CONSTRAINTS drops the table even if foreign keys reference it
            cur.execute(f"DROP TABLE {table_name} CASCADE CONSTRAINTS")
            
        # Execute the provided DDL string to create the table from scratch
        print(f"  Creating table {table_name}...")
        cur.execute(ddl)
        
        # If a list of index creation statements was provided, apply them sequentially
        if indexes:
            for idx_sql in indexes:
                try:
                    cur.execute(idx_sql)
                except Exception as e:
                    # Catch and log exceptions (like syntax errors or edge-case duplicates)
                    # so a minor index issue doesn't crash the entire generation run.
                    print(f"    Index note: {e}")
                    
        # Commit the transaction so the drop/create operations are saved
        conn.commit()
        print(f"  Table {table_name} ready.")


# =====================================================================================
# TABLE 1: invoices
# =====================================================================================
INVOICES_DDL = """
CREATE TABLE invoices (
    invoice_id          NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    invoice_number      VARCHAR2(50) NOT NULL UNIQUE,
    customer_id         NUMBER NOT NULL,
    customer_code       VARCHAR2(20) NOT NULL,
    customer_name       VARCHAR2(255) NOT NULL,
    customer_address    CLOB NOT NULL,
    billing_country     VARCHAR2(100) NOT NULL,
    shipping_country    VARCHAR2(100),
    currency_code       VARCHAR2(3) NOT NULL,
    payment_terms       VARCHAR2(50),
    sales_representative VARCHAR2(100),
    total_amount        NUMBER(15,2) NOT NULL,
    tax_amount          NUMBER(12,2) NOT NULL,
    discount_amount     NUMBER(10,2) DEFAULT 0.00,
    shipping_cost       NUMBER(10,2) DEFAULT 0.00,
    subtotal_amount     NUMBER(15,2) NOT NULL,
    items_count         NUMBER(4) NOT NULL,
    revision_number     NUMBER(4) DEFAULT 0,
    processing_days     NUMBER(4),
    exchange_rate       BINARY_DOUBLE DEFAULT 1.0,
    tax_rate            BINARY_DOUBLE NOT NULL,
    is_paid             NUMBER(1) DEFAULT 0 CHECK (is_paid IN (0,1)),
    is_shipped          NUMBER(1) DEFAULT 0 CHECK (is_shipped IN (0,1)),
    is_recurring        NUMBER(1) DEFAULT 0 CHECK (is_recurring IN (0,1)),
    requires_approval   NUMBER(1) DEFAULT 0 CHECK (requires_approval IN (0,1)),
    is_international    NUMBER(1) DEFAULT 0 CHECK (is_international IN (0,1)),
    invoice_date        DATE NOT NULL,
    due_date            DATE NOT NULL,
    created_datetime    TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_datetime    TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    payment_date        TIMESTAMP WITH TIME ZONE,
    shipment_date       TIMESTAMP WITH TIME ZONE,
    approval_date       TIMESTAMP WITH TIME ZONE,
    invoice_timezone    VARCHAR2(50) DEFAULT 'UTC',
    invoice_items       CLOB NOT NULL,
    digital_signature   BLOB,
    metadata_json       CLOB,
    status              VARCHAR2(20) DEFAULT 'DRAFT',
    project_code        VARCHAR2(30),
    cost_center         VARCHAR2(20),
    manufacturing_plant VARCHAR2(50),
    quality_check_passed NUMBER(1) CHECK (quality_check_passed IN (0,1)),
    compliance_verified  NUMBER(1) CHECK (compliance_verified IN (0,1)),
    CONSTRAINT chk_inv_total CHECK (total_amount >= 0),
    CONSTRAINT chk_inv_tax   CHECK (tax_amount >= 0),
    CONSTRAINT chk_inv_disc  CHECK (discount_amount >= 0)
)
"""
INVOICES_IDX = [
    "CREATE INDEX idx_inv_cust   ON invoices(customer_code)",
    "CREATE INDEX idx_inv_date   ON invoices(invoice_date)",
    "CREATE INDEX idx_inv_paid   ON invoices(is_paid)",
    "CREATE INDEX idx_inv_status ON invoices(status)",
]
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
    :1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18,:19,:20,
    :21,:22,:23,:24,:25,:26,:27,:28,:29,:30,:31,:32,:33,:34,:35,:36,:37,:38,:39,:40,:41
)
"""

def gen_invoice(idx):
    billing = random.choice(COUNTRIES)
    shipping = random.choice(COUNTRIES)
    items_count = random.randint(1, 15)
    items, subtotal = [], Decimal('0.00')
    for j in range(items_count):
        up = Decimal(str(round(random.uniform(10, 1000), 2)))
        qty = random.randint(1, 50)
        total = up * qty
        items.append({'item_id': j+1, 'product_code': f"PROD-{random.randint(1000,9999)}",
                      'quantity': qty, 'unit_price': float(up), 'total_amount': float(total)})
        subtotal += total
    discount = Decimal(str(round(random.uniform(0, min(float(subtotal*Decimal('0.2')), 500)), 2)))
    ship_cost = Decimal(str(round(random.uniform(0, 200), 2)))
    tax_rate = round(random.uniform(0.05, 0.25), 3)
    tax_amt = (subtotal - discount) * Decimal(str(tax_rate))
    total_amt = subtotal - discount + tax_amt + ship_cost
    base_dt = date.today() - timedelta(days=random.randint(0, 365))
    created = datetime.now() - timedelta(days=random.randint(0, 30))
    pay_dt = created - timedelta(days=random.randint(1,15)) if random.random() > 0.5 else None
    ship_dt = created - timedelta(days=random.randint(1,10)) if random.random() > 0.5 else None
    appr_dt = created - timedelta(days=random.randint(1,5)) if random.random() > 0.5 else None

    return (
        f"INV-{date.today().strftime('%Y%m')}-{idx+1:06d}",
        random.randint(1000,9999), f"CUST-{random.randint(10000,99999)}",
        f"Customer {random.randint(1,1000)} Corp.",
        f"{random.randint(1,9999)} Main St, City {random.randint(1,100)}, {billing}",
        billing, shipping, random.choice(CURRENCIES),
        random.choice(['NET30','NET60','DUE_ON_RECEIPT','NET15']),
        f"SalesRep-{random.randint(1,50)}",
        float(total_amt), float(tax_amt), float(discount), float(ship_cost), float(subtotal),
        items_count, random.randint(0,5), random.randint(1,10),
        round(random.uniform(0.8,1.2), 4), tax_rate,
        1 if random.random()>0.5 else 0, 1 if random.random()>0.5 else 0,
        1 if random.random()>0.5 else 0, 1 if random.random()>0.5 else 0,
        1 if billing != shipping else 0,
        base_dt, base_dt + timedelta(days=random.randint(15,90)),
        created, pay_dt, ship_dt, appr_dt,
        random.choice(TIMEZONES_LIST), json.dumps(items),
        bytes(random.getrandbits(8) for _ in range(64)),
        json.dumps({'version':'1.0','batch_id': str(uuid.uuid4())}),
        random.choice(['DRAFT','SENT','PAID','OVERDUE','CANCELLED']),
        f"PROJ-{random.randint(1000,9999)}", f"CC-{random.randint(100,999)}",
        random.choice(PLANTS),
        1 if random.random()>0.5 else 0, 1 if random.random()>0.5 else 0
    )


# =====================================================================================
# TABLE 2: employees (Oracle-specific: RAW(16) option, INTERVAL types)
# =====================================================================================
EMPLOYEES_DDL = """
CREATE TABLE employees (
    employee_id         NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    employee_uuid       VARCHAR2(36) NOT NULL,
    employee_uuid_raw   RAW(16),
    employee_code       VARCHAR2(20) NOT NULL UNIQUE,
    first_name          VARCHAR2(100) NOT NULL,
    last_name           VARCHAR2(100) NOT NULL,
    email               VARCHAR2(255) NOT NULL,
    phone_number        VARCHAR2(30),
    gender              VARCHAR2(20) CHECK (gender IN ('Male','Female','Non-Binary','Prefer Not to Say')),
    employment_type     VARCHAR2(30) NOT NULL CHECK (employment_type IN ('Full-Time','Part-Time','Contract','Intern')),
    department          VARCHAR2(100) NOT NULL,
    job_title           VARCHAR2(150) NOT NULL,
    job_level           NUMBER(3) NOT NULL,
    base_salary         NUMBER(12,2) NOT NULL CHECK (base_salary >= 0),
    bonus_pct           BINARY_FLOAT,
    stock_options       NUMBER(10) DEFAULT 0,
    years_experience    NUMBER(3),
    employee_rating     BINARY_DOUBLE,
    badge_number        NUMBER(18),
    is_active           NUMBER(1) DEFAULT 1 CHECK (is_active IN (0,1)),
    is_manager          NUMBER(1) DEFAULT 0 CHECK (is_manager IN (0,1)),
    has_remote_access   NUMBER(1) DEFAULT 0 CHECK (has_remote_access IN (0,1)),
    background_check_ok NUMBER(1) CHECK (background_check_ok IN (0,1)),
    date_of_birth       DATE,
    hire_date           DATE NOT NULL,
    termination_date    DATE,
    last_login_time     TIMESTAMP,
    shift_start         TIMESTAMP WITH TIME ZONE,
    created_at          TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    probation_interval  INTERVAL YEAR TO MONTH,
    tenure_interval     INTERVAL DAY(9) TO SECOND,
    office_ip           VARCHAR2(45),
    vpn_mac             VARCHAR2(17),
    skills_json         CLOB,
    certifications_json CLOB,
    project_ids_json    CLOB,
    address_json        CLOB,
    emergency_contact   CLOB,
    preferences_json    CLOB,
    profile_photo_thumb BLOB,
    bio                 CLOB,
    notes               CLOB
)
"""
EMPLOYEES_IDX = [
    "CREATE INDEX idx_emp_uuid ON employees(employee_uuid)",
    "CREATE INDEX idx_emp_dept ON employees(department)",
    "CREATE INDEX idx_emp_active ON employees(is_active)",
]
EMPLOYEES_INSERT = """
INSERT INTO employees (
    employee_uuid, employee_uuid_raw, employee_code, first_name, last_name,
    email, phone_number, gender, employment_type, department, job_title, job_level,
    base_salary, bonus_pct, stock_options, years_experience, employee_rating, badge_number,
    is_active, is_manager, has_remote_access, background_check_ok,
    date_of_birth, hire_date, termination_date, last_login_time, shift_start,
    created_at, updated_at, probation_interval, tenure_interval,
    office_ip, vpn_mac,
    skills_json, certifications_json, project_ids_json,
    address_json, emergency_contact, preferences_json,
    profile_photo_thumb, bio, notes
) VALUES (
    :1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18,
    :19,:20,:21,:22,:23,:24,:25,:26,:27,:28,:29,:30,:31,:32,:33,
    :34,:35,:36,:37,:38,:39,:40,:41,:42
)
"""

def gen_employee(idx):
    fn = random.choice(FIRST_NAMES)
    ln = random.choice(LAST_NAMES)
    emp_uuid = uuid.uuid4()
    dob = date(random.randint(1960,2002), random.randint(1,12), random.randint(1,28))
    hire = date(random.randint(2010,2025), random.randint(1,12), random.randint(1,28))
    term = hire + timedelta(days=random.randint(180,1800)) if random.random() < 0.15 else None
    tenure_days = random.randint(30, 3650)

    return (
        str(emp_uuid),
        emp_uuid.bytes,                                     # RAW(16)
        f"EMP-{idx+1:06d}", fn, ln,
        f"{fn.lower()}.{ln.lower()}{random.randint(1,99)}@example.com",
        f"+{random.randint(1,99)}-{random.randint(100,999)}-{random.randint(1000,9999)}",
        random.choice(['Male','Female','Non-Binary','Prefer Not to Say']),
        random.choice(['Full-Time','Part-Time','Contract','Intern']),
        random.choice(DEPARTMENTS), random.choice(JOB_TITLES), random.randint(1,10),
        float(round(random.uniform(30000,250000), 2)),
        round(random.uniform(0,0.30), 4),
        random.randint(0,50000), random.randint(0,35),
        round(random.uniform(1.0,5.0), 6), random.randint(100000,999999),
        1 if random.random()>0.5 else 0, 1 if random.random()>0.5 else 0,
        1 if random.random()>0.5 else 0,
        1 if random.random()>0.3 else 0,
        dob, hire, term,
        datetime(2025, 1, 1, random.randint(0,23), random.randint(0,59), random.randint(0,59)),
        datetime(2025, 1, 1, random.randint(6,10), 0, 0),
        datetime.now() - timedelta(days=random.randint(0,30)),
        datetime.now() - timedelta(days=random.randint(0,10)),
        f"+00-{random.choice([3,6]):02d}",                  # INTERVAL YEAR TO MONTH: '+00-03' = 3 months
        f"+{tenure_days:09d} 08:00:00.000000",              # INTERVAL DAY TO SECOND
        rand_ipv4(), rand_mac(),
        json.dumps(random.sample(SKILLS_POOL, k=random.randint(2,7))),
        json.dumps(random.sample(CERTS_POOL, k=random.randint(0,4))),
        json.dumps([random.randint(1000,9999) for _ in range(random.randint(1,5))]),
        json.dumps({'street': f"{random.randint(1,9999)} Oak St",
                    'city': random.choice(['Tokyo','Berlin','Mumbai','NYC']),
                    'country': random.choice(COUNTRIES)}),
        json.dumps({'name': f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}",
                    'phone': f"+1-555-{random.randint(1000,9999)}"}),
        json.dumps({'theme': random.choice(['dark','light']),
                    'language': random.choice(['en','de','ja','hi'])}),
        bytes(random.getrandbits(8) for _ in range(128)),
        f"Experienced {random.choice(JOB_TITLES).lower()} with {random.randint(1,20)} years.",
        f"Note: {rand_str(50)}" if random.random() > 0.5 else None
    )


# =====================================================================================
# TABLE 3: sensor_readings (Oracle: NUMBER(38) max precision, RAW for checksums)
# =====================================================================================
SENSOR_DDL = """
CREATE TABLE sensor_readings (
    reading_id          NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    reading_uuid        VARCHAR2(36) NOT NULL,
    device_id           VARCHAR2(50) NOT NULL,
    device_serial       CHAR(16) NOT NULL,
    firmware_version    VARCHAR2(20),
    temperature_c       NUMBER(8,4),
    humidity_pct        NUMBER(6,3),
    pressure_hpa        NUMBER(10,4),
    voltage             BINARY_FLOAT,
    current_amps        BINARY_DOUBLE,
    power_watts         NUMBER(12,6),
    latitude            NUMBER(10,7),
    longitude           NUMBER(11,7),
    altitude_m          BINARY_FLOAT,
    signal_strength_dbm NUMBER(5),
    error_code          NUMBER(10) DEFAULT 0,
    uptime_seconds      NUMBER(18),
    is_anomaly          NUMBER(1) DEFAULT 0 CHECK (is_anomaly IN (0,1)),
    is_calibrated       NUMBER(1) DEFAULT 1 CHECK (is_calibrated IN (0,1)),
    battery_low         NUMBER(1) DEFAULT 0 CHECK (battery_low IN (0,1)),
    reading_timestamp   TIMESTAMP WITH TIME ZONE NOT NULL,
    server_received_at  TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    reading_date        DATE NOT NULL,
    reading_time        TIMESTAMP,
    tag_ids_json        CLOB,
    sensor_labels_json  CLOB,
    raw_samples_json    CLOB,
    device_metadata     CLOB,
    alert_config        CLOB,
    raw_payload         BLOB,
    checksum_raw        RAW(32),
    location_name       VARCHAR2(200),
    notes               CLOB
)
"""
SENSOR_IDX = [
    "CREATE INDEX idx_sr_device ON sensor_readings(device_id)",
    "CREATE INDEX idx_sr_ts     ON sensor_readings(reading_timestamp)",
    "CREATE INDEX idx_sr_date   ON sensor_readings(reading_date)",
    "CREATE INDEX idx_sr_anom   ON sensor_readings(is_anomaly)",
]
SENSOR_INSERT = """
INSERT INTO sensor_readings (
    reading_uuid, device_id, device_serial, firmware_version,
    temperature_c, humidity_pct, pressure_hpa, voltage, current_amps, power_watts,
    latitude, longitude, altitude_m, signal_strength_dbm, error_code, uptime_seconds,
    is_anomaly, is_calibrated, battery_low,
    reading_timestamp, server_received_at, reading_date, reading_time,
    tag_ids_json, sensor_labels_json, raw_samples_json,
    device_metadata, alert_config,
    raw_payload, checksum_raw, location_name, notes
) VALUES (
    :1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18,:19,
    :20,:21,:22,:23,:24,:25,:26,:27,:28,:29,:30,:31,:32
)
"""

def gen_sensor_reading(idx):
    ts = datetime.now() - timedelta(days=random.randint(0,90), hours=random.randint(0,23),
                                     minutes=random.randint(0,59), seconds=random.randint(0,59))
    return (
        str(uuid.uuid4()), f"DEV-{random.randint(1,200):04d}",
        rand_str(16).upper(), rand_semver(),
        float(round(random.uniform(-40,85), 4)),
        float(round(random.uniform(0,100), 3)),
        float(round(random.uniform(900,1100), 4)),
        round(random.uniform(0,48), 3), round(random.uniform(0,10), 8),
        float(round(random.uniform(0,5000), 6)),
        float(round(random.uniform(-90,90), 7)),
        float(round(random.uniform(-180,180), 7)),
        round(random.uniform(-50,5000), 2),
        random.randint(-120,0), random.choice([0,0,0,1,2,99]),
        random.randint(0, 10_000_000),
        1 if random.random() < 0.05 else 0,
        0 if random.random() < 0.02 else 1,
        1 if random.random() < 0.1 else 0,
        ts, ts + timedelta(milliseconds=random.randint(50,2000)),
        ts, ts,  # reading_date (DATE) and reading_time (TIMESTAMP)
        json.dumps([random.randint(1,500) for _ in range(random.randint(1,5))]),
        json.dumps(random.sample(['temp','humidity','pressure','vibration'], k=random.randint(1,3))),
        json.dumps([round(random.uniform(-50,150), 6) for _ in range(random.randint(5,20))]),
        json.dumps({'manufacturer': random.choice(['Bosch','Siemens','Honeywell']),
                    'model': f"M{random.randint(100,999)}"}),
        json.dumps({'temp_high': round(random.uniform(50,80), 1)}),
        bytes(random.getrandbits(8) for _ in range(random.randint(32,256))),
        bytes(random.getrandbits(8) for _ in range(32)),     # RAW(32)
        random.choice(SENSOR_LOCATIONS),
        f"Reading #{idx+1}" if random.random() > 0.7 else None
    )


# =====================================================================================
# TABLE 4: product_catalog
# =====================================================================================
CATALOG_DDL = """
CREATE TABLE product_catalog (
    product_id          NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    product_uuid        VARCHAR2(36) NOT NULL,
    sku                 VARCHAR2(40) NOT NULL UNIQUE,
    product_name        VARCHAR2(300) NOT NULL,
    category            VARCHAR2(100) NOT NULL,
    subcategory         VARCHAR2(100),
    brand               VARCHAR2(100),
    description_short   VARCHAR2(500),
    description_long    CLOB,
    unit_price          NUMBER(12,4) NOT NULL CHECK (unit_price >= 0),
    wholesale_price     NUMBER(12,4),
    cost_price          NUMBER(12,4),
    weight_kg           BINARY_FLOAT,
    length_cm           BINARY_FLOAT,
    width_cm            BINARY_FLOAT,
    height_cm           BINARY_FLOAT,
    volume_cm3          BINARY_DOUBLE,
    stock_quantity      NUMBER(10) DEFAULT 0 NOT NULL CHECK (stock_quantity >= 0),
    reorder_level       NUMBER(5) DEFAULT 10,
    max_order_qty       NUMBER(10),
    popularity_score    BINARY_DOUBLE,
    avg_rating          NUMBER(3,2),
    total_reviews       NUMBER(10) DEFAULT 0,
    view_count          NUMBER(18) DEFAULT 0,
    is_active           NUMBER(1) DEFAULT 1 CHECK (is_active IN (0,1)),
    is_digital          NUMBER(1) DEFAULT 0 CHECK (is_digital IN (0,1)),
    is_fragile          NUMBER(1) DEFAULT 0 CHECK (is_fragile IN (0,1)),
    is_hazardous        NUMBER(1) DEFAULT 0 CHECK (is_hazardous IN (0,1)),
    requires_assembly   NUMBER(1) DEFAULT 0 CHECK (requires_assembly IN (0,1)),
    tax_exempt          NUMBER(1) DEFAULT 0 CHECK (tax_exempt IN (0,1)),
    launch_date         DATE,
    discontinue_date    DATE,
    last_restock_date   DATE,
    created_at          TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    tags_json           CLOB,
    color_options_json  CLOB,
    compatible_skus_json CLOB,
    warehouse_ids_json  CLOB,
    specifications      CLOB,
    shipping_info       CLOB,
    supplier_info       CLOB,
    seo_metadata        CLOB,
    thumbnail_blob      BLOB,
    internal_notes      CLOB,
    country_of_origin   VARCHAR2(100),
    hs_tariff_code      VARCHAR2(20)
)
"""
CATALOG_IDX = [
    "CREATE INDEX idx_cat_cat    ON product_catalog(category)",
    "CREATE INDEX idx_cat_brand  ON product_catalog(brand)",
    "CREATE INDEX idx_cat_active ON product_catalog(is_active)",
]
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
    tags_json, color_options_json, compatible_skus_json, warehouse_ids_json,
    specifications, shipping_info, supplier_info, seo_metadata,
    thumbnail_blob, internal_notes, country_of_origin, hs_tariff_code
) VALUES (
    :1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18,:19,:20,
    :21,:22,:23,:24,:25,:26,:27,:28,:29,:30,:31,:32,:33,:34,:35,:36,:37,:38,
    :39,:40,:41,:42,:43,:44,:45,:46
)
"""

def gen_product(idx):
    cat = random.choice(list(CATEGORIES.keys()))
    subcat = random.choice(CATEGORIES[cat])
    brand = random.choice(BRANDS)
    up = round(random.uniform(1,5000), 4)
    ws = round(up * random.uniform(0.5,0.9), 4)
    cp = round(ws * random.uniform(0.4,0.8), 4)
    l, w, h = round(random.uniform(1,200),2), round(random.uniform(1,200),2), round(random.uniform(1,200),2)
    launch = date(random.randint(2015,2025), random.randint(1,12), random.randint(1,28))
    disc = launch + timedelta(days=random.randint(365,3650)) if random.random() < 0.1 else None

    return (
        str(uuid.uuid4()), f"SKU-{cat[:3].upper()}-{idx+1:06d}",
        f"{brand} {subcat} {random.choice(['Pro','Standard','Elite'])} {random.randint(100,999)}",
        cat, subcat, brand,
        f"High-quality {subcat.lower()} for industrial use.",
        f"Detailed description for {subcat} product by {brand}. " * random.randint(3,10),
        up, ws, cp,
        round(random.uniform(0.01,100), 3), l, w, h, round(l*w*h, 4),
        random.randint(0,10000), random.randint(5,100), random.randint(50,5000),
        round(random.uniform(0,100), 8),
        float(round(random.uniform(1,5), 2)),
        random.randint(0,5000), random.randint(0,1_000_000),
        1 if random.random()>0.25 else 0, 1 if random.random()<0.1 else 0,
        1 if random.random()<0.2 else 0, 1 if cat=='Chemicals' else 0,
        1 if random.random()<0.3 else 0, 1 if random.random()<0.05 else 0,
        launch, disc, date.today() - timedelta(days=random.randint(0,90)),
        datetime.now() - timedelta(days=random.randint(0,60)), datetime.now(),
        json.dumps(random.sample(['industrial','premium','sale','new','eco','certified'], k=random.randint(1,4))),
        json.dumps(random.sample(COLORS, k=random.randint(1,4))),
        json.dumps([f"SKU-{random.choice(list(CATEGORIES.keys()))[:3].upper()}-{random.randint(1,9999):06d}"
                    for _ in range(random.randint(0,3))]),
        json.dumps([random.randint(1,20) for _ in range(random.randint(1,4))]),
        json.dumps({'material': random.choice(['Steel','Aluminum','Plastic']),
                    'ip_rating': f"IP{random.choice([54,65,67,68])}"}),
        json.dumps({'ship_class': random.choice(['Standard','Oversize','Hazmat']),
                    'est_days': random.randint(1,14)}),
        json.dumps({'supplier_id': f"SUP-{random.randint(100,999)}",
                    'lead_time_days': random.randint(7,90)}),
        json.dumps({'title': f"Buy {subcat} from {brand}"}),
        bytes(random.getrandbits(8) for _ in range(64)),
        f"Margin tier {random.choice(['A','B','C'])}" if random.random() > 0.6 else None,
        random.choice(COUNTRIES),
        f"{random.randint(1000,9999)}.{random.randint(10,99)}.{random.randint(10,99)}"
    )


# =====================================================================================
# Runner
# =====================================================================================
TABLE_CONFIG = [
    ('invoices',        INVOICES_DDL,  INVOICES_IDX,  INVOICES_INSERT,  gen_invoice),
    ('employees',       EMPLOYEES_DDL, EMPLOYEES_IDX, EMPLOYEES_INSERT, gen_employee),
    ('sensor_readings', SENSOR_DDL,    SENSOR_IDX,    SENSOR_INSERT,    gen_sensor_reading),
    ('product_catalog', CATALOG_DDL,   CATALOG_IDX,   CATALOG_INSERT,   gen_product),
]

def run(user, password, dsn, total_records=1000, batch_size=100):
    conn = oracledb.connect(user=user, password=password, dsn=dsn)
    try:
        for table_name, ddl, indexes, insert_sql, gen_fn in TABLE_CONFIG:
            print(f"\n{'='*60}")
            print(f"  Processing table: {table_name}")
            print(f"{'='*60}")

            ensure_table(conn, table_name, ddl, indexes)

            inserted = 0
            for batch_start in range(0, total_records, batch_size):
                batch_end = min(batch_start + batch_size, total_records)
                batch = [gen_fn(i) for i in range(batch_start, batch_end)]
                with conn.cursor() as cur:
                    cur.executemany(insert_sql, batch)
                conn.commit()
                inserted += len(batch)
                print(f"  [{table_name}] Inserted {inserted}/{total_records}")

            print(f"  Completed {table_name}: {inserted} records total.")
    finally:
        conn.close()

    print(f"\nAll tables populated successfully.")


def main():
    # ---------------------------------------------------------------------------------
    # Oracle connection config
    # ---------------------------------------------------------------------------------
    # For thin mode (no Oracle Client needed - recommended):
    #   Just connect directly, oracledb uses thin mode by default since v2.0
    #
    # For thick mode (Oracle Instant Client required):
    #   Uncomment the init_oracle_client line below and set your lib_dir path.
    #   On Windows with your setup:
    #     oracledb.init_oracle_client(lib_dir=r"C:\path\to\oracle_instantclient")
    #   Make sure the PATH env is also set (see README).
    # ---------------------------------------------------------------------------------

    # Uncomment ONE of the following based on your mode:

    # --- Option A: Thin mode (default, no client needed) ---
    # Nothing to do, just connect.

    # --- Option B: Thick mode (Oracle Instant Client) ---
    # oracledb.init_oracle_client(lib_dir=r"C:\path\to\oracle_instantclient")

    user = "sentinel"
    password = "Test_123_Password"
    dsn = "localhost:1521/FREEPDB1"

    run(
        user=user,
        password=password,
        dsn=dsn,
        total_records=5000,
        batch_size=500
    )

if __name__ == "__main__":
    main()
