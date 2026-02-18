# mysql_data_generator.py
# =====================================================================================
# MySQL Data Generator — Full Parquet Data-Type Coverage
# =====================================================================================
# Tables: invoices, employees, sensor_readings, product_catalog
#
# MySQL-specific type notes:
#   - No native UUID type → CHAR(36) or BINARY(16)
#   - No native ARRAY type → JSON arrays
#   - BOOLEAN → TINYINT(1)
#   - No INTERVAL type → VARCHAR representation
#   - JSON type available since 5.7
#   - ENUM type is native
# =====================================================================================

import pymysql
import random
import string
from decimal import Decimal
from datetime import datetime, date, timedelta, time, timezone
import uuid
import json
import ipaddress
import sys

# -------------------------------------------------------------------------------------
# Shared helpers
# -------------------------------------------------------------------------------------
COUNTRIES = ['USA','Germany','Japan','China','Brazil','India','UK','France',
             'Australia','Canada','South Korea','Singapore']
CURRENCIES = ['USD','EUR','JPY','CNY','BRL','INR','GBP','CHF','AUD','CAD']
PLANTS = ['US-PLANT-01','GER-PLANT-02','JP-PLANT-03','CN-PLANT-04','IN-PLANT-05']
TIMEZONES = ['UTC','US/Eastern','Europe/Berlin','Asia/Tokyo','Asia/Shanghai',
             'Asia/Kolkata','Australia/Sydney']

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
def ensure_table(conn, table_name: str, ddl: str):
    """
    Ensures that a specified table exists in the database with the latest schema.
    Drops the table if it already exists to provide a clean slate.
    """
    with conn.cursor() as cur:
        # MySQL handles existence checks natively during the drop
        print(f"  Checking and dropping {table_name} if it exists...")
        cur.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        # Execute the provided DDL string to create the table and its indexes from scratch
        print(f"  Creating table {table_name}...")
        cur.execute(ddl)
        
    # Commit the transaction so the drop/create operations are saved
    conn.commit()
    print(f"  Table '{table_name}' ready.")

# =====================================================================================
# TABLE 1: invoices
# =====================================================================================
INVOICES_DDL = """
CREATE TABLE IF NOT EXISTS invoices (
    invoice_id          INT AUTO_INCREMENT PRIMARY KEY,
    invoice_number      VARCHAR(50) UNIQUE NOT NULL,
    customer_id         INT NOT NULL,
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

    exchange_rate       DOUBLE DEFAULT 1.0,
    tax_rate            DOUBLE NOT NULL,

    is_paid             BOOLEAN DEFAULT FALSE,
    is_shipped          BOOLEAN DEFAULT FALSE,
    is_recurring        BOOLEAN DEFAULT FALSE,
    requires_approval   BOOLEAN DEFAULT FALSE,
    is_international    BOOLEAN DEFAULT FALSE,

    invoice_date        DATE NOT NULL,
    due_date            DATE NOT NULL,
    created_datetime    DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_datetime    DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    payment_date        DATETIME NULL,
    shipment_date       DATETIME NULL,
    approval_date       DATETIME NULL,

    invoice_timezone    VARCHAR(50) DEFAULT 'UTC',
    invoice_items       JSON NOT NULL,
    digital_signature   BLOB,
    metadata_json       JSON,
    status              VARCHAR(20) DEFAULT 'DRAFT',

    project_code        VARCHAR(30),
    cost_center         VARCHAR(20),
    manufacturing_plant VARCHAR(50),
    quality_check_passed BOOLEAN,
    compliance_verified  BOOLEAN,

    INDEX idx_inv_number (invoice_number),
    INDEX idx_inv_cust   (customer_code),
    INDEX idx_inv_date   (invoice_date),
    INDEX idx_inv_paid   (is_paid),
    INDEX idx_inv_status (status),

    CHECK (total_amount >= 0),
    CHECK (tax_amount >= 0),
    CHECK (discount_amount >= 0)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
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

def gen_invoice(idx):
    billing = random.choice(COUNTRIES)
    shipping = random.choice(COUNTRIES)
    items_count = random.randint(1, 15)
    items, subtotal = [], Decimal('0.00')
    for j in range(items_count):
        up = Decimal(str(round(random.uniform(10, 1000), 2)))
        qty = random.randint(1, 50)
        total = up * qty
        items.append({
            'item_id': j+1, 'product_code': f"PROD-{random.randint(1000,9999)}",
            'description': f'Part {random.randint(100,999)}', 'quantity': qty,
            'unit_price': float(up), 'total_amount': float(total),
            'uom': random.choice(['PCS','KG','M','L']),
            'hs_code': f"{random.randint(1000,9999)}.{random.randint(10,99)}"
        })
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
        random.choice([True,False]), random.choice([True,False]),
        random.choice([True,False]), random.choice([True,False]),
        billing != shipping,
        base_dt, base_dt + timedelta(days=random.randint(15,90)),
        created, pay_dt, ship_dt, appr_dt,
        random.choice(TIMEZONES), json.dumps(items),
        bytes(random.getrandbits(8) for _ in range(64)),
        json.dumps({'version':'1.0','batch_id': str(uuid.uuid4())}),
        random.choice(['DRAFT','SENT','PAID','OVERDUE','CANCELLED']),
        f"PROJ-{random.randint(1000,9999)}", f"CC-{random.randint(100,999)}",
        random.choice(PLANTS), random.choice([True,False]), random.choice([True,False])
    )


# =====================================================================================
# TABLE 2: employees  (MySQL uses ENUM natively, JSON for arrays)
# =====================================================================================
EMPLOYEES_DDL = """
CREATE TABLE IF NOT EXISTS employees (
    employee_id         INT AUTO_INCREMENT PRIMARY KEY,
    employee_uuid       CHAR(36) NOT NULL,
    employee_code       VARCHAR(20) UNIQUE NOT NULL,
    first_name          VARCHAR(100) NOT NULL,
    last_name           VARCHAR(100) NOT NULL,
    full_name           VARCHAR(201) GENERATED ALWAYS AS (CONCAT(first_name, ' ', last_name)) STORED,
    email               VARCHAR(255) NOT NULL,
    phone_number        VARCHAR(30),

    gender              ENUM('Male','Female','Non-Binary','Prefer Not to Say'),
    employment_type     ENUM('Full-Time','Part-Time','Contract','Intern') NOT NULL,
    department          VARCHAR(100) NOT NULL,
    job_title           VARCHAR(150) NOT NULL,
    job_level           SMALLINT NOT NULL,

    base_salary         DECIMAL(12,2) NOT NULL,
    bonus_pct           FLOAT,                              -- 32-bit
    stock_options       INT DEFAULT 0,
    years_experience    SMALLINT,
    employee_rating     DOUBLE,                             -- 64-bit
    badge_number        BIGINT,

    is_active           BOOLEAN DEFAULT TRUE,
    is_manager          BOOLEAN DEFAULT FALSE,
    has_remote_access   BOOLEAN DEFAULT FALSE,
    background_check_ok BOOLEAN,

    date_of_birth       DATE,
    hire_date           DATE NOT NULL,
    termination_date    DATE,
    last_login_time     TIME,
    shift_start         TIME,
    created_at          DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at          DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    probation_period    VARCHAR(30),                         -- no native INTERVAL

    office_ip           VARCHAR(45),                         -- IPv4/IPv6 string
    vpn_mac             VARCHAR(17),

    -- Arrays stored as JSON
    skills              JSON,
    certifications      JSON,
    project_ids         JSON,

    address_json        JSON,
    emergency_contact   JSON,
    preferences         JSON,

    profile_photo_thumb BLOB,
    bio                 TEXT,
    notes               TEXT,

    INDEX idx_emp_uuid (employee_uuid),
    INDEX idx_emp_code (employee_code),
    INDEX idx_emp_dept (department),
    INDEX idx_emp_active (is_active),

    CHECK (base_salary >= 0)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
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

def gen_employee(idx):
    fn = random.choice(FIRST_NAMES)
    ln = random.choice(LAST_NAMES)
    dob = date(random.randint(1960,2002), random.randint(1,12), random.randint(1,28))
    hire = date(random.randint(2010,2025), random.randint(1,12), random.randint(1,28))
    term = hire + timedelta(days=random.randint(180,1800)) if random.random() < 0.15 else None

    return (
        str(uuid.uuid4()), f"EMP-{idx+1:06d}", fn, ln,
        f"{fn.lower()}.{ln.lower()}{random.randint(1,99)}@example.com",
        f"+{random.randint(1,99)}-{random.randint(100,999)}-{random.randint(1000,9999)}",
        random.choice(['Male','Female','Non-Binary','Prefer Not to Say']),
        random.choice(['Full-Time','Part-Time','Contract','Intern']),
        random.choice(DEPARTMENTS), random.choice(JOB_TITLES), random.randint(1,10),
        float(round(random.uniform(30000,250000), 2)),
        round(random.uniform(0,0.30), 4),
        random.randint(0,50000), random.randint(0,35),
        round(random.uniform(1.0,5.0), 6), random.randint(100000,999999),
        random.choice([True,False]), random.choice([True,False]),
        random.choice([True,False]), random.choice([True,False,None]),
        dob, hire, term,
        time(random.randint(0,23), random.randint(0,59), random.randint(0,59)),
        time(random.randint(6,10), 0, 0),
        datetime.now() - timedelta(days=random.randint(0,30)),
        datetime.now() - timedelta(days=random.randint(0,10)),
        f"{random.choice([3,6])} months",
        rand_ipv4(), rand_mac(),
        json.dumps(random.sample(SKILLS_POOL, k=random.randint(2,7))),
        json.dumps(random.sample(CERTS_POOL, k=random.randint(0,4))),
        json.dumps([random.randint(1000,9999) for _ in range(random.randint(1,5))]),
        json.dumps({'street': f"{random.randint(1,9999)} Oak St",
                    'city': random.choice(['Tokyo','Berlin','Mumbai','NYC']),
                    'country': random.choice(COUNTRIES)}),
        json.dumps({'name': f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}",
                    'phone': f"+1-555-{random.randint(1000,9999)}",
                    'relation': random.choice(['Spouse','Parent','Sibling'])}),
        json.dumps({'theme': random.choice(['dark','light']),
                    'language': random.choice(['en','de','ja','hi'])}),
        bytes(random.getrandbits(8) for _ in range(128)),
        f"Experienced {random.choice(JOB_TITLES).lower()} with {random.randint(1,20)} years.",
        f"Note: {rand_str(50)}" if random.random() > 0.5 else None
    )


# =====================================================================================
# TABLE 3: sensor_readings
# =====================================================================================
SENSOR_DDL = """
CREATE TABLE IF NOT EXISTS sensor_readings (
    reading_id          BIGINT AUTO_INCREMENT PRIMARY KEY,
    reading_uuid        CHAR(36) NOT NULL,
    device_id           VARCHAR(50) NOT NULL,
    device_serial       CHAR(16) NOT NULL,
    firmware_version    VARCHAR(20),

    temperature_c       DECIMAL(8,4),
    humidity_pct        DECIMAL(6,3),
    pressure_hpa        DECIMAL(10,4),
    voltage             FLOAT,
    current_amps        DOUBLE,
    power_watts         DECIMAL(12,6),
    latitude            DECIMAL(10,7),
    longitude           DECIMAL(11,7),
    altitude_m          FLOAT,
    signal_strength_dbm SMALLINT,
    error_code          INT DEFAULT 0,
    uptime_seconds      BIGINT,

    is_anomaly          BOOLEAN DEFAULT FALSE,
    is_calibrated       BOOLEAN DEFAULT TRUE,
    battery_low         BOOLEAN DEFAULT FALSE,

    reading_timestamp   DATETIME(6) NOT NULL,               -- microsecond precision
    server_received_at  DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
    reading_date        DATE NOT NULL,
    reading_time        TIME NOT NULL,

    -- Arrays as JSON
    tag_ids             JSON,
    sensor_labels       JSON,
    raw_samples         JSON,

    device_metadata     JSON,
    alert_config        JSON,

    raw_payload         BLOB,
    location_name       VARCHAR(200),
    notes               TEXT,

    INDEX idx_sr_device (device_id),
    INDEX idx_sr_ts     (reading_timestamp),
    INDEX idx_sr_date   (reading_date),
    INDEX idx_sr_anom   (is_anomaly)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
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
        random.random() < 0.05, random.random() > 0.02, random.random() < 0.1,
        ts, ts + timedelta(milliseconds=random.randint(50,2000)),
        ts.date(), ts.time(),
        json.dumps([random.randint(1,500) for _ in range(random.randint(1,5))]),
        json.dumps(random.sample(['temp','humidity','pressure','vibration'], k=random.randint(1,3))),
        json.dumps([round(random.uniform(-50,150), 6) for _ in range(random.randint(5,20))]),
        json.dumps({'manufacturer': random.choice(['Bosch','Siemens','Honeywell']),
                    'model': f"M{random.randint(100,999)}"}),
        json.dumps({'temp_high': round(random.uniform(50,80), 1),
                    'notify': random.choice(['email','sms'])}),
        bytes(random.getrandbits(8) for _ in range(random.randint(32,256))),
        random.choice(SENSOR_LOCATIONS),
        f"Reading #{idx+1}" if random.random() > 0.7 else None
    )


# =====================================================================================
# TABLE 4: product_catalog
# =====================================================================================
CATALOG_DDL = """
CREATE TABLE IF NOT EXISTS product_catalog (
    product_id          INT AUTO_INCREMENT PRIMARY KEY,
    product_uuid        CHAR(36) NOT NULL,
    sku                 VARCHAR(40) UNIQUE NOT NULL,
    product_name        VARCHAR(300) NOT NULL,
    category            VARCHAR(100) NOT NULL,
    subcategory         VARCHAR(100),
    brand               VARCHAR(100),
    description_short   VARCHAR(500),
    description_long    TEXT,

    unit_price          DECIMAL(12,4) NOT NULL,
    wholesale_price     DECIMAL(12,4),
    cost_price          DECIMAL(12,4),
    weight_kg           FLOAT,
    length_cm           FLOAT,
    width_cm            FLOAT,
    height_cm           FLOAT,
    volume_cm3          DOUBLE,
    stock_quantity      INT NOT NULL DEFAULT 0,
    reorder_level       SMALLINT DEFAULT 10,
    max_order_qty       INT,
    popularity_score    DOUBLE,
    avg_rating          DECIMAL(3,2),
    total_reviews       INT DEFAULT 0,
    view_count          BIGINT DEFAULT 0,

    is_active           BOOLEAN DEFAULT TRUE,
    is_digital          BOOLEAN DEFAULT FALSE,
    is_fragile          BOOLEAN DEFAULT FALSE,
    is_hazardous        BOOLEAN DEFAULT FALSE,
    requires_assembly   BOOLEAN DEFAULT FALSE,
    tax_exempt          BOOLEAN DEFAULT FALSE,

    launch_date         DATE,
    discontinue_date    DATE,
    last_restock_date   DATE,
    created_at          DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at          DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    -- Arrays as JSON
    tags                JSON,
    color_options       JSON,
    compatible_skus     JSON,
    warehouse_ids       JSON,

    specifications      JSON,
    shipping_info       JSON,
    supplier_info       JSON,
    seo_metadata        JSON,

    thumbnail_blob      BLOB,
    internal_notes      TEXT,
    country_of_origin   VARCHAR(100),
    hs_tariff_code      VARCHAR(20),

    INDEX idx_cat_sku      (sku),
    INDEX idx_cat_category (category),
    INDEX idx_cat_brand    (brand),
    INDEX idx_cat_active   (is_active),

    CHECK (unit_price >= 0),
    CHECK (stock_quantity >= 0)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
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

def gen_product(idx):
    cat = random.choice(list(CATEGORIES.keys()))
    subcat = random.choice(CATEGORIES[cat])
    brand = random.choice(BRANDS)
    up = Decimal(str(round(random.uniform(1,5000), 4)))
    ws = up * Decimal(str(round(random.uniform(0.5,0.9), 2)))
    cp = ws * Decimal(str(round(random.uniform(0.4,0.8), 2)))
    l, w, h = round(random.uniform(1,200),2), round(random.uniform(1,200),2), round(random.uniform(1,200),2)

    launch = date(random.randint(2015,2025), random.randint(1,12), random.randint(1,28))
    disc = launch + timedelta(days=random.randint(365,3650)) if random.random() < 0.1 else None

    return (
        str(uuid.uuid4()), f"SKU-{cat[:3].upper()}-{idx+1:06d}",
        f"{brand} {subcat} {random.choice(['Pro','Standard','Elite'])} {random.randint(100,999)}",
        cat, subcat, brand,
        f"High-quality {subcat.lower()} for industrial use.",
        f"Detailed description for {subcat} product by {brand}. " * random.randint(3,10),
        float(up), float(ws), float(cp),
        round(random.uniform(0.01,100), 3), l, w, h, round(l*w*h, 4),
        random.randint(0,10000), random.randint(5,100), random.randint(50,5000),
        round(random.uniform(0,100), 8),
        float(round(random.uniform(1,5), 2)),
        random.randint(0,5000), random.randint(0,1_000_000),
        random.choice([True,True,True,False]), random.random()<0.1,
        random.random()<0.2, cat=='Chemicals', random.random()<0.3, random.random()<0.05,
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
        json.dumps({'title': f"Buy {subcat} from {brand}",
                    'keywords': random.sample(['industrial','tools','safety'], k=2)}),
        bytes(random.getrandbits(8) for _ in range(64)),
        f"Margin tier {random.choice(['A','B','C'])}" if random.random() > 0.6 else None,
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

def run(host, user, password, database, port=3306, total_records=1000, batch_size=100):
    conn = pymysql.connect(host=host, user=user, password=password, database=database,
                           port=port, charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
    try:
        for table_name, ddl, insert_sql, gen_fn in TABLE_CONFIG:
            print(f"\n{'='*60}")
            print(f"  Processing table: {table_name}")
            print(f"{'='*60}")

            # Ensure a clean table exists
            ensure_table(conn, table_name, ddl)

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
    run(
        host="localhost",
        user="sentinel",
        password="Test_123_Password",
        database="citadel",
        port=3306,
        total_records=5000,
        batch_size=500
    )

if __name__ == "__main__":
    main()
