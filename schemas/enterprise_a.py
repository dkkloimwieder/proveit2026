"""SQLite schema for Enterprise A - Glass Manufacturing.

Data Model:
- Equipment hierarchy: Site > Line > Area > Equipment
- Batch processing: Silos -> Mixer -> Charger -> Furnace
- Hot End: Furnace -> Forehearth -> IS Machine
- Cold End: Lehr -> Inspector -> Palletizer
- Utilities: Compressors, Air Dryers, Electrical Panels
"""

import sqlite3
from pathlib import Path


SCHEMA_A = """
-- ============================================================
-- EQUIPMENT HIERARCHY
-- ============================================================

CREATE TABLE IF NOT EXISTS sites (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL,
    description TEXT,
    address TEXT,
    timezone TEXT,
    first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS areas (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    site_id INTEGER REFERENCES sites(id),
    name TEXT NOT NULL,
    description TEXT,
    area_type TEXT,  -- BatchHouse, HotEnd, ColdEnd, Utilities
    UNIQUE(site_id, name)
);

CREATE TABLE IF NOT EXISTS equipment (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    area_id INTEGER REFERENCES areas(id),
    asset_id INTEGER UNIQUE,
    name TEXT NOT NULL,
    description TEXT,
    equipment_type TEXT,  -- Silo, Mixer, Furnace, ISMachine, etc.
    parent_equipment_id INTEGER REFERENCES equipment(id),
    sort_order INTEGER,
    first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_equipment_area ON equipment(area_id);
CREATE INDEX IF NOT EXISTS idx_equipment_type ON equipment(equipment_type);

-- ============================================================
-- EQUIPMENT STATE TRACKING
-- ============================================================

CREATE TABLE IF NOT EXISTS equipment_states (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    equipment_id INTEGER REFERENCES equipment(id),
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    state_code INTEGER,
    state_reason TEXT,
    prev_state_code INTEGER,
    prev_state_reason TEXT,
    duration_seconds REAL
);

CREATE INDEX IF NOT EXISTS idx_equip_states_time ON equipment_states(timestamp);
CREATE INDEX IF NOT EXISTS idx_equip_states_equip ON equipment_states(equipment_id);

-- ============================================================
-- PROCESS MEASUREMENTS (10-second buckets)
-- ============================================================

CREATE TABLE IF NOT EXISTS process_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    bucket TIMESTAMP NOT NULL,
    equipment_id INTEGER REFERENCES equipment(id),

    -- Silo measurements
    level_pct REAL,
    material TEXT,

    -- Mixer measurements
    batch_weight REAL,
    motor_current REAL,

    -- Charger measurements
    feed_rate REAL,
    belt_speed REAL,

    -- Furnace measurements
    temperature REAL,
    glass_level REAL,

    -- IS Machine measurements
    gob_weight REAL,
    mold_temp REAL,
    forming_pressure REAL,

    -- Lehr measurements
    lehr_temperature REAL,
    conveyor_speed REAL,

    -- Inspector measurements
    defect_count INTEGER,
    pass_count INTEGER,
    reject_count INTEGER,

    UNIQUE(bucket, equipment_id)
);

CREATE INDEX IF NOT EXISTS idx_process_bucket ON process_data(bucket);
CREATE INDEX IF NOT EXISTS idx_process_equip ON process_data(equipment_id);

-- ============================================================
-- RAW SENSOR DATA (edge/ topics)
-- ============================================================

CREATE TABLE IF NOT EXISTS sensor_readings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    equipment_id INTEGER REFERENCES equipment(id),
    sensor_name TEXT NOT NULL,
    value REAL,
    raw_value TEXT,
    unit TEXT
);

CREATE INDEX IF NOT EXISTS idx_sensor_time ON sensor_readings(timestamp);
CREATE INDEX IF NOT EXISTS idx_sensor_equip ON sensor_readings(equipment_id, sensor_name);

-- ============================================================
-- UTILITIES MONITORING
-- ============================================================

CREATE TABLE IF NOT EXISTS utility_readings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    bucket TIMESTAMP NOT NULL,
    utility_type TEXT NOT NULL,  -- Compressor, AirDryer, Panel
    equipment_name TEXT NOT NULL,

    -- Common metrics
    state_value REAL,
    state_name TEXT,

    -- Compressor metrics
    discharge_pressure REAL,
    discharge_temp REAL,
    oil_pressure REAL,
    oil_temp REAL,
    load_pct REAL,
    runtime_hours REAL,

    -- Electrical panel metrics
    voltage_a REAL,
    voltage_b REAL,
    voltage_c REAL,
    current_a REAL,
    current_b REAL,
    current_c REAL,
    power_kw REAL,
    power_factor REAL,

    UNIQUE(bucket, utility_type, equipment_name)
);

CREATE INDEX IF NOT EXISTS idx_utility_bucket ON utility_readings(bucket);
CREATE INDEX IF NOT EXISTS idx_utility_type ON utility_readings(utility_type);

-- ============================================================
-- OEE METRICS (from OEE topics)
-- ============================================================

CREATE TABLE IF NOT EXISTS oee_metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    bucket TIMESTAMP NOT NULL,
    site TEXT NOT NULL,
    line TEXT NOT NULL,

    availability REAL,
    performance REAL,
    quality REAL,
    oee REAL,

    target_count INTEGER,
    actual_count INTEGER,
    good_count INTEGER,
    reject_count INTEGER,

    planned_runtime REAL,
    actual_runtime REAL,
    downtime REAL,

    UNIQUE(bucket, site, line)
);

CREATE INDEX IF NOT EXISTS idx_oee_bucket ON oee_metrics(bucket);

-- ============================================================
-- RAW MESSAGE CAPTURE
-- ============================================================

CREATE TABLE IF NOT EXISTS messages_raw (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    topic TEXT NOT NULL,
    payload BLOB,
    payload_text TEXT,
    payload_type TEXT CHECK(payload_type IN ('json', 'text', 'binary')),
    received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_messages_topic ON messages_raw(topic);
CREATE INDEX IF NOT EXISTS idx_messages_received ON messages_raw(received_at);

-- ============================================================
-- SCHEMA INFO
-- ============================================================

CREATE TABLE IF NOT EXISTS schema_info (
    version INTEGER PRIMARY KEY,
    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    description TEXT
);

INSERT OR IGNORE INTO schema_info (version, description)
VALUES (1, 'v1: Initial Enterprise A schema - glass manufacturing');
"""


def init_db_a(db_path: str = "proveit_a.db") -> sqlite3.Connection:
    """Initialize Enterprise A database."""
    path = Path(db_path)
    conn = sqlite3.connect(path)
    conn.executescript(SCHEMA_A)
    conn.commit()
    print(f"Enterprise A database initialized: {path.absolute()}")
    return conn
