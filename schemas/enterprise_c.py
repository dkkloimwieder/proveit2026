"""SQLite schema for Enterprise C - Biotech Batch Processing.

Data Model:
- Flat tag structure following ISA-5.1 / ISA-88 conventions
- Units: chrom (chromatography), sub (SUB bioreactor), sum (SUM bioreactor), tff (TFF filtration)
- Tag suffixes: _PV (process value), _SP (setpoint), _DESC (description), _EU (engineering unit)
- Batch context: BATCH_ID, RECIPE, FORMULA, PHASE, STATE
"""

import sqlite3
from pathlib import Path


SCHEMA_C = """
-- ============================================================
-- UNIT DEFINITIONS (process units)
-- ============================================================

CREATE TABLE IF NOT EXISTS units (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT UNIQUE NOT NULL,  -- chrom, sub, sum, tff
    name TEXT,
    description TEXT,
    unit_type TEXT,  -- chromatography, bioreactor, filtration
    first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Pre-populate known units
INSERT OR IGNORE INTO units (code, name, unit_type) VALUES
    ('chrom', 'Chromatography System', 'chromatography'),
    ('sub', 'SUB Bioreactor', 'bioreactor'),
    ('sum', 'SUM Bioreactor', 'bioreactor'),
    ('tff', 'TFF Filtration System', 'filtration');

-- ============================================================
-- TAG DEFINITIONS (master data for tags)
-- ============================================================

CREATE TABLE IF NOT EXISTS tags (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    unit_id INTEGER REFERENCES units(id),
    tag_name TEXT UNIQUE NOT NULL,  -- Full tag name: CHR01_TT001
    tag_type TEXT,  -- TIC, FIC, PIC, SIC, etc.
    tag_number TEXT,  -- 001, 002, etc.
    description TEXT,  -- From _DESC suffix
    engineering_unit TEXT,  -- From _EU suffix
    first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_tags_unit ON tags(unit_id);
CREATE INDEX IF NOT EXISTS idx_tags_type ON tags(tag_type);

-- ============================================================
-- TAG VALUES (time-series process data)
-- ============================================================

CREATE TABLE IF NOT EXISTS tag_values (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    tag_id INTEGER REFERENCES tags(id),
    value_type TEXT NOT NULL,  -- PV, SP, MODE, STATUS, ACTIVE, START
    value_numeric REAL,
    value_text TEXT,
    batch_id TEXT  -- Current batch context
);

CREATE INDEX IF NOT EXISTS idx_tagval_time ON tag_values(timestamp);
CREATE INDEX IF NOT EXISTS idx_tagval_tag ON tag_values(tag_id);
CREATE INDEX IF NOT EXISTS idx_tagval_batch ON tag_values(batch_id);

-- ============================================================
-- BATCH TRACKING
-- ============================================================

CREATE TABLE IF NOT EXISTS batches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id TEXT UNIQUE NOT NULL,
    unit_id INTEGER REFERENCES units(id),
    recipe_name TEXT,
    formula_name TEXT,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    final_state TEXT,
    operator_id TEXT,
    first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_batches_unit ON batches(unit_id);
CREATE INDEX IF NOT EXISTS idx_batches_time ON batches(start_time);

-- ============================================================
-- PHASE TRACKING (batch phases)
-- ============================================================

CREATE TABLE IF NOT EXISTS phases (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id INTEGER REFERENCES batches(id),
    phase_name TEXT NOT NULL,
    start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    end_time TIMESTAMP,
    state TEXT,  -- RUNNING, COMPLETE, ABORTED
    sequence_number INTEGER
);

CREATE INDEX IF NOT EXISTS idx_phases_batch ON phases(batch_id);
CREATE INDEX IF NOT EXISTS idx_phases_time ON phases(start_time);

-- ============================================================
-- OPERATOR PROMPTS/MESSAGES
-- ============================================================

CREATE TABLE IF NOT EXISTS operator_messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    unit_id INTEGER REFERENCES units(id),
    batch_id INTEGER REFERENCES batches(id),
    message_text TEXT,
    operator_id TEXT,
    acknowledged INTEGER DEFAULT 0,
    ack_time TIMESTAMP,
    verified INTEGER DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_opmsg_batch ON operator_messages(batch_id);

-- ============================================================
-- PROCESS METRICS (10-second buckets by unit)
-- ============================================================

CREATE TABLE IF NOT EXISTS process_metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    bucket TIMESTAMP NOT NULL,
    unit_id INTEGER REFERENCES units(id),
    batch_id INTEGER REFERENCES batches(id),

    -- Temperature measurements
    temperature_pv REAL,
    temperature_sp REAL,

    -- Flow measurements
    flow_pv REAL,
    flow_sp REAL,
    flow_unit TEXT,  -- LPM, SLPM, mL_per_min

    -- Pressure measurements
    pressure_pv REAL,
    pressure_sp REAL,

    -- Speed measurements (agitation, pumps)
    speed_pv REAL,
    speed_sp REAL,
    speed_unit TEXT,  -- RPM, LPM

    -- Weight measurements
    weight_pv REAL,

    -- pH measurements
    ph_pv REAL,
    ph_sp REAL,

    -- Conductivity
    conductivity_pv REAL,

    -- UV absorbance (for chromatography)
    uv_absorbance REAL,

    UNIQUE(bucket, unit_id)
);

CREATE INDEX IF NOT EXISTS idx_procmet_bucket ON process_metrics(bucket);
CREATE INDEX IF NOT EXISTS idx_procmet_unit ON process_metrics(unit_id);
CREATE INDEX IF NOT EXISTS idx_procmet_batch ON process_metrics(batch_id);

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
-- VIEWS
-- ============================================================

CREATE VIEW IF NOT EXISTS v_active_batches AS
SELECT
    b.batch_id,
    u.code as unit_code,
    u.name as unit_name,
    b.recipe_name,
    b.formula_name,
    b.start_time,
    (julianday('now') - julianday(b.start_time)) * 24 as hours_running
FROM batches b
JOIN units u ON b.unit_id = u.id
WHERE b.end_time IS NULL;

CREATE VIEW IF NOT EXISTS v_latest_tag_values AS
SELECT
    t.tag_name,
    u.code as unit_code,
    tv.value_type,
    tv.value_numeric,
    tv.value_text,
    tv.timestamp,
    tv.batch_id
FROM tag_values tv
JOIN tags t ON tv.tag_id = t.id
JOIN units u ON t.unit_id = u.id
WHERE tv.id IN (
    SELECT MAX(id) FROM tag_values GROUP BY tag_id, value_type
);

-- ============================================================
-- SCHEMA INFO
-- ============================================================

CREATE TABLE IF NOT EXISTS schema_info (
    version INTEGER PRIMARY KEY,
    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    description TEXT
);

INSERT OR IGNORE INTO schema_info (version, description)
VALUES (1, 'v1: Initial Enterprise C schema - biotech batch processing');
"""


def init_db_c(db_path: str = "proveit_c.db") -> sqlite3.Connection:
    """Initialize Enterprise C database."""
    path = Path(db_path)
    conn = sqlite3.connect(path)
    conn.executescript(SCHEMA_C)
    conn.commit()
    print(f"Enterprise C database initialized: {path.absolute()}")
    return conn
