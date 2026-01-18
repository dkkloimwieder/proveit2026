"""SQLite schema for Enterprise B - Beverage Manufacturing.

Schema Design:
- Reference tables: Stable/master data (products, lots, work_orders, states, asset_types)
- Assets table: Equipment hierarchy with foreign keys to reference data
- Events table: State changes, lot changes, work order events (append-only log)
- Metrics_10s table: 10-second bucketed OEE metrics by line
"""

import sqlite3
from pathlib import Path


SCHEMA_B = """
-- ============================================================
-- REFERENCE TABLES (stable/master data)
-- ============================================================

-- Products/Items
CREATE TABLE IF NOT EXISTS products (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    item_id INTEGER UNIQUE,
    name TEXT,
    item_class TEXT,
    bottle_size REAL,
    pack_count INTEGER,
    label_variant TEXT,
    parent_item_id INTEGER,
    first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_products_name ON products(name);

-- Lots (batches)
CREATE TABLE IF NOT EXISTS lots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    lot_number_id INTEGER UNIQUE,
    lot_number TEXT,
    product_id INTEGER REFERENCES products(id),
    first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_lots_number ON lots(lot_number);

-- Work Orders
CREATE TABLE IF NOT EXISTS work_orders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    work_order_id INTEGER UNIQUE,
    work_order_number TEXT,
    quantity_target INTEGER,
    quantity_actual INTEGER,
    quantity_defect INTEGER,
    uom TEXT,
    asset_id INTEGER,
    lot_id INTEGER REFERENCES lots(id),
    product_id INTEGER REFERENCES products(id),
    site TEXT,
    line TEXT,
    first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_work_orders_number ON work_orders(work_order_number);
CREATE INDEX IF NOT EXISTS idx_work_orders_site ON work_orders(site);

-- State Definitions
CREATE TABLE IF NOT EXISTS states (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code INTEGER,
    name TEXT NOT NULL,
    type TEXT,
    UNIQUE(code, name, type)
);

CREATE INDEX IF NOT EXISTS idx_states_name ON states(name);

-- Asset Types
CREATE TABLE IF NOT EXISTS asset_types (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL
);

-- ============================================================
-- ASSET/EQUIPMENT TABLE
-- ============================================================

CREATE TABLE IF NOT EXISTS assets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    asset_id INTEGER UNIQUE,
    asset_name TEXT,
    asset_path TEXT,
    display_name TEXT,
    asset_type_id INTEGER REFERENCES asset_types(id),
    parent_asset_id INTEGER,
    sort_order INTEGER,
    site TEXT,
    area TEXT,
    line TEXT,
    equipment TEXT,
    first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_assets_site ON assets(site);
CREATE INDEX IF NOT EXISTS idx_assets_path ON assets(asset_path);
CREATE INDEX IF NOT EXISTS idx_assets_line ON assets(site, area, line);

-- ============================================================
-- EVENT LOG
-- ============================================================

CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    site TEXT NOT NULL,
    area TEXT,
    line TEXT,
    equipment TEXT,
    event_type TEXT NOT NULL,
    state_id INTEGER REFERENCES states(id),
    state_duration REAL,
    work_order_id INTEGER REFERENCES work_orders(id),
    lot_id INTEGER REFERENCES lots(id),
    product_id INTEGER REFERENCES products(id),
    prev_state_id INTEGER REFERENCES states(id),
    prev_work_order_id INTEGER REFERENCES work_orders(id),
    prev_lot_id INTEGER REFERENCES lots(id)
);

CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events(timestamp);
CREATE INDEX IF NOT EXISTS idx_events_site ON events(site);
CREATE INDEX IF NOT EXISTS idx_events_line ON events(site, line);
CREATE INDEX IF NOT EXISTS idx_events_type ON events(event_type);
CREATE INDEX IF NOT EXISTS idx_events_state ON events(state_id);
CREATE INDEX IF NOT EXISTS idx_events_wo ON events(work_order_id);

-- ============================================================
-- METRICS (10-second buckets)
-- ============================================================

CREATE TABLE IF NOT EXISTS metrics_10s (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    bucket TIMESTAMP NOT NULL,
    site TEXT NOT NULL,
    area TEXT,
    line TEXT NOT NULL,
    availability REAL,
    performance REAL,
    quality REAL,
    oee REAL,
    count_infeed INTEGER,
    count_outfeed INTEGER,
    count_defect INTEGER,
    time_running REAL,
    time_idle REAL,
    time_down_planned REAL,
    time_down_unplanned REAL,
    rate_actual REAL,
    rate_standard REAL,
    temperature REAL,
    flow_rate REAL,
    weight REAL,
    work_order_id INTEGER REFERENCES work_orders(id),
    lot_id INTEGER REFERENCES lots(id),
    equipment_count INTEGER,
    UNIQUE(bucket, site, line)
);

CREATE INDEX IF NOT EXISTS idx_metrics_bucket ON metrics_10s(bucket);
CREATE INDEX IF NOT EXISTS idx_metrics_site ON metrics_10s(site);
CREATE INDEX IF NOT EXISTS idx_metrics_line ON metrics_10s(site, line);
CREATE INDEX IF NOT EXISTS idx_metrics_wo ON metrics_10s(work_order_id);

-- ============================================================
-- WORK ORDER COMPLETIONS
-- ============================================================

CREATE TABLE IF NOT EXISTS work_order_completions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    completed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    site TEXT NOT NULL,
    area TEXT,
    line TEXT NOT NULL,
    equipment TEXT,
    work_order_id INTEGER,
    work_order_number TEXT,
    final_quantity INTEGER,
    quantity_target INTEGER,
    quantity_defect INTEGER,
    uom TEXT,
    pct_complete REAL,
    final_oee REAL,
    final_availability REAL,
    final_performance REAL,
    final_quality REAL,
    final_count_infeed INTEGER,
    final_count_outfeed INTEGER,
    next_work_order_id INTEGER,
    next_work_order_number TEXT,
    duration_seconds REAL,
    UNIQUE(work_order_id, site, line, completed_at)
);

CREATE INDEX IF NOT EXISTS idx_wo_completions_time ON work_order_completions(completed_at);
CREATE INDEX IF NOT EXISTS idx_wo_completions_wo ON work_order_completions(work_order_number);

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
-- TOPIC REGISTRY
-- ============================================================

CREATE TABLE IF NOT EXISTS topics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    topic TEXT UNIQUE NOT NULL,
    site TEXT,
    area TEXT,
    line TEXT,
    equipment TEXT,
    category TEXT,
    data_type TEXT,
    message_type TEXT,
    first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    message_count INTEGER DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_topics_site ON topics(site);

-- ============================================================
-- VIEWS
-- ============================================================

CREATE VIEW IF NOT EXISTS v_production_by_line AS
SELECT
    site, line,
    CASE
        WHEN line LIKE 'mixroom%' THEN 'MIX'
        WHEN line LIKE 'filling%' THEN 'FILL'
        WHEN line LIKE 'labeler%' THEN 'PACK'
        WHEN line LIKE 'palletizer%' THEN 'PALLETIZE'
        ELSE 'OTHER'
    END as stage,
    COUNT(*) as wo_completions,
    SUM(final_quantity) as total_output,
    AVG(pct_complete) as avg_completion_pct
FROM work_order_completions
WHERE final_quantity > 0
GROUP BY site, line;

CREATE VIEW IF NOT EXISTS v_oee_by_line AS
SELECT
    site, line,
    COUNT(*) as buckets,
    ROUND(AVG(availability) * 100, 1) as avg_availability_pct,
    ROUND(AVG(performance) * 100, 1) as avg_performance_pct,
    ROUND(AVG(quality) * 100, 1) as avg_quality_pct,
    ROUND(AVG(oee) * 100, 1) as avg_oee_pct
FROM metrics_10s
WHERE oee IS NOT NULL
GROUP BY site, line;

-- ============================================================
-- SCHEMA INFO
-- ============================================================

CREATE TABLE IF NOT EXISTS schema_info (
    version INTEGER PRIMARY KEY,
    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    description TEXT
);

INSERT OR IGNORE INTO schema_info (version, description)
VALUES (3, 'v3: Enterprise B schema - beverage manufacturing');
"""


def init_db_b(db_path: str = "proveit_b.db") -> sqlite3.Connection:
    """Initialize Enterprise B database."""
    path = Path(db_path)
    conn = sqlite3.connect(path)
    conn.executescript(SCHEMA_B)
    conn.commit()
    print(f"Enterprise B database initialized: {path.absolute()}")
    return conn
