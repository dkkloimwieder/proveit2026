"""SQLite schema for Enterprise B manufacturing data.

Schema Design:
- Reference tables: Stable/master data (products, lots, work_orders, states, asset_types)
- Assets table: Equipment hierarchy with foreign keys to reference data
- Events table: State changes, lot changes, work order events (append-only log)
- Metrics_10s table: 10-second bucketed OEE metrics by line

Data Flow:
1. New product/lot/work_order → upsert into reference tables
2. State change → insert into events with state_id FK
3. Every 10s → snapshot line metrics into metrics_10s
"""

import sqlite3
from pathlib import Path

SCHEMA = """
-- ============================================================
-- REFERENCE TABLES (stable/master data)
-- ============================================================

-- Products/Items
CREATE TABLE IF NOT EXISTS products (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    item_id INTEGER UNIQUE,              -- from MQTT item/itemid
    name TEXT,                           -- item/itemname
    item_class TEXT,                     -- item/itemclass (Pack, etc.)
    bottle_size REAL,                    -- item/bottlesize
    pack_count INTEGER,                  -- item/packcount
    label_variant TEXT,                  -- item/labelvariant
    parent_item_id INTEGER,              -- item/parentitemid
    first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_products_name ON products(name);

-- Lots (batches)
CREATE TABLE IF NOT EXISTS lots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    lot_number_id INTEGER UNIQUE,        -- from MQTT lotnumber/lotnumberid
    lot_number TEXT,                     -- lotnumber/lotnumber (not unique, can repeat)
    product_id INTEGER REFERENCES products(id),
    first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_lots_number ON lots(lot_number);

-- Work Orders
CREATE TABLE IF NOT EXISTS work_orders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    work_order_id INTEGER UNIQUE,        -- from MQTT workorder/workorderid
    work_order_number TEXT,              -- workorder/workordernumber
    quantity_target INTEGER,             -- workorder/quantitytarget
    quantity_actual INTEGER,             -- workorder/quantityactual (latest)
    quantity_defect INTEGER,             -- workorder/quantitydefect (latest)
    uom TEXT,                            -- workorder/uom
    asset_id INTEGER,                    -- workorder/assetid
    lot_id INTEGER REFERENCES lots(id),
    product_id INTEGER REFERENCES products(id),
    site TEXT,
    line TEXT,
    first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_work_orders_number ON work_orders(work_order_number);
CREATE INDEX IF NOT EXISTS idx_work_orders_site ON work_orders(site);

-- State Definitions (lookup table)
CREATE TABLE IF NOT EXISTS states (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code INTEGER,                        -- state/code
    name TEXT NOT NULL,                  -- state/name
    type TEXT,                           -- state/type
    UNIQUE(code, name, type)
);

CREATE INDEX IF NOT EXISTS idx_states_name ON states(name);

-- Asset Types (lookup table)
CREATE TABLE IF NOT EXISTS asset_types (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL            -- assetidentifier/assettypename
);

-- ============================================================
-- ASSET/EQUIPMENT TABLE
-- ============================================================

CREATE TABLE IF NOT EXISTS assets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    asset_id INTEGER UNIQUE,             -- from MQTT assetidentifier/assetid
    asset_name TEXT,                     -- assetidentifier/assetname
    asset_path TEXT,                     -- assetidentifier/assetpath
    display_name TEXT,                   -- assetidentifier/displayname
    asset_type_id INTEGER REFERENCES asset_types(id),
    parent_asset_id INTEGER,             -- assetidentifier/parentassetid
    sort_order INTEGER,                  -- assetidentifier/sortorder
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
-- EVENT LOG (state changes, lot/wo transitions)
-- ============================================================

CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    site TEXT NOT NULL,
    area TEXT,
    line TEXT,
    equipment TEXT,
    event_type TEXT NOT NULL,            -- 'state', 'lot_start', 'lot_end', 'wo_start', 'wo_end'
    state_id INTEGER REFERENCES states(id),
    state_duration REAL,                 -- state/duration
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
-- METRICS (10-second buckets by line)
-- ============================================================

CREATE TABLE IF NOT EXISTS metrics_10s (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    bucket TIMESTAMP NOT NULL,           -- rounded to 10s boundary
    site TEXT NOT NULL,
    area TEXT,
    line TEXT NOT NULL,

    -- OEE metrics (aggregated across equipment in line)
    availability REAL,
    performance REAL,
    quality REAL,
    oee REAL,

    -- Count metrics (summed across equipment)
    count_infeed INTEGER,
    count_outfeed INTEGER,
    count_defect INTEGER,

    -- Time metrics (summed across equipment, in seconds)
    time_running REAL,
    time_idle REAL,
    time_down_planned REAL,
    time_down_unplanned REAL,

    -- Rate metrics
    rate_actual REAL,
    rate_standard REAL,

    -- Process metrics (avg across equipment)
    temperature REAL,
    flow_rate REAL,
    weight REAL,

    -- Context
    work_order_id INTEGER REFERENCES work_orders(id),
    lot_id INTEGER REFERENCES lots(id),
    equipment_count INTEGER,             -- how many equipment contributed

    UNIQUE(bucket, site, line)
);

CREATE INDEX IF NOT EXISTS idx_metrics_bucket ON metrics_10s(bucket);
CREATE INDEX IF NOT EXISTS idx_metrics_site ON metrics_10s(site);
CREATE INDEX IF NOT EXISTS idx_metrics_line ON metrics_10s(site, line);
CREATE INDEX IF NOT EXISTS idx_metrics_wo ON metrics_10s(work_order_id);

-- ============================================================
-- RAW MESSAGE CAPTURE (for debugging/replay)
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
-- TOPIC REGISTRY (discovered topics)
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
-- SCHEMA VERSION
-- ============================================================

CREATE TABLE IF NOT EXISTS schema_info (
    version INTEGER PRIMARY KEY,
    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    description TEXT
);

INSERT OR IGNORE INTO schema_info (version, description)
VALUES (2, 'v2: Reference tables, events log, 10s metric buckets');
"""


def init_db(db_path: str = "proveit.db") -> sqlite3.Connection:
    """Initialize the database with schema."""
    path = Path(db_path)
    conn = sqlite3.connect(path)
    conn.executescript(SCHEMA)
    conn.commit()
    print(f"Database initialized: {path.absolute()}")
    return conn


def get_connection(db_path: str = "proveit.db") -> sqlite3.Connection:
    """Get a database connection."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def reset_db(db_path: str = "proveit.db"):
    """Delete and reinitialize the database."""
    path = Path(db_path)
    if path.exists():
        path.unlink()
        print(f"Deleted: {path}")
    return init_db(db_path)


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "reset":
        reset_db()
    else:
        init_db()
