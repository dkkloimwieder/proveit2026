"""SQLite schema for Enterprise B manufacturing data."""

import sqlite3
from pathlib import Path

SCHEMA = """
-- Raw message capture for replay/debugging
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

-- Topic hierarchy discovery
CREATE TABLE IF NOT EXISTS topics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    topic TEXT UNIQUE NOT NULL,
    site TEXT,           -- Site1, Site2, Site3
    area TEXT,           -- fillerproduction, liquidprocessing, packaging, palletizing
    line TEXT,           -- fillingline01, mixroom01, etc.
    equipment TEXT,      -- caploader, filler, labeler, etc.
    category TEXT,       -- metric, node, processdata, workorder
    data_type TEXT,      -- availability, oee, assetid, state, etc.
    message_type TEXT,   -- json, text, binary
    first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    message_count INTEGER DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_topics_site ON topics(site);
CREATE INDEX IF NOT EXISTS idx_topics_area ON topics(area);
CREATE INDEX IF NOT EXISTS idx_topics_equipment ON topics(equipment);

-- Assets (equipment/nodes)
CREATE TABLE IF NOT EXISTS assets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    asset_id INTEGER UNIQUE,
    asset_name TEXT,
    asset_path TEXT,
    asset_type_name TEXT,
    display_name TEXT,
    parent_asset_id INTEGER,
    sort_order INTEGER,
    site TEXT,
    area TEXT,
    line TEXT,
    equipment TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_assets_site ON assets(site);
CREATE INDEX IF NOT EXISTS idx_assets_path ON assets(asset_path);

-- OEE Metrics time-series
CREATE TABLE IF NOT EXISTS metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    asset_id INTEGER,
    site TEXT NOT NULL,
    area TEXT,
    line TEXT,
    equipment TEXT,

    -- OEE values (0-1 or 0-100 scale)
    availability REAL,
    performance REAL,
    quality REAL,
    oee REAL,

    -- Input metrics
    count_defect INTEGER,
    count_infeed INTEGER,
    count_outfeed INTEGER,
    rate_actual REAL,
    rate_standard REAL,
    time_down_planned REAL,
    time_down_unplanned REAL,
    time_idle REAL,
    time_running REAL,

    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (asset_id) REFERENCES assets(asset_id)
);

CREATE INDEX IF NOT EXISTS idx_metrics_asset ON metrics(asset_id);
CREATE INDEX IF NOT EXISTS idx_metrics_site ON metrics(site);
CREATE INDEX IF NOT EXISTS idx_metrics_equipment ON metrics(equipment);
CREATE INDEX IF NOT EXISTS idx_metrics_recorded ON metrics(recorded_at);

-- Process data (state, work orders, etc.)
CREATE TABLE IF NOT EXISTS process_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    site TEXT NOT NULL,
    area TEXT,
    line TEXT,
    equipment TEXT,

    -- State info
    state_name TEXT,
    state_type TEXT,
    state_code TEXT,
    state_duration REAL,

    -- Work order info
    work_order TEXT,
    lot_number TEXT,
    item_name TEXT,

    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_process_site ON process_data(site);
CREATE INDEX IF NOT EXISTS idx_process_equipment ON process_data(equipment);
CREATE INDEX IF NOT EXISTS idx_process_recorded ON process_data(recorded_at);

-- Schema version tracking
CREATE TABLE IF NOT EXISTS schema_info (
    version INTEGER PRIMARY KEY,
    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    description TEXT
);

INSERT OR IGNORE INTO schema_info (version, description) VALUES (1, 'Initial schema for Enterprise B data');
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


if __name__ == "__main__":
    init_db()
