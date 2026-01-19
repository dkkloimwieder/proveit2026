"""Enterprise C data collector - Biotech Batch Processing.

Handles flat tag structure with ISA-5.1/ISA-88 naming conventions.
Stores tag values, tracks batches and phases.
"""

import json
import re
import sqlite3
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from parsers import TopicInfo, EnterpriseCParser
from schemas.enterprise_c import SCHEMA_C


@dataclass
class TagInfo:
    """Parsed tag information."""
    tag_name: str
    base_name: str  # Tag name without suffix
    suffix: str | None = None  # PV, SP, DESC, EU, etc.
    tag_type: str | None = None  # TIC, FIC, etc.
    unit_number: str | None = None


class EnterpriseCCollector:
    """Collects Enterprise C biotech data into SQLite."""

    # Value suffixes that indicate different data types
    VALUE_SUFFIXES = {
        "_PV": "PV",
        "_SP": "SP",
        "_DESC": "DESC",
        "_EU": "EU",
        "_ACTIVE": "ACTIVE",
        "_MODE": "MODE",
        "_STATUS": "STATUS",
        "_START": "START",
        "_CMD": "CMD",
        "_ACK": "ACK",
    }

    # Tag type patterns
    TAG_TYPES = ("TIC", "TI", "FIC", "FI", "FCV", "PIC", "PI", "SIC", "AIC", "AI", "WI", "HV", "XV", "CI", "UV", "DI")

    def __init__(self, db_path: str = "proveit_c.db", capture_raw: bool = False):
        self.db_path = db_path
        self.parser = EnterpriseCParser()
        self.capture_raw = capture_raw

        # Initialize database (check_same_thread=False for MQTT background thread)
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.executescript(SCHEMA_C)
        self.conn.commit()
        print(f"Enterprise C database initialized: {Path(db_path).absolute()}")

        # Counters
        self.message_count = 0
        self.stored_count = 0
        self.start_time = datetime.now()

        # Caches
        self.unit_cache: dict[str, int] = {}  # unit_code -> id
        self.tag_cache: dict[str, int] = {}   # tag_name -> id
        self.batch_cache: dict[str, str] = {} # unit_code -> current batch_id

        # Pending tag metadata (DESC, EU)
        self.pending_tag_meta: dict[str, dict] = {}  # base_tag_name -> {desc, eu}

        # Raw message buffer
        self.raw_buffer: list[tuple] = []
        self.raw_batch_size = 100

        # Lock for thread safety
        self.lock = threading.Lock()

        # Pre-load unit cache
        self._load_unit_cache()

    def _load_unit_cache(self):
        """Load unit IDs from database."""
        cursor = self.conn.cursor()
        cursor.execute("SELECT id, code FROM units")
        for row in cursor.fetchall():
            self.unit_cache[row[1]] = row[0]

    def handle_message(self, topic: str, payload: bytes):
        """Process incoming MQTT message."""
        self.message_count += 1

        info = self.parser.parse_topic(topic)
        if not info:
            return

        # Decode payload
        try:
            decoded = payload.decode("utf-8")
            try:
                value = json.loads(decoded)
                msg_type = "json"
            except json.JSONDecodeError:
                value = decoded
                msg_type = "text"
        except UnicodeDecodeError:
            value = payload.hex()
            msg_type = "binary"

        with self.lock:
            # Capture raw if enabled
            if self.capture_raw:
                self.raw_buffer.append((
                    topic, payload,
                    decoded if msg_type != "binary" else None,
                    msg_type
                ))
                if len(self.raw_buffer) >= self.raw_batch_size:
                    self._flush_raw_buffer()

            # Process the tag data
            if info.unit and info.tag:
                self._process_tag(info, value)
                self.stored_count += 1

        # Progress
        if self.message_count % 500 == 0:
            elapsed = (datetime.now() - self.start_time).total_seconds()
            print(f"\rMsgs: {self.message_count} | Stored: {self.stored_count} | "
                  f"Time: {elapsed:.1f}s", end="", flush=True)

    def _process_tag(self, info: TopicInfo, value: Any):
        """Process a tag value."""
        tag_name = info.tag
        unit_code = info.unit

        # Parse the tag name to extract suffix and base name
        tag_info = self._parse_tag_name(tag_name)

        # Handle metadata tags (DESC, EU) - store for later use
        if tag_info.suffix == "DESC":
            self._store_tag_metadata(tag_info.base_name, "description", str(value))
            return
        elif tag_info.suffix == "EU":
            self._store_tag_metadata(tag_info.base_name, "engineering_unit", str(value))
            return

        # Handle batch-related tags
        if self._is_batch_tag(tag_name):
            self._handle_batch_tag(unit_code, tag_name, value)
            return

        # Get or create tag record
        tag_id = self._get_or_create_tag(unit_code, tag_info)

        # Determine value type
        value_type = tag_info.suffix or "VALUE"

        # Store the tag value
        self._store_tag_value(tag_id, value_type, value)

    def _parse_tag_name(self, tag_name: str) -> TagInfo:
        """Parse tag name into components."""
        info = TagInfo(tag_name=tag_name, base_name=tag_name)

        # Check for value suffixes
        for suffix, suffix_type in self.VALUE_SUFFIXES.items():
            if tag_name.endswith(suffix):
                info.suffix = suffix_type
                info.base_name = tag_name[:-len(suffix)]
                break

        # Also handle suffixes with units like _PV_Celsius, _SP_psi
        if not info.suffix:
            for suffix in self.VALUE_SUFFIXES:
                if suffix in tag_name:
                    idx = tag_name.find(suffix)
                    info.suffix = self.VALUE_SUFFIXES[suffix]
                    info.base_name = tag_name[:idx]
                    break

        # Extract tag type from beginning
        for tag_type in self.TAG_TYPES:
            if info.base_name.startswith(tag_type) or f"-{tag_type}" in info.base_name or f"_{tag_type}" in info.base_name:
                info.tag_type = tag_type
                break

        # Extract unit number
        unit_match = re.search(r'[-_](\d{3})[-_]?', tag_name)
        if unit_match:
            info.unit_number = unit_match.group(1)

        return info

    def _store_tag_metadata(self, base_name: str, field: str, value: str):
        """Store tag metadata (description, engineering unit) for later use."""
        if base_name not in self.pending_tag_meta:
            self.pending_tag_meta[base_name] = {}
        self.pending_tag_meta[base_name][field] = value

        # If tag already exists, update it
        if base_name in self.tag_cache:
            tag_id = self.tag_cache[base_name]
            cursor = self.conn.cursor()
            if field == "description":
                cursor.execute("UPDATE tags SET description = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                             (value, tag_id))
            elif field == "engineering_unit":
                cursor.execute("UPDATE tags SET engineering_unit = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                             (value, tag_id))
            self.conn.commit()

    def _is_batch_tag(self, tag_name: str) -> bool:
        """Check if tag is batch-related."""
        batch_keywords = ["BATCH_ID", "BATCH-ID", "RECIPE", "FORMULA", "PHASE", "STATE", "OPR_ID", "OPR_VRF"]
        return any(kw in tag_name for kw in batch_keywords)

    def _handle_batch_tag(self, unit_code: str, tag_name: str, value: Any):
        """Handle batch-related tags."""
        cursor = self.conn.cursor()
        unit_id = self.unit_cache.get(unit_code)

        str_value = str(value) if value else None

        # Batch ID changes
        if "BATCH_ID" in tag_name or "BATCH-ID" in tag_name:
            if str_value and str_value != self.batch_cache.get(unit_code):
                # New batch
                cursor.execute("""
                    INSERT OR IGNORE INTO batches (batch_id, unit_id, start_time)
                    VALUES (?, ?, CURRENT_TIMESTAMP)
                """, (str_value, unit_id))
                self.batch_cache[unit_code] = str_value
                self.conn.commit()

        # Update batch metadata
        current_batch = self.batch_cache.get(unit_code)
        if current_batch:
            if "RECIPE" in tag_name:
                cursor.execute("UPDATE batches SET recipe_name = ?, updated_at = CURRENT_TIMESTAMP WHERE batch_id = ?",
                             (str_value, current_batch))
            elif "FORMULA" in tag_name:
                cursor.execute("UPDATE batches SET formula_name = ?, updated_at = CURRENT_TIMESTAMP WHERE batch_id = ?",
                             (str_value, current_batch))
            elif "OPR_ID" in tag_name:
                cursor.execute("UPDATE batches SET operator_id = ?, updated_at = CURRENT_TIMESTAMP WHERE batch_id = ?",
                             (str_value, current_batch))
            elif "STATE" in tag_name and "STATUS" not in tag_name:
                cursor.execute("UPDATE batches SET final_state = ?, updated_at = CURRENT_TIMESTAMP WHERE batch_id = ?",
                             (str_value, current_batch))
            self.conn.commit()

    def _get_or_create_tag(self, unit_code: str, tag_info: TagInfo) -> int:
        """Get or create a tag record, return tag ID."""
        # Use base_name as the canonical tag identifier
        cache_key = f"{unit_code}/{tag_info.base_name}"

        if cache_key in self.tag_cache:
            return self.tag_cache[cache_key]

        unit_id = self.unit_cache.get(unit_code)
        cursor = self.conn.cursor()

        # Check for pending metadata
        meta = self.pending_tag_meta.get(tag_info.base_name, {})

        cursor.execute("""
            INSERT INTO tags (unit_id, tag_name, tag_type, tag_number, description, engineering_unit)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(tag_name) DO UPDATE SET
                tag_type = COALESCE(excluded.tag_type, tag_type),
                description = COALESCE(excluded.description, description),
                engineering_unit = COALESCE(excluded.engineering_unit, engineering_unit),
                updated_at = CURRENT_TIMESTAMP
        """, (
            unit_id,
            tag_info.base_name,
            tag_info.tag_type,
            tag_info.unit_number,
            meta.get("description"),
            meta.get("engineering_unit")
        ))

        cursor.execute("SELECT id FROM tags WHERE tag_name = ?", (tag_info.base_name,))
        row = cursor.fetchone()
        if row:
            self.tag_cache[cache_key] = row[0]
            self.conn.commit()
            return row[0]

        self.conn.commit()
        return 0

    def _store_tag_value(self, tag_id: int, value_type: str, value: Any):
        """Store a tag value."""
        if not tag_id:
            return

        # Determine numeric vs text value
        value_numeric = None
        value_text = None

        if isinstance(value, (int, float)):
            value_numeric = float(value)
        elif isinstance(value, str):
            try:
                value_numeric = float(value)
            except ValueError:
                value_text = value
        else:
            value_text = str(value)

        # Get current batch context
        # (We'd need to track which unit this tag belongs to for proper batch context)
        batch_id = None

        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT INTO tag_values (tag_id, value_type, value_numeric, value_text, batch_id)
            VALUES (?, ?, ?, ?, ?)
        """, (tag_id, value_type, value_numeric, value_text, batch_id))
        self.conn.commit()

    def _flush_raw_buffer(self):
        """Flush raw messages to database."""
        if not self.raw_buffer:
            return
        cursor = self.conn.cursor()
        cursor.executemany(
            "INSERT INTO messages_raw (topic, payload, payload_text, payload_type) VALUES (?, ?, ?, ?)",
            self.raw_buffer
        )
        self.conn.commit()
        self.raw_buffer.clear()

    def close(self):
        """Flush buffers and close connection."""
        with self.lock:
            if self.capture_raw:
                self._flush_raw_buffer()
        self.conn.close()

    def print_summary(self):
        """Print collection summary."""
        elapsed = (datetime.now() - self.start_time).total_seconds()
        print(f"\n\n{'='*60}")
        print("ENTERPRISE C DATA COLLECTION SUMMARY")
        print("="*60)
        print(f"Messages received: {self.message_count}")
        print(f"Messages stored: {self.stored_count}")
        print(f"Duration: {elapsed:.1f}s")
        print(f"Rate: {self.stored_count / elapsed:.1f} msg/s" if elapsed > 0 else "")

        # Re-open connection for summary (may have been closed)
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        tables = ["units", "tags", "tag_values", "batches", "phases", "messages_raw"]
        print("\nTable counts:")
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            print(f"  {table}: {cursor.fetchone()[0]}")

        # Show sample tags
        print("\nSample tags:")
        cursor.execute("SELECT tag_name, tag_type, description FROM tags LIMIT 10")
        for row in cursor.fetchall():
            print(f"  {row[0]}: type={row[1]}, desc={row[2][:30] if row[2] else None}...")

        # Show batch info
        cursor.execute("SELECT batch_id, recipe_name, formula_name FROM batches LIMIT 5")
        batches = cursor.fetchall()
        if batches:
            print("\nBatches:")
            for row in batches:
                print(f"  {row[0]}: recipe={row[1]}, formula={row[2]}")

        conn.close()
