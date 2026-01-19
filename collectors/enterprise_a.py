"""Enterprise A data collector - Glass Manufacturing.

Handles hierarchical equipment data, process measurements, and utilities monitoring.
"""

import json
import sqlite3
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from parsers import TopicInfo, EnterpriseAParser
from schemas.enterprise_a import SCHEMA_A


class EnterpriseACollector:
    """Collects Enterprise A glass manufacturing data into SQLite."""

    def __init__(self, db_path: str = "proveit_a.db", capture_raw: bool = False):
        self.db_path = db_path
        self.parser = EnterpriseAParser()
        self.capture_raw = capture_raw

        # Initialize database (check_same_thread=False for MQTT background thread)
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.executescript(SCHEMA_A)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.commit()
        print(f"Enterprise A database initialized: {Path(db_path).absolute()}")

        # Counters
        self.message_count = 0
        self.stored_count = 0
        self.start_time = datetime.now()

        # Caches
        self.site_cache: dict[str, int] = {}
        self.area_cache: dict[str, int] = {}
        self.equipment_cache: dict[str, int] = {}

        # Current state tracking
        self.current_states: dict[str, tuple] = {}  # equipment_key -> (code, reason)

        # Metric buckets for 10-second aggregation
        self.metric_buckets: dict[tuple, dict] = {}
        self.bucket_interval = 10
        self.current_bucket = self._get_bucket_ts()

        # Raw message buffer
        self.raw_buffer: list[tuple] = []
        self.raw_batch_size = 100

        # Lock for thread safety
        self.lock = threading.Lock()

    def _get_bucket_ts(self) -> int:
        """Get current bucket timestamp."""
        now = int(time.time())
        return now - (now % self.bucket_interval)

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

            # Check bucket rollover
            bucket_ts = self._get_bucket_ts()
            if bucket_ts != self.current_bucket:
                self._flush_metrics()
                self.current_bucket = bucket_ts

            # Process the data
            self._process_data(info, value)
            self.stored_count += 1

        # Progress
        if self.message_count % 500 == 0:
            elapsed = (datetime.now() - self.start_time).total_seconds()
            print(f"\rMsgs: {self.message_count} | Stored: {self.stored_count} | "
                  f"Time: {elapsed:.1f}s", end="", flush=True)

    def _process_data(self, info: TopicInfo, value: Any):
        """Route data to appropriate handler based on category."""
        cat = info.category
        dt = info.data_type

        if cat == "State":
            self._handle_state(info, dt, value)
        elif cat == "Status":
            self._handle_status(info, dt, value)
        elif cat == "edge":
            self._handle_sensor(info, dt, value)
        elif cat == "OEE":
            self._handle_oee(info, dt, value)
        elif cat in ("asset_info", "location_info"):
            self._handle_asset_info(info, value)
        elif info.area == "Utilities":
            self._handle_utility(info, dt, value)

    def _handle_state(self, info: TopicInfo, data_type: str, value: Any):
        """Handle equipment state changes."""
        equip_key = f"{info.site}/{info.area}/{info.equipment}"

        if data_type == "StateCurrent":
            state_code = int(value) if value else None
            prev = self.current_states.get(equip_key, (None, None))
            if state_code != prev[0]:
                self._insert_state_change(info, state_code, prev[0], prev[1])
                self.current_states[equip_key] = (state_code, prev[1])

        elif data_type == "StateReason":
            state_reason = str(value) if value else None
            prev = self.current_states.get(equip_key, (None, None))
            self.current_states[equip_key] = (prev[0], state_reason)

    def _insert_state_change(self, info: TopicInfo, code: int, prev_code: int, prev_reason: str):
        """Insert state change event."""
        equip_id = self._get_or_create_equipment(info)
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT INTO equipment_states (equipment_id, state_code, state_reason, prev_state_code, prev_state_reason)
            VALUES (?, ?, ?, ?, ?)
        """, (equip_id, code, self.current_states.get(f"{info.site}/{info.area}/{info.equipment}", (None, None))[1],
              prev_code, prev_reason))
        self.conn.commit()

    def _handle_status(self, info: TopicInfo, data_type: str, value: Any):
        """Handle process status measurements."""
        bucket_key = (self.current_bucket, info.site, info.area, info.equipment)
        if bucket_key not in self.metric_buckets:
            self.metric_buckets[bucket_key] = {}

        metrics = self.metric_buckets[bucket_key]

        try:
            val = float(value) if value not in (None, "") else None
        except (ValueError, TypeError):
            val = None

        if val is not None:
            # Map data_type to metric field
            field_map = {
                "Level": "level_pct",
                "BatchWeight": "batch_weight",
                "FeedRate": "feed_rate",
                "Temperature": "temperature",
                "Material": "material",
            }
            field = field_map.get(data_type)
            if field:
                metrics[field] = val

    def _handle_sensor(self, info: TopicInfo, data_type: str, value: Any):
        """Handle raw sensor readings."""
        equip_id = self._get_or_create_equipment(info)

        try:
            val = float(value) if value not in (None, "") else None
        except (ValueError, TypeError):
            val = None

        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT INTO sensor_readings (equipment_id, sensor_name, value, raw_value)
            VALUES (?, ?, ?, ?)
        """, (equip_id, data_type, val, str(value) if value else None))
        self.conn.commit()

    def _handle_oee(self, info: TopicInfo, data_type: str, value: Any):
        """Handle OEE metrics."""
        bucket_key = (self.current_bucket, info.site, info.line)
        if bucket_key not in self.metric_buckets:
            self.metric_buckets[bucket_key] = {"type": "oee"}

        metrics = self.metric_buckets[bucket_key]

        try:
            val = float(value) if value not in (None, "") else None
        except (ValueError, TypeError):
            val = None

        if val is not None and data_type:
            metrics[data_type.lower()] = val

    def _handle_utility(self, info: TopicInfo, data_type: str, value: Any):
        """Handle utility equipment readings."""
        bucket_key = (self.current_bucket, info.category, info.equipment)
        if bucket_key not in self.metric_buckets:
            self.metric_buckets[bucket_key] = {"type": "utility"}

        metrics = self.metric_buckets[bucket_key]

        try:
            val = float(value) if value not in (None, "") else None
        except (ValueError, TypeError):
            val = str(value) if value else None

        if data_type:
            metrics[data_type.lower()] = val

    def _handle_asset_info(self, info: TopicInfo, value: Any):
        """Handle asset/location info JSON payloads."""
        if isinstance(value, dict):
            # Could extract and store equipment metadata here
            pass

    def _get_or_create_equipment(self, info: TopicInfo) -> int:
        """Get or create equipment record."""
        cache_key = f"{info.site}/{info.area}/{info.equipment}"
        if cache_key in self.equipment_cache:
            return self.equipment_cache[cache_key]

        # Ensure site and area exist
        site_id = self._get_or_create_site(info.site)
        area_id = self._get_or_create_area(site_id, info.area)

        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT OR IGNORE INTO equipment (area_id, name, equipment_type)
            VALUES (?, ?, ?)
        """, (area_id, info.equipment, info.equipment))

        cursor.execute("SELECT id FROM equipment WHERE area_id = ? AND name = ?", (area_id, info.equipment))
        row = cursor.fetchone()
        if row:
            self.equipment_cache[cache_key] = row[0]
            self.conn.commit()
            return row[0]

        self.conn.commit()
        return 0

    def _get_or_create_site(self, site_name: str) -> int:
        """Get or create site record."""
        if not site_name:
            return 0
        if site_name in self.site_cache:
            return self.site_cache[site_name]

        cursor = self.conn.cursor()
        cursor.execute("INSERT OR IGNORE INTO sites (name) VALUES (?)", (site_name,))
        cursor.execute("SELECT id FROM sites WHERE name = ?", (site_name,))
        row = cursor.fetchone()
        if row:
            self.site_cache[site_name] = row[0]
            self.conn.commit()
            return row[0]

        self.conn.commit()
        return 0

    def _get_or_create_area(self, site_id: int, area_name: str) -> int:
        """Get or create area record."""
        if not area_name:
            return 0
        cache_key = f"{site_id}/{area_name}"
        if cache_key in self.area_cache:
            return self.area_cache[cache_key]

        cursor = self.conn.cursor()
        cursor.execute("INSERT OR IGNORE INTO areas (site_id, name) VALUES (?, ?)", (site_id, area_name))
        cursor.execute("SELECT id FROM areas WHERE site_id = ? AND name = ?", (site_id, area_name))
        row = cursor.fetchone()
        if row:
            self.area_cache[cache_key] = row[0]
            self.conn.commit()
            return row[0]

        self.conn.commit()
        return 0

    def _flush_metrics(self):
        """Flush metric buckets to database."""
        if not self.metric_buckets:
            return

        cursor = self.conn.cursor()

        for bucket_key, metrics in self.metric_buckets.items():
            metric_type = metrics.pop("type", None)

            if metric_type == "oee":
                bucket_ts, site, line = bucket_key
                bucket_dt = datetime.fromtimestamp(bucket_ts).isoformat()
                cursor.execute("""
                    INSERT INTO oee_metrics (bucket, site, line, availability, performance, quality, oee)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(bucket, site, line) DO UPDATE SET
                        availability = excluded.availability,
                        performance = excluded.performance,
                        quality = excluded.quality,
                        oee = excluded.oee
                """, (bucket_dt, site, line,
                      metrics.get("availability"),
                      metrics.get("performance"),
                      metrics.get("quality"),
                      metrics.get("oee")))

            elif metric_type == "utility":
                bucket_ts, utility_type, equipment = bucket_key
                bucket_dt = datetime.fromtimestamp(bucket_ts).isoformat()
                cursor.execute("""
                    INSERT INTO utility_readings (bucket, utility_type, equipment_name, state_value, state_name)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(bucket, utility_type, equipment_name) DO UPDATE SET
                        state_value = excluded.state_value,
                        state_name = excluded.state_name
                """, (bucket_dt, utility_type, equipment,
                      metrics.get("value"), metrics.get("state")))

            else:
                # Process data metrics
                bucket_ts, site, area, equipment = bucket_key
                bucket_dt = datetime.fromtimestamp(bucket_ts).isoformat()
                equip_id = self.equipment_cache.get(f"{site}/{area}/{equipment}", 0)
                if equip_id:
                    cursor.execute("""
                        INSERT INTO process_data (bucket, equipment_id, level_pct, batch_weight, feed_rate, temperature)
                        VALUES (?, ?, ?, ?, ?, ?)
                        ON CONFLICT(bucket, equipment_id) DO UPDATE SET
                            level_pct = COALESCE(excluded.level_pct, level_pct),
                            batch_weight = COALESCE(excluded.batch_weight, batch_weight),
                            feed_rate = COALESCE(excluded.feed_rate, feed_rate),
                            temperature = COALESCE(excluded.temperature, temperature)
                    """, (bucket_dt, equip_id,
                          metrics.get("level_pct"),
                          metrics.get("batch_weight"),
                          metrics.get("feed_rate"),
                          metrics.get("temperature")))

        self.conn.commit()
        self.metric_buckets.clear()

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
            self._flush_metrics()
            if self.capture_raw:
                self._flush_raw_buffer()
        self.conn.close()

    def print_summary(self):
        """Print collection summary."""
        elapsed = (datetime.now() - self.start_time).total_seconds()
        print(f"\n\n{'='*60}")
        print("ENTERPRISE A DATA COLLECTION SUMMARY")
        print("="*60)
        print(f"Messages received: {self.message_count}")
        print(f"Messages stored: {self.stored_count}")
        print(f"Duration: {elapsed:.1f}s")
        print(f"Rate: {self.stored_count / elapsed:.1f} msg/s" if elapsed > 0 else "")

        # Re-open connection for summary (may have been closed)
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        tables = ["sites", "areas", "equipment", "equipment_states", "process_data",
                  "sensor_readings", "utility_readings", "oee_metrics", "messages_raw"]
        print("\nTable counts:")
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            print(f"  {table}: {cursor.fetchone()[0]}")

        conn.close()
