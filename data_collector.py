"""Data collector for Enterprise B manufacturing data."""

import json
import re
import signal
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime

from mqtt_client import MQTTClient
from schema import get_connection, init_db

# Topics to ignore
IGNORED_PREFIXES = ("maintainx/", "abelara/", "roeslein/")


@dataclass
class TopicInfo:
    """Parsed topic information."""
    topic: str
    site: str | None = None
    area: str | None = None
    line: str | None = None
    equipment: str | None = None
    category: str | None = None
    data_type: str | None = None


def parse_topic(topic: str) -> TopicInfo | None:
    """Parse Enterprise B topic into components.

    Format: Enterprise B/{Site}/{area}/{line}/{equipment}/{category}/{data_type}
    Example: Enterprise B/Site1/fillerproduction/fillingline01/caploader/metric/availability
    """
    if not topic.startswith("Enterprise B/"):
        return None

    # Skip ignored topics
    remainder = topic[len("Enterprise B/"):]
    if any(remainder.startswith(p) for p in IGNORED_PREFIXES):
        return None

    parts = topic.split("/")
    info = TopicInfo(topic=topic)

    if len(parts) >= 2:
        info.site = parts[1] if parts[1].startswith("Site") else None
    if len(parts) >= 3:
        info.area = parts[2]
    if len(parts) >= 4:
        info.line = parts[3]
    if len(parts) >= 5:
        info.equipment = parts[4]
    if len(parts) >= 6:
        info.category = parts[5]
    if len(parts) >= 7:
        info.data_type = "/".join(parts[6:])

    return info


class DataCollector:
    """Collects and stores MQTT data to SQLite."""

    def __init__(self, db_path: str = "proveit.db"):
        self.db_path = db_path
        self.conn = get_connection(db_path)
        self.message_count = 0
        self.stored_count = 0
        self.start_time = datetime.now()

        # Batch insert buffers
        self.raw_buffer: list[tuple] = []
        self.batch_size = 100

        # Track metrics by equipment for aggregation
        self.metric_cache: dict[str, dict] = {}

    def handle_message(self, topic: str, payload: bytes):
        """Process incoming MQTT message."""
        self.message_count += 1

        # Parse topic
        info = parse_topic(topic)
        if not info:
            return  # Skip non-Enterprise B or ignored topics

        self.stored_count += 1

        # Decode payload
        try:
            decoded = payload.decode("utf-8")
            try:
                data = json.loads(decoded)
                msg_type = "json"
            except json.JSONDecodeError:
                data = decoded
                msg_type = "text"
        except UnicodeDecodeError:
            data = payload.hex()
            msg_type = "binary"

        # Store raw message
        self.raw_buffer.append((
            topic,
            payload,
            decoded if msg_type != "binary" else None,
            msg_type,
        ))

        # Update topic registry
        self._update_topic(info, msg_type)

        # Process specific data types
        if info.category == "metric":
            self._process_metric(info, data)
        elif info.category == "node":
            self._process_node(info, data)
        elif info.category in ("processdata", "workorder"):
            self._process_data(info, data)

        # Flush buffer if full
        if len(self.raw_buffer) >= self.batch_size:
            self._flush_raw_buffer()

        # Progress output
        if self.message_count % 500 == 0:
            elapsed = (datetime.now() - self.start_time).total_seconds()
            print(f"\rProcessed: {self.message_count} | Stored: {self.stored_count} | "
                  f"Time: {elapsed:.1f}s", end="", flush=True)

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

    def _update_topic(self, info: TopicInfo, msg_type: str):
        """Update topic registry."""
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT INTO topics (topic, site, area, line, equipment, category, data_type, message_type, message_count)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)
            ON CONFLICT(topic) DO UPDATE SET
                last_seen = CURRENT_TIMESTAMP,
                message_count = message_count + 1
        """, (info.topic, info.site, info.area, info.line, info.equipment, info.category, info.data_type, msg_type))

    def _process_metric(self, info: TopicInfo, value):
        """Process OEE metric data."""
        if not info.equipment or not info.data_type:
            return

        # Build equipment key
        key = f"{info.site}/{info.area}/{info.line}/{info.equipment}"

        if key not in self.metric_cache:
            self.metric_cache[key] = {
                "site": info.site,
                "area": info.area,
                "line": info.line,
                "equipment": info.equipment,
            }

        # Map data_type to column
        metric_map = {
            "availability": "availability",
            "performance": "performance",
            "quality": "quality",
            "oee": "oee",
            "input/countdefect": "count_defect",
            "input/countinfeed": "count_infeed",
            "input/countoutfeed": "count_outfeed",
            "input/rateactual": "rate_actual",
            "input/ratestandard": "rate_standard",
            "input/timedownplanned": "time_down_planned",
            "input/timedownunplanned": "time_down_unplanned",
            "input/timeidle": "time_idle",
            "input/timerunning": "time_running",
        }

        col = metric_map.get(info.data_type)
        if col and value is not None:
            try:
                self.metric_cache[key][col] = float(value) if value != "" else None
            except (ValueError, TypeError):
                pass

    def _process_node(self, info: TopicInfo, value):
        """Process asset/node data."""
        if not info.data_type:
            return

        # Extract asset identifier data
        node_map = {
            "assetidentifier/assetid": "asset_id",
            "assetidentifier/assetname": "asset_name",
            "assetidentifier/assetpath": "asset_path",
            "assetidentifier/assettypename": "asset_type_name",
            "assetidentifier/displayname": "display_name",
            "assetidentifier/parentassetid": "parent_asset_id",
            "assetidentifier/sortorder": "sort_order",
        }

        col = node_map.get(info.data_type)
        if col and value is not None:
            # Use upsert pattern for assets
            cursor = self.conn.cursor()
            if col == "asset_id":
                try:
                    cursor.execute("""
                        INSERT INTO assets (asset_id, site, area, line, equipment)
                        VALUES (?, ?, ?, ?, ?)
                        ON CONFLICT(asset_id) DO UPDATE SET
                            site = excluded.site,
                            area = excluded.area,
                            line = excluded.line,
                            equipment = excluded.equipment,
                            updated_at = CURRENT_TIMESTAMP
                    """, (int(value), info.site, info.area, info.line, info.equipment))
                except (ValueError, TypeError):
                    pass

    def _process_data(self, info: TopicInfo, value):
        """Process state/work order data."""
        if not info.data_type or value is None:
            return

        cursor = self.conn.cursor()

        # Map common process data fields
        if "state/name" in info.data_type:
            cursor.execute("""
                INSERT INTO process_data (site, area, line, equipment, state_name)
                VALUES (?, ?, ?, ?, ?)
            """, (info.site, info.area, info.line, info.equipment, str(value)))
        elif "lotnumber" in info.data_type or "workorder" in info.data_type:
            if "itemname" in info.data_type:
                cursor.execute("""
                    INSERT INTO process_data (site, area, line, equipment, item_name)
                    VALUES (?, ?, ?, ?, ?)
                """, (info.site, info.area, info.line, info.equipment, str(value)))

    def flush_metrics(self):
        """Flush cached metrics to database."""
        if not self.metric_cache:
            return

        cursor = self.conn.cursor()
        for key, data in self.metric_cache.items():
            cols = [k for k in data.keys() if k not in ("site", "area", "line", "equipment")]
            if not cols:
                continue

            # Only insert if we have actual metric values
            metric_cols = [c for c in cols if data.get(c) is not None]
            if not metric_cols:
                continue

            all_cols = ["site", "area", "line", "equipment"] + metric_cols
            placeholders = ", ".join(["?"] * len(all_cols))
            col_names = ", ".join(all_cols)
            values = [data.get(c) for c in all_cols]

            cursor.execute(f"INSERT INTO metrics ({col_names}) VALUES ({placeholders})", values)

        self.conn.commit()
        self.metric_cache.clear()

    def close(self):
        """Flush buffers and close connection."""
        self._flush_raw_buffer()
        self.flush_metrics()
        self.conn.close()

    def print_summary(self):
        """Print collection summary."""
        elapsed = (datetime.now() - self.start_time).total_seconds()
        print(f"\n\n{'=' * 50}")
        print("DATA COLLECTION SUMMARY")
        print("=" * 50)
        print(f"Total messages received: {self.message_count}")
        print(f"Enterprise B messages stored: {self.stored_count}")
        print(f"Duration: {elapsed:.1f}s")
        print(f"Rate: {self.stored_count / elapsed:.1f} msg/s")

        # Query stats
        conn = get_connection(self.db_path)
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM messages_raw")
        print(f"\nRaw messages: {cursor.fetchone()[0]}")

        cursor.execute("SELECT COUNT(*) FROM topics")
        print(f"Unique topics: {cursor.fetchone()[0]}")

        cursor.execute("SELECT COUNT(*) FROM assets WHERE asset_id IS NOT NULL")
        print(f"Assets discovered: {cursor.fetchone()[0]}")

        cursor.execute("SELECT COUNT(*) FROM metrics")
        print(f"Metric records: {cursor.fetchone()[0]}")

        cursor.execute("SELECT COUNT(*) FROM process_data")
        print(f"Process data records: {cursor.fetchone()[0]}")

        # Top sites by message count
        cursor.execute("""
            SELECT site, COUNT(*) as cnt FROM topics
            WHERE site IS NOT NULL
            GROUP BY site ORDER BY cnt DESC
        """)
        print("\nTopics by site:")
        for row in cursor.fetchall():
            print(f"  {row[0]}: {row[1]} topics")

        conn.close()


def main():
    # Initialize DB if needed
    init_db()

    collector = DataCollector()
    client = MQTTClient()
    client.add_message_handler(collector.handle_message)

    def signal_handler(sig, frame):
        print("\n\nStopping collection...")
        client.stop()
        collector.close()
        collector.print_summary()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    if not client.connect():
        print("Failed to connect to MQTT broker")
        sys.exit(1)

    # Subscribe only to Enterprise B
    print("Subscribing to Enterprise B/#...")
    print("Press Ctrl+C to stop and see summary\n")
    client.subscribe("Enterprise B/#")

    try:
        client.start()
    except KeyboardInterrupt:
        pass
    finally:
        client.stop()
        collector.close()
        collector.print_summary()


if __name__ == "__main__":
    main()
