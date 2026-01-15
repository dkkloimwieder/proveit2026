"""Data collector for Enterprise B manufacturing data.

Collects MQTT data into SQLite with:
- Reference tables for products, lots, work orders, states
- Event log for state/lot/work order changes
- 10-second bucketed metrics by line
"""

import json
import signal
import sqlite3
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

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


CATEGORY_NAMES = ("metric", "node", "workorder", "lotnumber", "processdata", "state")


def parse_topic(topic: str) -> TopicInfo | None:
    """Parse Enterprise B topic into components.

    Handles variable depth topics:
    - Enterprise-level: Node/category/data_type (4 parts)
    - Site-level: Site/node/category/data_type (5 parts)
    - Area-level: Site/area/category/data_type (6 parts)
    - Line-level: Site/area/line/category/data_type (7 parts)
    - Equipment-level: Site/area/line/equipment/category/data_type (8 parts)
    """
    if not topic.startswith("Enterprise B/"):
        return None

    remainder = topic[len("Enterprise B/"):]
    if any(remainder.startswith(p) for p in IGNORED_PREFIXES):
        return None

    parts = topic.split("/")
    info = TopicInfo(topic=topic)

    # Handle enterprise-level topics (Enterprise B/Node/... or Enterprise B/Metric/...)
    if len(parts) >= 2 and parts[1] in ("Node", "Metric"):
        info.site = None
        info.area = None
        info.category = parts[1].lower()  # 'node' or 'metric'
        if len(parts) >= 3:
            info.data_type = "/".join(parts[2:])
        return info

    if len(parts) >= 2:
        info.site = parts[1] if parts[1].startswith("Site") else None

    # Handle site-level topics (Enterprise B/Site/node/...)
    if len(parts) >= 3 and parts[2] in CATEGORY_NAMES:
        info.area = None
        info.category = parts[2]
        if len(parts) >= 4:
            info.data_type = "/".join(parts[3:])
        return info

    if len(parts) >= 3:
        info.area = parts[2]

    # Detect topic depth by checking where category appears
    if len(parts) >= 4:
        if parts[3] in CATEGORY_NAMES:
            # Area-level: parts[3] is category
            info.line = None
            info.equipment = None
            info.category = parts[3]
            if len(parts) >= 5:
                info.data_type = "/".join(parts[4:])
        elif len(parts) >= 5 and parts[4] in CATEGORY_NAMES:
            # Line-level: parts[3] is line, parts[4] is category
            info.line = parts[3]
            info.equipment = None
            info.category = parts[4]
            if len(parts) >= 6:
                info.data_type = "/".join(parts[5:])
        else:
            # Equipment-level: standard structure
            info.line = parts[3]
            if len(parts) >= 5:
                info.equipment = parts[4]
            if len(parts) >= 6:
                info.category = parts[5]
            if len(parts) >= 7:
                info.data_type = "/".join(parts[6:])

    return info


@dataclass
class LineMetrics:
    """Accumulated metrics for a line within a bucket."""
    availability: list[float] = field(default_factory=list)
    performance: list[float] = field(default_factory=list)
    quality: list[float] = field(default_factory=list)
    oee: list[float] = field(default_factory=list)
    count_infeed: int = 0
    count_outfeed: int = 0
    count_defect: int = 0
    time_running: float = 0
    time_idle: float = 0
    time_down_planned: float = 0
    time_down_unplanned: float = 0
    rate_actual: list[float] = field(default_factory=list)
    rate_standard: list[float] = field(default_factory=list)
    temperature: list[float] = field(default_factory=list)
    flow_rate: list[float] = field(default_factory=list)
    weight: list[float] = field(default_factory=list)
    equipment_seen: set = field(default_factory=set)
    work_order_id: int | None = None
    lot_id: int | None = None


class DataCollector:
    """Collects MQTT data into SQLite."""

    def __init__(self, db_path: str = "proveit.db", capture_raw: bool = False):
        self.db_path = db_path
        self.conn = get_connection(db_path)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.capture_raw = capture_raw

        self.message_count = 0
        self.stored_count = 0
        self.start_time = datetime.now()

        # Caches for reference data IDs
        self.product_cache: dict[int, int] = {}      # item_id -> db id
        self.lot_cache: dict[int, int] = {}          # lot_number_id -> db id
        self.work_order_cache: dict[int, int] = {}   # work_order_id -> db id
        self.state_cache: dict[tuple, int] = {}      # (code, name, type) -> db id
        self.asset_type_cache: dict[str, int] = {}   # name -> db id

        # Current state tracking for change detection
        self.current_state: dict[str, int] = {}      # equipment_key -> state_id
        self.current_lot: dict[str, int] = {}        # equipment_key -> lot_id
        self.current_wo: dict[str, int] = {}         # location_key -> work_order_id
        self.wo_first_seen: dict[str, datetime] = {} # location_key -> first_seen timestamp
        self.wo_data_cache: dict[str, dict] = {}     # location_key -> last known WO data

        # Metric buckets: (bucket_ts, site, line) -> LineMetrics
        self.metric_buckets: dict[tuple, LineMetrics] = {}
        self.bucket_interval = 10  # seconds
        self.current_bucket: int = self._get_bucket_ts()

        # Pending reference data updates
        self.pending_products: dict[int, dict] = {}
        self.pending_lots: dict[str, dict] = {}  # keyed by path
        self.pending_work_orders: dict[int, dict] = {}
        self.pending_assets: dict[int, dict] = {}

        # Raw message buffer
        self.raw_buffer: list[tuple] = []
        self.raw_batch_size = 100

        # Lock for thread safety
        self.lock = threading.Lock()

    def _get_bucket_ts(self) -> int:
        """Get current bucket timestamp (rounded to interval)."""
        now = int(time.time())
        return now - (now % self.bucket_interval)

    def handle_message(self, topic: str, payload: bytes):
        """Process incoming MQTT message."""
        self.message_count += 1

        info = parse_topic(topic)
        if not info:
            return

        self.stored_count += 1

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

            # Process by data type
            if info.data_type:
                self._process_data(info, value)

        # Progress
        if self.message_count % 1000 == 0:
            elapsed = (datetime.now() - self.start_time).total_seconds()
            print(f"\rMsgs: {self.message_count} | Stored: {self.stored_count} | "
                  f"Time: {elapsed:.1f}s", end="", flush=True)

    def _process_data(self, info: TopicInfo, value: Any):
        """Route data to appropriate handler."""
        dt = info.data_type
        cat = info.category

        # Product/Item data (also nested under lotnumber/)
        if dt and (dt.startswith("item/") or dt.startswith("lotnumber/item/")):
            field = dt.split("/")[-1]  # Get last segment
            self._handle_product(info, field, value)

        # Lot data (direct or nested under workorder/)
        elif dt and (dt.startswith("lotnumber/") or dt.startswith("workorder/lotnumber/")):
            # Extract field - check lotnumberid BEFORE lotnumber (substring issue)
            if dt.endswith("lotnumberid"):
                self._handle_lot(info, "lotnumberid", value)
            elif dt.endswith("lotnumber"):
                self._handle_lot(info, "lotnumber", value)
            elif dt.startswith("lotnumber/") and not dt.startswith("lotnumber/item/"):
                self._handle_lot(info, dt[10:], value)

        # Lot fields at lotnumber category level (direct fields like lotnumberid, lotnumber)
        elif cat == "lotnumber" and dt in ("lotnumberid", "lotnumber"):
            self._handle_lot(info, dt, value)

        # Work order data (direct fields under workorder/ prefix)
        elif dt and dt.startswith("workorder/") and not dt.startswith("workorder/lotnumber/"):
            self._handle_work_order(info, dt[10:], value)

        # Work order fields at workorder category level
        elif cat == "workorder" and dt in ("quantityactual", "quantitydefect", "quantitytarget",
                                            "workorderid", "workordernumber", "uom", "assetid"):
            self._handle_work_order(info, dt, value)

        # State data
        elif dt and dt.startswith("state/"):
            self._handle_state(info, dt[6:], value)

        # Asset identifier data
        elif dt and dt.startswith("assetidentifier/"):
            self._handle_asset(info, dt[16:], value)
        elif cat == "node" and dt:
            # node/assetidentifier/field -> dt is "assetidentifier/field"
            if dt.startswith("assetidentifier/"):
                self._handle_asset(info, dt[16:], value)
        elif cat == "assetidentifier" and dt in (
            "assetid", "assetname", "assetpath", "displayname",
            "parentassetid", "sortorder", "assettypename"
        ):
            self._handle_asset(info, dt, value)

        # Metrics (under metric category)
        elif cat == "metric" or dt in (
            "availability", "performance", "quality", "oee",
            "input/countdefect", "input/countinfeed", "input/countoutfeed",
            "input/rateactual", "input/ratestandard",
            "input/timedownplanned", "input/timedownunplanned",
            "input/timeidle", "input/timerunning"
        ):
            self._handle_metric(info, dt, value)

        # Process data counts/rates (under processdata category)
        elif cat == "processdata" or (dt and dt.startswith("processdata/")):
            self._handle_processdata(info, dt, value)

        # Process measurements (temperature, flow, weight)
        elif dt and dt.startswith("process/"):
            self._handle_process(info, dt[8:], value)

    def _handle_product(self, info: TopicInfo, field: str, value: Any):
        """Handle product/item data - accumulate by path then flush when complete."""
        path_key = f"{info.site}/{info.area}/{info.line}/{info.equipment}"

        if path_key not in self.pending_products:
            self.pending_products[path_key] = {}

        product = self.pending_products[path_key]

        if field == "itemid" and value:
            product["item_id"] = int(value)
            # Also store in pending_lots for linking
            if path_key in self.pending_lots:
                self.pending_lots[path_key]["item_id"] = int(value)
        elif field == "itemname" and value:
            product["name"] = str(value)
        elif field == "itemclass" and value:
            product["class"] = str(value)
        elif field == "bottlesize":
            product["bottlesize"] = float(value) if value else 0
        elif field == "packcount":
            product["packcount"] = int(value) if value else 0
        elif field == "labelvariant":
            product["labelvariant"] = str(value) if value else None
        elif field == "parentitemid":
            product["parentitemid"] = int(value) if value else None

        # Flush when we have all expected fields (item_id, name, class, bottlesize, packcount)
        required = {"item_id", "name", "class", "bottlesize", "packcount"}
        if required.issubset(product.keys()):
            self._flush_pending_product(path_key, product)
            del self.pending_products[path_key]

    def _handle_lot(self, info: TopicInfo, field: str, value: Any):
        """Handle lot data - accumulate by path then flush when complete."""
        path_key = f"{info.site}/{info.area}/{info.line}/{info.equipment}"

        if path_key not in self.pending_lots:
            self.pending_lots[path_key] = {}

        lot = self.pending_lots[path_key]

        if field == "lotnumberid" and value:
            lot["lot_number_id"] = int(value)
        elif field == "lotnumber" and value:
            lot["lot_number"] = str(value)

        # Flush if we have both id and number
        if "lot_number_id" in lot and "lot_number" in lot:
            self._flush_pending_lots()
            self.pending_lots.pop(path_key, None)

    def _handle_work_order(self, info: TopicInfo, field: str, value: Any):
        """Handle work order data and detect WO changes."""
        # Track per equipment (different vats have different WOs)
        location_key = f"{info.site}/{info.line}/{info.equipment or 'line'}"

        # Initialize or get cached data for this location
        if location_key not in self.wo_data_cache:
            self.wo_data_cache[location_key] = {
                "site": info.site,
                "line": info.line,
                "equipment": info.equipment
            }
        wo_data = self.wo_data_cache[location_key]

        if field == "workorderid" and value:
            wo_id = int(value)

            # Check for WO change at this location BEFORE updating cache
            prev_wo_id = self.current_wo.get(location_key)
            if prev_wo_id and prev_wo_id != wo_id:
                # WO changed - log completion with PREVIOUS data
                prev_data = wo_data.copy()  # Copy before we modify it
                self._log_wo_completion(info, prev_wo_id, prev_data, wo_id, "")
                # Reset cache for new WO
                self.wo_data_cache[location_key] = {
                    "site": info.site,
                    "line": info.line,
                    "equipment": info.equipment,
                    "work_order_id": wo_id
                }
                wo_data = self.wo_data_cache[location_key]
                self.current_wo[location_key] = wo_id
                self.wo_first_seen[location_key] = datetime.now()
            elif not prev_wo_id:
                # First WO at this location
                wo_data["work_order_id"] = wo_id
                self.current_wo[location_key] = wo_id
                self.wo_first_seen[location_key] = datetime.now()
            else:
                # Same WO, just update
                wo_data["work_order_id"] = wo_id

            # Also add to pending for DB upsert
            if wo_id not in self.pending_work_orders:
                self.pending_work_orders[wo_id] = {
                    "work_order_id": wo_id,
                    "site": info.site,
                    "line": info.line
                }

        elif field == "workordernumber" and value:
            wo_data["work_order_number"] = str(value)
            # Update pending if exists
            for wo in self.pending_work_orders.values():
                if "work_order_number" not in wo:
                    wo["work_order_number"] = str(value)
                    break

        elif field in ("quantitytarget", "quantityactual", "quantitydefect", "uom", "assetid"):
            col = {
                "quantitytarget": "quantity_target",
                "quantityactual": "quantity_actual",
                "quantitydefect": "quantity_defect",
                "uom": "uom",
                "assetid": "asset_id"
            }.get(field)
            if col:
                wo_data[col] = value
                # Update pending if exists
                for wo in self.pending_work_orders.values():
                    if col not in wo:
                        wo[col] = value
                        break

        self._flush_pending_work_orders()

    def _handle_state(self, info: TopicInfo, field: str, value: Any):
        """Handle state data and detect changes."""
        equip_key = f"{info.site}/{info.area}/{info.line}/{info.equipment}"

        if field == "name" and value:
            state_name = str(value)
            state_id = self._get_or_create_state(None, state_name, None)

            prev_state_id = self.current_state.get(equip_key)
            if prev_state_id != state_id:
                # State changed - log event
                self._insert_event(
                    info, "state",
                    state_id=state_id,
                    prev_state_id=prev_state_id
                )
                self.current_state[equip_key] = state_id

        elif field == "code":
            pass  # Handle with name
        elif field == "duration":
            pass  # Could track duration separately

    def _handle_work_order_change(self, info: TopicInfo, wo_id: int, wo_number: str, wo_data: dict):
        """Detect WO changes and log completion."""
        location_key = f"{info.site}/{info.line}/{info.equipment or 'line'}"

        prev_wo_id = self.current_wo.get(location_key)

        if prev_wo_id and prev_wo_id != wo_id:
            # WO CHANGED - log completion of previous WO
            prev_data = self.wo_data_cache.get(location_key, {})
            self._log_wo_completion(info, prev_wo_id, prev_data, wo_id, wo_number)

        # Update tracking
        if prev_wo_id != wo_id:
            # New WO at this location
            self.wo_first_seen[location_key] = datetime.now()

        self.current_wo[location_key] = wo_id
        self.wo_data_cache[location_key] = wo_data.copy()

    def _log_wo_completion(self, info: TopicInfo, prev_wo_id: int, prev_data: dict,
                          next_wo_id: int, next_wo_number: str):
        """Log WO completion with final quantities and metrics."""
        location_key = f"{info.site}/{info.line}/{info.equipment or 'line'}"

        # Query DB for most accurate final values (cache may be incomplete after restart)
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT work_order_number, quantity_actual, quantity_target, quantity_defect, uom
            FROM work_orders WHERE work_order_id = ?
        """, (prev_wo_id,))
        row = cursor.fetchone()
        if row:
            wo_number, actual, target, defect, uom = row
        else:
            # Fall back to cache
            wo_number = prev_data.get("work_order_number")
            actual = prev_data.get("quantity_actual")
            target = prev_data.get("quantity_target")
            defect = prev_data.get("quantity_defect")
            uom = prev_data.get("uom")

        # Get duration
        first_seen = self.wo_first_seen.get(location_key)
        duration = (datetime.now() - first_seen).total_seconds() if first_seen else None

        # Calculate completion percentage
        pct = None
        if target and target > 0 and actual:
            pct = (actual / target) * 100

        # Get current metrics for this line
        bucket_key = (self.current_bucket, info.site, info.line)
        metrics = self.metric_buckets.get(bucket_key)

        # Get latest OEE values
        final_oee = metrics.oee[-1] if metrics and metrics.oee else None
        final_avail = metrics.availability[-1] if metrics and metrics.availability else None
        final_perf = metrics.performance[-1] if metrics and metrics.performance else None
        final_qual = metrics.quality[-1] if metrics and metrics.quality else None
        final_infeed = metrics.count_infeed if metrics else None
        final_outfeed = metrics.count_outfeed if metrics else None

        cursor.execute("""
            INSERT INTO work_order_completions (
                site, area, line, equipment,
                work_order_id, work_order_number,
                final_quantity, quantity_target, quantity_defect, uom, pct_complete,
                final_oee, final_availability, final_performance, final_quality,
                final_count_infeed, final_count_outfeed,
                next_work_order_id, next_work_order_number,
                duration_seconds
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            info.site, info.area, info.line, info.equipment,
            prev_wo_id, wo_number,
            actual, target, defect, uom, pct,
            final_oee, final_avail, final_perf, final_qual,
            final_infeed, final_outfeed,
            next_wo_id, next_wo_number,
            duration
        ))
        self.conn.commit()

        print(f"\n[WO COMPLETED] {wo_number} @ {info.site}/{info.line} "
              f"qty={actual} -> next_wo_id={next_wo_id}")

    def _handle_asset(self, info: TopicInfo, field: str, value: Any):
        """Handle asset identifier data."""
        if field == "assettypename" and value:
            self._get_or_create_asset_type(str(value))

        if field == "assetid" and value:
            asset_id = int(value)
            if asset_id not in self.pending_assets:
                self.pending_assets[asset_id] = {
                    "asset_id": asset_id,
                    "site": info.site,
                    "area": info.area,
                    "line": info.line,
                    "equipment": info.equipment
                }
        elif field in ("assetname", "assetpath", "displayname", "parentassetid", "sortorder", "assettypename"):
            for asset in self.pending_assets.values():
                col = {
                    "assetname": "asset_name",
                    "assetpath": "asset_path",
                    "displayname": "display_name",
                    "parentassetid": "parent_asset_id",
                    "sortorder": "sort_order",
                    "assettypename": "asset_type_name"
                }.get(field)
                if col and col not in asset:
                    asset[col] = value
                    break

        self._flush_pending_assets()

    def _handle_metric(self, info: TopicInfo, data_type: str, value: Any):
        """Handle metric data - accumulate into bucket."""
        if not info.site or not info.line:
            return

        bucket_key = (self.current_bucket, info.site, info.line)
        if bucket_key not in self.metric_buckets:
            self.metric_buckets[bucket_key] = LineMetrics()

        metrics = self.metric_buckets[bucket_key]
        if info.equipment:
            metrics.equipment_seen.add(info.equipment)

        try:
            val = float(value) if value not in (None, "") else None
        except (ValueError, TypeError):
            return

        if val is None:
            return

        # Map data type to metric field
        if data_type == "availability":
            metrics.availability.append(val)
        elif data_type == "performance":
            metrics.performance.append(val)
        elif data_type == "quality":
            metrics.quality.append(val)
        elif data_type == "oee":
            metrics.oee.append(val)
        elif data_type == "input/countinfeed":
            metrics.count_infeed = int(val)
        elif data_type == "input/countoutfeed":
            metrics.count_outfeed = int(val)
        elif data_type == "input/countdefect":
            metrics.count_defect = int(val)
        elif data_type == "input/timerunning":
            metrics.time_running = val
        elif data_type == "input/timeidle":
            metrics.time_idle = val
        elif data_type == "input/timedownplanned":
            metrics.time_down_planned = val
        elif data_type == "input/timedownunplanned":
            metrics.time_down_unplanned = val
        elif data_type == "input/rateactual":
            metrics.rate_actual.append(val)
        elif data_type == "input/ratestandard":
            metrics.rate_standard.append(val)

    def _handle_process(self, info: TopicInfo, field: str, value: Any):
        """Handle process data (temperature, flow, weight)."""
        if not info.site or not info.line:
            return

        bucket_key = (self.current_bucket, info.site, info.line)
        if bucket_key not in self.metric_buckets:
            self.metric_buckets[bucket_key] = LineMetrics()

        metrics = self.metric_buckets[bucket_key]

        try:
            val = float(value) if value not in (None, "") else None
        except (ValueError, TypeError):
            return

        if val is None:
            return

        if field == "temperature":
            metrics.temperature.append(val)
        elif field == "flowrate":
            metrics.flow_rate.append(val)
        elif field == "weight":
            metrics.weight.append(val)

    def _handle_processdata(self, info: TopicInfo, data_type: str, value: Any):
        """Handle processdata counts, rates, and inputs."""
        if not info.site or not info.line:
            return

        bucket_key = (self.current_bucket, info.site, info.line)
        if bucket_key not in self.metric_buckets:
            self.metric_buckets[bucket_key] = LineMetrics()

        metrics = self.metric_buckets[bucket_key]
        if info.equipment:
            metrics.equipment_seen.add(info.equipment)

        try:
            val = float(value) if value not in (None, "") else None
        except (ValueError, TypeError):
            return

        if val is None:
            return

        # Map processdata fields to metrics
        # processdata/count/infeed, count/outfeed, count/defect
        if "count/infeed" in data_type or data_type == "count/infeed":
            metrics.count_infeed = int(val)
        elif "count/outfeed" in data_type or data_type == "count/outfeed":
            metrics.count_outfeed = int(val)
        elif "count/defect" in data_type or data_type == "count/defect":
            metrics.count_defect = int(val)
        # processdata/rate/instant
        elif "rate/instant" in data_type or data_type == "rate/instant":
            metrics.rate_actual.append(val)
        # processdata/input/infeedtooutfeed (yield ratio)
        elif "infeedtooutfeed" in data_type:
            pass  # Could track separately if needed

    # --- Reference data helpers ---

    def _get_or_create_state(self, code: int | None, name: str, stype: str | None) -> int:
        """Get or create state, return ID."""
        key = (code, name, stype)
        if key in self.state_cache:
            return self.state_cache[key]

        cursor = self.conn.cursor()
        cursor.execute(
            "INSERT OR IGNORE INTO states (code, name, type) VALUES (?, ?, ?)",
            (code, name, stype)
        )
        cursor.execute(
            "SELECT id FROM states WHERE name = ? AND (code IS ? OR code = ?) AND (type IS ? OR type = ?)",
            (name, code, code, stype, stype)
        )
        row = cursor.fetchone()
        if row:
            self.state_cache[key] = row[0]
            return row[0]
        return 0

    def _get_or_create_asset_type(self, name: str) -> int:
        """Get or create asset type, return ID."""
        if name in self.asset_type_cache:
            return self.asset_type_cache[name]

        cursor = self.conn.cursor()
        cursor.execute("INSERT OR IGNORE INTO asset_types (name) VALUES (?)", (name,))
        cursor.execute("SELECT id FROM asset_types WHERE name = ?", (name,))
        row = cursor.fetchone()
        if row:
            self.asset_type_cache[name] = row[0]
            return row[0]
        return 0

    def _flush_pending_product(self, path_key: str, data: dict):
        """Flush a single product to database."""
        cursor = self.conn.cursor()
        item_id = data.get("item_id")
        if not item_id:
            return
        cursor.execute("""
            INSERT INTO products (item_id, name, item_class, bottle_size, pack_count, label_variant, parent_item_id)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(item_id) DO UPDATE SET
                name = COALESCE(excluded.name, name),
                item_class = COALESCE(excluded.item_class, item_class),
                bottle_size = COALESCE(excluded.bottle_size, bottle_size),
                pack_count = COALESCE(excluded.pack_count, pack_count),
                label_variant = COALESCE(excluded.label_variant, label_variant),
                parent_item_id = COALESCE(excluded.parent_item_id, parent_item_id),
                updated_at = CURRENT_TIMESTAMP
        """, (
            item_id,
            data.get("name"),
            data.get("class"),
            data.get("bottlesize"),
            data.get("packcount"),
            data.get("labelvariant"),
            data.get("parentitemid")
        ))
        cursor.execute("SELECT id FROM products WHERE item_id = ?", (item_id,))
        row = cursor.fetchone()
        if row:
            self.product_cache[item_id] = row[0]
        self.conn.commit()

    def _flush_pending_products(self):
        """Flush all pending products to database."""
        for path_key, data in list(self.pending_products.items()):
            if "item_id" in data and "name" in data:
                self._flush_pending_product(path_key, data)
        self.pending_products.clear()

    def _flush_pending_lots(self):
        """Flush pending lots to database."""
        cursor = self.conn.cursor()
        for path_key, data in list(self.pending_lots.items()):
            if "lot_number_id" in data and "lot_number" in data:
                lot_num_id = data["lot_number_id"]

                # Try to link to product via item_id in lot data, pending_products, or cache
                product_id = None
                item_id = data.get("item_id")  # Stored when item/itemid arrived
                if not item_id and path_key in self.pending_products:
                    item_id = self.pending_products[path_key].get("item_id")
                if item_id:
                    product_id = self.product_cache.get(item_id)

                cursor.execute("""
                    INSERT INTO lots (lot_number_id, lot_number, product_id)
                    VALUES (?, ?, ?)
                    ON CONFLICT(lot_number_id) DO UPDATE SET
                        lot_number = COALESCE(excluded.lot_number, lot_number),
                        product_id = COALESCE(excluded.product_id, product_id),
                        updated_at = CURRENT_TIMESTAMP
                """, (lot_num_id, data.get("lot_number"), product_id))
                cursor.execute("SELECT id FROM lots WHERE lot_number_id = ?", (lot_num_id,))
                row = cursor.fetchone()
                if row:
                    self.lot_cache[lot_num_id] = row[0]
                del self.pending_lots[path_key]
        self.conn.commit()

    def _flush_pending_work_orders(self):
        """Flush pending work orders to database."""
        cursor = self.conn.cursor()
        for wo_id, data in list(self.pending_work_orders.items()):
            if "work_order_number" in data:
                cursor.execute("""
                    INSERT INTO work_orders (work_order_id, work_order_number, quantity_target, quantity_actual, quantity_defect, uom, asset_id, site, line)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(work_order_id) DO UPDATE SET
                        work_order_number = COALESCE(excluded.work_order_number, work_order_number),
                        quantity_actual = COALESCE(excluded.quantity_actual, quantity_actual),
                        quantity_defect = COALESCE(excluded.quantity_defect, quantity_defect),
                        updated_at = CURRENT_TIMESTAMP
                """, (
                    data.get("work_order_id"),
                    data.get("work_order_number"),
                    data.get("quantity_target"),
                    data.get("quantity_actual"),
                    data.get("quantity_defect"),
                    data.get("uom"),
                    data.get("asset_id"),
                    data.get("site"),
                    data.get("line")
                ))
                cursor.execute("SELECT id FROM work_orders WHERE work_order_id = ?", (wo_id,))
                row = cursor.fetchone()
                if row:
                    self.work_order_cache[wo_id] = row[0]
                del self.pending_work_orders[wo_id]
        self.conn.commit()

    def _flush_pending_assets(self):
        """Flush pending assets to database."""
        cursor = self.conn.cursor()
        for asset_id, data in list(self.pending_assets.items()):
            if "asset_name" in data or "asset_path" in data:
                # Get or create asset_type if present
                asset_type_id = None
                if "asset_type_name" in data:
                    asset_type_id = self._get_or_create_asset_type(data["asset_type_name"])

                cursor.execute("""
                    INSERT INTO assets (asset_id, asset_name, asset_path, display_name, asset_type_id, parent_asset_id, sort_order, site, area, line, equipment)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(asset_id) DO UPDATE SET
                        asset_name = COALESCE(excluded.asset_name, asset_name),
                        asset_path = COALESCE(excluded.asset_path, asset_path),
                        display_name = COALESCE(excluded.display_name, display_name),
                        updated_at = CURRENT_TIMESTAMP
                """, (
                    data.get("asset_id"),
                    data.get("asset_name"),
                    data.get("asset_path"),
                    data.get("display_name"),
                    asset_type_id,
                    data.get("parent_asset_id"),
                    data.get("sort_order"),
                    data.get("site"),
                    data.get("area"),
                    data.get("line"),
                    data.get("equipment")
                ))
                del self.pending_assets[asset_id]
        self.conn.commit()

    def _insert_event(self, info: TopicInfo, event_type: str, **kwargs):
        """Insert an event into the events table."""
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT INTO events (site, area, line, equipment, event_type, state_id, prev_state_id, work_order_id, lot_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            info.site, info.area, info.line, info.equipment,
            event_type,
            kwargs.get("state_id"),
            kwargs.get("prev_state_id"),
            kwargs.get("work_order_id"),
            kwargs.get("lot_id")
        ))
        self.conn.commit()

    def _flush_metrics(self):
        """Flush metric buckets to database."""
        if not self.metric_buckets:
            return

        cursor = self.conn.cursor()

        for (bucket_ts, site, line), metrics in self.metric_buckets.items():
            bucket_dt = datetime.fromtimestamp(bucket_ts).isoformat()

            # Average OEE values
            avg = lambda lst: sum(lst) / len(lst) if lst else None

            cursor.execute("""
                INSERT INTO metrics_10s (
                    bucket, site, area, line,
                    availability, performance, quality, oee,
                    count_infeed, count_outfeed, count_defect,
                    time_running, time_idle, time_down_planned, time_down_unplanned,
                    rate_actual, rate_standard,
                    temperature, flow_rate, weight,
                    equipment_count
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(bucket, site, line) DO UPDATE SET
                    availability = excluded.availability,
                    performance = excluded.performance,
                    quality = excluded.quality,
                    oee = excluded.oee,
                    count_infeed = excluded.count_infeed,
                    count_outfeed = excluded.count_outfeed,
                    count_defect = excluded.count_defect,
                    equipment_count = excluded.equipment_count
            """, (
                bucket_dt, site, None, line,
                avg(metrics.availability),
                avg(metrics.performance),
                avg(metrics.quality),
                avg(metrics.oee),
                metrics.count_infeed or None,
                metrics.count_outfeed or None,
                metrics.count_defect or None,
                metrics.time_running or None,
                metrics.time_idle or None,
                metrics.time_down_planned or None,
                metrics.time_down_unplanned or None,
                avg(metrics.rate_actual),
                avg(metrics.rate_standard),
                avg(metrics.temperature),
                avg(metrics.flow_rate),
                avg(metrics.weight),
                len(metrics.equipment_seen)
            ))

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
        """Flush all buffers and close connection."""
        with self.lock:
            self._flush_metrics()
            self._flush_pending_products()
            self._flush_pending_lots()
            self._flush_pending_work_orders()
            self._flush_pending_assets()
            if self.capture_raw:
                self._flush_raw_buffer()
        self.conn.close()

    def print_summary(self):
        """Print collection summary."""
        elapsed = (datetime.now() - self.start_time).total_seconds()
        print(f"\n\n{'='*60}")
        print("DATA COLLECTION SUMMARY")
        print("="*60)
        print(f"Messages received: {self.message_count}")
        print(f"Messages stored: {self.stored_count}")
        print(f"Duration: {elapsed:.1f}s")
        print(f"Rate: {self.stored_count / elapsed:.1f} msg/s")

        conn = get_connection(self.db_path)
        cursor = conn.cursor()

        tables = ["products", "lots", "work_orders", "states", "assets",
                  "events", "metrics_10s", "messages_raw"]
        print("\nTable counts:")
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            print(f"  {table}: {cursor.fetchone()[0]}")

        conn.close()


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Collect Enterprise B MQTT data")
    parser.add_argument("--raw", action="store_true", help="Capture raw messages")
    parser.add_argument("--reset", action="store_true", help="Reset database before starting")
    args = parser.parse_args()

    if args.reset:
        from schema import reset_db
        reset_db()
    else:
        init_db()

    collector = DataCollector(capture_raw=args.raw)
    client = MQTTClient()
    client.add_message_handler(collector.handle_message)

    stopped = False

    def signal_handler(sig, frame):
        nonlocal stopped
        if stopped:
            return
        stopped = True
        print("\n\nStopping collection...")
        client.stop()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    if not client.connect():
        print("Failed to connect to MQTT broker")
        sys.exit(1)

    print("Subscribing to Enterprise B/#...")
    print("Press Ctrl+C to stop\n")
    client.subscribe("Enterprise B/#")

    try:
        client.start()
    except KeyboardInterrupt:
        pass
    finally:
        if not stopped:
            client.stop()
        collector.close()
        collector.print_summary()


if __name__ == "__main__":
    main()
