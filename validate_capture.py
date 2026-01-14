"""Validate that data_collector captures all MQTT data types."""

import json
import signal
import sys
from collections import defaultdict
from datetime import datetime

from mqtt_client import MQTTClient

IGNORED_PREFIXES = ("maintainx/", "abelara/", "roeslein/")

CATEGORY_NAMES = ("metric", "node", "workorder", "lotnumber", "processdata", "state")

# Data types that data_collector.py handles
HANDLED_PATTERNS = [
    "item/",           # Products
    "lotnumber/",      # Lots
    "workorder/",      # Work orders
    "state/",          # States
    "assetidentifier/", # Assets
    "process/",        # Process data (temp, flow, weight)
    # Metrics handled by category or explicit match
]

HANDLED_METRICS = {
    "availability", "performance", "quality", "oee",
    "input/countdefect", "input/countinfeed", "input/countoutfeed",
    "input/rateactual", "input/ratestandard",
    "input/timedownplanned", "input/timedownunplanned",
    "input/timeidle", "input/timerunning"
}


class CaptureValidator:
    def __init__(self):
        self.seen_data_types: dict[str, list] = defaultdict(list)
        self.handled_count = 0
        self.unhandled_count = 0
        self.unhandled_types: dict[str, list] = defaultdict(list)
        self.start_time = datetime.now()
        self.message_count = 0

    def is_handled(self, topic: str, data_type: str) -> bool:
        """Check if this data type is handled by data_collector.

        Uses same variable-depth parsing as data_collector.
        """
        if not data_type:
            return False

        parts = topic.split("/")

        # Parse with smart depth detection
        category = None
        dt = None

        if len(parts) >= 4:
            if parts[3] in CATEGORY_NAMES:
                # Area-level: Site/area/category/data_type
                category = parts[3]
                if len(parts) >= 5:
                    dt = "/".join(parts[4:])
            elif len(parts) >= 5 and parts[4] in CATEGORY_NAMES:
                # Line-level: Site/area/line/category/data_type
                category = parts[4]
                if len(parts) >= 6:
                    dt = "/".join(parts[5:])
            else:
                # Equipment-level: Site/area/line/equipment/category/data_type
                if len(parts) >= 6:
                    category = parts[5]
                if len(parts) >= 7:
                    dt = "/".join(parts[6:])

        # Check explicit patterns
        for pattern in HANDLED_PATTERNS:
            if dt and dt.startswith(pattern):
                return True

        # Check metrics
        if dt in HANDLED_METRICS:
            return True

        # Check if it's under a metric category
        if category == "metric":
            return True

        # Work order fields at workorder category level
        if category == "workorder" and dt in (
            "workorderid", "workordernumber", "quantityactual", "quantitydefect",
            "quantitytarget", "uom", "assetid", "lotnumber/lotnumber", "lotnumber/lotnumberid"
        ):
            return True

        # Lot number at lotnumber category
        if category == "lotnumber" and dt in ("lotnumber", "lotnumberid"):
            return True

        # Process data counts/rates
        if category == "processdata" or (dt and dt.startswith("processdata/")):
            return True

        # Asset identifiers under node category
        if category == "node" and dt and dt.startswith("assetidentifier/"):
            return True

        return False

    def handle_message(self, topic: str, payload: bytes):
        if not topic.startswith("Enterprise B/"):
            return

        remainder = topic[len("Enterprise B/"):]
        if any(remainder.startswith(p) for p in IGNORED_PREFIXES):
            return

        self.message_count += 1
        parts = topic.split("/")

        # Extract data_type (everything after equipment/category)
        data_type = None
        if len(parts) >= 7:
            data_type = "/".join(parts[6:])
        elif len(parts) >= 6:
            data_type = parts[5]

        # Decode payload for sample
        try:
            decoded = payload.decode("utf-8")
            try:
                value = json.loads(decoded)
            except json.JSONDecodeError:
                value = decoded
        except UnicodeDecodeError:
            value = f"<binary {len(payload)} bytes>"

        # Track
        if data_type:
            if len(self.seen_data_types[data_type]) < 2:
                self.seen_data_types[data_type].append({
                    "topic": topic,
                    "sample": str(value)[:100]
                })

            if self.is_handled(topic, data_type):
                self.handled_count += 1
            else:
                self.unhandled_count += 1
                if len(self.unhandled_types[data_type]) < 2:
                    self.unhandled_types[data_type].append({
                        "topic": topic,
                        "sample": str(value)[:100]
                    })

        if self.message_count % 500 == 0:
            print(f"\rMessages: {self.message_count} | Types: {len(self.seen_data_types)} | "
                  f"Unhandled: {len(self.unhandled_types)}", end="", flush=True)

    def print_report(self):
        elapsed = (datetime.now() - self.start_time).total_seconds()
        print(f"\n\n{'='*70}")
        print("CAPTURE VALIDATION REPORT")
        print("="*70)
        print(f"Duration: {elapsed:.1f}s")
        print(f"Total messages: {self.message_count}")
        print(f"Unique data types seen: {len(self.seen_data_types)}")
        print(f"Handled messages: {self.handled_count}")
        print(f"Unhandled messages: {self.unhandled_count}")

        if self.unhandled_types:
            print(f"\n{'='*70}")
            print(f"UNHANDLED DATA TYPES ({len(self.unhandled_types)})")
            print("="*70)
            for dt, examples in sorted(self.unhandled_types.items()):
                print(f"\n  {dt}")
                for ex in examples[:1]:
                    print(f"    Topic: {ex['topic']}")
                    print(f"    Sample: {ex['sample']}")
        else:
            print("\n*** ALL DATA TYPES ARE HANDLED ***")

        # Show all seen types for reference
        print(f"\n{'='*70}")
        print(f"ALL DATA TYPES SEEN ({len(self.seen_data_types)})")
        print("="*70)
        for dt in sorted(self.seen_data_types.keys()):
            handled = "✓" if self.is_handled(self.seen_data_types[dt][0]["topic"], dt) else "✗"
            print(f"  {handled} {dt}")


def main():
    validator = CaptureValidator()
    client = MQTTClient()
    client.add_message_handler(validator.handle_message)

    def signal_handler(sig, frame):
        print("\n\nStopping...")
        client.stop()
        validator.print_report()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    if not client.connect():
        print("Failed to connect")
        sys.exit(1)

    print("Validating capture coverage for Enterprise B/#...")
    print("Press Ctrl+C after 30-60s to see report\n")
    client.subscribe("Enterprise B/#")

    try:
        client.start()
    except KeyboardInterrupt:
        pass
    finally:
        client.stop()
        validator.print_report()


if __name__ == "__main__":
    main()
