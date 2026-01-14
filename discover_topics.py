"""Discover MQTT topics and message structures from the broker."""

import json
import signal
import sys
from collections import defaultdict
from datetime import datetime

from mqtt_client import MQTTClient


class TopicDiscovery:
    """Discovers and catalogs MQTT topics and their message structures."""

    def __init__(self):
        self.topics: dict[str, list[dict]] = defaultdict(list)
        self.topic_counts: dict[str, int] = defaultdict(int)
        self.start_time = datetime.now()
        self.message_count = 0

    def handle_message(self, topic: str, payload: bytes):
        """Process incoming MQTT messages."""
        self.message_count += 1
        self.topic_counts[topic] += 1

        # Try to decode payload
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

        # Store sample (keep up to 3 samples per topic)
        if len(self.topics[topic]) < 3:
            self.topics[topic].append({
                "type": msg_type,
                "sample": data if msg_type != "json" else data,
                "size": len(payload),
                "timestamp": datetime.now().isoformat(),
            })

        # Print progress
        if self.message_count % 10 == 0:
            elapsed = (datetime.now() - self.start_time).total_seconds()
            print(f"\rMessages: {self.message_count} | Topics: {len(self.topics)} | "
                  f"Time: {elapsed:.1f}s", end="", flush=True)

    def print_summary(self):
        """Print a summary of discovered topics."""
        print("\n\n" + "=" * 60)
        print("TOPIC DISCOVERY SUMMARY")
        print("=" * 60)
        print(f"Total messages: {self.message_count}")
        print(f"Unique topics: {len(self.topics)}")
        print(f"Duration: {(datetime.now() - self.start_time).total_seconds():.1f}s")

        # Build topic tree
        print("\n--- TOPIC TREE ---")
        roots: dict[str, set] = defaultdict(set)
        for topic in sorted(self.topics.keys()):
            parts = topic.split("/")
            if len(parts) > 0:
                roots[parts[0]].add(topic)

        for root in sorted(roots.keys()):
            print(f"\n{root}/")
            topics_under_root = sorted(roots[root])
            for topic in topics_under_root[:20]:  # Show max 20 per root
                count = self.topic_counts[topic]
                sample = self.topics[topic][0] if self.topics[topic] else {}
                msg_type = sample.get("type", "unknown")
                print(f"  {topic} [{msg_type}] (x{count})")
            if len(topics_under_root) > 20:
                print(f"  ... and {len(topics_under_root) - 20} more")

        # Show sample messages for interesting topics
        print("\n--- SAMPLE MESSAGES ---")
        for topic in list(self.topics.keys())[:10]:
            sample = self.topics[topic][0] if self.topics[topic] else {}
            print(f"\nTopic: {topic}")
            print(f"  Type: {sample.get('type', 'unknown')}")
            print(f"  Size: {sample.get('size', 0)} bytes")
            data = sample.get("sample", "")
            if isinstance(data, dict):
                print(f"  Fields: {list(data.keys())}")
                # Pretty print JSON (truncated)
                json_str = json.dumps(data, indent=2)
                if len(json_str) > 500:
                    json_str = json_str[:500] + "..."
                print(f"  Data: {json_str}")
            else:
                data_str = str(data)
                if len(data_str) > 200:
                    data_str = data_str[:200] + "..."
                print(f"  Data: {data_str}")


def main():
    discovery = TopicDiscovery()
    client = MQTTClient()
    client.add_message_handler(discovery.handle_message)

    def signal_handler(sig, frame):
        print("\n\nStopping discovery...")
        client.stop()
        discovery.print_summary()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)

    if not client.connect():
        print("Failed to connect to MQTT broker")
        sys.exit(1)

    # Subscribe to all topics to discover the structure
    print("Subscribing to all topics (#)...")
    print("Press Ctrl+C to stop and see summary\n")
    client.subscribe("#")

    client.start()


if __name__ == "__main__":
    main()
