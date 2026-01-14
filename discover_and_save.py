"""Discover MQTT topics and save results to JSON."""

import json
import signal
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from mqtt_client import MQTTClient


class TopicDiscovery:
    """Discovers and catalogs MQTT topics and their message structures."""

    def __init__(self, output_file: str = "discovery_results.json"):
        self.output_file = Path(output_file)
        self.topics: dict[str, list[dict]] = defaultdict(list)
        self.topic_counts: dict[str, int] = defaultdict(int)
        self.start_time = datetime.now()
        self.message_count = 0
        self.max_samples_per_topic = 3

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

        # Store sample
        if len(self.topics[topic]) < self.max_samples_per_topic:
            self.topics[topic].append({
                "type": msg_type,
                "sample": data,
                "size": len(payload),
                "timestamp": datetime.now().isoformat(),
            })

        # Progress every 100 messages
        if self.message_count % 100 == 0:
            print(f"\rMessages: {self.message_count} | Topics: {len(self.topics)}", end="", flush=True)

    def save_results(self):
        """Save discovery results to JSON file."""
        # Build topic tree structure
        topic_tree: dict = {}
        for topic in self.topics:
            parts = topic.split("/")
            current = topic_tree
            for part in parts:
                if part not in current:
                    current[part] = {}
                current = current[part]

        results = {
            "discovery_time": self.start_time.isoformat(),
            "duration_seconds": (datetime.now() - self.start_time).total_seconds(),
            "total_messages": self.message_count,
            "unique_topics": len(self.topics),
            "topic_tree": topic_tree,
            "topics": {
                topic: {
                    "count": self.topic_counts[topic],
                    "samples": samples,
                }
                for topic, samples in self.topics.items()
            },
        }

        self.output_file.write_text(json.dumps(results, indent=2, default=str))
        print(f"\n\nResults saved to {self.output_file}")

    def print_summary(self):
        """Print a summary of discovered topics."""
        print("\n\n" + "=" * 60)
        print("TOPIC DISCOVERY SUMMARY")
        print("=" * 60)
        print(f"Total messages: {self.message_count}")
        print(f"Unique topics: {len(self.topics)}")
        print(f"Duration: {(datetime.now() - self.start_time).total_seconds():.1f}s")

        # Get root topics
        roots: dict[str, int] = defaultdict(int)
        for topic in self.topics:
            root = topic.split("/")[0]
            roots[root] += self.topic_counts[topic]

        print("\n--- ROOT TOPICS (by message count) ---")
        for root, count in sorted(roots.items(), key=lambda x: -x[1])[:20]:
            topic_count = sum(1 for t in self.topics if t.startswith(f"{root}/") or t == root)
            print(f"  {root}/ - {count} messages, {topic_count} topics")


def main():
    discovery = TopicDiscovery()
    client = MQTTClient()
    client.add_message_handler(discovery.handle_message)

    def signal_handler(sig, frame):
        print("\n\nStopping discovery...")
        client.stop()
        discovery.save_results()
        discovery.print_summary()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    if not client.connect():
        print("Failed to connect to MQTT broker")
        sys.exit(1)

    print("Subscribing to all topics (#)...")
    print("Press Ctrl+C to stop and save results\n")
    client.subscribe("#")

    try:
        client.start()
    except KeyboardInterrupt:
        pass
    finally:
        client.stop()
        discovery.save_results()
        discovery.print_summary()


if __name__ == "__main__":
    main()
