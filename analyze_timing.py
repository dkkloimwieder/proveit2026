"""Analyze MQTT message timing patterns to inform schema design."""

import signal
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime

from mqtt_client import MQTTClient

IGNORED_PREFIXES = ("maintainx/", "abelara/", "roeslein/")


@dataclass
class MetricStats:
    """Track timing stats for a metric."""
    first_seen: datetime = None
    last_seen: datetime = None
    count: int = 0
    intervals: list[float] = field(default_factory=list)

    def record(self, now: datetime):
        if self.first_seen is None:
            self.first_seen = now
        if self.last_seen is not None:
            interval = (now - self.last_seen).total_seconds()
            if interval > 0:
                self.intervals.append(interval)
        self.last_seen = now
        self.count += 1

    @property
    def avg_interval(self) -> float:
        return sum(self.intervals) / len(self.intervals) if self.intervals else 0

    @property
    def min_interval(self) -> float:
        return min(self.intervals) if self.intervals else 0

    @property
    def max_interval(self) -> float:
        return max(self.intervals) if self.intervals else 0


class TimingAnalyzer:
    """Analyze update frequency patterns."""

    def __init__(self):
        self.by_topic: dict[str, MetricStats] = defaultdict(MetricStats)
        self.by_equipment: dict[str, MetricStats] = defaultdict(MetricStats)
        self.by_metric_type: dict[str, MetricStats] = defaultdict(MetricStats)
        self.by_line: dict[str, MetricStats] = defaultdict(MetricStats)
        self.by_site: dict[str, MetricStats] = defaultdict(MetricStats)
        self.start_time = datetime.now()
        self.message_count = 0

    def handle_message(self, topic: str, payload: bytes):
        if not topic.startswith("Enterprise B/"):
            return
        remainder = topic[len("Enterprise B/"):]
        if any(remainder.startswith(p) for p in IGNORED_PREFIXES):
            return

        self.message_count += 1
        now = datetime.now()
        parts = topic.split("/")

        # Track by full topic
        self.by_topic[topic].record(now)

        # Track by site
        if len(parts) >= 2 and parts[1].startswith("Site"):
            self.by_site[parts[1]].record(now)

        # Track by line (site/area/line)
        if len(parts) >= 4:
            line_key = "/".join(parts[1:4])
            self.by_line[line_key].record(now)

        # Track by equipment (site/area/line/equipment)
        if len(parts) >= 5:
            equip_key = "/".join(parts[1:5])
            self.by_equipment[equip_key].record(now)

        # Track by metric type (last segment)
        if len(parts) >= 6:
            metric_type = parts[-1]
            self.by_metric_type[metric_type].record(now)

        if self.message_count % 500 == 0:
            elapsed = (datetime.now() - self.start_time).total_seconds()
            print(f"\rMessages: {self.message_count} | Time: {elapsed:.1f}s", end="", flush=True)

    def print_analysis(self):
        elapsed = (datetime.now() - self.start_time).total_seconds()
        print(f"\n\n{'='*70}")
        print(f"TIMING ANALYSIS ({elapsed:.1f}s collection)")
        print("="*70)

        # By site
        print("\n--- UPDATE RATE BY SITE ---")
        for site, stats in sorted(self.by_site.items()):
            rate = stats.count / elapsed if elapsed > 0 else 0
            print(f"  {site}: {stats.count} msgs, {rate:.1f} msg/s")

        # By metric type - find fast vs slow
        print("\n--- UPDATE RATE BY METRIC TYPE (top 20) ---")
        print(f"  {'Metric':<25} {'Count':>8} {'Avg(s)':>10} {'Min(s)':>10} {'Max(s)':>10}")
        print("  " + "-"*65)
        sorted_metrics = sorted(self.by_metric_type.items(), key=lambda x: -x[1].count)
        for metric, stats in sorted_metrics[:20]:
            print(f"  {metric:<25} {stats.count:>8} {stats.avg_interval:>10.2f} {stats.min_interval:>10.2f} {stats.max_interval:>10.2f}")

        # By line - see if lines update together
        print("\n--- UPDATE RATE BY LINE (top 15) ---")
        print(f"  {'Line':<45} {'Count':>8} {'Rate':>10}")
        print("  " + "-"*65)
        sorted_lines = sorted(self.by_line.items(), key=lambda x: -x[1].count)
        for line, stats in sorted_lines[:15]:
            rate = stats.count / elapsed if elapsed > 0 else 0
            print(f"  {line:<45} {stats.count:>8} {rate:>10.1f}/s")

        # Categorize fast vs slow metrics
        print("\n--- FAST vs SLOW METRICS ---")
        fast = [(m, s) for m, s in self.by_metric_type.items() if s.avg_interval > 0 and s.avg_interval < 5]
        slow = [(m, s) for m, s in self.by_metric_type.items() if s.avg_interval >= 5]

        print(f"  FAST (<5s avg interval): {len(fast)} metrics")
        for m, s in sorted(fast, key=lambda x: x[1].avg_interval)[:10]:
            print(f"    {m}: avg {s.avg_interval:.2f}s")

        print(f"\n  SLOW (>=5s avg interval): {len(slow)} metrics")
        for m, s in sorted(slow, key=lambda x: -x[1].avg_interval)[:10]:
            print(f"    {m}: avg {s.avg_interval:.2f}s")

        # Equipment update correlation - do metrics arrive together?
        print("\n--- EQUIPMENT UPDATE PATTERNS (sample) ---")
        sample_equip = list(self.by_equipment.keys())[:5]
        for equip in sample_equip:
            stats = self.by_equipment[equip]
            print(f"  {equip}")
            print(f"    Updates: {stats.count}, Avg interval: {stats.avg_interval:.2f}s")


def main():
    analyzer = TimingAnalyzer()
    client = MQTTClient()
    client.add_message_handler(analyzer.handle_message)

    def signal_handler(sig, frame):
        print("\n\nStopping...")
        client.stop()
        analyzer.print_analysis()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    if not client.connect():
        print("Failed to connect")
        sys.exit(1)

    print("Collecting timing data from Enterprise B/#...")
    print("Press Ctrl+C after 30-60s to see analysis\n")
    client.subscribe("Enterprise B/#")

    try:
        client.start()
    except KeyboardInterrupt:
        pass
    finally:
        client.stop()
        analyzer.print_analysis()


if __name__ == "__main__":
    main()
