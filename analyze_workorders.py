"""Work order analysis for Enterprise B manufacturing data.

Analyzes:
- Work order lifecycle (additions, completions, changes over time)
- Cross-site work order flow
- Product/process hierarchy
- Quantity progression patterns
- Work order number patterns

Usage:
    python analyze_workorders.py              # Full analysis
    python analyze_workorders.py --lifecycle  # Just lifecycle analysis
    python analyze_workorders.py --crosssite  # Just cross-site analysis
"""

import argparse
import sqlite3
from collections import defaultdict
from datetime import datetime
from pathlib import Path


DB_PATH = "proveit.db"


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def print_header(title: str):
    print(f"\n{'=' * 70}")
    print(title)
    print("=" * 70)


def analyze_wo_lifecycle():
    """Analyze work order changes over time from raw messages."""
    print_header("WORK ORDER LIFECYCLE ANALYSIS")

    conn = get_connection()

    # Track work order ID changes over time by location
    print("\n## Work Order Changes Over Time (by location)")
    print("-" * 70)

    cursor = conn.execute("""
        SELECT
            datetime(received_at) as time,
            topic,
            payload_text as wo_id
        FROM messages_raw
        WHERE topic LIKE '%workorder/workorderid'
        ORDER BY topic, received_at
    """)

    # Group by location, track changes
    location_wos = defaultdict(list)
    for row in cursor:
        # Extract location from topic
        parts = row["topic"].split("/")
        if len(parts) >= 5:
            location = "/".join(parts[1:5])  # Site/area/line/equipment or Site/area/line
            location_wos[location].append({
                "time": row["time"],
                "wo_id": row["wo_id"]
            })

    # Find transitions
    print(f"\n{'Location':<45} {'Time':<20} {'Change'}")
    print("-" * 90)

    transitions = []
    for location, records in sorted(location_wos.items()):
        prev_wo = None
        for rec in records:
            if prev_wo and rec["wo_id"] != prev_wo:
                transitions.append({
                    "location": location,
                    "time": rec["time"],
                    "from_wo": prev_wo,
                    "to_wo": rec["wo_id"]
                })
                print(f"{location:<45} {rec['time']:<20} {prev_wo} -> {rec['wo_id']}")
            prev_wo = rec["wo_id"]

    print(f"\nTotal work order transitions detected: {len(transitions)}")

    # Analyze quantity progression for a sample WO
    print("\n## Quantity Progression Pattern (sample)")
    print("-" * 70)
    print("Shows how quantityactual changes - CUMULATIVE totals, not deltas")

    cursor = conn.execute("""
        SELECT
            datetime(received_at) as time,
            topic,
            CAST(payload_text AS INTEGER) as qty
        FROM messages_raw
        WHERE topic LIKE '%workorder/quantityactual'
        ORDER BY topic, received_at
        LIMIT 50
    """)

    rows = list(cursor)
    if rows:
        print(f"\nSample from: {rows[0]['topic']}")
        print(f"{'Time':<25} {'Quantity':<15} {'Delta'}")
        print("-" * 50)

        prev_qty = None
        for row in rows[:20]:
            delta = ""
            if prev_qty is not None:
                d = row["qty"] - prev_qty
                if d < 0:
                    delta = f"<-- WO CHANGE (reset from {prev_qty})"
                else:
                    delta = f"+{d}"
            print(f"{row['time']:<25} {row['qty']:<15} {delta}")
            prev_qty = row["qty"]

    conn.close()
    return transitions


def analyze_wo_completion():
    """Analyze how work order completion is detected."""
    print_header("WORK ORDER COMPLETION DETECTION")

    conn = get_connection()

    print("""
## How Completion is Detected

Work order completion is detected by monitoring workorderid/workordernumber changes:
1. When workorderid changes to a NEW value = previous WO completed
2. The final quantityactual before change = completion count
3. New WO starts with low quantity (often single digits)

## Evidence from Data
""")

    # Show workorderid + quantity pairs over time for a location
    cursor = conn.execute("""
        WITH wo_data AS (
            SELECT
                datetime(received_at) as time,
                CASE
                    WHEN topic LIKE '%workorderid' THEN 'id'
                    WHEN topic LIKE '%workordernumber' THEN 'number'
                    WHEN topic LIKE '%quantityactual' THEN 'qty'
                END as field,
                payload_text as value,
                -- Extract location
                substr(topic, 1, instr(topic, '/workorder') - 1) as location
            FROM messages_raw
            WHERE (topic LIKE '%workorder/workorderid'
                   OR topic LIKE '%workorder/workordernumber'
                   OR topic LIKE '%workorder/quantityactual')
              AND topic LIKE '%labelerline04%'
        )
        SELECT time, field, value, location
        FROM wo_data
        ORDER BY time
        LIMIT 100
    """)

    print(f"{'Time':<22} {'Field':<8} {'Value'}")
    print("-" * 60)

    prev_id = None
    for row in cursor:
        marker = ""
        if row["field"] == "id" and prev_id and row["value"] != prev_id:
            marker = " <-- WO CHANGED!"
        if row["field"] == "id":
            prev_id = row["value"]
        print(f"{row['time']:<22} {row['field']:<8} {row['value']}{marker}")

    conn.close()


def analyze_crosssite():
    """Analyze cross-site work order connections."""
    print_header("CROSS-SITE WORK ORDER ANALYSIS")

    conn = get_connection()

    print("\n## Same Work Order at Multiple Sites/Lines")
    print("-" * 70)

    cursor = conn.execute("""
        SELECT
            work_order_number,
            GROUP_CONCAT(DISTINCT site || '/' || line, ' | ') as locations,
            GROUP_CONCAT(DISTINCT uom, ', ') as uoms,
            COUNT(DISTINCT site) as site_count,
            COUNT(DISTINCT line) as line_count
        FROM work_orders
        GROUP BY work_order_number
        HAVING site_count > 1 OR line_count > 1
        ORDER BY site_count DESC, line_count DESC
    """)

    print(f"\n{'Work Order':<20} {'Sites':<6} {'Lines':<6} {'UOMs':<15} {'Locations'}")
    print("-" * 100)

    for row in cursor:
        print(f"{row['work_order_number']:<20} {row['site_count']:<6} {row['line_count']:<6} "
              f"{row['uoms'] or '-':<15} {row['locations']}")

    print("""
## Interpretation

Cross-site work orders indicate multi-stage manufacturing:
- Same WO number tracked through: mixing -> filling -> packaging
- Different UOMs at each stage: kg -> bottle -> CS (cases)
- Product flows BETWEEN sites during manufacturing
""")

    conn.close()


def analyze_process_flow():
    """Analyze the manufacturing process flow."""
    print_header("MANUFACTURING PROCESS FLOW")

    conn = get_connection()

    print("""
## Process Stages (from topic structure)

    liquidprocessing  ->  fillerproduction  ->  packaging  ->  palletizing
       (Mix, kg)           (Bottle)            (Pack, CS)

    Equipment: mixroom,    fillingline         labelerline    palletizer
               vat, tank
""")

    print("\n## Work Orders by Process Stage")
    print("-" * 70)

    cursor = conn.execute("""
        SELECT
            CASE uom
                WHEN 'kg' THEN '1. liquidprocessing'
                WHEN 'bottle' THEN '2. fillerproduction'
                WHEN 'CS' THEN '3. packaging'
                ELSE '4. other/unknown'
            END as stage,
            uom,
            COUNT(DISTINCT work_order_number) as wo_count,
            SUM(quantity_actual) as total_qty
        FROM work_orders
        WHERE uom IS NOT NULL
        GROUP BY uom
        ORDER BY stage
    """)

    print(f"\n{'Stage':<25} {'UOM':<10} {'WO Count':<10} {'Total Qty'}")
    print("-" * 60)

    for row in cursor:
        print(f"{row['stage']:<25} {row['uom']:<10} {row['wo_count']:<10} {row['total_qty'] or 0:.0f}")

    conn.close()


def analyze_wo_patterns():
    """Analyze work order number patterns."""
    print_header("WORK ORDER NUMBER PATTERNS")

    conn = get_connection()

    print("""
## Pattern: WO-Lxx-xxxx-Pxx

| Segment | Meaning              | Examples           |
|---------|----------------------|--------------------|
| WO-     | Prefix               | Always "WO-"       |
| Lxx     | Line code            | L01, L02, L03, L04 |
| xxxx    | Sequence number      | 0086, 0880, 0948   |
| -Pxx    | Pack variant (opt)   | P12, P16, P20, P24 |

Pack variants (P12/P16/P20/P24) indicate pack sizes (12-pack, 16-pack, etc.)
""")

    print("\n## Work Orders Decoded")
    print("-" * 70)

    cursor = conn.execute("""
        SELECT
            work_order_number,
            substr(work_order_number, 4, 3) as line_code,
            substr(work_order_number, 8, 4) as sequence,
            CASE
                WHEN work_order_number LIKE '%-P%'
                THEN substr(work_order_number, instr(work_order_number, '-P') + 1)
                ELSE NULL
            END as pack_variant,
            site,
            line,
            uom
        FROM work_orders
        ORDER BY line_code, sequence, pack_variant NULLS FIRST
    """)

    print(f"\n{'WO Number':<20} {'Line':<6} {'Seq':<6} {'Pack':<6} {'Site':<8} {'Line':<15} {'UOM'}")
    print("-" * 85)

    for row in cursor:
        print(f"{row['work_order_number']:<20} {row['line_code']:<6} {row['sequence']:<6} "
              f"{row['pack_variant'] or '-':<6} {row['site'] or '-':<8} {row['line'] or '-':<15} "
              f"{row['uom'] or '-'}")

    conn.close()


def analyze_products():
    """Analyze product hierarchy."""
    print_header("PRODUCT HIERARCHY")

    conn = get_connection()

    cursor = conn.execute("""
        SELECT
            id, item_id, name, item_class,
            bottle_size, pack_count, parent_item_id
        FROM products
        ORDER BY item_class, name
    """)

    print(f"\n{'ID':<4} {'Item ID':<8} {'Name':<20} {'Class':<8} {'Pack Count'}")
    print("-" * 60)

    for row in cursor:
        print(f"{row['id']:<4} {row['item_id'] or '-':<8} {row['name'] or '-':<20} "
              f"{row['item_class'] or '-':<8} {row['pack_count'] or '-'}")

    print("""
## Product Classes by Process Stage

| Class  | Process Stage      | Description              |
|--------|--------------------|--------------------------|
| Mix    | liquidprocessing   | Raw liquid mixture       |
| Bottle | fillerproduction   | Filled individual bottle |
| Pack   | packaging          | Packaged case (12/16/24) |
""")

    conn.close()


def show_summary():
    """Show current database summary."""
    print_header("DATABASE SUMMARY")

    conn = get_connection()

    tables = [
        ("work_orders", "Work Orders"),
        ("products", "Products"),
        ("lots", "Lots"),
        ("assets", "Assets"),
        ("states", "States"),
        ("events", "Events"),
        ("metrics_10s", "Metrics (10s buckets)"),
        ("messages_raw", "Raw Messages"),
    ]

    print(f"\n{'Table':<25} {'Count':<15} {'Description'}")
    print("-" * 60)

    for table, desc in tables:
        try:
            cursor = conn.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"{table:<25} {count:<15} {desc}")
        except sqlite3.OperationalError:
            print(f"{table:<25} {'N/A':<15} {desc} (table missing)")

    # Time range
    cursor = conn.execute("""
        SELECT
            datetime(MIN(received_at)) as first_msg,
            datetime(MAX(received_at)) as last_msg
        FROM messages_raw
    """)
    row = cursor.fetchone()
    if row["first_msg"]:
        print(f"\nData time range: {row['first_msg']} to {row['last_msg']}")

    conn.close()


def main():
    parser = argparse.ArgumentParser(description="Work order analysis")
    parser.add_argument("--lifecycle", action="store_true", help="Analyze WO lifecycle only")
    parser.add_argument("--completion", action="store_true", help="Analyze completion detection")
    parser.add_argument("--crosssite", action="store_true", help="Analyze cross-site flow only")
    parser.add_argument("--process", action="store_true", help="Analyze process flow only")
    parser.add_argument("--patterns", action="store_true", help="Analyze WO number patterns")
    parser.add_argument("--products", action="store_true", help="Analyze product hierarchy")
    parser.add_argument("--summary", action="store_true", help="Show database summary")
    args = parser.parse_args()

    # If no specific flag, run all
    run_all = not any([args.lifecycle, args.completion, args.crosssite,
                       args.process, args.patterns, args.products, args.summary])

    if run_all or args.summary:
        show_summary()

    if run_all or args.lifecycle:
        analyze_wo_lifecycle()

    if run_all or args.completion:
        analyze_wo_completion()

    if run_all or args.crosssite:
        analyze_crosssite()

    if run_all or args.process:
        analyze_process_flow()

    if run_all or args.patterns:
        analyze_wo_patterns()

    if run_all or args.products:
        analyze_products()

    print("\n" + "=" * 70)
    print("Analysis complete. Run with --help for specific analysis options.")
    print("=" * 70)


if __name__ == "__main__":
    main()
