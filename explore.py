"""Exploration queries for Enterprise B data.

Schema v2 tables:
- Reference: products, lots, work_orders, states, asset_types, assets
- Events: events (state changes, WO/lot transitions)
- Metrics: metrics_10s (10-second bucketed OEE data)
- Raw: messages_raw, topics

Usage:
    python explore.py              # Full overview
    python explore.py --assets     # Show assets only
    python explore.py --metrics    # Show OEE metrics only
    python explore.py --events     # Show recent events
    python explore.py --states     # Show state definitions
"""

import argparse
import sqlite3
from datetime import datetime

DB_PATH = "proveit.db"


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def print_header(title: str):
    print(f"\n{'=' * 70}")
    print(title)
    print("=" * 70)


def show_overview():
    """Show database overview statistics."""
    print_header("DATABASE OVERVIEW")

    conn = get_connection()

    tables = [
        ("products", "Products (items)"),
        ("lots", "Lots (batches)"),
        ("work_orders", "Work Orders"),
        ("states", "State Definitions"),
        ("assets", "Assets (equipment)"),
        ("events", "Events (state changes)"),
        ("metrics_10s", "Metrics (10s buckets)"),
        ("messages_raw", "Raw Messages"),
        ("topics", "Topics Registry"),
    ]

    print(f"\n{'Table':<20} {'Count':<12} {'Description'}")
    print("-" * 60)

    for table, desc in tables:
        try:
            cursor = conn.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"{table:<20} {count:<12} {desc}")
        except sqlite3.OperationalError:
            print(f"{table:<20} {'N/A':<12} {desc}")

    # Time range
    cursor = conn.execute("""
        SELECT
            datetime(MIN(received_at)) as first_msg,
            datetime(MAX(received_at)) as last_msg,
            ROUND((julianday(MAX(received_at)) - julianday(MIN(received_at))) * 24, 1) as hours
        FROM messages_raw
    """)
    row = cursor.fetchone()
    if row["first_msg"]:
        print(f"\nData collection period:")
        print(f"  From: {row['first_msg']}")
        print(f"  To:   {row['last_msg']}")
        print(f"  Duration: {row['hours']} hours")

    conn.close()


def show_products():
    """Show products/items."""
    print_header("PRODUCTS")

    conn = get_connection()
    cursor = conn.execute("""
        SELECT id, item_id, name, item_class, bottle_size, pack_count
        FROM products
        ORDER BY item_class, name
    """)

    print(f"\n{'ID':<4} {'ItemID':<8} {'Name':<25} {'Class':<10} {'Size':<8} {'Pack'}")
    print("-" * 70)

    for row in cursor:
        print(f"{row['id']:<4} {row['item_id'] or '-':<8} {row['name'] or '-':<25} "
              f"{row['item_class'] or '-':<10} {row['bottle_size'] or '-':<8} "
              f"{row['pack_count'] or '-'}")

    conn.close()


def show_work_orders():
    """Show work orders."""
    print_header("WORK ORDERS")

    conn = get_connection()
    cursor = conn.execute("""
        SELECT
            wo.work_order_number,
            wo.site,
            wo.line,
            wo.uom,
            wo.quantity_target,
            wo.quantity_actual,
            wo.quantity_defect,
            p.name as product_name
        FROM work_orders wo
        LEFT JOIN products p ON wo.product_id = p.id
        ORDER BY wo.work_order_number, wo.site
    """)

    print(f"\n{'WO Number':<20} {'Site':<8} {'Line':<15} {'UOM':<8} {'Target':<10} {'Actual':<10}")
    print("-" * 80)

    for row in cursor:
        print(f"{row['work_order_number']:<20} {row['site'] or '-':<8} {row['line'] or '-':<15} "
              f"{row['uom'] or '-':<8} {row['quantity_target'] or '-':<10} "
              f"{row['quantity_actual'] or 0:.0f}")

    conn.close()


def show_lots():
    """Show lots."""
    print_header("LOTS")

    conn = get_connection()
    cursor = conn.execute("""
        SELECT
            l.id, l.lot_number_id, l.lot_number,
            p.name as product_name,
            l.first_seen
        FROM lots l
        LEFT JOIN products p ON l.product_id = p.id
        ORDER BY l.first_seen DESC
        LIMIT 30
    """)

    print(f"\n{'ID':<4} {'LotNumID':<10} {'Lot Number':<20} {'Product':<20} {'First Seen'}")
    print("-" * 80)

    for row in cursor:
        print(f"{row['id']:<4} {row['lot_number_id'] or '-':<10} {row['lot_number'] or '-':<20} "
              f"{row['product_name'] or '-':<20} {row['first_seen']}")

    conn.close()


def show_states():
    """Show state definitions."""
    print_header("STATE DEFINITIONS")

    conn = get_connection()
    cursor = conn.execute("""
        SELECT id, code, name, type
        FROM states
        ORDER BY name
    """)

    print(f"\n{'ID':<4} {'Code':<8} {'Name':<25} {'Type'}")
    print("-" * 50)

    for row in cursor:
        print(f"{row['id']:<4} {row['code'] or '-':<8} {row['name']:<25} {row['type'] or '-'}")

    conn.close()


def show_assets():
    """Show discovered assets."""
    print_header("ASSETS")

    conn = get_connection()
    cursor = conn.execute("""
        SELECT
            a.asset_id, a.asset_name, a.site, a.area, a.line, a.equipment,
            at.name as asset_type
        FROM assets a
        LEFT JOIN asset_types at ON a.asset_type_id = at.id
        WHERE a.asset_id IS NOT NULL
        ORDER BY a.site, a.area, a.line, a.equipment
    """)

    print(f"\n{'AssetID':<10} {'Site':<8} {'Area':<18} {'Line':<15} {'Equipment':<15} {'Type'}")
    print("-" * 90)

    for row in cursor:
        print(f"{row['asset_id']:<10} {row['site'] or '-':<8} {row['area'] or '-':<18} "
              f"{row['line'] or '-':<15} {row['equipment'] or '-':<15} {row['asset_type'] or '-'}")

    conn.close()


def show_events(limit: int = 30):
    """Show recent events."""
    print_header(f"RECENT EVENTS (last {limit})")

    conn = get_connection()
    cursor = conn.execute("""
        SELECT
            datetime(e.timestamp) as time,
            e.site,
            e.line,
            e.equipment,
            e.event_type,
            s.name as state_name,
            e.state_duration
        FROM events e
        LEFT JOIN states s ON e.state_id = s.id
        ORDER BY e.timestamp DESC
        LIMIT ?
    """, (limit,))

    print(f"\n{'Time':<22} {'Site':<8} {'Line':<15} {'Type':<12} {'State':<20} {'Duration'}")
    print("-" * 95)

    for row in cursor:
        duration = f"{row['state_duration']:.1f}s" if row['state_duration'] else "-"
        print(f"{row['time']:<22} {row['site'] or '-':<8} {row['line'] or '-':<15} "
              f"{row['event_type']:<12} {row['state_name'] or '-':<20} {duration}")

    conn.close()


def show_metrics(limit: int = 20):
    """Show recent OEE metrics."""
    print_header(f"RECENT OEE METRICS (last {limit})")

    conn = get_connection()
    cursor = conn.execute("""
        SELECT
            datetime(bucket) as time,
            site,
            line,
            availability,
            performance,
            quality,
            oee,
            count_infeed,
            count_outfeed
        FROM metrics_10s
        WHERE oee IS NOT NULL
        ORDER BY bucket DESC
        LIMIT ?
    """, (limit,))

    print(f"\n{'Time':<22} {'Site':<8} {'Line':<15} {'Avail':<8} {'Perf':<8} {'Qual':<8} {'OEE':<8}")
    print("-" * 90)

    for row in cursor:
        avail = f"{row['availability']:.1f}" if row['availability'] else "-"
        perf = f"{row['performance']:.1f}" if row['performance'] else "-"
        qual = f"{row['quality']:.1f}" if row['quality'] else "-"
        oee = f"{row['oee']:.1f}" if row['oee'] else "-"
        print(f"{row['time']:<22} {row['site']:<8} {row['line']:<15} "
              f"{avail:<8} {perf:<8} {qual:<8} {oee:<8}")

    conn.close()


def show_topic_tree():
    """Show topic hierarchy summary."""
    print_header("TOPIC HIERARCHY")

    conn = get_connection()
    cursor = conn.execute("""
        SELECT
            site,
            category,
            COUNT(DISTINCT topic) as topic_count,
            SUM(message_count) as msg_count
        FROM topics
        WHERE site IS NOT NULL
        GROUP BY site, category
        ORDER BY site, msg_count DESC
    """)

    print(f"\n{'Site':<10} {'Category':<20} {'Topics':<10} {'Messages'}")
    print("-" * 55)

    current_site = None
    for row in cursor:
        if row["site"] != current_site:
            if current_site:
                print()
            current_site = row["site"]
        print(f"{row['site']:<10} {row['category'] or '-':<20} "
              f"{row['topic_count']:<10} {row['msg_count'] or 0}")

    conn.close()


def query_raw(sql: str):
    """Run a custom SQL query."""
    conn = get_connection()
    cursor = conn.execute(sql)
    rows = cursor.fetchall()
    conn.close()
    return rows


def main():
    parser = argparse.ArgumentParser(description="Explore Enterprise B data")
    parser.add_argument("--products", action="store_true", help="Show products")
    parser.add_argument("--workorders", "-wo", action="store_true", help="Show work orders")
    parser.add_argument("--lots", action="store_true", help="Show lots")
    parser.add_argument("--states", action="store_true", help="Show state definitions")
    parser.add_argument("--assets", action="store_true", help="Show assets")
    parser.add_argument("--events", action="store_true", help="Show recent events")
    parser.add_argument("--metrics", action="store_true", help="Show OEE metrics")
    parser.add_argument("--topics", action="store_true", help="Show topic hierarchy")
    parser.add_argument("--limit", type=int, default=30, help="Limit rows (default: 30)")
    args = parser.parse_args()

    # If no specific flag, show overview
    run_all = not any([args.products, args.workorders, args.lots, args.states,
                       args.assets, args.events, args.metrics, args.topics])

    if run_all:
        show_overview()
        show_products()
        show_work_orders()
        show_states()
        print("\nRun with --help for specific views (--events, --metrics, --assets, etc.)")
    else:
        if args.products:
            show_products()
        if args.workorders:
            show_work_orders()
        if args.lots:
            show_lots()
        if args.states:
            show_states()
        if args.assets:
            show_assets()
        if args.events:
            show_events(args.limit)
        if args.metrics:
            show_metrics(args.limit)
        if args.topics:
            show_topic_tree()


if __name__ == "__main__":
    main()
