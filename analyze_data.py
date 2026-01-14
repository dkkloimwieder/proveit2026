"""Comprehensive data analysis for Enterprise B manufacturing data.

This script provides repeatable analysis queries for:
- Work order status and processing
- Process stage mapping
- Product/lot linkage
- Target vs actual quantities
- Quantity overruns
- Early WO closures
- Cross-operation quantity flow
- Product data accuracy (bottle size, pack count)

Usage:
    python analyze_data.py                    # Full analysis
    python analyze_data.py --section wo       # Work orders only
    python analyze_data.py --section products # Products only
    python analyze_data.py --section flow     # Process flow only
    python analyze_data.py --output report.txt  # Save to file
"""

import argparse
import sqlite3
from datetime import datetime
from pathlib import Path

DB_PATH = "proveit.db"


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def print_header(title: str, output):
    output.write(f"\n{'=' * 80}\n")
    output.write(f"{title}\n")
    output.write(f"{'=' * 80}\n")


def print_table(headers: list, rows: list, output):
    """Print a formatted table."""
    if not rows:
        output.write("  (no data)\n")
        return

    # Calculate column widths
    widths = [len(h) for h in headers]
    for row in rows:
        for i, val in enumerate(row):
            widths[i] = max(widths[i], len(str(val) if val is not None else '-'))

    # Print header
    header_line = "  ".join(str(h).ljust(widths[i]) for i, h in enumerate(headers))
    output.write(f"\n{header_line}\n")
    output.write("-" * len(header_line) + "\n")

    # Print rows
    for row in rows:
        row_line = "  ".join(str(v if v is not None else '-').ljust(widths[i]) for i, v in enumerate(row))
        output.write(f"{row_line}\n")


# =============================================================================
# WORK ORDER ANALYSIS
# =============================================================================

def analyze_wo_status(output):
    """Issue proveit2026-kax: Analyze all work orders and their status."""
    print_header("WORK ORDER STATUS ANALYSIS (proveit2026-kax)", output)

    conn = get_connection()
    cursor = conn.execute("""
        SELECT
            work_order_number,
            site,
            line,
            uom,
            quantity_target,
            CAST(quantity_actual AS INTEGER) as qty_actual,
            CASE
                WHEN quantity_target IS NULL THEN 'NO_TARGET'
                WHEN quantity_actual >= quantity_target THEN 'COMPLETE'
                WHEN quantity_actual >= quantity_target * 0.95 THEN 'NEAR_COMPLETE'
                WHEN quantity_actual >= quantity_target * 0.5 THEN 'IN_PROGRESS'
                ELSE 'STARTING'
            END as status,
            CASE
                WHEN quantity_target > 0 THEN ROUND(100.0 * quantity_actual / quantity_target, 1)
                ELSE NULL
            END as pct_complete
        FROM work_orders
        ORDER BY work_order_number, site, line
    """)

    rows = [(r['work_order_number'], r['site'], r['line'], r['uom'],
             r['quantity_target'], r['qty_actual'], r['status'], r['pct_complete'])
            for r in cursor]

    print_table(['WO Number', 'Site', 'Line', 'UOM', 'Target', 'Actual', 'Status', '%Complete'],
                rows, output)

    # Summary counts
    cursor = conn.execute("""
        SELECT
            CASE
                WHEN quantity_target IS NULL THEN 'NO_TARGET'
                WHEN quantity_actual >= quantity_target THEN 'COMPLETE'
                WHEN quantity_actual >= quantity_target * 0.5 THEN 'IN_PROGRESS'
                ELSE 'STARTING'
            END as status,
            COUNT(*) as count
        FROM work_orders
        GROUP BY status
        ORDER BY status
    """)

    output.write("\n## Summary by Status\n")
    rows = [(r['status'], r['count']) for r in cursor]
    print_table(['Status', 'Count'], rows, output)

    conn.close()


def analyze_wo_stages(output):
    """Issue proveit2026-pss: Map work orders to process stages."""
    print_header("WORK ORDER PROCESS STAGES (proveit2026-pss)", output)

    conn = get_connection()
    cursor = conn.execute("""
        SELECT
            work_order_number,
            site,
            line,
            uom,
            CASE
                WHEN line LIKE 'mixroom%' THEN '1-MIXING'
                WHEN line LIKE 'filling%' THEN '2-FILLING'
                WHEN line LIKE 'labeler%' THEN '3-PACKAGING'
                WHEN line LIKE 'palletizer%' THEN '4-PALLETIZING'
                ELSE 'UNKNOWN'
            END as process_stage,
            CAST(quantity_actual AS INTEGER) as qty_actual
        FROM work_orders
        ORDER BY work_order_number, process_stage
    """)

    rows = [(r['work_order_number'], r['process_stage'], r['site'], r['line'],
             r['uom'], r['qty_actual']) for r in cursor]
    print_table(['WO Number', 'Stage', 'Site', 'Line', 'UOM', 'Actual'], rows, output)

    # Summary by stage
    cursor = conn.execute("""
        SELECT
            CASE
                WHEN line LIKE 'mixroom%' THEN '1-MIXING'
                WHEN line LIKE 'filling%' THEN '2-FILLING'
                WHEN line LIKE 'labeler%' THEN '3-PACKAGING'
                WHEN line LIKE 'palletizer%' THEN '4-PALLETIZING'
                ELSE 'UNKNOWN'
            END as stage,
            COUNT(*) as wo_count,
            SUM(CAST(quantity_actual AS INTEGER)) as total_qty
        FROM work_orders
        GROUP BY stage
        ORDER BY stage
    """)

    output.write("\n## Summary by Stage\n")
    rows = [(r['stage'], r['wo_count'], r['total_qty']) for r in cursor]
    print_table(['Stage', 'WO Count', 'Total Qty'], rows, output)

    conn.close()


def analyze_wo_products(output):
    """Issue proveit2026-l01: Link products and lots to work orders."""
    print_header("WORK ORDER PRODUCT/LOT LINKAGE (proveit2026-l01)", output)

    conn = get_connection()

    # Check current linkage in work_orders table
    cursor = conn.execute("""
        SELECT
            wo.work_order_number,
            wo.site,
            wo.line,
            p.name as product_name,
            l.lot_number
        FROM work_orders wo
        LEFT JOIN products p ON wo.product_id = p.id
        LEFT JOIN lots l ON wo.lot_id = l.id
        ORDER BY wo.work_order_number
    """)

    output.write("\n## Current Linkage in work_orders Table\n")
    rows = [(r['work_order_number'], r['site'], r['line'],
             r['product_name'], r['lot_number']) for r in cursor]
    print_table(['WO Number', 'Site', 'Line', 'Product', 'Lot'], rows, output)

    # Get linkage from raw MQTT data
    cursor = conn.execute("""
        WITH wo_products AS (
            SELECT DISTINCT
                substr(topic, 15, instr(substr(topic, 15), '/workorder') - 1) as location,
                payload_text as item_name
            FROM messages_raw
            WHERE topic LIKE '%/workorder/lotnumber/item/itemname'
        ),
        wo_numbers AS (
            SELECT DISTINCT
                substr(topic, 15, instr(substr(topic, 15), '/workorder') - 1) as location,
                payload_text as wo_number
            FROM messages_raw
            WHERE topic LIKE '%/workorder/workordernumber'
        )
        SELECT
            n.wo_number,
            n.location,
            p.item_name
        FROM wo_numbers n
        LEFT JOIN wo_products p ON n.location = p.location
        WHERE p.item_name IS NOT NULL
        ORDER BY n.wo_number, n.location
    """)

    output.write("\n## Actual WO-Product Associations (from raw MQTT)\n")
    rows = [(r['wo_number'], r['location'], r['item_name']) for r in cursor]
    print_table(['WO Number', 'Location', 'Product'], rows, output)

    output.write("\n## FINDING: Product/lot data EXISTS in raw MQTT but NOT linked in work_orders table\n")

    conn.close()


def analyze_target_vs_actual(output):
    """Issue proveit2026-32p: Analyze target vs actual quantities."""
    print_header("TARGET VS ACTUAL QUANTITIES (proveit2026-32p)", output)

    conn = get_connection()
    cursor = conn.execute("""
        SELECT
            work_order_number,
            site,
            line,
            uom,
            quantity_target,
            CAST(quantity_actual AS INTEGER) as qty_actual,
            CASE
                WHEN quantity_target > 0 THEN CAST(quantity_actual AS INTEGER) - quantity_target
                ELSE NULL
            END as variance,
            CASE
                WHEN quantity_target > 0 THEN ROUND(100.0 * quantity_actual / quantity_target, 1)
                ELSE NULL
            END as pct_complete
        FROM work_orders
        WHERE quantity_target IS NOT NULL
        ORDER BY pct_complete DESC
    """)

    rows = [(r['work_order_number'], r['site'], r['line'], r['uom'],
             r['quantity_target'], r['qty_actual'], r['variance'], r['pct_complete'])
            for r in cursor]
    print_table(['WO Number', 'Site', 'Line', 'UOM', 'Target', 'Actual', 'Variance', '%'], rows, output)

    conn.close()


def analyze_overruns(output):
    """Issue proveit2026-ruj: Investigate quantity overruns."""
    print_header("QUANTITY OVERRUNS (proveit2026-ruj)", output)

    conn = get_connection()
    cursor = conn.execute("""
        SELECT
            work_order_number,
            site,
            line,
            uom,
            quantity_target,
            CAST(quantity_actual AS INTEGER) as qty_actual,
            CAST(quantity_actual AS INTEGER) - quantity_target as overrun_amount,
            ROUND(100.0 * quantity_actual / quantity_target, 1) as pct_of_target
        FROM work_orders
        WHERE quantity_target IS NOT NULL
          AND quantity_actual > quantity_target
        ORDER BY pct_of_target DESC
    """)

    rows = [(r['work_order_number'], r['site'], r['line'], r['uom'],
             r['quantity_target'], r['qty_actual'], r['overrun_amount'], r['pct_of_target'])
            for r in cursor]

    output.write("\n## Work Orders Exceeding Target\n")
    print_table(['WO Number', 'Site', 'Line', 'UOM', 'Target', 'Actual', 'Overrun', '%Target'], rows, output)

    output.write(f"\n## FINDING: {len(rows)} work orders exceed their target quantity\n")
    output.write("## Targets appear to be MINIMUMS, not hard limits\n")

    conn.close()


def analyze_early_closures(output):
    """Issue proveit2026-4jz: Investigate early WO closures."""
    print_header("EARLY WORK ORDER CLOSURES (proveit2026-4jz)", output)

    conn = get_connection()

    # Find WO transitions
    cursor = conn.execute("""
        WITH wo_changes AS (
            SELECT
                datetime(m1.received_at) as change_time,
                m1.topic,
                substr(m1.topic, 15, instr(substr(m1.topic, 15), '/workorder') - 1) as location,
                m1.payload_text as new_wo_id,
                LAG(m1.payload_text) OVER (PARTITION BY m1.topic ORDER BY m1.received_at) as prev_wo_id
            FROM messages_raw m1
            WHERE m1.topic LIKE '%/workorder/workorderid'
            ORDER BY m1.topic, m1.received_at
        )
        SELECT
            change_time,
            location,
            prev_wo_id as closed_wo_id,
            new_wo_id
        FROM wo_changes
        WHERE prev_wo_id IS NOT NULL
          AND prev_wo_id != new_wo_id
        ORDER BY change_time
    """)

    output.write("\n## Detected WO Transitions (potential closures)\n")
    rows = [(r['change_time'], r['location'], r['closed_wo_id'], r['new_wo_id']) for r in cursor]
    print_table(['Time', 'Location', 'Closed WO ID', 'New WO ID'], rows, output)

    output.write(f"\n## FINDING: {len(rows)} work order transitions detected\n")
    output.write("## Early closures DO occur - WOs can be replaced before reaching target\n")

    conn.close()


# =============================================================================
# PROCESS FLOW ANALYSIS
# =============================================================================

def analyze_quantity_flow(output):
    """Issue proveit2026-e1v: Analyze quantity flow across operations."""
    print_header("QUANTITY FLOW ACROSS OPERATIONS (proveit2026-e1v)", output)

    conn = get_connection()

    # Show same WO at different stages
    cursor = conn.execute("""
        WITH wo_stages AS (
            SELECT
                CASE
                    WHEN work_order_number LIKE '%-P%'
                    THEN substr(work_order_number, 1, instr(work_order_number, '-P') - 1)
                    ELSE work_order_number
                END as base_wo,
                work_order_number,
                site,
                line,
                uom,
                CASE
                    WHEN line LIKE 'mixroom%' THEN '1-MIX'
                    WHEN line LIKE 'filling%' THEN '2-FILL'
                    WHEN line LIKE 'labeler%' THEN '3-PACK'
                    ELSE '4-OTHER'
                END as stage,
                CAST(quantity_actual AS INTEGER) as qty_actual
            FROM work_orders
        )
        SELECT
            base_wo,
            stage,
            work_order_number,
            site,
            line,
            uom,
            qty_actual
        FROM wo_stages
        WHERE base_wo IN (
            SELECT base_wo FROM wo_stages GROUP BY base_wo HAVING COUNT(DISTINCT stage) > 1
        )
        ORDER BY base_wo, stage, site
    """)

    output.write("\n## Work Orders Appearing at Multiple Stages\n")
    rows = [(r['base_wo'], r['stage'], r['work_order_number'], r['site'],
             r['line'], r['uom'], r['qty_actual']) for r in cursor]
    print_table(['Base WO', 'Stage', 'Full WO', 'Site', 'Line', 'UOM', 'Qty'], rows, output)

    # Summary by stage and UOM
    cursor = conn.execute("""
        SELECT
            CASE
                WHEN line LIKE 'mixroom%' THEN '1-MIX'
                WHEN line LIKE 'filling%' THEN '2-FILL'
                WHEN line LIKE 'labeler%' THEN '3-PACK'
            END as stage,
            uom,
            COUNT(*) as wo_count,
            SUM(CAST(quantity_actual AS INTEGER)) as total_qty
        FROM work_orders
        WHERE uom IS NOT NULL
        GROUP BY stage, uom
        ORDER BY stage, uom
    """)

    output.write("\n## Quantity Summary by Stage and UOM\n")
    rows = [(r['stage'], r['uom'], r['wo_count'], r['total_qty']) for r in cursor]
    print_table(['Stage', 'UOM', 'WO Count', 'Total Qty'], rows, output)

    output.write("""
## FINDINGS:
- Quantities DO NOT match across operations (UOMs differ)
- UOM changes: kg (mix) -> bottle (fill) -> CS/cases (pack)
- Conversion factors vary by product (bottle size, pack count)
""")

    conn.close()


# =============================================================================
# PRODUCT DATA ANALYSIS
# =============================================================================

def analyze_product_data(output):
    """Issue proveit2026-wg6: Verify bottle size and case count accuracy."""
    print_header("PRODUCT DATA ACCURACY (proveit2026-wg6)", output)

    conn = get_connection()

    # Current products table
    cursor = conn.execute("""
        SELECT id, item_id, name, item_class, bottle_size, pack_count
        FROM products
        ORDER BY name
    """)

    output.write("\n## Current Products Table Data\n")
    rows = [(r['id'], r['item_id'], r['name'], r['item_class'],
             r['bottle_size'], r['pack_count']) for r in cursor]
    print_table(['ID', 'ItemID', 'Name', 'Class', 'BottleSize', 'PackCount'], rows, output)

    # Raw MQTT bottle sizes
    cursor = conn.execute("""
        SELECT DISTINCT topic, payload_text
        FROM messages_raw
        WHERE topic LIKE '%bottlesize%'
        ORDER BY topic
    """)

    output.write("\n## Raw MQTT Bottle Size Data\n")
    rows = [(r['topic'][-60:], r['payload_text']) for r in cursor]
    print_table(['Topic (last 60 chars)', 'Value'], rows, output)

    # Raw MQTT pack counts
    cursor = conn.execute("""
        SELECT DISTINCT topic, payload_text
        FROM messages_raw
        WHERE topic LIKE '%packcount%'
          AND payload_text != '0'
        ORDER BY topic
    """)

    output.write("\n## Raw MQTT Pack Count Data (non-zero)\n")
    rows = [(r['topic'][-60:], r['payload_text']) for r in cursor]
    print_table(['Topic (last 60 chars)', 'Value'], rows, output)

    # Correct product data from MQTT
    cursor = conn.execute("""
        SELECT DISTINCT
            m1.payload_text as item_name,
            m2.payload_text as bottle_size,
            m3.payload_text as pack_count
        FROM messages_raw m1
        LEFT JOIN messages_raw m2 ON
            replace(m1.topic, 'itemname', 'bottlesize') = m2.topic
        LEFT JOIN messages_raw m3 ON
            replace(m1.topic, 'itemname', 'packcount') = m3.topic
        WHERE m1.topic LIKE '%/item/itemname'
        ORDER BY m1.payload_text
    """)

    output.write("\n## Correct Product Data (from raw MQTT)\n")
    rows = [(r['item_name'], r['bottle_size'], r['pack_count']) for r in cursor]
    print_table(['Product Name', 'Bottle Size', 'Pack Count'], rows, output)

    output.write("""
## FINDINGS:
- Products table has INCORRECT data (bottle_size=0, pack_count=0)
- Raw MQTT contains CORRECT values:
  - Bottle products: bottle_size = 0.5 (liters)
  - Pack products: pack_count = 12, 16, 20, or 24
- Collector is NOT capturing these fields correctly
- BUG: See issue proveit2026-1eo
""")

    conn.close()


# =============================================================================
# DATABASE SUMMARY
# =============================================================================

def show_summary(output):
    """Show database summary."""
    print_header("DATABASE SUMMARY", output)

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

    rows = []
    for table, desc in tables:
        try:
            cursor = conn.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            rows.append((table, count, desc))
        except sqlite3.OperationalError:
            rows.append((table, 'N/A', desc))

    print_table(['Table', 'Count', 'Description'], rows, output)

    # Time range
    cursor = conn.execute("""
        SELECT
            datetime(MIN(received_at)) as first_msg,
            datetime(MAX(received_at)) as last_msg,
            ROUND((julianday(MAX(received_at)) - julianday(MIN(received_at))) * 24, 2) as hours
        FROM messages_raw
    """)
    row = cursor.fetchone()
    if row['first_msg']:
        output.write(f"\nData collection period:\n")
        output.write(f"  From: {row['first_msg']}\n")
        output.write(f"  To:   {row['last_msg']}\n")
        output.write(f"  Duration: {row['hours']} hours\n")

    conn.close()


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="Comprehensive data analysis")
    parser.add_argument("--section", choices=['wo', 'flow', 'products', 'all'],
                        default='all', help="Section to analyze")
    parser.add_argument("--output", type=str, help="Output file (default: stdout)")
    args = parser.parse_args()

    # Set up output
    if args.output:
        output = open(args.output, 'w')
    else:
        import sys
        output = sys.stdout

    try:
        output.write(f"# Enterprise B Data Analysis Report\n")
        output.write(f"# Generated: {datetime.now().isoformat()}\n")
        output.write(f"# Database: {DB_PATH}\n")

        show_summary(output)

        if args.section in ['wo', 'all']:
            analyze_wo_status(output)
            analyze_wo_stages(output)
            analyze_wo_products(output)
            analyze_target_vs_actual(output)
            analyze_overruns(output)
            analyze_early_closures(output)

        if args.section in ['flow', 'all']:
            analyze_quantity_flow(output)

        if args.section in ['products', 'all']:
            analyze_product_data(output)

        output.write("\n" + "=" * 80 + "\n")
        output.write("END OF REPORT\n")
        output.write("=" * 80 + "\n")

    finally:
        if args.output:
            output.close()
            print(f"Report saved to: {args.output}")


if __name__ == "__main__":
    main()
