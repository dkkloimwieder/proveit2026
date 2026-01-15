"""Comprehensive data analysis for Enterprise B manufacturing data.

This script provides repeatable analysis queries for:
- Work order status and processing (proveit2026-kax)
- Process stage mapping (proveit2026-pss)
- Product/lot linkage (proveit2026-l01)
- Target vs actual quantities (proveit2026-32p)
- Quantity overruns (proveit2026-ruj)
- Early WO closures (proveit2026-4jz)
- Cross-operation quantity flow (proveit2026-e1v)
- Stage-to-stage target conversion (kg→bottle→case)
- Metrics collection per process (proveit2026-2r1)
- Product data accuracy (proveit2026-wg6)

Usage:
    python analyze_data.py                      # Full analysis
    python analyze_data.py --section clean      # Clean analysis (replay-aware)
    python analyze_data.py --section wo         # Work orders only
    python analyze_data.py --section flow       # Process flow only
    python analyze_data.py --section targets    # Stage-to-stage targets
    python analyze_data.py --section metrics    # Metrics collection
    python analyze_data.py --section products   # Products only
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

def analyze_stage_targets(output):
    """Analyze target quantities across stages and conversion factors."""
    print_header("STAGE-TO-STAGE TARGET ANALYSIS", output)

    conn = get_connection()

    output.write("""
## KG to Bottle Conversion (MIX → FILL)

Theoretical conversion based on bottle size:
- Bottle size: 0.5L
- Liquid density: ~1 kg/L (water equivalent)
- Formula: 1 kg = 1L = 2 bottles (0.5L each)
""")

    # Show mix targets with theoretical bottle conversion
    cursor = conn.execute("""
        SELECT DISTINCT
            (SELECT payload_text FROM messages_raw m2
             WHERE replace(m.topic, 'quantitytarget', 'workordernumber') = m2.topic LIMIT 1) as wo,
            CAST(payload_text AS INTEGER) as mix_kg,
            CAST(payload_text AS INTEGER) * 2 as theoretical_bottles
        FROM messages_raw m
        WHERE topic LIKE '%mixroom%/workorder/quantitytarget'
          AND CAST(payload_text AS INTEGER) > 0
        ORDER BY wo
    """)

    output.write("\n## Mix Targets with Theoretical Bottle Output\n")
    rows = [(r['wo'], r['mix_kg'], r['theoretical_bottles']) for r in cursor]
    print_table(['Mix WO', 'Target (kg)', 'Theoretical Bottles (×2)'], rows, output)

    # Show fill to pack conversion
    output.write("""
## Bottle to Case Conversion (FILL → PACK)

Formula: Cases Target × Pack Size = Bottles Target
""")

    cursor = conn.execute("""
        WITH fill_targets AS (
            SELECT DISTINCT
                CASE
                    WHEN (SELECT payload_text FROM messages_raw m2
                          WHERE replace(m.topic, 'quantitytarget', 'workordernumber') = m2.topic LIMIT 1) LIKE '%-P%'
                    THEN substr((SELECT payload_text FROM messages_raw m2
                          WHERE replace(m.topic, 'quantitytarget', 'workordernumber') = m2.topic LIMIT 1), 1,
                          instr((SELECT payload_text FROM messages_raw m2
                          WHERE replace(m.topic, 'quantitytarget', 'workordernumber') = m2.topic LIMIT 1), '-P') - 1)
                    ELSE (SELECT payload_text FROM messages_raw m2
                          WHERE replace(m.topic, 'quantitytarget', 'workordernumber') = m2.topic LIMIT 1)
                END as base_wo,
                CAST(payload_text AS INTEGER) as fill_target
            FROM messages_raw m
            WHERE topic LIKE '%filling%/workorder/quantitytarget'
              AND CAST(payload_text AS INTEGER) > 0
        ),
        pack_targets AS (
            SELECT DISTINCT
                CASE
                    WHEN (SELECT payload_text FROM messages_raw m2
                          WHERE replace(m.topic, 'quantitytarget', 'workordernumber') = m2.topic LIMIT 1) LIKE '%-P%'
                    THEN substr((SELECT payload_text FROM messages_raw m2
                          WHERE replace(m.topic, 'quantitytarget', 'workordernumber') = m2.topic LIMIT 1), 1,
                          instr((SELECT payload_text FROM messages_raw m2
                          WHERE replace(m.topic, 'quantitytarget', 'workordernumber') = m2.topic LIMIT 1), '-P') - 1)
                    ELSE (SELECT payload_text FROM messages_raw m2
                          WHERE replace(m.topic, 'quantitytarget', 'workordernumber') = m2.topic LIMIT 1)
                END as base_wo,
                (SELECT payload_text FROM messages_raw m2
                 WHERE replace(m.topic, 'quantitytarget', 'workordernumber') = m2.topic LIMIT 1) as full_wo,
                CAST(payload_text AS INTEGER) as pack_target,
                CASE
                    WHEN (SELECT payload_text FROM messages_raw m2
                          WHERE replace(m.topic, 'quantitytarget', 'workordernumber') = m2.topic LIMIT 1) LIKE '%-P12' THEN 12
                    WHEN (SELECT payload_text FROM messages_raw m2
                          WHERE replace(m.topic, 'quantitytarget', 'workordernumber') = m2.topic LIMIT 1) LIKE '%-P16' THEN 16
                    WHEN (SELECT payload_text FROM messages_raw m2
                          WHERE replace(m.topic, 'quantitytarget', 'workordernumber') = m2.topic LIMIT 1) LIKE '%-P20' THEN 20
                    WHEN (SELECT payload_text FROM messages_raw m2
                          WHERE replace(m.topic, 'quantitytarget', 'workordernumber') = m2.topic LIMIT 1) LIKE '%-P24' THEN 24
                    ELSE NULL
                END as pack_size
            FROM messages_raw m
            WHERE topic LIKE '%labeler%/workorder/quantitytarget'
              AND CAST(payload_text AS INTEGER) > 0
        )
        SELECT
            f.base_wo,
            f.fill_target as bottles,
            p.full_wo as pack_wo,
            p.pack_size,
            p.pack_target as cases,
            p.pack_target * p.pack_size as implied_bottles,
            ROUND(100.0 * (p.pack_target * p.pack_size) / f.fill_target, 1) as match_pct
        FROM fill_targets f
        JOIN pack_targets p ON f.base_wo = p.base_wo
        WHERE p.pack_size IS NOT NULL
        ORDER BY f.base_wo
    """)

    output.write("\n## Fill → Pack Target Conversion\n")
    rows = [(r['base_wo'], r['bottles'], r['pack_wo'], r['pack_size'],
             r['cases'], r['implied_bottles'], r['match_pct']) for r in cursor]
    print_table(['Base WO', 'Fill Bottles', 'Pack WO', 'Pack Size', 'Cases', 'Implied Bottles', 'Match%'],
                rows, output)

    output.write("""
## WO Naming Convention

Pattern: WO-Lxx-xxxx-Pxx

CRITICAL: Line codes ARE stage-specific:
- L01, L02 = MIX stage ONLY (liquidprocessing/mixroom)
- L03, L04 = FILL and PACK stages (fillerproduction, packaging)
- -Pxx suffix = Pack variant, ONLY at PACK stage

Stage Linkage:
- MIX → FILL: DISCONNECTED (different WO number series)
- FILL → PACK: CONNECTED (same base WO + -Pxx suffix)

Why disconnected? Mixing is a BATCH PROCESS producing bulk liquid in tanks.
Multiple FILL orders draw from the same mix batch - no 1:1 WO tracking.
""")

    # Show WOs by stage
    cursor = conn.execute("""
        SELECT
            CASE
                WHEN topic LIKE '%mixroom%' OR topic LIKE '%vat%' THEN '1-MIX'
                WHEN topic LIKE '%filling%' THEN '2-FILL'
                WHEN topic LIKE '%labeler%' THEN '3-PACK'
            END as stage,
            payload_text as wo_number
        FROM messages_raw
        WHERE topic LIKE '%/workorder/workordernumber'
        GROUP BY stage, wo_number
        ORDER BY stage, wo_number
    """)

    output.write("\n## WOs by Stage (confirms naming pattern)\n")
    rows = [(r['stage'], r['wo_number']) for r in cursor]
    print_table(['Stage', 'WO Number'], rows, output)

    conn.close()


def analyze_metrics_collection(output):
    """Issue proveit2026-2r1: Analyze what metrics/quantities are collected per process."""
    print_header("METRICS COLLECTION PER PROCESS (proveit2026-2r1)", output)

    conn = get_connection()

    output.write("""
## Question: Are total metrics/quantities per process collected in work_orders table?

## Answer: NO - work_orders stores LATEST SNAPSHOTS, not aggregated totals

The data collection uses TWO separate storage mechanisms:
1. work_orders table: Latest snapshot per site/line (cumulative running totals from MQTT)
2. metrics_10s table: 10-second bucketed aggregates (count_infeed, count_outfeed, etc.)
""")

    # Show work_orders structure
    output.write("\n## Work Order Quantities (SNAPSHOTS)\n")
    cursor = conn.execute("""
        SELECT
            work_order_number,
            site,
            line,
            uom,
            CAST(quantity_actual AS INTEGER) as qty_actual,
            datetime(updated_at) as last_updated
        FROM work_orders
        ORDER BY line, site
        LIMIT 15
    """)
    rows = [(r['work_order_number'], r['site'], r['line'], r['uom'],
             r['qty_actual'], r['last_updated']) for r in cursor]
    print_table(['WO Number', 'Site', 'Line', 'UOM', 'Qty Actual', 'Last Updated'], rows, output)

    output.write("""
NOTE: quantity_actual is the LAST VALUE seen from MQTT for this WO at this site/line.
It is NOT an aggregate of all equipment in that line.
""")

    # Show metrics_10s structure
    output.write("\n## Metrics Buckets (AGGREGATED COUNTS)\n")
    cursor = conn.execute("""
        SELECT
            bucket,
            site,
            line,
            count_infeed,
            count_outfeed,
            count_defect,
            equipment_count,
            oee
        FROM metrics_10s
        WHERE count_infeed IS NOT NULL
        ORDER BY bucket DESC
        LIMIT 10
    """)
    rows = [(r['bucket'], r['site'], r['line'], r['count_infeed'],
             r['count_outfeed'], r['count_defect'], r['equipment_count'],
             round(r['oee'], 3) if r['oee'] else None) for r in cursor]
    print_table(['Bucket', 'Site', 'Line', 'InFeed', 'OutFeed', 'Defect', 'Equip#', 'OEE'], rows, output)

    output.write("""
NOTE: metrics_10s.count_infeed/outfeed/defect are SUMMED across equipment in each line.
equipment_count shows how many pieces of equipment contributed to each bucket.
""")

    # Show MQTT topic structure
    output.write("\n## MQTT Topic Structure\n")
    cursor = conn.execute("""
        SELECT DISTINCT
            CASE
                WHEN topic LIKE '%/workorder/quantityactual' THEN 'WO Quantity'
                WHEN topic LIKE '%/metric/input/countinfeed' THEN 'Equipment InFeed'
                WHEN topic LIKE '%/metric/output/countoutfeed' THEN 'Equipment OutFeed'
            END as data_type,
            topic
        FROM messages_raw
        WHERE topic LIKE '%/workorder/quantityactual'
           OR topic LIKE '%/metric/input/countinfeed'
           OR topic LIKE '%/metric/output/countoutfeed'
        ORDER BY data_type, topic
        LIMIT 20
    """)
    rows = [(r['data_type'], r['topic'][-65:]) for r in cursor]
    print_table(['Data Type', 'Topic (last 65 chars)'], rows, output)

    # Total metrics summary
    output.write("\n## Metrics Summary (from metrics_10s)\n")
    cursor = conn.execute("""
        SELECT
            line,
            COUNT(*) as buckets,
            SUM(count_infeed) as total_infeed,
            SUM(count_outfeed) as total_outfeed,
            SUM(count_defect) as total_defect,
            ROUND(AVG(oee), 3) as avg_oee
        FROM metrics_10s
        WHERE count_infeed IS NOT NULL
        GROUP BY line
        ORDER BY line
    """)
    rows = [(r['line'], r['buckets'], r['total_infeed'], r['total_outfeed'],
             r['total_defect'], r['avg_oee']) for r in cursor]
    print_table(['Line', 'Buckets', 'Total InFeed', 'Total OutFeed', 'Total Defect', 'Avg OEE'], rows, output)

    output.write("""
## FINDINGS:
1. work_orders.quantity_actual = SNAPSHOT (last MQTT value per WO/site/line)
2. metrics_10s = AGGREGATED counts every 10 seconds (summed across equipment)
3. WO quantities come from LINE-level topics (not equipment level)
4. Equipment-level counts go to metrics_10s separately
5. To get total production per process: SUM(metrics_10s.count_outfeed) by line

## Data Flow:
  MQTT: Enterprise B/Site1/packaging/labelerline04/workorder/quantityactual
        -> work_orders.quantity_actual (latest snapshot)

  MQTT: Enterprise B/Site1/packaging/labelerline04/labeler/metric/output/countoutfeed
        -> metrics_10s.count_outfeed (bucketed sum)
""")

    conn.close()


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
# CLEAN ANALYSIS (handles simulator replay duplicates)
# =============================================================================

def analyze_replay_status(output):
    """Show current simulator replay status."""
    print_header("SIMULATOR REPLAY STATUS", output)

    conn = get_connection()

    # Get replay status
    cursor = conn.execute("SELECT * FROM v_replay_status")
    row = cursor.fetchone()

    if row:
        output.write(f"\nCurrent replay position:\n")
        output.write(f"  Progress: {float(row['progress_pct']) * 100:.2f}%\n")
        output.write(f"  Original data timestamp: {row['data_timestamp']}\n")
        output.write(f"  Replay timestamp: {row['generated_at']}\n")
        output.write(f"  Last update: {row['received_at']}\n")

        # Estimate time until reset
        progress = float(row['progress_pct'])
        remaining = 1.0 - progress
        hours_until_reset = remaining / 0.0066  # ~0.66% per hour
        output.write(f"\n  Estimated hours until 100% (reset): {hours_until_reset:.1f}\n")
    else:
        output.write("  (no metadata available)\n")

    # Show duplicate WO impact
    cursor = conn.execute("""
        SELECT duplicate_type, COUNT(*) as count
        FROM v_duplicate_work_orders
        GROUP BY duplicate_type
    """)
    rows = [(r['duplicate_type'], r['count']) for r in cursor]

    output.write("\n## Duplicate Work Order Impact\n")
    print_table(['Type', 'Count'], rows, output)

    cursor = conn.execute("""
        SELECT
            (SELECT COUNT(*) FROM work_orders) as total_rows,
            (SELECT COUNT(DISTINCT work_order_number) FROM work_orders) as unique_numbers,
            (SELECT COUNT(*) FROM v_duplicate_work_orders WHERE duplicate_type = 'REPLAY_DUPLICATE') as replay_dupes
    """)
    row = cursor.fetchone()
    output.write(f"\n  Total WO rows: {row['total_rows']}\n")
    output.write(f"  Unique WO numbers: {row['unique_numbers']}\n")
    output.write(f"  Replay duplicates: {row['replay_dupes']} ({100*row['replay_dupes']/row['unique_numbers']:.1f}% of unique numbers)\n")

    conn.close()


def analyze_clean_production(output):
    """Production analysis using clean views (handles duplicates)."""
    print_header("CLEAN PRODUCTION ANALYSIS (from completions)", output)

    conn = get_connection()

    output.write("\n## Production by Stage\n")
    output.write("(Each work order completion counted once - handles replay duplicates)\n")

    cursor = conn.execute("SELECT * FROM v_production_by_stage ORDER BY stage")
    rows = [(r['stage'], r['uom'], r['line_count'], r['wo_completions'],
             int(r['total_output']), int(r['avg_per_wo']),
             f"{r['avg_completion_pct']:.1f}%")
            for r in cursor]
    print_table(['Stage', 'UOM', 'Lines', 'WO Completions', 'Total Output', 'Avg/WO', 'Avg %Complete'],
                rows, output)

    output.write("\n## Production by Line\n")
    cursor = conn.execute("SELECT * FROM v_production_by_line ORDER BY stage, site, line")
    rows = [(r['site'], r['line'], r['stage'], r['wo_completions'],
             int(r['total_output']), int(r['avg_per_wo']),
             f"{r['avg_completion_pct']:.1f}%")
            for r in cursor]
    print_table(['Site', 'Line', 'Stage', 'WO Completions', 'Total Output', 'Avg/WO', 'Avg %Complete'],
                rows, output)

    conn.close()


def analyze_clean_oee(output):
    """OEE analysis using clean views."""
    print_header("CLEAN OEE ANALYSIS (from metrics_10s)", output)

    conn = get_connection()

    output.write("\n## OEE by Stage\n")
    output.write("(Metrics are time-bucketed, no duplicates)\n")

    cursor = conn.execute("""
        SELECT
            stage,
            COUNT(*) as lines,
            SUM(buckets) as total_buckets,
            ROUND(AVG(avg_availability_pct), 1) as availability,
            ROUND(AVG(avg_performance_pct), 1) as performance,
            ROUND(AVG(avg_quality_pct), 1) as quality,
            ROUND(AVG(avg_oee_pct), 1) as oee,
            SUM(total_outfeed) as total_outfeed
        FROM v_oee_by_line
        WHERE stage IN ('MIX', 'FILL', 'PACK')
        GROUP BY stage
        ORDER BY stage
    """)
    rows = [(r['stage'], r['lines'], r['total_buckets'],
             f"{r['availability']}%", f"{r['performance']}%",
             f"{r['quality']}%", f"{r['oee']}%",
             int(r['total_outfeed']) if r['total_outfeed'] else 0)
            for r in cursor]
    print_table(['Stage', 'Lines', 'Buckets', 'Avail', 'Perf', 'Qual', 'OEE', 'Total Outfeed'],
                rows, output)

    output.write("\n## OEE by Line (Production Stages Only)\n")
    cursor = conn.execute("""
        SELECT * FROM v_oee_by_line
        WHERE stage IN ('MIX', 'FILL', 'PACK')
        ORDER BY stage, site, line
    """)
    rows = [(r['site'], r['line'], r['stage'],
             f"{r['avg_availability_pct']}%", f"{r['avg_performance_pct']}%",
             f"{r['avg_quality_pct']}%", f"{r['avg_oee_pct']}%",
             int(r['total_outfeed']) if r['total_outfeed'] else 0,
             f"{r['avg_rate_actual']:.1f}" if r['avg_rate_actual'] else "-")
            for r in cursor]
    print_table(['Site', 'Line', 'Stage', 'Avail', 'Perf', 'Qual', 'OEE', 'Outfeed', 'Rate'],
                rows, output)

    conn.close()


def analyze_clean_rates(output):
    """Rate analysis - actual throughput from clean data."""
    print_header("CLEAN RATE ANALYSIS", output)

    conn = get_connection()

    output.write("\n## Throughput Rates by Line\n")
    output.write("(Calculated from metrics_10s over collection period)\n")

    cursor = conn.execute("""
        SELECT
            site, line,
            CASE
                WHEN line LIKE 'mixroom%' THEN 'MIX'
                WHEN line LIKE 'filling%' THEN 'FILL'
                WHEN line LIKE 'labeler%' THEN 'PACK'
                ELSE 'OTHER'
            END as stage,
            COUNT(*) as buckets,
            SUM(count_outfeed) as total_outfeed,
            ROUND(SUM(count_outfeed) * 1.0 / COUNT(*) / 10, 1) as units_per_sec,
            ROUND(SUM(count_outfeed) * 6.0 / COUNT(*), 1) as units_per_min,
            AVG(rate_standard) as std_rate
        FROM metrics_10s
        WHERE count_outfeed > 0
        GROUP BY site, line
        HAVING stage IN ('FILL', 'PACK')
        ORDER BY stage, site, line
    """)
    rows = [(r['site'], r['line'], r['stage'],
             int(r['total_outfeed']), f"{r['units_per_sec']}", f"{r['units_per_min']}",
             f"{r['std_rate']:.1f}" if r['std_rate'] else "-")
            for r in cursor]
    print_table(['Site', 'Line', 'Stage', 'Total Output', 'Units/sec', 'Units/min', 'Std Rate'],
                rows, output)

    # Duration-based calculation
    output.write("\n## Duration-Based Rates (from WO completions)\n")
    cursor = conn.execute("""
        SELECT
            site, line,
            CASE
                WHEN line LIKE 'mixroom%' THEN 'MIX'
                WHEN line LIKE 'filling%' THEN 'FILL'
                WHEN line LIKE 'labeler%' THEN 'PACK'
                ELSE 'OTHER'
            END as stage,
            COUNT(*) as completions,
            SUM(final_quantity) as total_qty,
            SUM(duration_seconds) as total_duration_sec,
            ROUND(SUM(final_quantity) * 60.0 / NULLIF(SUM(duration_seconds), 0), 1) as actual_rate_per_min
        FROM work_order_completions
        WHERE final_quantity > 0 AND duration_seconds > 0
        GROUP BY site, line
        ORDER BY stage, site, line
    """)
    rows = [(r['site'], r['line'], r['stage'], r['completions'],
             int(r['total_qty']),
             f"{r['total_duration_sec']/3600:.1f}h" if r['total_duration_sec'] else "-",
             r['actual_rate_per_min'] or "-")
            for r in cursor]
    print_table(['Site', 'Line', 'Stage', 'WOs', 'Total Qty', 'Duration', 'Rate/min'],
                rows, output)

    conn.close()


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="Comprehensive data analysis")
    parser.add_argument("--section", choices=['wo', 'flow', 'products', 'metrics', 'targets', 'clean', 'all'],
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

        if args.section in ['clean', 'all']:
            analyze_replay_status(output)
            analyze_clean_production(output)
            analyze_clean_oee(output)
            analyze_clean_rates(output)

        if args.section in ['wo', 'all']:
            analyze_wo_status(output)
            analyze_wo_stages(output)
            analyze_wo_products(output)
            analyze_target_vs_actual(output)
            analyze_overruns(output)
            analyze_early_closures(output)

        if args.section in ['flow', 'all']:
            analyze_quantity_flow(output)

        if args.section in ['targets', 'all']:
            analyze_stage_targets(output)

        if args.section in ['metrics', 'all']:
            analyze_metrics_collection(output)

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
