# Enterprise B Data Analysis

This document describes the manufacturing data structure discovered through MQTT exploration.

## Quick Start

```bash
# View database overview and current data
python explore.py

# Analyze work orders specifically
python analyze_workorders.py

# Validate MQTT capture coverage
python validate_capture.py
```

## Data Storage

All data is stored in `proveit.db` (SQLite). See `schema.py` for table definitions.

### Tables

| Table | Type | Description |
|-------|------|-------------|
| `products` | Reference | Product/item master data |
| `lots` | Reference | Batch/lot numbers |
| `work_orders` | Reference | Work order definitions |
| `states` | Reference | State name lookup |
| `assets` | Reference | Equipment hierarchy |
| `events` | Time-series | State changes, WO/lot transitions |
| `metrics_10s` | Time-series | 10-second OEE metric buckets |
| `messages_raw` | Archive | Raw MQTT payloads |
| `topics` | Registry | Discovered topic hierarchy |

---

## Manufacturing Process Flow

```
liquidprocessing  →  fillerproduction  →  packaging  →  palletizing
   (Mix, kg)          (Bottle)            (Pack, CS)

   mixroom            fillingline         labelerline   palletizer
   vat, tank
```

### Process Stages by UOM

| Stage | Area | Product Class | UOM | Equipment |
|-------|------|---------------|-----|-----------|
| 1 | liquidprocessing | Mix | kg | mixroom, vat, tank |
| 2 | fillerproduction | Bottle | bottle | fillingline |
| 3 | packaging | Pack | CS (cases) | labelerline |
| 4 | palletizing | - | - | palletizer |

### Product Hierarchy

| Class | Stage | Example |
|-------|-------|---------|
| Mix | liquidprocessing | Orange Soda Mix, Cola Mix |
| Bottle | fillerproduction | Orange Soda 0.5L, Cola Soda 0.5L |
| Pack | packaging | Orange 0.5L 12Pk, Cola 0.5L 20Pk |

---

## Work Orders

### Number Pattern: `WO-Lxx-xxxx-Pxx`

| Segment | Meaning | Examples |
|---------|---------|----------|
| `WO-` | Prefix | Always "WO-" |
| `Lxx` | Line code | L01, L02, L03, L04 |
| `xxxx` | Sequence number | 0086, 0880, 0948 |
| `-Pxx` | Pack variant (optional) | P12, P16, P20, P24 |

Pack variants indicate pack sizes: P12 = 12-pack, P16 = 16-pack, etc.

### Cross-Site Flow

**Same work order number appears at multiple sites/lines** tracking product through the full manufacturing process:

```
WO-L01-0880:
  Site1/mixroom01 (kg)        → raw liquid mixing
  Site2/labelerline02 (CS)    → final packaging
```

This means work orders span the entire production chain, not just single equipment.

### Quantity Tracking

**Quantities are CUMULATIVE (running totals), not incremental deltas.**

```
Time                  Quantity    Delta
2026-01-14 18:33:01   4           (new WO started)
2026-01-14 18:33:20   8           +4
2026-01-14 18:33:21   13          +5
2026-01-14 18:33:30   17          +4
...
```

A large drop (e.g., 2919 → 4) indicates a **work order change**.

### Completion Detection

Work order completion is detected by **change in workorderid/workordernumber**:

1. Monitor `workorder/workorderid` topic
2. When value changes to a NEW ID = previous WO completed
3. Final `quantityactual` before change = completion count
4. New WO starts with low count (often single digits)

Example:
```
18:29:52  WO-L04-0142-P16 (id 6100)  qty=2919  ← previous WO final count
18:33:01  WO-L03-0964-P12 (id 6107)  qty=4     ← new WO started
```

---

## Analysis Scripts

### `analyze_data.py` (PRIMARY - Full Analysis)

Comprehensive repeatable analysis covering all tracked issues:

```bash
python analyze_data.py                      # Full analysis to stdout
python analyze_data.py --output report.txt  # Save to file
python analyze_data.py --section wo         # Work orders only
python analyze_data.py --section flow       # Process flow only
python analyze_data.py --section products   # Product data only
```

**Covers issues:**
- proveit2026-kax: Work order status analysis
- proveit2026-pss: Process stage mapping
- proveit2026-l01: Product/lot linkage
- proveit2026-32p: Target vs actual quantities
- proveit2026-ruj: Quantity overruns
- proveit2026-4jz: Early WO closures
- proveit2026-e1v: Cross-operation quantity flow
- proveit2026-wg6: Product data accuracy
- proveit2026-2r1: Metrics collection per process

### `analyze_workorders.py`

Work order lifecycle and patterns:

```bash
python analyze_workorders.py              # Full analysis
python analyze_workorders.py --lifecycle  # WO changes over time
python analyze_workorders.py --completion # How completion is detected
python analyze_workorders.py --crosssite  # Cross-site WO flow
python analyze_workorders.py --process    # Manufacturing process flow
python analyze_workorders.py --patterns   # WO number pattern decode
python analyze_workorders.py --products   # Product hierarchy
python analyze_workorders.py --summary    # Database summary
```

### `explore.py`

General data exploration:

```bash
python explore.py              # Overview + products + work orders + states
python explore.py --assets     # Equipment hierarchy
python explore.py --events     # Recent state changes
python explore.py --metrics    # OEE metrics (10s buckets)
python explore.py --lots       # Batch/lot numbers
python explore.py --topics     # MQTT topic hierarchy
```

### `validate_capture.py`

Validate all MQTT data types are being captured:

```bash
python validate_capture.py
# Press Ctrl+C after 30-60s to see report
```

---

## Key Findings

### 1. Work Order Status (proveit2026-kax)
- ~20% COMPLETE (exceeded target)
- ~15% IN_PROGRESS (50-95% of target)
- ~55% STARTING (<50% of target)
- ~10% NO_TARGET (no target quantity set)

### 2. Target vs Actual (proveit2026-32p, proveit2026-ruj)
- **Overruns are common**: Some WOs exceed target by 1500%+
- Targets appear to be **MINIMUMS**, not hard limits
- Example: WO-L03-0948 at 1579% of target

### 3. Early Closures (proveit2026-4jz)
- **YES, early closures occur**
- WOs can be replaced before reaching target
- Example: WO-L04-0142-P16 closed at 59.9% (2919/4875)

### 3a. Completed Work Orders (observed)
6 WO transitions detected during ~1.3 hours of data collection:

| Time | WO Number | Location | Final Qty | Target | Status |
|------|-----------|----------|-----------|--------|--------|
| 18:33:01 | WO-L04-0142 | fillingline03 | 77,946 | 78,000 | 99.9% (EARLY) |
| 18:33:01 | WO-L04-0142-P12 | labelerline03 | 6,165 | - | NO_TARGET |
| 18:33:11 | WO-L03-0964-P12 | labelerline04 | 4 | 4,333 | 0.1% (EARLY) |
| 18:48:41 | WO-L02-1486 | mixroom01/vat03 | 12,415 | 13,000 | 95.5% (EARLY) |
| 18:53:21 | WO-L02-0239 | mixroom01/vat02 | 0 | 13,000 | 0.0% (EARLY) |
| 19:02:21 | WO-L02-1259 | mixroom01/vat01 | 11,493 | 7,000 | **164.2% (MET)** |

Only 1 of 6 met target - others closed early or had no target.

### 4. Cross-Operation Quantity Flow (proveit2026-e1v)
- Quantities **DO NOT** match across operations
- UOM changes: kg → bottle → CS (cases)
- Conversion factors vary by product (bottle size, pack count)

### 5. Product Data Issues (proveit2026-wg6)
- **BUG**: Products table has incorrect data
- bottle_size = 0 (should be 0.5)
- pack_count = 0 (should be 12/16/20/24)
- Raw MQTT has correct values, collector not capturing

### 6. Product/Lot Linkage (proveit2026-l01)
- work_orders.product_id is NULL
- work_orders.lot_id is NULL
- Data EXISTS in raw MQTT, not linked in tables

### 7. Multi-Site Manufacturing
- Products flow between sites during manufacturing
- Same WO number tracked across entire production chain
- Not isolated per-equipment tracking

### 8. UOM Indicates Process Stage
- `kg` = raw materials (liquidprocessing)
- `bottle` = filled bottles (fillerproduction)
- `CS` = cases/packs (packaging)

### 9. Pack Variants
- `-P12`, `-P16`, `-P20`, `-P24` suffixes indicate pack size
- Multiple variants can run from same base WO

### 10. State Machine
States captured: Running, Idle, CIP, Cleaning, Fill, Mix, Transfer, Planned Downtime, Unplanned Downtime, Pasteurize

### 11. Stage-to-Stage Target Conversion

**KG to Bottle (MIX → FILL):**
- Bottle size: 0.5L
- Theoretical conversion: 1 kg ≈ 2 bottles (assuming ~1 kg/L density)
- Mix targets: 7,000 - 13,000 kg → 14,000 - 26,000 theoretical bottles
- **No direct WO linkage** between MIX and FILL stages in data

**Bottle to Case (FILL → PACK):**
- Formula: `Cases Target × Pack Size = Bottles Target`
- This relationship is **100% accurate** for linked WOs
- Example: WO-L03-0948 → 36,000 bottles = 3,000 cases × 12 bottles/case

**WO Naming Convention:**
- Pattern: `WO-Lxx-xxxx-Pxx`
- **CRITICAL FINDING**: Line codes ARE stage-specific:
  - `L01`, `L02` = MIX stage ONLY (liquidprocessing/mixroom)
  - `L03`, `L04` = FILL and PACK stages (fillerproduction, packaging)
- `-Pxx` suffix indicates pack variant, ONLY at PACK stage

**Stage Linkage:**
- **MIX → FILL**: DISCONNECTED - Different WO number series (L01/L02 vs L03/L04)
- **FILL → PACK**: CONNECTED - Same base WO, adds -Pxx suffix
- Example: WO-L03-0948 (FILL) → WO-L03-0948-P12 (PACK)

**Why disconnected?** Mixing is a BATCH PROCESS that produces bulk liquid stored in tanks.
Multiple FILL orders can draw from the same mix batch. No 1:1 WO tracking.

Run analysis: `python analyze_data.py --section targets`

### 12. Bill of Materials (BOM) Estimates

> **⚠️ PRELIMINARY DATA**: Analysis based on ~7 hours of MQTT collection. Rates and ratios may change with more data. MIX stage lacks direct rate metrics - values derived from OEE calculations.

#### Product Structure
```
2 Base Products: Cola, Orange
       ↓
2 Mix Products: Cola Mix, Orange Soda Mix
       ↓
2 Bottle Products: Cola Soda 0.5L, Orange Soda 0.5L
       ↓
10 Pack Variants: 4, 6, 12, 16, 20, 24 packs
```

#### BOM Conversions

| Conversion | Formula | Source |
|------------|---------|--------|
| PACK → BOTTLE | `Bottles = Cases × Pack Size` | Verified from WO targets |
| BOTTLE → MIX | `Mix (kg) = Bottles × 0.5` | Theoretical (0.5L bottle, ~1 kg/L density) |

Pack sizes: 4, 6, 12, 16, 20, 24 bottles per case

#### Production Rates (OEE-backed estimates)

| Stage | Standard Rate | Availability | Effective Rate | Equipment |
|-------|---------------|--------------|----------------|-----------|
| MIX | ~381 kg/min* | 61% | 466 bottles/min equiv | 4 vats |
| FILL | 289 bottles/min | 89% | 258 bottles/min | 3 lines |
| PACK | 282 cases/min | 76% | 215 cases/min | 4 lines |

*MIX rate derived from WO output ÷ (duration × availability). No direct `ratestandard` metric available.

#### MIX → FILL Work Order Relationship

```
WO Naming: MIX uses L01/L02, FILL uses L03/L04 (disconnected series)

1 MIX WO (~20,000 kg = 40,000 bottles equiv)
    ↓
~1.8 FILL WOs (~22,500 bottles each)

System throughput per hour:
  MIX: 2.8 WOs worth (1,862 bottles/min × 4 vats)
  FILL: 2.1 WOs worth (258 bottles/min × 3 lines)

  → MIX oversupplies FILL by ~2.4x
  → Inventory accumulates in tanks between stages
```

#### Data Gaps
- MIX `rateactual` and `ratestandard` metrics are 0 in MQTT
- MIX rate estimated from WO completions + OEE, not direct measurement
- Limited WO completion samples (19 completions observed)
- Unknown: actual tank inventory levels, transfer timing between stages

#### Products by Line

**MIX (mixroom)**
| Product | Sites |
|---------|-------|
| Cola Mix | Site1, Site2, Site3 |
| Orange Soda Mix | Site1, Site2 |

**FILL (fillingline)**
| Product | Lines |
|---------|-------|
| Cola Soda 0.5L | Site1/fill02, Site1/fill03, Site2/fill01, Site2/fill02, Site3/fill01 |
| Orange Soda 0.5L | Site1/fill01, Site1/fill03, Site2/fill01, Site2/fill02 |

**PACK (labelerline)**
| Product | Lines |
|---------|-------|
| Cola 0.5L 4Pk | Site1/label02, Site2/label02 |
| Cola 0.5L 6Pk | Site2/label01, Site3/label01 |
| Cola 0.5L 12Pk | Site1/label03 |
| Cola 0.5L 16Pk | Site1/label02, Site1/label04 |
| Cola 0.5L 20Pk | Site1/label02, Site3/label01 |
| Cola 0.5L 24Pk | Site1/label03, Site3/label01 |
| Orange 0.5L 12Pk | Site1/label04, Site2/label02 |
| Orange 0.5L 16Pk | Site1/label01, Site2/label01 |
| Orange 0.5L 20Pk | Site1/label01 |
| Orange 0.5L 24Pk | Site1/label03 |

### 13. Metrics Collection Per Process (proveit2026-2r1)
- **work_orders.quantity_actual** = SNAPSHOT (last MQTT value per WO/site/line)
- **metrics_10s** = AGGREGATED counts every 10 seconds (summed across equipment)
- WO quantities come from LINE-level MQTT topics
- Equipment-level counts go to metrics_10s table separately
- To get total production per line: `SUM(metrics_10s.count_outfeed)` grouped by line

**Data Flow:**
```
MQTT Line-level WO topic:
  Enterprise B/Site1/packaging/labelerline04/workorder/quantityactual
  -> work_orders.quantity_actual (latest snapshot)

MQTT Equipment-level metric topic:
  Enterprise B/Site1/packaging/labelerline04/labeler/metric/output/countoutfeed
  -> metrics_10s.count_outfeed (10s bucketed sum)
```

---

## Operational Analysis

This section provides real-time interpretation of the beverage production process.

### Current Production Status

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   MIX (Stage 1) │     │  FILL (Stage 2) │     │  PACK (Stage 3) │     │ PALLETIZE (S4)  │
│   liquidprocess │ ──► │ fillerproduction│ ──► │    packaging    │ ──► │   palletizing   │
│                 │     │                 │     │                 │     │                 │
│  UOM: kg        │     │  UOM: bottle    │     │  UOM: CS        │     │  UOM: pallet    │
│  Product: Mix   │     │  Product: Bottle│     │  Product: Pack  │     │                 │
└─────────────────┘     └─────────────────┘     └─────────────────┘     └─────────────────┘
```

### Multi-Site Operation

| Site | Lines | Primary Products |
|------|-------|------------------|
| Site1 | fillingline01-03, labelerline01-04 | Cola & Orange packs |
| Site2 | fillingline01-02, labelerline01-02, mixroom01 | Cola & Orange mixes |
| Site3 | fillingline01, labelerline01, mixroom01 | Cola & Orange |

### Equipment State Distribution

| State | Events | % | Description |
|-------|--------|---|-------------|
| Running | 7,604 | 46% | Active production |
| Idle | 5,658 | 34% | Waiting for work |
| Unplanned Downtime | 1,519 | 9% | Equipment issues |
| CIP | 326 | 2% | Clean-in-place |
| Other (Transfer, Cool, Mix, Fill, etc.) | 1,727 | 10% | Process states |

### Active Work Orders (Sample)

| Work Order | Product | Site/Line | Actual | Target | Status |
|------------|---------|-----------|--------|--------|--------|
| WO-L04-1420 | - | Site2/fillingline01 | 89,570 | 56,000 | 160% (Overrun) |
| WO-L03-0290-P24 | Pack | Site2/labelerline02 | 766 | 9,000 | 9% (In Progress) |
| WO-L02-1164 | - | Site3/labelerline01 | 24,687 | 9,000 | 274% (Overrun) |
| WO-L04-0720-P24 | Pack | Site1/labelerline04 | 24,630 | 7,000 | 352% (Overrun) |

**Note**: Overruns are normal - targets appear to be minimums, not hard limits.

### Cross-Site Work Order Flow

Work orders track product through the entire manufacturing chain:

```
WO-L04-1420:
  Site2/fillingline01 (bottles)  → fills bottles
  Site2/labelerline01 (cases)    → packs into cases (WO-L04-1420-P24)
  Site3/mixroom01 (kg)           → supplies mix (WO-L04-1420-P04)
```

### What This Tells Us

1. **Multi-site production** - Same WO number tracked across different sites/stages
2. **High utilization** - 46% running, 34% idle (waiting for upstream)
3. **Target flexibility** - WOs routinely exceed targets (targets are minimums)
4. **Discrete + batch** - Mix stage is batch, Fill/Pack are discrete
5. **Pack variants** - Same base WO spawns multiple pack sizes (-P04, -P12, -P24)
6. **CIP cycles** - Regular cleaning between product changeovers

---

## Simulator Replay Behavior

> **⚠️ CRITICAL**: The MQTT data is **simulated replay data** from a historical dataset. This affects data integrity and analysis.

### How the Simulator Works

The MQTT broker replays historical manufacturing data from November 2025. Metadata topic reveals:

```json
{
  "data_timestamp": "2025-11-04T16:57:19Z",  // Original recording time
  "timestamp": "2026-01-14T18:29:50Z",        // Current replay time
  "progress_pct": 0.5508...                   // 55% through the dataset
}
```

- **Progress rate**: ~0.66% per hour (~11.9% per 18 hours)
- **Reset expected**: When `progress_pct` reaches 1.0, data will loop back to start
- **ID regeneration**: Same WO/lot numbers get NEW IDs on each replay cycle

### Impact on Data

| Entity | Unique Constraint | Replay Behavior | Impact |
|--------|-------------------|-----------------|--------|
| work_orders | work_order_id | New ID assigned per replay | 40% of WO numbers have multiple IDs |
| lots | lot_number_id | New ID assigned per replay | Minimal observed so far |
| products | item_id | Stable across replay | No duplicates |
| metrics_10s | (bucket, site, line) | Overwrites per bucket | Clean - no duplicates |

### Example: Duplicate Work Order

```
WO-L04-0292 on Site2/mixroom01:
  ID 1476 @ 04:59 → qty 40,257, target 3,375 (1192% complete)
  ID 538  @ 06:41 → qty 6,777,  target 9,000 (75% complete)
```

These are two different "runs" of the same WO number from different points in the replay.

### Clean Analysis Queries

**WRONG** - Double-counts production:
```sql
SELECT work_order_number, SUM(final_quantity)
FROM work_order_completions
GROUP BY work_order_number;
```

**CORRECT** - Each completion is a unique event:
```sql
SELECT work_order_id, work_order_number, final_quantity
FROM work_order_completions;
-- OR for totals by line:
SELECT site, line, SUM(final_quantity) as total
FROM work_order_completions
GROUP BY site, line;
```

### Detecting Replay Position

```sql
SELECT
    json_extract(payload_text, '$.virtual_devices.Site1.progress_pct') as progress
FROM messages_raw
WHERE topic = 'Enterprise B/metadata'
ORDER BY received_at DESC
LIMIT 1;
```

---

## Known Bugs / Issues

| Issue | Description | Status |
|-------|-------------|--------|
| proveit2026-1eo | bottle_size and pack_count not captured correctly | **Fixed** |
| proveit2026-tjq | Lots not linked to products | **Fixed** |
| proveit2026-vbu | WO transition events not logged | **Fixed** - see `work_order_completions` table |
| proveit2026-cnx | Lot transition events not logged | Open |
| - | MQTT client_id collision causing disconnects | **Fixed** - unique client_id per instance |
| - | Process data (temp/flow/weight) not captured | **Fixed** - now stored in metrics_10s |

---

## Open Questions

See `bd list --status=open` for current issues. Key unknowns:

1. **Event Detection**: Are we capturing all WO/lot transitions as events?
2. **Metrics Aggregation**: How should line-level metrics aggregate equipment data?

---

## Data Collection

Run continuous collection:

```bash
python data_collector.py          # Standard collection
python data_collector.py --raw    # Also capture raw MQTT messages
```

**Note**: Raw messages consume ~160 MB/hour. Purge with:
```sql
DELETE FROM messages_raw; VACUUM;
```

The collector:
- Subscribes to `Enterprise B/#`
- Parses variable-depth topics (area/line/equipment levels)
- Upserts reference data (products, lots, work orders, assets)
- Logs state change events
- Buckets metrics every 10 seconds
- Stores raw messages for debugging
