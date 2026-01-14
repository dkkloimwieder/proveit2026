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

### `analyze_workorders.py`

Comprehensive work order analysis:

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

### 1. Multi-Site Manufacturing
- Products flow between sites during manufacturing
- Same WO number tracked across entire production chain
- Not isolated per-equipment tracking

### 2. UOM Indicates Process Stage
- `kg` = raw materials (liquidprocessing)
- `bottle` = filled bottles (fillerproduction)
- `CS` = cases/packs (packaging)

### 3. Pack Variants
- `-P12`, `-P16`, `-P20`, `-P24` suffixes indicate pack size
- Multiple variants can run from same base WO

### 4. State Machine
States captured: Running, Idle, CIP, Cleaning, Fill, Mix, Transfer, Planned Downtime, Unplanned Downtime

---

## Open Questions

See beads issues for tracking. Key unknowns:

1. **Lot-Product Linking**: `lots.product_id` is always NULL - need correlation logic
2. **Event Detection**: Are we capturing all WO/lot transitions as events?
3. **Metrics Aggregation**: How should line-level metrics aggregate equipment data?

---

## Data Collection

Run continuous collection:

```bash
python data_collector.py
```

The collector:
- Subscribes to `Enterprise B/#`
- Parses variable-depth topics (area/line/equipment levels)
- Upserts reference data (products, lots, work orders, assets)
- Logs state change events
- Buckets metrics every 10 seconds
- Stores raw messages for debugging
