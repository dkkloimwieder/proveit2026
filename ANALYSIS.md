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

---

## Known Bugs / Issues

| Issue | Description | Status |
|-------|-------------|--------|
| proveit2026-1eo | bottle_size and pack_count not captured correctly | Open |
| proveit2026-tjq | Lots not linked to products | Open |
| proveit2026-vbu | WO transition events not logged | Open |
| proveit2026-cnx | Lot transition events not logged | Open |

---

## Open Questions

See `bd list --status=open` for current issues. Key unknowns:

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
