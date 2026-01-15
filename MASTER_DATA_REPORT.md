# Enterprise B Master Data Report

Generated: 2026-01-15

## 1. Product Hierarchy

### 1.1 Product Structure

```
BASE PRODUCTS (2)
├── Cola
│   └── Cola Mix (item_id: 2)
│       └── Cola Soda 0.5L (item_id: 4, bottle_size: 0.5L)
│           ├── Cola 0.5L 4Pk  (item_id: 11, pack: 4)
│           ├── Cola 0.5L 6Pk  (item_id: 12, pack: 6)
│           ├── Cola 0.5L 12Pk (item_id: 13, pack: 12)
│           ├── Cola 0.5L 16Pk (item_id: 14, pack: 16)
│           ├── Cola 0.5L 20Pk (item_id: 15, pack: 20)
│           └── Cola 0.5L 24Pk (item_id: 16, pack: 24)
│
└── Orange
    └── Orange Soda Mix (item_id: 1)
        └── Orange Soda 0.5L (item_id: 3, bottle_size: 0.5L)
            ├── Orange 0.5L 4Pk  (item_id: 5, pack: 4)
            ├── Orange 0.5L 12Pk (item_id: 7, pack: 12)
            ├── Orange 0.5L 16Pk (item_id: 8, pack: 16)
            ├── Orange 0.5L 20Pk (item_id: 9, pack: 20)
            └── Orange 0.5L 24Pk (item_id: 10, pack: 24)
```

### 1.2 Product Master Table

| item_id | Name | Class | Bottle Size | Pack Count | Parent ID | Parent Name |
|---------|------|-------|-------------|------------|-----------|-------------|
| 1 | Orange Soda Mix | Mix | - | - | - | - |
| 2 | Cola Mix | Mix | - | - | - | - |
| 3 | Orange Soda 0.5L | Bottle | 0.5L | - | 1 | Orange Soda Mix |
| 4 | Cola Soda 0.5L | Bottle | 0.5L | - | 2 | Cola Mix |
| 5 | Orange 0.5L 4Pk | Pack | - | 4 | 3 | Orange Soda 0.5L |
| 7 | Orange 0.5L 12Pk | Pack | - | 12 | 3 | Orange Soda 0.5L |
| 8 | Orange 0.5L 16Pk | Pack | - | 16 | 3 | Orange Soda 0.5L |
| 9 | Orange 0.5L 20Pk | Pack | - | 20 | 3 | Orange Soda 0.5L |
| 10 | Orange 0.5L 24Pk | Pack | - | 24 | 3 | Orange Soda 0.5L |
| 11 | Cola 0.5L 4Pk | Pack | - | 4 | 4 | Cola Soda 0.5L |
| 12 | Cola 0.5L 6Pk | Pack | - | 6 | 4 | Cola Soda 0.5L |
| 13 | Cola 0.5L 12Pk | Pack | - | 12 | 4 | Cola Soda 0.5L |
| 14 | Cola 0.5L 16Pk | Pack | - | 16 | 4 | Cola Soda 0.5L |
| 15 | Cola 0.5L 20Pk | Pack | - | 20 | 4 | Cola Soda 0.5L |
| 16 | Cola 0.5L 24Pk | Pack | - | 24 | 4 | Cola Soda 0.5L |

---

## 2. Bill of Materials (BOM)

### 2.1 Stage-to-Stage Conversions

| From | To | Conversion | Formula |
|------|-----|------------|---------|
| MIX | FILL | kg → bottles | `bottles = kg × 2` (0.5L bottle ≈ 0.5kg) |
| FILL | PACK | bottles → cases | `cases = bottles ÷ pack_count` |

### 2.2 BOM Table

| Output Product | Input Product | Conversion Factor | UOM In | UOM Out |
|----------------|---------------|-------------------|--------|---------|
| Cola Soda 0.5L | Cola Mix | 2 bottles/kg | kg | bottle |
| Orange Soda 0.5L | Orange Soda Mix | 2 bottles/kg | kg | bottle |
| Cola 0.5L 4Pk | Cola Soda 0.5L | 4 bottles/case | bottle | CS |
| Cola 0.5L 6Pk | Cola Soda 0.5L | 6 bottles/case | bottle | CS |
| Cola 0.5L 12Pk | Cola Soda 0.5L | 12 bottles/case | bottle | CS |
| Cola 0.5L 16Pk | Cola Soda 0.5L | 16 bottles/case | bottle | CS |
| Cola 0.5L 20Pk | Cola Soda 0.5L | 20 bottles/case | bottle | CS |
| Cola 0.5L 24Pk | Cola Soda 0.5L | 24 bottles/case | bottle | CS |
| Orange 0.5L 4Pk | Orange Soda 0.5L | 4 bottles/case | bottle | CS |
| Orange 0.5L 12Pk | Orange Soda 0.5L | 12 bottles/case | bottle | CS |
| Orange 0.5L 16Pk | Orange Soda 0.5L | 16 bottles/case | bottle | CS |
| Orange 0.5L 20Pk | Orange Soda 0.5L | 20 bottles/case | bottle | CS |
| Orange 0.5L 24Pk | Orange Soda 0.5L | 24 bottles/case | bottle | CS |

---

## 3. Process Flow

### 3.1 Manufacturing Stages

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   MIX (Stage 1) │     │  FILL (Stage 2) │     │  PACK (Stage 3) │     │ PALLETIZE (S4)  │
│   liquidprocess │ ──► │ fillerproduction│ ──► │    packaging    │ ──► │   palletizing   │
│                 │     │                 │     │                 │     │                 │
│  UOM: kg        │     │  UOM: bottle    │     │  UOM: CS        │     │  UOM: pallet    │
│  Product: Mix   │     │  Product: Bottle│     │  Product: Pack  │     │                 │
└─────────────────┘     └─────────────────┘     └─────────────────┘     └─────────────────┘
     mixroom              fillingline            labelerline            palletizer
```

### 3.2 Stage Details

| Stage | Area | Equipment Type | UOM | Product Class | Sites |
|-------|------|----------------|-----|---------------|-------|
| MIX | liquidprocessing | mixroom | kg | Mix | Site1, Site2, Site3 |
| FILL | fillerproduction | fillingline | bottle | Bottle | Site1, Site2, Site3 |
| PACK | packaging | labelerline | CS (cases) | Pack | Site1, Site2, Site3 |
| PALLETIZE | palletizing | palletizer | pallet | - | Site1, Site2, Site3 |

---

## 4. Site & Line Configuration

### 4.1 Site Overview

| Site | MIX Lines | FILL Lines | PACK Lines | PALLETIZE Lines |
|------|-----------|------------|------------|-----------------|
| Site1 | 1 (mixroom01) | 3 (fill01-03) | 4 (label01-04) | 6 |
| Site2 | 1 (mixroom01) | 2 (fill01-02) | 2 (label01-02) | 3 |
| Site3 | 1 (mixroom01) | 1 (fill01) | 1 (label01) | 1 |
| **TOTAL** | **3** | **6** | **7** | **10** |

### 4.2 Lines by Stage

#### MIX Lines (3 total)

| Site | Line | Products | UOM | Standard Rate | Actual Rate | OEE |
|------|------|----------|-----|---------------|-------------|-----|
| Site1 | mixroom01 | Cola Mix, Orange Soda Mix | kg | N/A* | 72.6/min | 52.0% |
| Site2 | mixroom01 | Cola Mix, Orange Soda Mix | kg | N/A* | 77.8/min | 54.2% |
| Site3 | mixroom01 | Cola Mix, Orange Soda Mix | kg | N/A* | 132.8/min | 44.7% |

*MIX lines don't report standard rate in MQTT; actual rate derived from WO completions.

#### FILL Lines (6 total)

| Site | Line | Products | UOM | Standard Rate | Actual Rate | OEE |
|------|------|----------|-----|---------------|-------------|-----|
| Site1 | fillingline01 | Orange Soda 0.5L | bottle | 132.0/min | 148.1/min | 92.6% |
| Site1 | fillingline02 | Cola Soda 0.5L, Orange Soda 0.5L | bottle | 138.3/min | 150.3/min | 91.8% |
| Site1 | fillingline03 | Cola Soda 0.5L, Orange Soda 0.5L | bottle | 198.7/min | 209.6/min | 92.5% |
| Site2 | fillingline01 | Cola Soda 0.5L, Orange Soda 0.5L | bottle | 96.6/min | 107.4/min | 92.1% |
| Site2 | fillingline02 | Cola Soda 0.5L, Orange Soda 0.5L | bottle | 106.6/min | 123.0/min | 92.3% |
| Site3 | fillingline01 | Cola Soda 0.5L | bottle | 79.6/min | 92.4/min | 92.0% |

#### PACK Lines (7 total)

| Site | Line | Products | UOM | Standard Rate | Actual Rate | OEE |
|------|------|----------|-----|---------------|-------------|-----|
| Site1 | labelerline01 | Orange 0.5L (12, 16, 20, 24Pk) | CS | 314.0/min | 199.1/min | 70.3% |
| Site1 | labelerline02 | Cola 0.5L (4, 16, 20Pk), Orange 0.5L 4Pk | CS | 334.2/min | 212.7/min | 73.5% |
| Site1 | labelerline03 | Cola 0.5L (12, 16, 24Pk), Orange 0.5L 24Pk | CS | 347.1/min | 179.2/min | 56.7% |
| Site1 | labelerline04 | Cola 0.5L (4, 12, 16Pk), Orange 0.5L 12Pk | CS | 312.4/min | 180.2/min | 63.4% |
| Site2 | labelerline01 | Cola 0.5L (6, 12, 20Pk), Orange 0.5L 16Pk | CS | 235.0/min | 137.9/min | 64.2% |
| Site2 | labelerline02 | Cola 0.5L (4, 24Pk), Orange 0.5L (4, 12, 16Pk) | CS | 246.8/min | 153.8/min | 68.1% |
| Site3 | labelerline01 | Cola 0.5L (6, 20, 24Pk) | CS | 195.0/min | 126.2/min | 65.9% |

---

## 5. Expected Rates Summary

### 5.1 By Stage (Standard Rates)

| Stage | Lines | UOM | Avg Standard Rate | Total Capacity | Bottles Equiv |
|-------|-------|-----|-------------------|----------------|---------------|
| MIX | 3 | kg/min | ~95* | 285 kg/min | 570 bottles/min |
| FILL | 6 | bottles/min | 125.3 | 751.8 bottles/min | 751.8 bottles/min |
| PACK | 7 | CS/min | 283.5 | 1,984 CS/min | ~27,800 bottles/min** |

*MIX standard rate estimated from OEE back-calculation
**PACK capacity far exceeds FILL output; effectively limited by upstream

### 5.2 Observed Throughput (from WO Completions)

| Stage | Total Output | Duration | Actual Rate | Bottles Equiv |
|-------|--------------|----------|-------------|---------------|
| MIX | 372,264 kg | 76.6h | 81.0 kg/min | 162 bottles/min |
| FILL | 273,261 bottles | 62.4h | 72.9 bottles/min | 72.9 bottles/min |
| PACK | 353,241 CS | 72.7h | 81.0 CS/min | ~972 bottles/min |

### 5.3 System Constraint Analysis

```
CAPACITY vs ACTUAL THROUGHPUT:

MIX:  Standard ~285 kg/min → Actual 81 kg/min (28% utilization)
      Bottles equiv: 570 → 162 bottles/min

FILL: Standard 752 bottles/min → Actual 73 bottles/min (10% utilization)  ← BOTTLENECK
      Limited by: upstream MIX output + scheduling

PACK: Standard 1,984 CS/min → Actual 81 CS/min (4% utilization)
      Limited by: FILL output (starved for input)
```

**FILL is the system bottleneck** - even with low MIX throughput, FILL cannot keep up with MIX output (162 vs 73 bottles/min).

---

## 6. Product-Line Matrix

### 6.1 MIX Products by Line

| Product | Site1/mixroom01 | Site2/mixroom01 | Site3/mixroom01 |
|---------|-----------------|-----------------|-----------------|
| Cola Mix | ✓ | ✓ | ✓ |
| Orange Soda Mix | ✓ | ✓ | ✓ |

### 6.2 FILL Products by Line

| Product | S1/fill01 | S1/fill02 | S1/fill03 | S2/fill01 | S2/fill02 | S3/fill01 |
|---------|-----------|-----------|-----------|-----------|-----------|-----------|
| Cola Soda 0.5L | | ✓ | ✓ | ✓ | ✓ | ✓ |
| Orange Soda 0.5L | ✓ | ✓ | ✓ | ✓ | ✓ | |

### 6.3 PACK Products by Line

| Product | S1/L01 | S1/L02 | S1/L03 | S1/L04 | S2/L01 | S2/L02 | S3/L01 |
|---------|--------|--------|--------|--------|--------|--------|--------|
| Cola 0.5L 4Pk | | ✓ | | ✓ | | ✓ | |
| Cola 0.5L 6Pk | | | | | ✓ | | ✓ |
| Cola 0.5L 12Pk | | | ✓ | ✓ | ✓ | | |
| Cola 0.5L 16Pk | | ✓ | ✓ | ✓ | | | |
| Cola 0.5L 20Pk | | ✓ | | | ✓ | | ✓ |
| Cola 0.5L 24Pk | | | ✓ | | | ✓ | ✓ |
| Orange 0.5L 4Pk | | ✓ | | | | ✓ | |
| Orange 0.5L 12Pk | ✓ | | | ✓ | | ✓ | |
| Orange 0.5L 16Pk | ✓ | | | | ✓ | ✓ | |
| Orange 0.5L 20Pk | ✓ | | | | | | |
| Orange 0.5L 24Pk | ✓ | | ✓ | | | | |

---

## 7. Work Order Naming Convention

### 7.1 Pattern: `WO-Lxx-xxxx[-Pxx]`

| Segment | Meaning | Values |
|---------|---------|--------|
| `WO-` | Prefix | Always "WO-" |
| `Lxx` | Line series | L01, L02 = MIX; L03, L04 = FILL/PACK |
| `xxxx` | Sequence | 4-digit number (0001-9999) |
| `-Pxx` | Pack variant | P04, P06, P12, P16, P20, P24 (PACK stage only) |

### 7.2 Stage Linkage

```
MIX Stage:     WO-L01-xxxx  or  WO-L02-xxxx
                    │
                    │ (DISCONNECTED - different WO series)
                    ▼
FILL Stage:    WO-L03-xxxx  or  WO-L04-xxxx
                    │
                    │ (CONNECTED - adds -Pxx suffix)
                    ▼
PACK Stage:    WO-L03-xxxx-P12  or  WO-L04-xxxx-P16
```

---

## 8. Data Quality Notes

### 8.1 Simulator Replay

This data is from a **simulated replay** of November 2025 manufacturing data:
- Progress: ~67% through replay dataset
- Resets at 100% - data will loop
- Same WO numbers may appear with different IDs on replay

### 8.2 Known Issues

| Issue | Description | Impact |
|-------|-------------|--------|
| MIX rate_standard = 0 | MIX lines don't report standard rate | Must estimate from OEE |
| UOM inconsistency | Same line may show different UOMs | Due to replay duplicates |
| WO-product linkage | work_orders.product_id often NULL | Use lots table for linkage |

---

## Appendix: SQL Views for Analysis

```sql
-- Clean production by stage
SELECT * FROM v_production_by_stage;

-- OEE by line
SELECT * FROM v_oee_by_line;

-- Detect replay duplicates
SELECT * FROM v_duplicate_work_orders WHERE duplicate_type = 'REPLAY_DUPLICATE';

-- Current replay status
SELECT * FROM v_replay_status;
```
