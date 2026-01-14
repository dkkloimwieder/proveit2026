# Enterprise B Manufacturing Process Flow

This document explains how work orders and products progress through the Enterprise B beverage manufacturing facility.

## Overview

Enterprise B is a **beverage manufacturing operation** producing bottled sodas (Cola, Orange) in various pack sizes (12, 16, 20, 24 packs).

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         ENTERPRISE B PROCESS FLOW                           │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   ┌──────────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────┐ │
│   │   MIXING     │───▶│   FILLING    │───▶│  PACKAGING   │───▶│PALLETIZE │ │
│   │              │    │              │    │              │    │          │ │
│   │  Mix (kg)    │    │ Bottle (ea)  │    │ Cases (CS)   │    │ Pallets  │ │
│   └──────────────┘    └──────────────┘    └──────────────┘    └──────────┘ │
│                                                                             │
│   Equipment:          Equipment:          Equipment:          Equipment:   │
│   - mixroom           - fillingline       - labelerline       - palletizer │
│   - vat               - filler            - labeler                        │
│   - tank              - caploader         - packager                       │
│                       - washer            - sealer                         │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Stage 1: MIXING (liquidprocessing)

**Location**: `Site/liquidprocessing/mixroom01/vat*`

**What happens**: Raw ingredients are mixed to create beverage concentrate/syrup.

**Products at this stage**:
- Cola Mix
- Orange Soda Mix

**UOM**: `kg` (kilograms of liquid mixture)

**Equipment**:
- `mixroom01` - Main mixing area
- `vat01-vat04` - Individual mixing vats
- `tankstorage01/tank01-tank06` - Storage tanks

**Example**:
```
WO-L03-0948 at Site3/mixroom01
  Product: Orange Soda Mix
  UOM: kg
  Quantity: 1,461 kg
```

## Stage 2: FILLING (fillerproduction)

**Location**: `Site/fillerproduction/fillingline*`

**What happens**: Liquid mixture is pumped into bottles, capped, and washed.

**Products at this stage**:
- Cola Soda 0.5L
- Orange Soda 0.5L

**UOM**: `bottle` (count of individual bottles)

**Equipment**:
- `fillingline01-03` - Production lines
- `filler` - Bottle filling machine
- `caploader` - Cap application
- `washer` - Bottle washing

**Conversion**:
```
X kg of mix → Y bottles (0.5L each)
Approximate: 1 kg ≈ 2 bottles (0.5L = 0.5kg water equivalent)
```

**Example**:
```
WO-L03-0964 at Site2/fillingline01
  Product: Orange Soda 0.5L
  UOM: bottle
  Quantity: 13,600 bottles
```

## Stage 3: PACKAGING (packaging)

**Location**: `Site/packaging/labelerline*`

**What happens**: Individual bottles are labeled, grouped into packs, and sealed.

**Products at this stage**:
- Cola 0.5L 12Pk (12-pack)
- Cola 0.5L 16Pk (16-pack)
- Cola 0.5L 20Pk (20-pack)
- Orange 0.5L 12Pk
- Orange 0.5L 16Pk
- Orange 0.5L 24Pk

**UOM**: `CS` (cases)

**Equipment**:
- `labelerline01-04` - Packaging lines
- `labeler` - Label application
- `packager` - Case packing
- `sealer` - Case sealing

**Conversion**:
```
Y bottles → Z cases
12-pack: 12 bottles = 1 case
16-pack: 16 bottles = 1 case
20-pack: 20 bottles = 1 case
24-pack: 24 bottles = 1 case
```

**Example**:
```
WO-L03-0964-P12 at Site1/labelerline04
  Product: Orange 0.5L 12Pk
  UOM: CS
  Quantity: 47,389 cases
```

## Stage 4: PALLETIZING (palletizing)

**Location**: `Site/palletizing/palletizer*`

**What happens**: Cases are stacked onto pallets for shipping.

**Equipment**:
- `palletizer01-02` - Automated palletizers
- `palletizermanual01-04` - Manual palletizing stations
- `robot` - Robotic arm
- `wrapper` - Pallet wrapping

## Work Order Number Pattern

```
WO-Lxx-xxxx-Pxx
│   │   │    │
│   │   │    └── Pack variant (P12, P16, P20, P24)
│   │   └─────── Sequence number (0001-9999)
│   └─────────── Line code (L01, L02, L03, L04)
└─────────────── Prefix
```

**Key insight**: The `-Pxx` suffix indicates the **final pack size** for that production run.

## Work Order Progression Example

### Example: WO-L03-0964 Family

This shows how one production order flows through the system:

```
BASE ORDER: WO-L03-0964 (Orange Soda)
├── Stage 1: MIXING
│   ├── WO-L03-0964-P12 @ Site2/mixroom01 → Orange Soda Mix (kg)
│   └── WO-L03-0964-P24 @ Site2/mixroom01 → Orange Soda Mix (kg)
│
├── Stage 2: FILLING
│   ├── WO-L03-0964 @ Site2/fillingline01 → Orange Soda 0.5L (bottles)
│   └── WO-L03-0964-P24 @ Site1/fillingline01 → Orange Soda 0.5L (bottles)
│
└── Stage 3: PACKAGING
    ├── WO-L03-0964 @ Site1/labelerline03 → cases (CS)
    └── WO-L03-0964-P24 @ Site2/labelerline01 → Orange 0.5L 24Pk (CS)
```

## Multi-Site Flow

Products physically move between sites during manufacturing:

```
Site1/mixroom01 (kg)
        │
        ▼
Site2/fillingline01 (bottles)  ◄── Material transfer between sites
        │
        ▼
Site1/labelerline03 (cases)    ◄── Product returns to Site1
        │
        ▼
Site1/palletizer01 (pallets)
```

**The same work order number tracks the product through ALL sites.**

## Quantity Changes Across Stages

Quantities are **NOT** directly comparable because UOMs change:

| Stage | UOM | Example Qty | Meaning |
|-------|-----|-------------|---------|
| MIXING | kg | 1,461 | 1,461 kg of liquid mix |
| FILLING | bottle | 13,600 | 13,600 individual bottles |
| PACKAGING | CS | 47,389 | 47,389 cases (12/16/20/24 bottles each) |

### Conversion Factors

```
MIXING → FILLING:
  ~2 bottles per kg (for 0.5L bottles)
  1,461 kg → ~2,922 bottles

FILLING → PACKAGING:
  Depends on pack size:
  - 12-pack: 13,600 bottles ÷ 12 = 1,133 cases
  - 24-pack: 13,600 bottles ÷ 24 = 567 cases
```

## Key Observations

### 1. Work Orders Span Multiple Sites
- A single WO number appears at Site1, Site2, AND Site3
- This tracks the product through the entire supply chain

### 2. Pack Variants Split at Packaging
- Base WO (e.g., WO-L03-0964) may split into variants (-P12, -P16, -P24)
- Each variant represents a different final pack configuration

### 3. Quantities Can Exceed Targets
- Target quantities are **minimums**, not limits
- Some WOs produce 1500%+ of target
- Early closures also occur (WO replaced before target met)

### 4. UOM Determines Stage
- `kg` = Mixing stage
- `bottle` = Filling stage
- `CS` = Packaging stage

## Useful Queries

```bash
# See all work orders and their stages
python analyze_data.py --section wo

# Track a specific WO family
sqlite3 proveit.db "SELECT * FROM work_orders WHERE work_order_number LIKE 'WO-L03-0964%'"

# See products at each stage
python analyze_data.py --section products
```

## Related Issues

- proveit2026-mhq: This documentation
- proveit2026-1eo: Fix bottle_size/pack_count capture
- proveit2026-tjq: Link lots to products
