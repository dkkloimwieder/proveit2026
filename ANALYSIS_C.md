# Enterprise C Data Analysis (Biotech Batch Processing)

This document describes the biotech batch processing data structure discovered through MQTT exploration.

## Quick Start

```bash
# Collect Enterprise C data
python data_collector.py -e C --raw

# Query the database
sqlite3 proveit_c.db
```

## Data Storage

All data is stored in `proveit_c.db` (SQLite). See `schemas/enterprise_c.py` for table definitions.

### Tables

| Table | Type | Description |
|-------|------|-------------|
| `units` | Reference | Process units (bioreactors, filtration, chromatography) |
| `tags` | Reference | ISA-5.1 tag definitions |
| `tag_values` | Time-series | Process values (PV, SP, STATUS) |
| `batches` | Reference | Batch records with recipe/formula |
| `phases` | Time-series | Batch phase tracking |
| `operator_messages` | Time-series | Operator prompts/acknowledgments |
| `process_metrics` | Time-series | Aggregated process metrics |
| `messages_raw` | Archive | Raw MQTT payloads |

---

## Manufacturing Process Overview

Enterprise C is a **biotech batch processing** facility producing recombinant proteins (rBMN-42).

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  SUB Bioreactor │     │  SUM Bioreactor │     │  TFF Filtration │     │ Chromatography  │
│   (Upstream)    │ ──► │   (Upstream)    │ ──► │  (Downstream)   │ ──► │  (Downstream)   │
│                 │     │                 │     │                 │     │                 │
│   Cell Growth   │     │  Media Prep     │     │  Concentration  │     │  Purification   │
└─────────────────┘     └─────────────────┘     └─────────────────┘     └─────────────────┘
```

### Process Units

| Unit Code | Name | Type | Function |
|-----------|------|------|----------|
| `sub` | SUB250 | Bioreactor | Upstream cell culture |
| `sum` | SUM500 | Bioreactor | Media preparation |
| `tff` | TFF | Filtration | Tangential flow filtration |
| `chrom` | CHR01 | Chromatography | Protein purification |

---

## ISA-5.1 Tag Naming Convention

Enterprise C uses **ISA-5.1 instrument identification** standard:

### Tag Pattern: `{TYPE}{LOOP}-{SUFFIX}`

| Component | Meaning | Examples |
|-----------|---------|----------|
| Type | Instrument type (2-3 chars) | TIC, FIC, SIC, AIC |
| Loop | Loop number | 250, 501, 001 |
| Suffix | Value type | _PV, _SP, _ACTIVE, _MODE |

### Instrument Types

| Type | Full Name | Function | Count |
|------|-----------|----------|-------|
| SIC | Speed Indicator Controller | Agitator/pump speed control | 17 |
| HV | Hand Valve | Manual valve position | 5 |
| TIC | Temperature Indicator Controller | Temperature control | 4 |
| XV | On/Off Valve | Discrete valve control | 4 |
| AIC | Analytical Indicator Controller | pH, DO, conductivity | 3 |
| FCV | Flow Control Valve | Flow modulation | 3 |
| FIC | Flow Indicator Controller | Flow control | 3 |
| PI | Pressure Indicator | Pressure monitoring | 3 |
| TI | Temperature Indicator | Temperature monitoring | 3 |
| WI | Weight Indicator | Scale/load cell | 3 |
| FI | Flow Indicator | Flow monitoring | 2 |
| PIC | Pressure Indicator Controller | Pressure control | 1 |

### Value Suffixes

| Suffix | Meaning | Data Type |
|--------|---------|-----------|
| `_PV` | Process Value | Numeric (measured) |
| `_SP` | Setpoint | Numeric (target) |
| `_ACTIVE` | Controller active | Boolean (1/0) |
| `_MODE` | Operating mode | Text (AUTO/MAN) |
| `_STATUS` | Controller status | Text |
| `_START` | Start command | Boolean |
| `_CMD` | Command | Numeric |
| `_DESC` | Description | Text |
| `_EU` | Engineering Unit | Text |

---

## Tags by Unit

| Unit | Tags | Key Instruments |
|------|------|-----------------|
| sub (SUB250) | 32 | AIC (pH, DO), TIC, FIC, SIC (agitator) |
| sum (SUM500) | 21 | TIC, WI (scale), SIC (pumps) |
| tff (TFF) | 22 | FI, PI, TI, SIC (pumps) |
| chrom (CHR01) | 20 | AT (UV detector), FT (flow), PT (pressure), valves |

---

## Current Batches

| Batch ID | Recipe | Formula | Operator | Unit |
|----------|--------|---------|----------|------|
| 2025120842503 | PR_SUB_PROC | rBMN-42 | - | SUB |
| 2025112945204 | PROCESS | rBMN-42 | 351151 | - |
| 2025120842501 | PR_SUM_MEDIA | rBMN-42 | 357454 | SUM |
| 2025112942503 | PROC | rBMN-42 | - | - |

**Product**: rBMN-42 (recombinant protein)

---

## Topic Structure

### Flat Tag Topics (Primary)

```
Enterprise C/{unit}/{TAG}_{SUFFIX}

Examples:
Enterprise C/sub/TIC-250-001_PV_Celsius
Enterprise C/sub/TIC-250-001_SP_Celsius
Enterprise C/sub/FIC-250-001_PV_SLPM
Enterprise C/sub/AIC-250-003_PV_pH
Enterprise C/sum/WI501-PV_kg
Enterprise C/tff/PI5R8_psig
Enterprise C/chrom/CHR01_FT001
```

### Hierarchical Topics (Aveva)

```
Enterprise C/aveva/bioreactor/{UNIT}/controllers/{TAG}/{SUFFIX}

Examples:
Enterprise C/aveva/bioreactor/SUB250/controllers/TIC-250-001/PV
Enterprise C/aveva/bioreactor/SUB250/controllers/TIC-250-001/SP
Enterprise C/aveva/bioreactor/SUB250/controllers/SIC-250-002/STATUS
```

---

## Tag Values Summary

| Value Type | Count | Avg | Min | Max | Description |
|------------|-------|-----|-----|-----|-------------|
| PV | 702,412 | 59.8 | 0 | 414 | Process values |
| VALUE | 61,367 | 56.8 | -1 | 357,454 | Generic values |
| SP | 32,700 | 38.2 | 0 | 400 | Setpoints |
| START | 11,022 | 0.29 | 0 | 1 | Start commands |
| ACTIVE | 10,996 | 1.0 | 1 | 1 | Active indicators |
| CMD | 1,574 | 0.0 | 0 | 0 | Commands |

---

## Process Measurements

### Temperature Control

| Tag | Unit | Engineering Unit | Typical Range |
|-----|------|------------------|---------------|
| TIC-250-001 | SUB250 | Celsius | 30-40°C |
| TIC-250-002 | SUB250 | Celsius | 30-40°C |
| TIC501 | SUM500 | Celsius | 20-30°C |

### Flow Control

| Tag | Unit | Engineering Unit | Description |
|-----|------|------------------|-------------|
| FIC-250-001 | SUB250 | SLPM | Sparge air flow |
| FIC-250-002 | SUB250 | SLPM | Gas flow |
| FIC-250-003 | SUB250 | SLPM | Overlay gas |

### Analytical

| Tag | Unit | Measurement | Description |
|-----|------|-------------|-------------|
| AIC-250-001 | SUB250 | % DO | Dissolved oxygen |
| AIC-250-002 | SUB250 | - | Unknown |
| AIC-250-003 | SUB250 | pH | Culture pH |

### Speed Control

| Tag | Unit | Engineering Unit | Description |
|-----|------|------------------|-------------|
| SIC-250-002 | SUB250 | RPM | Agitator speed |
| SIC-250-003 | SUB250 | RPM | Pump speed |
| SIC501 | SUM500 | RPM | Mixer speed |

### Weight

| Tag | Unit | Engineering Unit | Description |
|-----|------|------------------|-------------|
| WI-250-001 | SUB250 | kg | Vessel weight |
| WI501 | SUM500 | kg | Media weight |

---

## Chromatography System

The CHR01 chromatography unit has specialized tags:

| Tag | Description | Unit |
|-----|-------------|------|
| CHR01_AT001 | UV detector 1 | AU |
| CHR01_AT003 | UV absorbance 280nm | AU |
| CHR01_FT001 | Volumetric flow | L/h |
| CHR01_PT002 | Column outlet pressure | bar |
| CHR01_PT003 | Column inlet pressure | bar |
| CHR01_V001 | Equilibration buffer valve | - |
| CHR01_V004 | Elution buffer A valve | - |

---

## Data Collection Statistics

| Metric | Value |
|--------|-------|
| Total raw messages | ~950K |
| Tags discovered | 99 |
| Tag values stored | ~850K |
| Active batches | 4 |
| Message rate | ~20 msgs/sec |

### Messages by Unit

| Unit | Messages | % of Total |
|------|----------|------------|
| Chromatography | 75,632 | 8% |
| TFF | 33,085 | 3% |
| SUB250 | 1,762 | 0.2% |
| SUM500 | 1,626 | 0.2% |
| Other (flat tags) | 844,916 | 89% |

---

## Key Findings

### 1. ISA-88/ISA-5.1 Compliance
- Standard instrument tag naming (TIC, FIC, SIC, etc.)
- Batch-centric data model
- Recipe/formula tracking per batch

### 2. Dual Topic Structure
- Flat tags: `Enterprise C/{unit}/{TAG}` (majority)
- Hierarchical: `Enterprise C/aveva/bioreactor/...` (structured)

### 3. Batch Process Nature
- Lower message rate (~20/sec vs ~150/sec for glass)
- Longer process cycles (hours/days vs seconds)
- Batch context for all measurements

### 4. Critical Process Parameters
- Temperature (bioreactor culture temp)
- pH (cell viability)
- Dissolved oxygen (aerobic culture)
- Agitation speed (mixing/oxygen transfer)

### 5. Downstream Processing
- TFF for concentration
- Chromatography for purification
- UV monitoring for product detection

---

## Simulator Replay Behavior

> **Note**: The MQTT data is **simulated replay data** from a historical biotech batch process. This affects data integrity and analysis.

### How the Simulator Works

The MQTT broker replays historical batch process data. Key characteristics:

- **Batch-centric**: Data represents ongoing batch processes (cell culture, purification)
- **Lower frequency**: ~20 msg/sec (vs ~150 msg/sec for glass) due to batch nature
- **Long cycles**: Biotech batches run hours/days, not seconds
- **Reset behavior**: When replay completes, batches restart with new IDs

### Impact on Data

| Entity | Behavior | Impact |
|--------|----------|--------|
| batches | New IDs on replay | Same batch_id may appear multiple times across resets |
| tags | Stable definitions | No duplicates expected |
| tag_values | Continuous stream | Time-series values aggregate cleanly |
| phases | Phase transitions | Sparse transitions, may miss rapid changes |

### Clean Analysis Queries

**Getting accurate tag values without replay overlap**:
```sql
-- Use time-bounded queries to avoid replay overlap
SELECT
    t.tag_name,
    tv.value_type,
    AVG(tv.value_numeric) as avg_value,
    COUNT(*) as samples
FROM tag_values tv
JOIN tags t ON tv.tag_id = t.id
WHERE tv.timestamp > datetime('now', '-1 hour')
  AND tv.value_numeric IS NOT NULL
GROUP BY t.tag_name, tv.value_type;
```

**Filtering by specific batch**:
```sql
-- Get data for a specific batch
SELECT b.batch_id, b.recipe_name, tv.*
FROM batches b
JOIN tag_values tv ON tv.timestamp BETWEEN b.start_time AND COALESCE(b.end_time, 'now')
WHERE b.batch_id = '2025120842503';
```

---

## Analysis Scripts

### `analyze_data.py`

Comprehensive repeatable analysis:

```bash
python analyze_data.py -e C              # Full analysis for Enterprise C
python analyze_data.py -e C --section tags  # Tag analysis only
```

### `explore.py`

General data exploration:

```bash
python explore.py -e C              # Overview of Enterprise C data
python explore.py -e C --tags       # Tag hierarchy
python explore.py -e C --batches    # Current batch status
```

---

## Known Issues / Bugs

| Issue | Description | Status |
|-------|-------------|--------|
| Phase gaps | Phase transitions not fully captured | Open |
| Sparse operator messages | Operator prompts/acknowledgments infrequent | Expected |
| No yield calculation | No titer or yield metrics calculated | By design |
| Dual topic structure | Data split between flat and hierarchical topics | By design |
| AIC-250-002 unknown | Unknown measurement type | Needs investigation |

---

## Data Quality Notes

### Simulator Characteristics
- Simulated biotech batch process
- Single product (rBMN-42)
- 4 concurrent batches

### Known Gaps
- Phase transitions not fully captured
- Operator messages sparse
- No yield/titer calculations

---

## SQL Examples

```sql
-- Tags by unit
SELECT u.code as unit, COUNT(*) as tags
FROM tags t
JOIN units u ON t.unit_id = u.id
GROUP BY u.code;

-- Current batches
SELECT batch_id, recipe_name, formula_name, operator_id
FROM batches;

-- Tag values for a specific tag
SELECT tv.timestamp, t.tag_name, tv.value_type, tv.value_numeric
FROM tag_values tv
JOIN tags t ON tv.tag_id = t.id
WHERE t.tag_name LIKE 'TIC-250%'
ORDER BY tv.timestamp DESC LIMIT 100;

-- PV vs SP comparison
SELECT t.tag_name,
       AVG(CASE WHEN tv.value_type = 'PV' THEN tv.value_numeric END) as avg_pv,
       AVG(CASE WHEN tv.value_type = 'SP' THEN tv.value_numeric END) as avg_sp
FROM tag_values tv
JOIN tags t ON tv.tag_id = t.id
WHERE tv.value_numeric IS NOT NULL
GROUP BY t.tag_name
HAVING avg_pv IS NOT NULL AND avg_sp IS NOT NULL;

-- Tag types distribution
SELECT tag_type, COUNT(*) as count
FROM tags
WHERE tag_type IS NOT NULL
GROUP BY tag_type
ORDER BY count DESC;

-- Value types distribution
SELECT value_type, COUNT(*) as count
FROM tag_values
GROUP BY value_type
ORDER BY count DESC;

-- Temperature control analysis (PV vs SP deviation)
SELECT
    t.tag_name,
    COUNT(*) as samples,
    AVG(tv.value_numeric) as avg_value,
    MIN(tv.value_numeric) as min_value,
    MAX(tv.value_numeric) as max_value,
    tv.value_type
FROM tag_values tv
JOIN tags t ON tv.tag_id = t.id
WHERE t.tag_type = 'TIC'
GROUP BY t.tag_name, tv.value_type
ORDER BY t.tag_name, tv.value_type;

-- Bioreactor critical parameters (SUB250)
SELECT
    t.tag_name,
    t.tag_type,
    tv.value_type,
    AVG(tv.value_numeric) as avg_value,
    t.engineering_unit
FROM tag_values tv
JOIN tags t ON tv.tag_id = t.id
JOIN units u ON t.unit_id = u.id
WHERE u.code = 'sub'
  AND tv.value_type = 'PV'
  AND tv.value_numeric IS NOT NULL
GROUP BY t.tag_name, t.tag_type, tv.value_type, t.engineering_unit;

-- Chromatography UV trace
SELECT
    tv.timestamp,
    t.tag_name,
    tv.value_numeric as absorbance
FROM tag_values tv
JOIN tags t ON tv.tag_id = t.id
WHERE t.tag_name LIKE 'CHR01_AT%'
ORDER BY tv.timestamp DESC LIMIT 200;

-- Time-series trend for specific tag (hourly averages)
SELECT
    strftime('%Y-%m-%d %H:00', tv.timestamp) as hour,
    t.tag_name,
    AVG(tv.value_numeric) as avg_value,
    COUNT(*) as samples
FROM tag_values tv
JOIN tags t ON tv.tag_id = t.id
WHERE t.tag_name = 'TIC-250-001'
  AND tv.value_type = 'PV'
GROUP BY hour, t.tag_name
ORDER BY hour;

-- Join tags, values, and batches (active batch context)
SELECT
    b.batch_id,
    b.recipe_name,
    t.tag_name,
    tv.value_type,
    AVG(tv.value_numeric) as avg_value
FROM batches b
CROSS JOIN tags t
JOIN tag_values tv ON tv.tag_id = t.id
WHERE t.tag_type IN ('TIC', 'AIC', 'SIC')
  AND tv.value_type = 'PV'
GROUP BY b.batch_id, b.recipe_name, t.tag_name, tv.value_type;

-- Controller status (ACTIVE flags)
SELECT
    t.tag_name,
    tv.value_type,
    SUM(CASE WHEN tv.value_numeric = 1 THEN 1 ELSE 0 END) as active_count,
    SUM(CASE WHEN tv.value_numeric = 0 THEN 1 ELSE 0 END) as inactive_count
FROM tag_values tv
JOIN tags t ON tv.tag_id = t.id
WHERE tv.value_type = 'ACTIVE'
GROUP BY t.tag_name, tv.value_type;

-- Filtration (TFF) pressure monitoring
SELECT
    t.tag_name,
    AVG(tv.value_numeric) as avg_pressure,
    MIN(tv.value_numeric) as min_pressure,
    MAX(tv.value_numeric) as max_pressure
FROM tag_values tv
JOIN tags t ON tv.tag_id = t.id
JOIN units u ON t.unit_id = u.id
WHERE u.code = 'tff'
  AND t.tag_type = 'PI'
GROUP BY t.tag_name;
```

---

## Glossary

| Term | Definition |
|------|------------|
| ISA-5.1 | Instrumentation Symbols and Identification standard |
| ISA-88 | Batch Control standard |
| PV | Process Value (measured/actual) |
| SP | Setpoint (target/desired) |
| TIC | Temperature Indicator Controller |
| FIC | Flow Indicator Controller |
| SIC | Speed Indicator Controller |
| AIC | Analytical Indicator Controller |
| DO | Dissolved Oxygen |
| SLPM | Standard Liters Per Minute |
| TFF | Tangential Flow Filtration |
| AU | Absorbance Units (UV spectroscopy) |
