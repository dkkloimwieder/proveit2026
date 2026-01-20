# Enterprise A Data Analysis (Glass Manufacturing)

This document describes the glass manufacturing data structure discovered through MQTT exploration.

## Quick Start

```bash
# Collect Enterprise A data
python data_collector.py -e A --raw

# Query the database
sqlite3 proveit_a.db
```

## Data Storage

All data is stored in `proveit_a.db` (SQLite). See `schemas/enterprise_a.py` for table definitions.

### Tables

| Table | Type | Description |
|-------|------|-------------|
| `sites` | Reference | Manufacturing sites (Dallas) |
| `areas` | Reference | Production areas (BatchHouse, HotEnd, ColdEnd) |
| `equipment` | Reference | Equipment hierarchy |
| `equipment_states` | Time-series | State change events |
| `process_data` | Time-series | 10-second bucketed process measurements |
| `sensor_readings` | Time-series | Raw edge sensor data |
| `utility_readings` | Time-series | Utilities monitoring data |
| `oee_metrics` | Time-series | OEE metrics by line |
| `messages_raw` | Archive | Raw MQTT payloads |

---

## Manufacturing Process Flow

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   BATCH HOUSE   │     │    HOT END      │     │    COLD END     │
│                 │ ──► │                 │ ──► │                 │
│  Raw Materials  │     │  Glass Forming  │     │   Inspection    │
│                 │     │                 │     │   & Packaging   │
└─────────────────┘     └─────────────────┘     └─────────────────┘
```

### Process Stages

| Stage | Area | Equipment | Function |
|-------|------|-----------|----------|
| 1 | BatchHouse | Silos (4) | Raw material storage |
| 1 | BatchHouse | BatchMixer | Material blending |
| 1 | BatchHouse | BatchCharger | Furnace feeding |
| 2 | HotEnd | Furnace | Glass melting (~2400°C) |
| 2 | HotEnd | Forehearth | Glass conditioning |
| 2 | HotEnd | ISMachine | Glass forming (bottles) |
| 3 | ColdEnd | Lehr | Annealing (controlled cooling) |
| 3 | ColdEnd | Inspector | Quality inspection |
| 3 | ColdEnd | Palletizer | Packaging |

---

## Site & Equipment Configuration

### Site Overview

| Site | Lines | Areas | Total Equipment |
|------|-------|-------|-----------------|
| Dallas | 4 | 3 | 46 |

### Equipment by Area

#### BatchHouse (24 equipment)

| Equipment | Count | Function | Key Measurements |
|-----------|-------|----------|------------------|
| Silo01-04 | 4 each | Raw material storage | Level %, Material type |
| BatchMixer | 4 | Material blending | Batch weight (kg) |
| BatchCharger | 4 | Furnace feeding | Feed rate |

#### HotEnd (12 equipment)

| Equipment | Count | Function | Key Measurements |
|-----------|-------|----------|------------------|
| Furnace | 4 | Glass melting | Temperature (2050-2705°C) |
| Forehearth | 4 | Glass conditioning | Temperature zones |
| ISMachine | 4 | Bottle forming | Gob weight, mold temp |

#### ColdEnd (10 equipment)

| Equipment | Count | Function | Key Measurements |
|-----------|-------|----------|------------------|
| Lehr | 4 | Annealing | Temperature, conveyor speed |
| Inspector | 3 | Quality inspection | Pass/reject counts |
| Palletizer | 3 | Packaging | - |

---

## Topic Structure

### Production Topics

```
Enterprise A/Dallas/Line {1-4}/{Area}/{Equipment}/{Category}/{DataType}

Examples:
Enterprise A/Dallas/Line 1/BatchHouse/Silo01/Status/Level
Enterprise A/Dallas/Line 1/BatchHouse/Silo01/Status/Material
Enterprise A/Dallas/Line 1/BatchHouse/BatchMixer/Status/BatchWeight
Enterprise A/Dallas/Line 1/HotEnd/Furnace/Status/Temperature
Enterprise A/Dallas/Line 1/HotEnd/ISMachine/State/StateCurrent
Enterprise A/Dallas/Line 1/ColdEnd/Inspector/edge/defect_count
```

### Category Types

| Category | Description | Data Types |
|----------|-------------|------------|
| `Status` | Process values | Level, Temperature, BatchWeight, FeedRate |
| `State` | Equipment state | StateCurrent (code), StateReason |
| `edge` | Raw sensors | Various edge device readings |
| `OEE` | Performance metrics | Availability, Performance, Quality |

### Utilities Topics

```
Enterprise A/opto22/Utilities/{Category}/{Equipment}/{Measurement}/{Type}

Examples:
Enterprise A/opto22/Utilities/Compressors/Compressor 1/Discharge pressure/Value
Enterprise A/opto22/Utilities/Air Dryers/Airtek SC400 Air Dryer/Dewpoint/Value
Enterprise A/opto22/Utilities/Electrical Panels/Panel L23/Phase A Current/Value
```

---

## Process Data Summary

### Silo Levels

| Metric | Count | Avg | Min | Max |
|--------|-------|-----|-----|-----|
| Level % | 15,734 | 67.6% | 45% | 82% |

Silos contain raw materials: Sand, Soda Ash, Limestone, Cullet (recycled glass).

### Furnace Temperatures

| Metric | Count | Avg | Min | Max |
|--------|-------|-----|-----|-----|
| Temperature (°C) | 7,860 | 2,377 | 2,050 | 2,705 |

Glass melting requires temperatures of 1,400-1,600°C; the furnace operates hotter to maintain throughput.

### Batch Mixing

| Metric | Count | Value |
|--------|-------|-------|
| Batch Weight (kg) | 3,932 | 2,500 (constant) |
| Feed Rate | 3,932 | 150 (constant) |

---

## Sensor Readings (Edge Data)

### Top Sensors by Volume

| Sensor | Readings | Avg Value | Description |
|--------|----------|-----------|-------------|
| raw_level_sensor | 15,750 | 27,693 | Silo level (raw ADC) |
| level_transmitter_ma | 15,750 | 13.8 mA | Silo level (4-20mA) |
| raw_motor_status | 7,874 | 1.0 | Motor running indicator |
| zone1_thermocouple_mv | 3,935 | 8,320 mV | Furnace zone 1 temp |
| zone2_thermocouple_mv | 3,935 | 5,950 mV | Furnace zone 2 temp |
| zone3_thermocouple_mv | 3,935 | 3,175 mV | Furnace zone 3 temp |

---

## OEE Metrics

| Metric | Value | Notes |
|--------|-------|-------|
| Availability | 98.5% | Very high uptime |
| Performance | 101.7% | Exceeding standard rate |
| Quality | 97.9% | Low defect rate |

OEE metrics are captured per line at 10-second intervals.

---

## Utilities Monitoring

### Equipment Monitored

| Category | Equipment | Readings |
|----------|-----------|----------|
| Compressors | Compressor 1, Compressor 2 | 9,434 |
| Air Dryers | Airtek SC400 Air Dryer | 4,717 |
| Electrical Panels | Panel H23, L23, L24 | 14,151 |
| Building Power | Service Entrance | 1,423 |
| Environmental | Manufacturing Utility Room 1 | 4,717 |

### Key Utility Metrics

- Compressor discharge pressure
- Air dryer dewpoint temperature
- Panel current (3-phase)
- Ambient temperature/humidity

---

## State Machine

Equipment states are tracked via `State/StateCurrent` topics:

| State Code | Meaning |
|------------|---------|
| 3 | Running (observed) |

State changes trigger entries in `equipment_states` table with previous state for transition analysis.

---

## Data Collection Statistics

| Metric | Value |
|--------|-------|
| Total raw messages | ~5.8M |
| Production messages | ~450K (Line 1-4) |
| Utilities messages | ~5.4M |
| Unique topics | 930 |
| Message rate | ~150 msgs/sec |

---

## Key Findings

### 1. Hierarchical Equipment Structure
- Single site (Dallas) with 4 production lines
- 3 main areas: BatchHouse → HotEnd → ColdEnd
- Clear process flow from raw materials to packaged bottles

### 2. Dual Topic Sources
- **Production data**: `Enterprise A/Dallas/Line X/...`
- **Utilities data**: `Enterprise A/opto22/Utilities/...`
- Utilities generate ~12x more messages than production

### 3. High OEE Performance
- 98.5% availability indicates minimal downtime
- 101.7% performance suggests operating above standard rate
- 97.9% quality shows low defect rates

### 4. Constant Batch Parameters
- Batch weight fixed at 2,500 kg
- Feed rate fixed at 150 units
- Suggests batch recipe consistency

### 5. Temperature Monitoring
- Furnace temperatures 2,050-2,705°C
- Multiple thermocouple zones for precise control
- Critical for glass quality

---

## Data Quality Notes

### Simulator Characteristics
- Data is from simulated glass plant
- Steady-state operation (no startup/shutdown sequences observed)
- Limited state transitions during collection period

### Known Gaps
- No product/SKU tracking (unlike Enterprise B)
- No work order system
- Limited defect categorization

---

## Simulator Replay Behavior

> **Note**: The MQTT data is **simulated replay data** from a historical dataset. This affects data integrity and analysis.

### How the Simulator Works

The MQTT broker replays historical glass manufacturing data. Key characteristics:

- **Steady-state operation**: Data represents continuous running, no startup/shutdown sequences
- **Reset behavior**: When replay completes, data loops back to the start
- **Stable IDs**: Equipment and area IDs remain consistent across replay cycles

### Impact on Data

| Entity | Behavior | Impact |
|--------|----------|--------|
| equipment | Stable IDs | No duplicates expected |
| process_data | Continuous stream | 10-second buckets aggregate cleanly |
| equipment_states | State transitions logged | Minimal state changes during observation |
| oee_metrics | Per-line buckets | Overwrites per bucket, clean data |

### Clean Analysis Queries

**Getting accurate production stats**:
```sql
-- Use time-bounded queries to avoid replay overlap
SELECT line,
       AVG(availability) as avg_avail,
       AVG(performance) as avg_perf,
       AVG(quality) as avg_qual
FROM oee_metrics
WHERE bucket > datetime('now', '-1 hour')
GROUP BY line;
```

---

## Analysis Scripts

### `analyze_data.py`

Comprehensive repeatable analysis:

```bash
python analyze_data.py -e A              # Full analysis for Enterprise A
python analyze_data.py -e A --section oee  # OEE metrics only
```

### `explore.py`

General data exploration:

```bash
python explore.py -e A              # Overview of Enterprise A data
python explore.py -e A --assets     # Equipment hierarchy
python explore.py -e A --metrics    # Process metrics
```

---

## Known Issues / Bugs

| Issue | Description | Status |
|-------|-------------|--------|
| Limited state variety | Only state code 3 (Running) observed during collection | Expected (simulator) |
| Fixed batch params | BatchWeight and FeedRate constant (2500 kg, 150) | Expected (consistent recipe) |
| No product tracking | Unlike Enterprise B, no SKU/work order system | By design |
| Utilities volume | Utilities data is ~12x production data volume | Expected |

---

## SQL Examples

```sql
-- Equipment by area
SELECT a.name as area, e.name as equipment, COUNT(*) as instances
FROM equipment e
JOIN areas a ON e.area_id = a.id
GROUP BY a.name, e.name;

-- Silo levels over time
SELECT bucket, equipment_id, level_pct, material
FROM process_data
WHERE level_pct IS NOT NULL
ORDER BY bucket DESC LIMIT 100;

-- Furnace temperatures
SELECT bucket, temperature
FROM process_data
WHERE temperature IS NOT NULL
ORDER BY bucket DESC LIMIT 100;

-- State changes
SELECT es.timestamp, e.name, es.state_code, es.state_reason
FROM equipment_states es
JOIN equipment e ON es.equipment_id = e.id
ORDER BY es.timestamp DESC LIMIT 50;

-- OEE by line
SELECT bucket, line, availability, performance, quality, oee
FROM oee_metrics
ORDER BY bucket DESC LIMIT 50;

-- Average furnace temperature by line
SELECT
    e.line,
    COUNT(*) as samples,
    AVG(pd.temperature) as avg_temp,
    MIN(pd.temperature) as min_temp,
    MAX(pd.temperature) as max_temp
FROM process_data pd
JOIN equipment e ON pd.equipment_id = e.id
WHERE pd.temperature IS NOT NULL
GROUP BY e.line;

-- Silo material inventory
SELECT
    e.name as silo,
    pd.material,
    AVG(pd.level_pct) as avg_level,
    MIN(pd.level_pct) as min_level,
    MAX(pd.level_pct) as max_level
FROM process_data pd
JOIN equipment e ON pd.equipment_id = e.id
WHERE pd.level_pct IS NOT NULL
GROUP BY e.name, pd.material;

-- OEE trend over time
SELECT
    strftime('%Y-%m-%d %H:00', bucket) as hour,
    AVG(availability) as avail,
    AVG(performance) as perf,
    AVG(quality) as qual,
    AVG(oee) as oee
FROM oee_metrics
GROUP BY hour
ORDER BY hour;

-- Utility readings summary
SELECT
    category,
    equipment,
    measurement,
    COUNT(*) as readings,
    AVG(value) as avg_value
FROM utility_readings
GROUP BY category, equipment, measurement
ORDER BY readings DESC
LIMIT 20;

-- Inspector pass/reject rates (if available)
SELECT
    e.line,
    SUM(sr.value) as total_count
FROM sensor_readings sr
JOIN equipment e ON sr.equipment_id = e.id
WHERE sr.sensor_name LIKE '%count%'
GROUP BY e.line;
```
