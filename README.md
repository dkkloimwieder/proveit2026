# ProveIt - Manufacturing Data Collection

ProveIt collects real-time manufacturing data from MQTT brokers and stores it in SQLite databases for analysis. It supports three different manufacturing enterprises with distinct process types.

## Enterprises

| Enterprise | Industry | Process Type | Database |
|------------|----------|--------------|----------|
| **A** | Glass Manufacturing | Continuous | `proveit_a.db` |
| **B** | Beverage Production | Discrete/Batch | `proveit_b.db` |
| **C** | Biotech | Batch | `proveit_c.db` |

### Enterprise A - Glass Manufacturing (Dallas)
- 4 production lines: BatchHouse → HotEnd → ColdEnd
- Equipment: Silos, BatchMixer, Furnace, ISMachine, Lehr, Inspector, Palletizer
- Key metrics: Temperature (furnace ~2400°C), silo levels, OEE

### Enterprise B - Beverage Production
- 3 sites with multiple lines
- Process flow: Mix (kg) → Fill (bottles) → Pack (cases) → Palletize
- Work order tracking with cross-site flow
- Products: Cola, Orange in various pack sizes (4, 6, 12, 16, 20, 24)

### Enterprise C - Biotech Batch Processing
- ISA-88/ISA-5.1 compliant tag naming
- Units: SUB250 (bioreactor), SUM500 (media prep), TFF (filtration), CHR01 (chromatography)
- Product: rBMN-42 recombinant protein
- Critical parameters: Temperature, pH, dissolved oxygen, agitation

## Quick Start

### Prerequisites
- Python 3.13+
- [uv](https://github.com/astral-sh/uv) package manager
- Access to MQTT broker (configured via `.env`)

### Installation

```bash
# Clone and enter directory
cd proveit2026

# Install dependencies
uv sync
```

### Environment Setup

Create a `.env` file with your MQTT broker settings:

```bash
MQTT_BROKER=your-broker-host
MQTT_PORT=1883
# MQTT_USERNAME=optional
# MQTT_PASSWORD=optional
```

### Running the Collector

```bash
# Collect from all enterprises (recommended)
uv run python data_collector.py --all

# Collect from a specific enterprise
uv run python data_collector.py -e A    # Glass manufacturing
uv run python data_collector.py -e B    # Beverage production
uv run python data_collector.py -e C    # Biotech

# Include raw MQTT messages (for debugging)
uv run python data_collector.py -e B --raw

# Reset database before collecting
uv run python data_collector.py -e B --reset
```

Press `Ctrl+C` to stop collection gracefully.

### Running Tests

```bash
uv run pytest                     # Run all tests
uv run pytest tests/ -v           # Verbose output
uv run pytest tests/test_enterprise_a.py  # Specific test file
```

## Architecture

```
┌─────────────────┐     ┌─────────────────────┐     ┌─────────────────┐
│   MQTT Broker   │     │   data_collector.py │     │  SQLite DBs     │
│                 │ ──► │                     │ ──► │                 │
│  Enterprise A   │     │  - Parse topics     │     │  proveit_a.db   │
│  Enterprise B   │     │  - Extract data     │     │  proveit_b.db   │
│  Enterprise C   │     │  - Bucket metrics   │     │  proveit_c.db   │
└─────────────────┘     └─────────────────────┘     └─────────────────┘
```

### Topic Structure by Enterprise

**Enterprise A** (Glass):
```
Enterprise A/Dallas/Line {1-4}/{Area}/{Equipment}/{Category}/{DataType}
Enterprise A/opto22/Utilities/{Category}/{Equipment}/{Measurement}/Value
```

**Enterprise B** (Beverage):
```
Enterprise B/{Site}/{area}/{line}/{equipment}/{category}/{field}
```

**Enterprise C** (Biotech):
```
Enterprise C/{unit}/{TAG}_{SUFFIX}
Enterprise C/aveva/bioreactor/{UNIT}/controllers/{TAG}/{SUFFIX}
```

## Database Tables

### Enterprise A (`proveit_a.db`)
| Table | Description |
|-------|-------------|
| `sites` | Manufacturing sites |
| `areas` | Production areas (BatchHouse, HotEnd, ColdEnd) |
| `equipment` | Equipment hierarchy |
| `equipment_states` | State change events |
| `process_data` | 10-second bucketed measurements |
| `oee_metrics` | OEE metrics by line |
| `messages_raw` | Raw MQTT payloads |

### Enterprise B (`proveit_b.db`)
| Table | Description |
|-------|-------------|
| `products` | Product/item master data |
| `lots` | Batch/lot numbers |
| `work_orders` | Work order definitions |
| `states` | State name lookup |
| `assets` | Equipment hierarchy |
| `events` | State/WO/lot transitions |
| `metrics_10s` | 10-second OEE buckets |
| `messages_raw` | Raw MQTT payloads |

### Enterprise C (`proveit_c.db`)
| Table | Description |
|-------|-------------|
| `units` | Process units (bioreactors, filtration, etc.) |
| `tags` | ISA-5.1 tag definitions |
| `tag_values` | Process values (PV, SP, STATUS) |
| `batches` | Batch records with recipe/formula |
| `phases` | Batch phase tracking |
| `messages_raw` | Raw MQTT payloads |

## Documentation

| Document | Description |
|----------|-------------|
| [ANALYSIS.md](ANALYSIS.md) | Enterprise B detailed analysis |
| [ANALYSIS_A.md](ANALYSIS_A.md) | Enterprise A glass manufacturing analysis |
| [ANALYSIS_C.md](ANALYSIS_C.md) | Enterprise C biotech analysis |
| [MASTER_DATA_REPORT.md](MASTER_DATA_REPORT.md) | Enterprise B product/BOM data |
| [PROCESS_FLOW.md](PROCESS_FLOW.md) | Manufacturing process flows |

## Project Structure

```
proveit2026/
├── data_collector.py    # Main MQTT collection logic
├── mqtt_client.py       # MQTT connection wrapper
├── schema.py            # Database schemas
├── parsers/             # Enterprise-specific topic parsers
│   ├── enterprise_a.py
│   ├── enterprise_b.py
│   └── enterprise_c.py
├── collectors/          # Enterprise-specific collectors
├── schemas/             # Enterprise-specific schemas
├── tests/               # Test suite
│   ├── test_enterprise_a.py
│   └── test_enterprise_c.py
├── explore.py           # Data exploration utility
├── analyze_data.py      # Data analysis scripts
└── *.db                 # SQLite databases (generated)
```

## Troubleshooting

### MQTT Connection Issues
- Verify broker host/port in `.env`
- Check network connectivity: `ping $MQTT_BROKER`
- Ensure credentials are correct (if required)

### No Data Appearing
- Confirm the enterprise simulator is running
- Check subscription topic: collector logs show subscribed topics
- Try `--raw` flag to see if raw messages arrive

### Database Locked Errors
- Only run one collector per enterprise at a time
- Use WAL mode (enabled by default) for better concurrency

## Data Quality Notes

The MQTT data comes from manufacturing simulators that replay historical datasets:
- Data timestamps may show November 2025 (original recording time)
- Progress resets when replay completes (~11.9% per 18 hours)
- Same work order numbers may get new IDs on replay cycles

See individual ANALYSIS documents for enterprise-specific data quality notes.
