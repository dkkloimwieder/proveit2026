"""ProveIt - Enterprise B MQTT Data Explorer.

Usage:
    python main.py collect    - Collect live data from MQTT broker
    python main.py explore    - Explore collected data in database
    python main.py discover   - Discover MQTT topic structure
    python main.py init       - Initialize/reset database
"""

import sys


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        return

    command = sys.argv[1].lower()

    if command == "collect":
        from data_collector import main as collect_main
        collect_main()

    elif command == "explore":
        from explore import main as explore_main
        explore_main()

    elif command == "discover":
        from discover_and_save import main as discover_main
        discover_main()

    elif command == "init":
        from schema import init_db
        init_db()

    else:
        print(f"Unknown command: {command}")
        print(__doc__)


if __name__ == "__main__":
    main()
