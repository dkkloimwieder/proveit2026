"""Exploration queries for Enterprise B data."""

from schema import get_connection


def show_overview():
    """Show database overview statistics."""
    conn = get_connection()
    cursor = conn.cursor()

    print("=" * 60)
    print("DATABASE OVERVIEW")
    print("=" * 60)

    # Basic counts
    tables = ["messages_raw", "topics", "assets", "metrics", "process_data"]
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        print(f"{table}: {cursor.fetchone()[0]} rows")

    conn.close()


def show_topic_tree():
    """Show topic hierarchy."""
    conn = get_connection()
    cursor = conn.cursor()

    print("\n" + "=" * 60)
    print("TOPIC HIERARCHY")
    print("=" * 60)

    cursor.execute("""
        SELECT site, area, COUNT(*) as topic_count, SUM(message_count) as msg_count
        FROM topics
        WHERE site IS NOT NULL
        GROUP BY site, area
        ORDER BY site, msg_count DESC
    """)

    current_site = None
    for row in cursor.fetchall():
        if row["site"] != current_site:
            current_site = row["site"]
            print(f"\n{current_site}/")
        print(f"  {row['area']}: {row['topic_count']} topics, {row['msg_count']} messages")

    conn.close()


def show_assets():
    """Show discovered assets."""
    conn = get_connection()
    cursor = conn.cursor()

    print("\n" + "=" * 60)
    print("DISCOVERED ASSETS")
    print("=" * 60)

    cursor.execute("""
        SELECT site, area, line, equipment, asset_id, asset_name, asset_path
        FROM assets
        WHERE asset_id IS NOT NULL
        ORDER BY site, area, line, equipment
    """)

    current_site = None
    for row in cursor.fetchall():
        if row["site"] != current_site:
            current_site = row["site"]
            print(f"\n{current_site}/")
        path = f"{row['area']}/{row['line']}/{row['equipment']}"
        print(f"  [{row['asset_id']}] {path}")
        if row["asset_name"]:
            print(f"        Name: {row['asset_name']}")

    conn.close()


def show_latest_metrics():
    """Show most recent OEE metrics by equipment."""
    conn = get_connection()
    cursor = conn.cursor()

    print("\n" + "=" * 60)
    print("LATEST OEE METRICS")
    print("=" * 60)

    cursor.execute("""
        SELECT site, area, line, equipment,
               availability, performance, quality, oee,
               recorded_at
        FROM metrics
        WHERE oee IS NOT NULL
        ORDER BY recorded_at DESC
        LIMIT 20
    """)

    print(f"\n{'Equipment':<50} {'Avail':>7} {'Perf':>7} {'Qual':>7} {'OEE':>7}")
    print("-" * 80)

    for row in cursor.fetchall():
        equip = f"{row['site']}/{row['area']}/{row['line']}/{row['equipment']}"
        if len(equip) > 48:
            equip = equip[:45] + "..."
        avail = f"{row['availability']:.1f}" if row["availability"] else "-"
        perf = f"{row['performance']:.1f}" if row["performance"] else "-"
        qual = f"{row['quality']:.1f}" if row["quality"] else "-"
        oee = f"{row['oee']:.1f}" if row["oee"] else "-"
        print(f"{equip:<50} {avail:>7} {perf:>7} {qual:>7} {oee:>7}")

    conn.close()


def show_metric_inputs():
    """Show metric input values."""
    conn = get_connection()
    cursor = conn.cursor()

    print("\n" + "=" * 60)
    print("METRIC INPUT VALUES (Latest)")
    print("=" * 60)

    cursor.execute("""
        SELECT site, equipment,
               count_infeed, count_outfeed, count_defect,
               rate_actual, rate_standard,
               time_running, time_idle, time_down_planned, time_down_unplanned
        FROM metrics
        WHERE count_infeed IS NOT NULL OR time_running IS NOT NULL
        ORDER BY recorded_at DESC
        LIMIT 15
    """)

    for row in cursor.fetchall():
        print(f"\n{row['site']}/{row['equipment']}:")
        if row["count_infeed"] is not None:
            print(f"  Counts: infeed={row['count_infeed']}, outfeed={row['count_outfeed']}, defect={row['count_defect']}")
        if row["rate_actual"] is not None:
            print(f"  Rates: actual={row['rate_actual']:.1f}, standard={row['rate_standard']:.1f}")
        if row["time_running"] is not None:
            print(f"  Time: running={row['time_running']:.0f}, idle={row['time_idle']:.0f}, "
                  f"down_planned={row['time_down_planned']:.0f}, down_unplanned={row['time_down_unplanned']:.0f}")

    conn.close()


def show_process_states():
    """Show process state data."""
    conn = get_connection()
    cursor = conn.cursor()

    print("\n" + "=" * 60)
    print("PROCESS STATES (Recent)")
    print("=" * 60)

    cursor.execute("""
        SELECT site, area, line, equipment, state_name, item_name, recorded_at
        FROM process_data
        WHERE state_name IS NOT NULL OR item_name IS NOT NULL
        ORDER BY recorded_at DESC
        LIMIT 20
    """)

    for row in cursor.fetchall():
        equip = f"{row['site']}/{row['equipment']}" if row["equipment"] else row["site"]
        if row["state_name"]:
            print(f"{equip}: State = {row['state_name']}")
        if row["item_name"]:
            print(f"{equip}: Item = {row['item_name']}")

    conn.close()


def query_raw(sql: str):
    """Run a custom SQL query on raw messages."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()
    conn.close()
    return rows


def main():
    """Run all exploration queries."""
    show_overview()
    show_topic_tree()
    show_assets()
    show_latest_metrics()
    show_metric_inputs()
    show_process_states()


if __name__ == "__main__":
    main()
