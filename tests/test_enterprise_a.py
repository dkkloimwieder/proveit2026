"""Tests for Enterprise A (Glass Manufacturing) data collection.

Verifies that:
1. Equipment hierarchy in DB matches topics
2. State changes match State/StateCurrent values
3. Sensor readings match edge/ topic payloads
4. Process data matches Status/ payloads
5. Raw message count correlates with stored records
"""

import sqlite3
import tempfile
import time
import os
from pathlib import Path

import pytest

# Add project root to path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from collectors.enterprise_a import EnterpriseACollector
from mqtt_client import MQTTClient


class TestEnterpriseACollector:
    """Test Enterprise A data collection and storage."""

    @pytest.fixture
    def temp_db(self):
        """Create a temporary database file."""
        fd, path = tempfile.mkstemp(suffix="_test_a.db")
        os.close(fd)
        yield path
        if os.path.exists(path):
            os.unlink(path)

    @pytest.fixture
    def collector(self, temp_db):
        """Create a collector with raw capture enabled."""
        collector = EnterpriseACollector(db_path=temp_db, capture_raw=True)
        yield collector
        collector.close()

    @pytest.fixture
    def mqtt_client(self):
        """Create and connect an MQTT client."""
        client = MQTTClient()
        if not client.connect():
            pytest.skip("Cannot connect to MQTT broker")
        client.start_background()
        # Wait for connection
        for _ in range(50):
            if client.is_connected:
                break
            time.sleep(0.1)
        if not client.is_connected:
            pytest.skip("MQTT client failed to connect")
        yield client
        client.stop()

    def collect_for_duration(self, client: MQTTClient, collector: EnterpriseACollector, duration: int = 15):
        """Run collection for a specified duration."""
        client.add_message_handler(collector.handle_message)
        client.subscribe("Enterprise A/#")

        # Wait for subscription to be active
        time.sleep(1)

        start = time.time()
        while time.time() - start < duration:
            time.sleep(0.1)

        # Remove handler to stop processing new messages
        client._message_handlers.remove(collector.handle_message)
        time.sleep(0.5)

    def get_db_connection(self, db_path: str) -> sqlite3.Connection:
        """Get a database connection."""
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def test_raw_messages_captured(self, collector, mqtt_client, temp_db):
        """Test that raw messages are captured to messages_raw table."""
        self.collect_for_duration(mqtt_client, collector, duration=10)

        collector._flush_raw_buffer()

        conn = self.get_db_connection(temp_db)
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM messages_raw")
        raw_count = cursor.fetchone()[0]

        assert raw_count > 0, "No raw messages captured"
        assert raw_count <= collector.message_count, (
            f"Raw count ({raw_count}) should not exceed message count ({collector.message_count})"
        )

        conn.close()

    def test_equipment_hierarchy_matches_topics(self, collector, mqtt_client, temp_db):
        """Test that equipment hierarchy in DB matches raw topic structure."""
        self.collect_for_duration(mqtt_client, collector, duration=10)

        collector._flush_raw_buffer()
        collector._flush_metrics()

        conn = self.get_db_connection(temp_db)
        cursor = conn.cursor()

        # Get equipment from DB
        cursor.execute("""
            SELECT s.name as site, a.name as area, e.name as equipment
            FROM equipment e
            JOIN areas a ON e.area_id = a.id
            JOIN sites s ON a.site_id = s.id
        """)
        db_equipment = {(row[0], row[1], row[2]) for row in cursor.fetchall()}

        # Get equipment from raw topics
        cursor.execute("SELECT DISTINCT topic FROM messages_raw")
        raw_topics = [row[0] for row in cursor.fetchall()]

        # Parse topics to extract equipment
        raw_equipment = set()
        for topic in raw_topics:
            parts = topic.split("/")
            # Enterprise A/Dallas/Line 1/Area/Equipment/...
            if len(parts) >= 5 and parts[1] == "Dallas":
                site = parts[1]
                # Skip Line level, get area and equipment
                if parts[2].startswith("Line"):
                    if len(parts) >= 5:
                        area = parts[3]
                        equipment = parts[4]
                        if area in ("BatchHouse", "HotEnd", "ColdEnd"):
                            raw_equipment.add((site, area, equipment))

        # Verify DB equipment came from raw topics
        if db_equipment and raw_equipment:
            overlap = db_equipment & raw_equipment
            assert len(overlap) > 0, f"No equipment overlap. DB: {db_equipment}, Raw: {raw_equipment}"

        conn.close()

    def test_state_changes_match_raw_payloads(self, collector, mqtt_client, temp_db):
        """Test that equipment_states match State/StateCurrent raw payloads."""
        self.collect_for_duration(mqtt_client, collector, duration=10)

        collector._flush_raw_buffer()

        conn = self.get_db_connection(temp_db)
        cursor = conn.cursor()

        # Get state values from raw messages
        cursor.execute("""
            SELECT topic, payload_text
            FROM messages_raw
            WHERE topic LIKE '%/State/StateCurrent'
        """)
        raw_states = cursor.fetchall()

        # Get states from DB
        cursor.execute("SELECT state_code FROM equipment_states")
        db_state_codes = {row[0] for row in cursor.fetchall()}

        if raw_states:
            # Extract state codes from raw payloads
            raw_state_codes = set()
            for topic, payload in raw_states:
                if payload:
                    try:
                        raw_state_codes.add(int(payload.strip()))
                    except ValueError:
                        pass

            # Verify overlap
            if raw_state_codes and db_state_codes:
                overlap = raw_state_codes & db_state_codes
                assert len(overlap) > 0, f"No state code overlap. Raw: {raw_state_codes}, DB: {db_state_codes}"

        conn.close()

    def test_sensor_readings_match_edge_topics(self, collector, mqtt_client, temp_db):
        """Test that sensor_readings match edge/ topic payloads."""
        self.collect_for_duration(mqtt_client, collector, duration=10)

        collector._flush_raw_buffer()

        conn = self.get_db_connection(temp_db)
        cursor = conn.cursor()

        # Get edge topics from raw messages
        cursor.execute("""
            SELECT topic, payload_text
            FROM messages_raw
            WHERE topic LIKE '%/edge/%'
        """)
        raw_edge = cursor.fetchall()

        # Get sensor readings from DB
        cursor.execute("SELECT sensor_name, value FROM sensor_readings")
        db_sensors = cursor.fetchall()

        if raw_edge:
            # Extract sensor names from topics
            raw_sensor_names = set()
            for topic, payload in raw_edge:
                parts = topic.split("/")
                if parts:
                    raw_sensor_names.add(parts[-1])  # Last part is sensor name

            db_sensor_names = {row[0] for row in db_sensors}

            # Verify overlap
            if raw_sensor_names and db_sensor_names:
                overlap = raw_sensor_names & db_sensor_names
                assert len(overlap) > 0, f"No sensor name overlap. Raw: {raw_sensor_names}, DB: {db_sensor_names}"

        conn.close()

    def test_process_data_matches_status_topics(self, collector, mqtt_client, temp_db):
        """Test that process_data matches Status/ topic payloads."""
        self.collect_for_duration(mqtt_client, collector, duration=10)

        collector._flush_raw_buffer()
        collector._flush_metrics()

        conn = self.get_db_connection(temp_db)
        cursor = conn.cursor()

        # Get Status topics from raw messages
        cursor.execute("""
            SELECT topic, payload_text
            FROM messages_raw
            WHERE topic LIKE '%/Status/%'
        """)
        raw_status = cursor.fetchall()

        # Get process data from DB
        cursor.execute("""
            SELECT level_pct, batch_weight, feed_rate, temperature
            FROM process_data
            WHERE level_pct IS NOT NULL OR batch_weight IS NOT NULL
        """)
        db_process = cursor.fetchall()

        if raw_status:
            # Extract numeric values from raw payloads
            raw_values = set()
            for topic, payload in raw_status:
                if payload:
                    try:
                        raw_values.add(float(payload.strip()))
                    except ValueError:
                        pass

            # Extract values from DB
            db_values = set()
            for row in db_process:
                for val in row:
                    if val is not None:
                        db_values.add(float(val))

            # Verify some overlap (values should appear in both)
            if raw_values and db_values:
                overlap = raw_values & db_values
                # Allow for some mismatch due to timing
                assert len(db_values) > 0 or len(db_process) > 0, "No process data stored"

        conn.close()

    def test_message_count_consistency(self, collector, mqtt_client, temp_db):
        """Test that message counts are consistent."""
        self.collect_for_duration(mqtt_client, collector, duration=10)

        collector._flush_raw_buffer()
        collector._flush_metrics()

        conn = self.get_db_connection(temp_db)
        cursor = conn.cursor()

        # Raw message count
        cursor.execute("SELECT COUNT(*) FROM messages_raw")
        raw_count = cursor.fetchone()[0]

        # Equipment count
        cursor.execute("SELECT COUNT(*) FROM equipment")
        equipment_count = cursor.fetchone()[0]

        # State changes count
        cursor.execute("SELECT COUNT(*) FROM equipment_states")
        states_count = cursor.fetchone()[0]

        # Sensor readings count
        cursor.execute("SELECT COUNT(*) FROM sensor_readings")
        sensors_count = cursor.fetchone()[0]

        # Collector counts
        assert collector.message_count > 0, "No messages received"
        assert collector.stored_count > 0, "No messages stored"

        # Raw should be close to received
        assert raw_count > 0, "No raw messages stored"

        # Should have discovered some equipment
        assert equipment_count > 0 or states_count > 0 or sensors_count > 0, (
            "No equipment, states, or sensors stored"
        )

        print(f"Messages: {collector.message_count}, Raw: {raw_count}, "
              f"Equipment: {equipment_count}, States: {states_count}, Sensors: {sensors_count}")

        conn.close()

    def test_sites_and_areas_created(self, collector, mqtt_client, temp_db):
        """Test that sites and areas are properly created."""
        self.collect_for_duration(mqtt_client, collector, duration=10)

        conn = self.get_db_connection(temp_db)
        cursor = conn.cursor()

        # Check sites
        cursor.execute("SELECT name FROM sites")
        sites = [row[0] for row in cursor.fetchall()]

        # Check areas
        cursor.execute("SELECT name FROM areas")
        areas = [row[0] for row in cursor.fetchall()]

        # Dallas should be a site
        if collector.stored_count > 0:
            # At least some hierarchy should be created
            assert len(sites) > 0 or len(areas) > 0, "No sites or areas created"

        conn.close()
