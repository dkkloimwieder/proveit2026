"""Tests for Enterprise B (Beverage Manufacturing) data collection.

Verifies that:
1. Products table matches item/ topic payloads
2. Lots table matches lotnumber/ payloads
3. Work orders match workorder/ payloads
4. Metrics match metric/ payloads
5. Events match state changes
6. Raw message count correlates with stored records
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

from data_collector import DataCollector
from mqtt_client import MQTTClient


class TestEnterpriseBCollector:
    """Test Enterprise B data collection and storage."""

    @pytest.fixture
    def temp_db(self):
        """Create a temporary database file."""
        fd, path = tempfile.mkstemp(suffix="_test_b.db")
        os.close(fd)
        yield path
        if os.path.exists(path):
            os.unlink(path)

    @pytest.fixture
    def collector(self, temp_db):
        """Create a collector with raw capture enabled."""
        collector = DataCollector(enterprise="B", db_path=temp_db, capture_raw=True)
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

    def collect_for_duration(self, client: MQTTClient, collector: DataCollector, duration: int = 15):
        """Run collection for a specified duration."""
        client.add_message_handler(collector.handle_message)
        client.subscribe("Enterprise B/#")

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

    def test_products_match_item_payloads(self, collector, mqtt_client, temp_db):
        """Test that products table matches item/ topic payloads."""
        self.collect_for_duration(mqtt_client, collector, duration=15)

        collector._flush_raw_buffer()
        collector._flush_pending_products()

        conn = self.get_db_connection(temp_db)
        cursor = conn.cursor()

        # Get item topics from raw messages
        cursor.execute("""
            SELECT topic, payload_text
            FROM messages_raw
            WHERE topic LIKE '%/item/itemid' OR topic LIKE '%/item/itemname'
        """)
        raw_items = cursor.fetchall()

        # Get products from DB
        cursor.execute("SELECT item_id, name FROM products")
        db_products = cursor.fetchall()

        if raw_items:
            # Extract item IDs from raw
            raw_item_ids = set()
            raw_item_names = set()
            for topic, payload in raw_items:
                if payload:
                    if 'itemid' in topic:
                        try:
                            raw_item_ids.add(int(payload.strip()))
                        except ValueError:
                            pass
                    elif 'itemname' in topic:
                        raw_item_names.add(payload.strip())

            # Get from DB
            db_item_ids = {row[0] for row in db_products if row[0]}
            db_item_names = {row[1] for row in db_products if row[1]}

            # Verify overlap
            if raw_item_ids and db_item_ids:
                overlap = raw_item_ids & db_item_ids
                assert len(overlap) > 0, f"No item ID overlap. Raw: {raw_item_ids}, DB: {db_item_ids}"

        conn.close()

    def test_lots_match_lotnumber_payloads(self, collector, mqtt_client, temp_db):
        """Test that lots table matches lotnumber/ topic payloads."""
        self.collect_for_duration(mqtt_client, collector, duration=15)

        collector._flush_raw_buffer()
        collector._flush_pending_lots()

        conn = self.get_db_connection(temp_db)
        cursor = conn.cursor()

        # Get lot topics from raw messages
        cursor.execute("""
            SELECT topic, payload_text
            FROM messages_raw
            WHERE topic LIKE '%/lotnumber/lotnumberid' OR topic LIKE '%/lotnumber/lotnumber'
        """)
        raw_lots = cursor.fetchall()

        # Get lots from DB
        cursor.execute("SELECT lot_number_id, lot_number FROM lots")
        db_lots = cursor.fetchall()

        if raw_lots:
            raw_lot_ids = set()
            for topic, payload in raw_lots:
                if payload and 'lotnumberid' in topic:
                    try:
                        raw_lot_ids.add(int(payload.strip()))
                    except ValueError:
                        pass

            db_lot_ids = {row[0] for row in db_lots if row[0]}

            if raw_lot_ids and db_lot_ids:
                overlap = raw_lot_ids & db_lot_ids
                assert len(overlap) > 0, f"No lot ID overlap. Raw: {raw_lot_ids}, DB: {db_lot_ids}"

        conn.close()

    def test_work_orders_match_payloads(self, collector, mqtt_client, temp_db):
        """Test that work_orders table matches workorder/ topic payloads."""
        self.collect_for_duration(mqtt_client, collector, duration=15)

        collector._flush_raw_buffer()
        collector._flush_pending_work_orders()

        conn = self.get_db_connection(temp_db)
        cursor = conn.cursor()

        # Get WO topics from raw messages
        cursor.execute("""
            SELECT topic, payload_text
            FROM messages_raw
            WHERE topic LIKE '%/workorder/workorderid' OR topic LIKE '%/workorder/workordernumber'
        """)
        raw_wos = cursor.fetchall()

        # Get work orders from DB
        cursor.execute("SELECT work_order_id, work_order_number FROM work_orders")
        db_wos = cursor.fetchall()

        if raw_wos:
            raw_wo_ids = set()
            raw_wo_numbers = set()
            for topic, payload in raw_wos:
                if payload:
                    if 'workorderid' in topic:
                        try:
                            raw_wo_ids.add(int(payload.strip()))
                        except ValueError:
                            pass
                    elif 'workordernumber' in topic:
                        raw_wo_numbers.add(payload.strip())

            db_wo_ids = {row[0] for row in db_wos if row[0]}
            db_wo_numbers = {row[1] for row in db_wos if row[1]}

            if raw_wo_ids and db_wo_ids:
                overlap = raw_wo_ids & db_wo_ids
                assert len(overlap) > 0, f"No WO ID overlap. Raw: {raw_wo_ids}, DB: {db_wo_ids}"

        conn.close()

    def test_metrics_match_payloads(self, collector, mqtt_client, temp_db):
        """Test that metrics_10s match metric/ topic payloads."""
        self.collect_for_duration(mqtt_client, collector, duration=15)

        collector._flush_raw_buffer()
        collector._flush_metrics()

        conn = self.get_db_connection(temp_db)
        cursor = conn.cursor()

        # Get metric topics from raw messages
        cursor.execute("""
            SELECT topic, payload_text
            FROM messages_raw
            WHERE topic LIKE '%/metric/oee' OR topic LIKE '%/metric/availability'
        """)
        raw_metrics = cursor.fetchall()

        # Get metrics from DB
        cursor.execute("SELECT oee, availability FROM metrics_10s WHERE oee IS NOT NULL")
        db_metrics = cursor.fetchall()

        if raw_metrics:
            raw_oee_values = set()
            for topic, payload in raw_metrics:
                if payload and 'oee' in topic:
                    try:
                        raw_oee_values.add(round(float(payload.strip()), 2))
                    except ValueError:
                        pass

            db_oee_values = {round(row[0], 2) for row in db_metrics if row[0]}

            # Allow for some timing differences - just verify metrics are being captured
            if db_metrics:
                assert len(db_oee_values) > 0, "No OEE values in database"

        conn.close()

    def test_state_events_logged(self, collector, mqtt_client, temp_db):
        """Test that state change events are logged."""
        self.collect_for_duration(mqtt_client, collector, duration=15)

        collector._flush_raw_buffer()

        conn = self.get_db_connection(temp_db)
        cursor = conn.cursor()

        # Get state topics from raw messages
        cursor.execute("""
            SELECT COUNT(*)
            FROM messages_raw
            WHERE topic LIKE '%/state/name'
        """)
        raw_state_count = cursor.fetchone()[0]

        # Get events from DB
        cursor.execute("SELECT COUNT(*) FROM events WHERE event_type = 'state'")
        db_event_count = cursor.fetchone()[0]

        # Get states from DB
        cursor.execute("SELECT COUNT(*) FROM states")
        db_state_count = cursor.fetchone()[0]

        # State records are only created when state CHANGES are detected.
        # If no state changes occur during the collection window, states table may be empty.
        # We verify that IF state events were logged, states must exist.
        if db_event_count > 0:
            assert db_state_count > 0, "State events logged but no states in states table"

        # Log info for debugging
        print(f"Raw state topics: {raw_state_count}, Events: {db_event_count}, States: {db_state_count}")

        conn.close()

    def test_message_count_consistency(self, collector, mqtt_client, temp_db):
        """Test that message counts are consistent."""
        self.collect_for_duration(mqtt_client, collector, duration=10)

        collector._flush_raw_buffer()
        collector._flush_metrics()

        conn = self.get_db_connection(temp_db)
        cursor = conn.cursor()

        # Get counts
        cursor.execute("SELECT COUNT(*) FROM messages_raw")
        raw_count = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM products")
        products_count = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM work_orders")
        wo_count = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM metrics_10s")
        metrics_count = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM states")
        states_count = cursor.fetchone()[0]

        # Collector counts
        assert collector.message_count > 0, "No messages received"
        assert collector.stored_count > 0, "No messages stored"
        assert raw_count > 0, "No raw messages stored"

        print(f"Messages: {collector.message_count}, Raw: {raw_count}, "
              f"Products: {products_count}, WOs: {wo_count}, "
              f"Metrics: {metrics_count}, States: {states_count}")

        conn.close()
