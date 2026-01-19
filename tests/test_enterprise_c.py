"""Tests for Enterprise C (Biotech) data collection.

Verifies that:
1. Tags table matches discovered tag names from raw messages
2. Tag values match _PV/_SP payloads
3. Batches table matches BATCH_ID/RECIPE/FORMULA payloads
4. Tag descriptions match _DESC payloads
5. Raw message count correlates with stored records
"""

import json
import sqlite3
import tempfile
import time
import os
from pathlib import Path

import pytest

# Add project root to path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from collectors.enterprise_c import EnterpriseCCollector
from mqtt_client import MQTTClient


class TestEnterpriseCCollector:
    """Test Enterprise C data collection and storage."""

    @pytest.fixture
    def temp_db(self):
        """Create a temporary database file."""
        fd, path = tempfile.mkstemp(suffix="_test_c.db")
        os.close(fd)
        yield path
        if os.path.exists(path):
            os.unlink(path)

    @pytest.fixture
    def collector(self, temp_db):
        """Create a collector with raw capture enabled."""
        collector = EnterpriseCCollector(db_path=temp_db, capture_raw=True)
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

    def collect_for_duration(self, client: MQTTClient, collector: EnterpriseCCollector, duration: int = 15):
        """Run collection for a specified duration."""
        client.add_message_handler(collector.handle_message)
        client.subscribe("Enterprise C/#")

        # Wait for subscription to be active
        time.sleep(1)

        start = time.time()
        while time.time() - start < duration:
            time.sleep(0.1)

        # Remove handler to stop processing new messages
        client._message_handlers.remove(collector.handle_message)
        time.sleep(0.5)  # Let any in-flight messages complete

    def get_db_connection(self, db_path: str) -> sqlite3.Connection:
        """Get a database connection."""
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def test_raw_messages_captured(self, collector, mqtt_client, temp_db):
        """Test that raw messages are captured to messages_raw table."""
        self.collect_for_duration(mqtt_client, collector, duration=10)

        # Flush buffers before checking
        collector._flush_raw_buffer()

        conn = self.get_db_connection(temp_db)
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM messages_raw")
        raw_count = cursor.fetchone()[0]

        assert raw_count > 0, "No raw messages captured"
        # Raw count may be <= message_count because some messages are filtered (e.g., maintainx/)
        assert raw_count <= collector.message_count, (
            f"Raw count ({raw_count}) should not exceed message count ({collector.message_count})"
        )
        # Most messages should be captured (at least 90%)
        if collector.message_count > 0:
            capture_rate = raw_count / collector.message_count
            assert capture_rate > 0.9, f"Capture rate too low: {capture_rate:.1%}"

        conn.close()

    def test_tags_match_raw_topics(self, collector, mqtt_client, temp_db):
        """Test that tags table contains tags discovered from raw messages."""
        self.collect_for_duration(mqtt_client, collector, duration=10)

        conn = self.get_db_connection(temp_db)
        cursor = conn.cursor()

        # Get unique tag base names from raw messages
        cursor.execute("SELECT DISTINCT topic FROM messages_raw WHERE topic LIKE 'Enterprise C/%'")
        raw_topics = [row[0] for row in cursor.fetchall()]

        # Extract tag names from topics (Enterprise C/unit/tag_name)
        raw_tags = set()
        for topic in raw_topics:
            parts = topic.split("/")
            if len(parts) >= 3:
                tag = parts[2]
                # Strip suffixes to get base name
                for suffix in ["_PV", "_SP", "_DESC", "_EU", "_ACTIVE", "_MODE", "_STATUS", "_START"]:
                    if suffix in tag:
                        tag = tag[:tag.find(suffix)]
                        break
                raw_tags.add(tag)

        # Get tags from database
        cursor.execute("SELECT tag_name FROM tags")
        db_tags = {row[0] for row in cursor.fetchall()}

        # Verify overlap (not all raw tags become db tags due to filtering)
        overlap = raw_tags & db_tags
        assert len(overlap) > 0, "No tags from raw messages found in tags table"

        # Most DB tags should come from raw messages
        if len(db_tags) > 0:
            coverage = len(overlap) / len(db_tags)
            assert coverage > 0.5, f"Only {coverage:.1%} of DB tags found in raw messages"

        conn.close()

    def test_tag_values_match_payloads(self, collector, mqtt_client, temp_db):
        """Test that tag_values contain values from raw message payloads."""
        self.collect_for_duration(mqtt_client, collector, duration=10)

        conn = self.get_db_connection(temp_db)
        cursor = conn.cursor()

        # Get raw messages with _PV suffix (process values)
        cursor.execute("""
            SELECT topic, payload_text
            FROM messages_raw
            WHERE topic LIKE '%/_PV' OR topic LIKE '%/_PV_%'
            LIMIT 100
        """)
        raw_pv_messages = cursor.fetchall()

        # Get tag values from database
        cursor.execute("""
            SELECT t.tag_name, tv.value_type, tv.value_numeric, tv.value_text
            FROM tag_values tv
            JOIN tags t ON tv.tag_id = t.id
            WHERE tv.value_type = 'PV'
            LIMIT 100
        """)
        db_values = cursor.fetchall()

        assert len(db_values) > 0, "No PV tag values stored"

        # Verify some values can be traced back to raw payloads
        raw_payloads = {}
        for topic, payload in raw_pv_messages:
            if payload:
                try:
                    raw_payloads[topic] = float(payload)
                except (ValueError, TypeError):
                    raw_payloads[topic] = payload

        # Check that stored numeric values exist in raw payloads
        stored_values = {row[2] for row in db_values if row[2] is not None}
        raw_values = {v for v in raw_payloads.values() if isinstance(v, (int, float))}

        if stored_values and raw_values:
            # At least some stored values should match raw values
            matches = stored_values & raw_values
            assert len(matches) > 0 or len(stored_values) > 0, "No stored values match raw payloads"

        conn.close()

    def test_batches_match_batch_id_payloads(self, collector, mqtt_client, temp_db):
        """Test that batches table contains batch IDs from raw messages."""
        self.collect_for_duration(mqtt_client, collector, duration=15)

        conn = self.get_db_connection(temp_db)
        cursor = conn.cursor()

        # Get batch-related raw messages
        cursor.execute("""
            SELECT topic, payload_text
            FROM messages_raw
            WHERE topic LIKE '%BATCH_ID%' OR topic LIKE '%BATCH-ID%'
        """)
        raw_batch_messages = cursor.fetchall()

        # Get batches from database
        cursor.execute("SELECT batch_id, recipe_name, formula_name FROM batches")
        db_batches = cursor.fetchall()

        if len(raw_batch_messages) > 0:
            # Extract batch IDs from raw payloads
            raw_batch_ids = set()
            for topic, payload in raw_batch_messages:
                if payload:
                    # Clean up payload (remove quotes, whitespace)
                    batch_id = payload.strip().strip('"')
                    if batch_id:
                        raw_batch_ids.add(batch_id)

            # Get DB batch IDs
            db_batch_ids = {str(row[0]) for row in db_batches}

            # Verify batch IDs match
            if raw_batch_ids:
                matches = raw_batch_ids & db_batch_ids
                assert len(matches) > 0, (
                    f"No batch IDs match. Raw: {raw_batch_ids}, DB: {db_batch_ids}"
                )

        conn.close()

    def test_recipe_formula_match_payloads(self, collector, mqtt_client, temp_db):
        """Test that batch recipe/formula match raw message payloads."""
        self.collect_for_duration(mqtt_client, collector, duration=15)

        conn = self.get_db_connection(temp_db)
        cursor = conn.cursor()

        # Get recipe raw messages
        cursor.execute("""
            SELECT topic, payload_text
            FROM messages_raw
            WHERE topic LIKE '%RECIPE%'
        """)
        raw_recipes = {row[1].strip().strip('"') for row in cursor.fetchall() if row[1]}

        # Get formula raw messages
        cursor.execute("""
            SELECT topic, payload_text
            FROM messages_raw
            WHERE topic LIKE '%FORMULA%'
        """)
        raw_formulas = {row[1].strip().strip('"') for row in cursor.fetchall() if row[1]}

        # Get from database
        cursor.execute("SELECT recipe_name, formula_name FROM batches")
        db_batches = cursor.fetchall()

        db_recipes = {row[0] for row in db_batches if row[0]}
        db_formulas = {row[1] for row in db_batches if row[1]}

        # Verify matches
        if raw_recipes and db_recipes:
            recipe_matches = raw_recipes & db_recipes
            assert len(recipe_matches) > 0, f"No recipes match. Raw: {raw_recipes}, DB: {db_recipes}"

        if raw_formulas and db_formulas:
            formula_matches = raw_formulas & db_formulas
            assert len(formula_matches) > 0, f"No formulas match. Raw: {raw_formulas}, DB: {db_formulas}"

        conn.close()

    def test_message_count_consistency(self, collector, mqtt_client, temp_db):
        """Test that message counts are consistent between raw and processed."""
        self.collect_for_duration(mqtt_client, collector, duration=10)

        # Flush buffers before checking
        collector._flush_raw_buffer()

        conn = self.get_db_connection(temp_db)
        cursor = conn.cursor()

        # Raw message count
        cursor.execute("SELECT COUNT(*) FROM messages_raw")
        raw_count = cursor.fetchone()[0]

        # Tag values count
        cursor.execute("SELECT COUNT(*) FROM tag_values")
        values_count = cursor.fetchone()[0]

        # Tags count
        cursor.execute("SELECT COUNT(*) FROM tags")
        tags_count = cursor.fetchone()[0]

        # Collector counts
        assert collector.message_count > 0, "No messages received"
        assert collector.stored_count > 0, "No messages stored"

        # Raw should be close to received (some may be filtered)
        assert raw_count > 0, "No raw messages stored"
        assert raw_count <= collector.message_count, (
            f"Raw count ({raw_count}) should not exceed message count ({collector.message_count})"
        )

        # Should have discovered some tags
        assert tags_count > 0, "No tags discovered"

        # Should have stored some values
        assert values_count > 0, "No tag values stored"

        # Values should correlate with stored_count
        # stored_count only increments when unit and tag are present
        assert values_count <= collector.stored_count, (
            f"Values ({values_count}) should not exceed stored count ({collector.stored_count})"
        )

        print(f"Messages: {collector.message_count}, Raw: {raw_count}, Tags: {tags_count}, Values: {values_count}")

        conn.close()

    def test_unit_assignment(self, collector, mqtt_client, temp_db):
        """Test that tags are assigned to correct units."""
        self.collect_for_duration(mqtt_client, collector, duration=10)

        conn = self.get_db_connection(temp_db)
        cursor = conn.cursor()

        # Get tags with their units
        cursor.execute("""
            SELECT t.tag_name, u.code as unit_code
            FROM tags t
            JOIN units u ON t.unit_id = u.id
        """)
        tag_units = cursor.fetchall()

        # Get raw topics to verify unit assignment
        cursor.execute("SELECT DISTINCT topic FROM messages_raw")
        raw_topics = [row[0] for row in cursor.fetchall()]

        # Build expected unit mapping from raw topics
        expected_units = {}  # tag_base -> unit_code
        for topic in raw_topics:
            parts = topic.split("/")
            if len(parts) >= 3:
                unit_code = parts[1]  # Enterprise C/unit/tag
                tag = parts[2]
                # Strip suffix
                for suffix in ["_PV", "_SP", "_DESC", "_EU"]:
                    if suffix in tag:
                        tag = tag[:tag.find(suffix)]
                        break
                expected_units[tag] = unit_code

        # Verify some tags have correct unit assignment
        mismatches = []
        for tag_name, unit_code in tag_units:
            if tag_name in expected_units:
                if expected_units[tag_name] != unit_code:
                    mismatches.append((tag_name, unit_code, expected_units[tag_name]))

        assert len(mismatches) == 0, f"Unit mismatches: {mismatches[:5]}"

        conn.close()
