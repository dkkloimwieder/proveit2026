"""Shared pytest fixtures for collector tests."""

import os
import sqlite3
import tempfile
import time
from pathlib import Path

import pytest

# Add project root to path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from mqtt_client import MQTTClient


@pytest.fixture
def temp_db():
    """Create a temporary database file."""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    yield path
    # Cleanup
    if os.path.exists(path):
        os.unlink(path)


@pytest.fixture
def mqtt_client():
    """Create and connect an MQTT client."""
    client = MQTTClient()
    if not client.connect():
        pytest.skip("Cannot connect to MQTT broker")
    yield client
    client.stop()


def collect_messages(client: MQTTClient, topic: str, duration: int = 10) -> list[tuple[str, bytes]]:
    """Collect MQTT messages for a duration.

    Returns list of (topic, payload) tuples.
    """
    messages = []

    def handler(t: str, p: bytes):
        messages.append((t, p))

    client.add_message_handler(handler)
    client.subscribe(topic)

    # Collect for duration
    start = time.time()
    while time.time() - start < duration:
        time.sleep(0.1)

    return messages


def get_db_connection(db_path: str) -> sqlite3.Connection:
    """Get a database connection with row factory."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn
