"""MQTT client for connecting to the ProveIt virtual factory broker."""

import os
import ssl
import uuid
from collections.abc import Callable
from urllib.parse import urlparse

import paho.mqtt.client as mqtt
from dotenv import load_dotenv


class MQTTClient:
    """MQTT client with connection handling, auth, and reconnection logic."""

    def __init__(self):
        load_dotenv(override=True)

        url = os.getenv("URL", "mqtt://localhost")
        parsed = urlparse(url)

        self.host = parsed.hostname or "localhost"
        self.port = int(os.getenv("PORT", parsed.port or 1883))
        self.username = os.getenv("MQTT_USER") or os.getenv("USER")
        self.password = os.getenv("MQTT_PASS") or os.getenv("PASS")
        self.use_tls = parsed.scheme in ("mqtts", "ssl")

        # Generate unique client ID to avoid conflicts
        client_id = f"proveit-collector-{uuid.uuid4().hex[:8]}"

        self.client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=client_id,
            protocol=mqtt.MQTTv311,
            reconnect_on_failure=True,
        )

        if self.username and self.password:
            self.client.username_pw_set(self.username, self.password)

        if self.use_tls:
            self.client.tls_set(cert_reqs=ssl.CERT_REQUIRED)

        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_message = self._on_message

        self._message_handlers: list[Callable[[str, bytes], None]] = []
        self._subscriptions: set[str] = set()
        self._connected = False

    def _on_connect(self, client, userdata, flags, reason_code, properties=None):
        if reason_code == 0 or str(reason_code) == "Success":
            print(f"Connected to {self.host}:{self.port}")
            self._connected = True
            # Re-subscribe on reconnect
            for topic in self._subscriptions:
                client.subscribe(topic, qos=0)
                print(f"Re-subscribed to: {topic}")
        else:
            print(f"Connection failed: {reason_code}")

    def _on_disconnect(self, client, userdata, flags=None, reason_code=None, properties=None):
        self._connected = False
        if reason_code != 0 and reason_code is not None:
            print(f"Disconnected (rc={reason_code}), will auto-reconnect...")

    def _on_message(self, client, userdata, msg):
        for handler in self._message_handlers:
            handler(msg.topic, msg.payload)

    def add_message_handler(self, handler: Callable[[str, bytes], None]):
        """Add a callback to be invoked when messages are received."""
        self._message_handlers.append(handler)

    def connect(self) -> bool:
        """Connect to the MQTT broker."""
        try:
            print(f"Connecting to {self.host}:{self.port}...")
            self.client.connect(self.host, self.port, keepalive=60)
            return True
        except Exception as e:
            print(f"Connection error: {e}")
            return False

    def subscribe(self, topic: str, qos: int = 0):
        """Subscribe to a topic pattern."""
        self._subscriptions.add(topic)
        self.client.subscribe(topic, qos=qos)
        print(f"Subscribed to: {topic}")

    def start(self):
        """Start the MQTT client loop (blocking)."""
        self.client.loop_forever()

    def start_background(self):
        """Start the MQTT client loop in the background."""
        self.client.loop_start()

    def stop(self):
        """Stop the MQTT client loop and disconnect."""
        self.client.loop_stop()
        self.client.disconnect()

    @property
    def is_connected(self) -> bool:
        return self._connected
