"""
subscriber.py — MQTT subscriber for pskr-mqtt-cache.

Connects to mqtt.pskreporter.info, subscribes to the full spot firehose,
parses JSON payloads, and inserts spots into the SQLite database.

Runs in its own thread. Reconnects automatically on disconnect.
"""

import json
import time
import logging
import threading

import uuid
import paho.mqtt.client as mqtt

from .config import MQTTConfig
from .database import SpotDatabase

log = logging.getLogger(__name__)


class SpotSubscriber:
    def __init__(self, cfg: MQTTConfig, db: SpotDatabase):
        self.cfg = cfg
        self.db  = db

        self._connected   = False
        self._running     = False
        self._thread      = None

        # Stats
        self.spots_received  = 0
        self.spots_inserted  = 0
        self.last_spot_time  = None
        self.connect_time    = None

    # ── MQTT Callbacks ────────────────────────────────────────────────────────

    def _on_connect(self, client, userdata, flags, rc, properties=None):
        if rc == 0:
            self._connected  = True
            self.connect_time = time.time()
            log.info("Connected to MQTT broker %s:%d", self.cfg.host, self.cfg.port)
            client.subscribe(self.cfg.topic)
            log.info("Subscribed to topic: %s", self.cfg.topic)
        else:
            log.error("MQTT connect failed, rc=%d", rc)

    def _on_disconnect(self, client, userdata, rc, properties=None, reasoncode=None):
        self._connected = False
        # paho-mqtt v2 may pass rc as None or a ReasonCode object
        rc_val = int(rc) if rc is not None and isinstance(rc, int) else 0
        if rc_val != 0 or rc is None:
            log.warning("MQTT disconnected unexpectedly — will reconnect. (rc=%s)", rc)
        else:
            log.info("MQTT disconnected cleanly.")

    def _on_message(self, client, userdata, msg):
        try:
            spot = json.loads(msg.payload)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            log.debug("Bad payload: %s", exc)
            return

        self.spots_received += 1
        self.last_spot_time  = time.time()

        # Skip spots with no locator — useless for grid-based filtering
        if not spot.get("sl") and not spot.get("rl"):
            return

        if self.db.insert_spot(spot):
            self.spots_inserted += 1

        # Periodic stats log
        if self.spots_received % 10000 == 0:
            log.info("Stats: received=%d inserted=%d db_total=%d",
                     self.spots_received, self.spots_inserted, self.db.count())

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def _run(self):
        """Main subscriber loop — runs in its own thread."""
        while self._running:
            # Append unique suffix to avoid duplicate client ID kicks from broker
            client_id = f"{self.cfg.client_id}-{uuid.uuid4().hex[:8]}"
            client = mqtt.Client(
                client_id=client_id,
                protocol=mqtt.MQTTv311,
            )
            client.on_connect    = self._on_connect
            client.on_disconnect = self._on_disconnect
            client.on_message    = self._on_message

            if self.cfg.tls:
                client.tls_set()

            try:
                log.info("Connecting to %s:%d …", self.cfg.host, self.cfg.port)
                client.connect(self.cfg.host, self.cfg.port, self.cfg.keepalive)
                client.loop_forever()
            except Exception as exc:
                log.error("MQTT error: %s", exc)

            if self._running:
                log.info("Reconnecting in %ds …", self.cfg.reconnect_delay)
                time.sleep(self.cfg.reconnect_delay)

        log.info("Subscriber stopped.")

    def start(self):
        """Start the subscriber in a background thread."""
        self._running = True
        self._thread  = threading.Thread(target=self._run, name="mqtt-subscriber", daemon=True)
        self._thread.start()
        log.info("MQTT subscriber thread started.")

    def stop(self):
        """Signal the subscriber to stop."""
        self._running = False
        log.info("MQTT subscriber stopping …")

    @property
    def is_connected(self) -> bool:
        return self._connected

    def stats(self) -> dict:
        return {
            "connected":       self._connected,
            "connect_time":    self.connect_time,
            "spots_received":  self.spots_received,
            "spots_inserted":  self.spots_inserted,
            "last_spot_time":  self.last_spot_time,
        }
