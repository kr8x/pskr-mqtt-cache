"""
pruner.py — Background thread that periodically prunes expired spots.
"""

import time
import logging
import threading

from .database import SpotDatabase
from .config import DatabaseConfig

log = logging.getLogger(__name__)


class Pruner:
    def __init__(self, db: SpotDatabase, cfg: DatabaseConfig):
        self.db       = db
        self.interval = cfg.prune_interval_minutes * 60
        self._running = False
        self._thread  = None

    def _run(self):
        log.info("Pruner started (interval=%ds)", self.interval)
        while self._running:
            time.sleep(self.interval)
            if self._running:
                self.db.prune()

    def start(self):
        self._running = True
        self._thread  = threading.Thread(target=self._run, name="pruner", daemon=True)
        self._thread.start()

    def stop(self):
        self._running = False
