# src/cx_shell/management/history_logger.py

import json
from pathlib import Path
import sqlite3
from datetime import datetime, timezone
from typing import Dict, List, Optional
import structlog

# Import schemas and constants from their canonical locations
from cx_core_schemas.vfs import RunManifest
from .utils import CX_HOME

# Initialize logger for this module
logger = structlog.get_logger(__name__)

# --- Constants ---
# These will be updated by the __init__ method for testability
FEEDBACK_LOG_FILE = CX_HOME / "feedback_log.jsonl"
CONTEXT_DIR = CX_HOME / "context"
HISTORY_DB_FILE = CONTEXT_DIR / "history.sqlite"
RUNS_DIR = CX_HOME / "runs"


class HistoryLogger:
    """
    Logs structured events to both a JSONL file for feedback and an SQLite
    database for fast, structured retrieval by the agent's Context Engine.
    Also provides methods to query the ground-truth run history from the
    Data Fabric's Run Manifests.

    This service is designed to be "fire-and-forget" and highly resilient,
    ensuring that logging failures never interrupt the user's session.
    """

    def __init__(self, cx_home_path: Optional[Path] = None):
        _cx_home = cx_home_path or CX_HOME
        # Update module-level constants to respect the isolated path for testing
        global FEEDBACK_LOG_FILE, CONTEXT_DIR, HISTORY_DB_FILE, RUNS_DIR
        FEEDBACK_LOG_FILE = _cx_home / "feedback_log.jsonl"
        CONTEXT_DIR = _cx_home / "context"
        HISTORY_DB_FILE = CONTEXT_DIR / "history.sqlite"
        RUNS_DIR = _cx_home / "runs"

        try:
            CONTEXT_DIR.mkdir(parents=True, exist_ok=True)
            self._init_db()
        except Exception as e:
            logger.error("history_logger.init.failed", error=str(e), exc_info=True)

    def _init_db(self):
        """Initializes the SQLite database schema if it doesn't exist."""
        with sqlite3.connect(HISTORY_DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                event_type TEXT NOT NULL,
                actor TEXT NOT NULL,
                status TEXT,
                content TEXT,
                duration_ms INTEGER
            )
            """)
            conn.commit()

    def _log_to_jsonl(self, event_type: str, data: dict):
        """Appends a structured, timestamped event to the JSONL feedback log file."""
        try:
            log_entry = {
                "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                "event_type": event_type,
                "data": data,
            }
            with open(FEEDBACK_LOG_FILE, "a") as f:
                f.write(json.dumps(log_entry) + "\n")
        except Exception as e:
            logger.error("history_logger.jsonl.failed", error=str(e), exc_info=True)

    def _log_to_sqlite(
        self,
        event_type: str,
        actor: str,
        status: str,
        content: str,
        duration_ms: int = -1,
    ):
        """Writes a structured event to the SQLite database."""
        try:
            with sqlite3.connect(HISTORY_DB_FILE) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT INTO events (timestamp, event_type, actor, status, content, duration_ms) VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        datetime.now(timezone.utc).isoformat(),
                        event_type,
                        actor,
                        status,
                        content,
                        duration_ms,
                    ),
                )
                conn.commit()
        except Exception as e:
            logger.error("history_logger.sqlite.failed", error=str(e), exc_info=True)

    def log_command(self, command_text: str, status: str, duration_ms: int):
        """Logs a command executed by the user."""
        self._log_to_sqlite("COMMAND", "USER", status, command_text, duration_ms)

    def log_agent_turn(self, summary: str, status: str = "SUCCESS"):
        """Logs a summary of a completed agent turn."""
        self._log_to_sqlite("AGENT_TURN", "AGENT", status, summary)

    def log_user_correction(self, intent: str, agent_command: str, user_command: str):
        """Logs a high-value event where the user corrects the agent."""
        data = {
            "intent": intent,
            "agent_command": agent_command,
            "user_command": user_command,
        }
        self._log_to_jsonl("user_correction", data)
        self._log_to_sqlite("USER_CORRECTION", "USER", "SUCCESS", json.dumps(data))

    def query_recent_runs(self, limit: int = 50) -> List[Dict]:
        """
        Queries the Data Fabric for the most recent runs by scanning the
        `~/.cx/runs` directory and parsing the `manifest.json` files.

        This method provides the definitive, ground-truth history of all
        computational runs.

        Args:
            limit: The maximum number of recent runs to return.

        Returns:
            A list of dictionaries, each summarizing a single run.
        """
        manifest_paths = []
        if RUNS_DIR.is_dir():
            manifest_paths = list(RUNS_DIR.rglob("manifest.json"))

        if not manifest_paths:
            return []

        try:
            manifest_paths.sort(key=lambda p: p.stat().st_mtime, reverse=True)
        except FileNotFoundError:
            return []

        recent_runs = []
        for path in manifest_paths[:limit]:
            try:
                manifest = RunManifest.model_validate_json(path.read_text())
                status_icon = "✅" if manifest.status == "completed" else "❌"

                recent_runs.append(
                    {
                        "run_id": manifest.run_id,
                        "flow_id": manifest.flow_id,
                        "status": f"{status_icon} {manifest.status.capitalize()}",
                        "timestamp_utc": manifest.timestamp_utc.isoformat(),
                        "parameters": manifest.parameters,
                    }
                )
            except Exception as e:
                # --- START OF DEFINITIVE FIX ---
                # Use the 'logger' object defined at the module level.
                logger.warn(
                    "history_logger.query.parse_error",
                    manifest_path=str(path),
                    error=str(e),
                )
                # --- END OF DEFINITIVE FIX ---
                continue

        return recent_runs
