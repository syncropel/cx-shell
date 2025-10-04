import json
from pathlib import Path
import sqlite3
from datetime import datetime, timezone
from typing import Optional
import structlog

from .engine.connector.config import CX_HOME

# Initialize logger for this module
logger = structlog.get_logger(__name__)

# --- Constants ---
FEEDBACK_LOG_FILE = CX_HOME / "feedback_log.jsonl"
CONTEXT_DIR = CX_HOME / "context"
HISTORY_DB_FILE = CONTEXT_DIR / "history.sqlite"


class HistoryLogger:
    """
    Logs structured events to both a JSONL file for feedback and an SQLite
    database for fast, structured retrieval by the agent's Context Engine.

    This service is designed to be "fire-and-forget" and highly resilient,
    ensuring that logging failures never interrupt the user's session.
    """

    def __init__(self, cx_home_path: Optional[Path] = None):
        _cx_home = cx_home_path or CX_HOME
        # Update module-level constants
        global FEEDBACK_LOG_FILE, CONTEXT_DIR, HISTORY_DB_FILE
        FEEDBACK_LOG_FILE = _cx_home / "feedback_log.jsonl"
        CONTEXT_DIR = _cx_home / "context"
        HISTORY_DB_FILE = CONTEXT_DIR / "history.sqlite"

        try:
            CONTEXT_DIR.mkdir(parents=True, exist_ok=True)
            self._init_db()
        except Exception as e:
            logger.error("history_logger.init.failed", error=str(e), exc_info=True)

    def _init_db(self):
        """Initializes the SQLite database schema if it doesn't exist."""
        with sqlite3.connect(HISTORY_DB_FILE) as conn:
            cursor = conn.cursor()
            # Added more detailed columns for better querying
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                event_type TEXT NOT NULL, /* 'COMMAND', 'AGENT_TURN', 'USER_CORRECTION' */
                actor TEXT NOT NULL, /* 'USER', 'AGENT' */
                status TEXT, /* 'SUCCESS', 'FAILED' */
                content TEXT, /* Command text, summary, or JSON payload of the event */
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
        """
        Logs a high-value event where the user corrects the agent. This is
        logged to both the SQLite DB and the dedicated JSONL feedback file.
        """
        data = {
            "intent": intent,
            "agent_command": agent_command,
            "user_command": user_command,
        }
        # Log to both sinks
        self._log_to_jsonl("user_correction", data)
        self._log_to_sqlite("USER_CORRECTION", "USER", "SUCCESS", json.dumps(data))
