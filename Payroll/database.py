import sqlite3
from datetime import datetime, UTC
import json
from typing import Optional, Dict, List
from xmlrpc.client import DateTime


class MappingDatabase:
    def __init__(self, db_path: str = "user_mappings.db"):
        """
        Initialize mapping database.

        Args:
            db_path: Path to SQLite database file
                     For Cloud Functions, use '/tmp/user_mappings.db'
                     For local dev, use 'user_mappings.db'
        """
        self.db_path = db_path
        self.init_database()

    def init_database(self):
        """Create tables if they don't exist."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_mappings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                harvest_user_id TEXT NOT NULL UNIQUE,
                harvest_email TEXT,
                harvest_name TEXT,
                deel_contract_id TEXT NOT NULL,
                deel_email TEXT,
                deel_name TEXT,
                match_method TEXT NOT NULL,
                confidence_score REAL NOT NULL,
                match_signals TEXT,
                verification_status TEXT DEFAULT 'auto_matched',
                verified_by TEXT,
                verified_at TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                is_active INTEGER DEFAULT 1
            )
        ''')

        # Index for fast lookups
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_harvest_user_id 
            ON user_mappings(harvest_user_id)
        ''')

        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_deel_contract_id 
            ON user_mappings(deel_contract_id)
        ''')

        conn.commit()
        conn.close()

    def create_mapping(self, harvest_user_id: str, harvest_email: str, harvest_name: str,
                       deel_contract_id: str, deel_email: str, deel_name: str,
                       match_method: str, confidence_score: float,
                       match_signals: Dict, verification_status: str = 'auto_matched'):
        """Create or update a user mapping."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute('''
            INSERT OR REPLACE INTO user_mappings 
            (harvest_user_id, harvest_email, harvest_name, deel_contract_id, 
             deel_email, deel_name, match_method, confidence_score, match_signals, 
             verification_status, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            str(harvest_user_id), harvest_email, harvest_name,
            deel_contract_id, deel_email, deel_name,
            match_method, confidence_score, json.dumps(match_signals),
            verification_status, datetime.now(UTC).isoformat()
        ))

        conn.commit()
        conn.close()

    def get_deel_contract_by_harvest_id(self, harvest_user_id: str) -> Optional[str]:
        """Get Deel contract ID for a given Harvest user ID."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute('''
            SELECT deel_contract_id FROM user_mappings 
            WHERE harvest_user_id = ? AND is_active = 1
        ''', (str(harvest_user_id),))

        result = cursor.fetchone()
        conn.close()

        return result[0] if result else None

    def get_pending_reviews(self) -> List[Dict]:
        """Get all mappings that need manual review."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute('''
            SELECT * FROM user_mappings 
            WHERE verification_status = 'needs_review' AND is_active = 1
            ORDER BY confidence_score DESC
        ''')

        results = [dict(row) for row in cursor.fetchall()]
        conn.close()

        return results

    def verify_mapping(self, harvest_user_id: str, approved: bool, verified_by: str):
        """Manually verify or reject a mapping."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        if approved:
            cursor.execute('''
                UPDATE user_mappings 
                SET verification_status = 'human_verified',
                    verified_by = ?,
                    verified_at = ?,
                    updated_at = ?
                WHERE harvest_user_id = ?
            ''', (verified_by, datetime.now(UTC).isoformat(),
                  datetime.now(UTC).isoformat(), str(harvest_user_id)))
        else:
            cursor.execute('''
                UPDATE user_mappings 
                SET verification_status = 'human_rejected',
                    is_active = 0,
                    verified_by = ?,
                    verified_at = ?,
                    updated_at = ?
                WHERE harvest_user_id = ?
            ''', (verified_by, datetime.now(UTC).isoformat(),
                  datetime.now(UTC).isoformat(), str(harvest_user_id)))

        conn.commit()
        conn.close()

    def get_all_mappings(self) -> List[Dict]:
        """Get all active mappings."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute('''
            SELECT * FROM user_mappings 
            WHERE is_active = 1
            ORDER BY created_at DESC
        ''')

        results = [dict(row) for row in cursor.fetchall()]
        conn.close()

        return results
