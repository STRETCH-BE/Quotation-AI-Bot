"""
email_session_mixin.py
STRETCH Bot — Email Session DB Methods  v1.2

CHANGE v1.1: Added get_email_session_by_quote_number() as a fallback
lookup for customer replies where the Graph conversationId doesn't match.

CHANGE v1.2: get_email_session_by_quote_number() now also includes
'confirmed' in the status filter so customers can keep replying after
their intent (site_visit, acceptance) has been acknowledged.
"""

import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


class EmailSessionMixin:
    """
    Mixin providing email_quote_sessions table operations.
    Requires self.execute_query() and self.get_connection()
    from EnhancedDatabaseManager.
    """

    # ─────────────────────────────────────────────────────────────────────────
    #  Create
    # ─────────────────────────────────────────────────────────────────────────

    def create_email_session(
        self,
        conversation_id: str,
        message_id: str,
        sender_email: str,
        sender_name: str,
        client_group: str,
        original_message: str,
        received_at,
    ) -> Optional[int]:
        """
        Insert a new email_quote_sessions row (status='processing').
        Returns the new row id, or None on failure.
        """
        try:
            connection = self.get_connection()
            if not connection:
                return None
            cursor = connection.cursor()
            cursor.execute(
                """
                INSERT INTO email_quote_sessions (
                    conversation_id, message_id,
                    sender_email, sender_name,
                    client_group, original_message,
                    status, received_at,
                    parsed_data, assumed_data
                ) VALUES (
                    %s, %s,
                    %s, %s,
                    %s, %s,
                    'processing', %s,
                    '{}', '[]'
                )
                """,
                (
                    conversation_id, message_id,
                    sender_email, sender_name,
                    client_group, original_message,
                    received_at,
                ),
            )
            session_id = cursor.lastrowid
            connection.commit()
            logger.info(
                f"✅ email_quote_session created: id={session_id}, "
                f"conv={conversation_id[:30]}"
            )
            return session_id
        except Exception as e:
            logger.error(f"❌ create_email_session: {e}")
            return None
        finally:
            try:
                cursor.close()
                connection.close()
            except Exception:
                pass

    # ─────────────────────────────────────────────────────────────────────────
    #  Read
    # ─────────────────────────────────────────────────────────────────────────

    def get_email_session_by_conversation(
        self, conversation_id: str
    ) -> Optional[Dict]:
        """
        Look up the most recent session for a Graph conversationId.
        Returns the row as a dict (JSON fields parsed), or None.
        """
        rows = self.execute_query(
            """
            SELECT *
            FROM email_quote_sessions
            WHERE conversation_id = %s
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (conversation_id,),
            fetch=True,
        )
        if not rows:
            return None
        return self._parse_email_session_row(rows[0])

    def get_email_session_by_id(self, session_id: int) -> Optional[Dict]:
        """Fetch one email_quote_sessions row by primary key."""
        rows = self.execute_query(
            "SELECT * FROM email_quote_sessions WHERE id = %s",
            (session_id,),
            fetch=True,
        )
        if not rows:
            return None
        return self._parse_email_session_row(rows[0])

    def get_email_session_by_quote_number(
        self, quote_number: str
    ) -> Optional[Dict]:
        """
        Fallback lookup by quote_number when conversationId doesn't match.

        This happens when a customer replies from a different email client
        (e.g. mobile app, forwarded email) that starts a new Graph conversation
        thread instead of continuing the original one.

        We extract the QT number from the subject line (e.g. 'Re: Uw Offerte
        Rekplafond – QT20260322BAB9B1C3') and use it to find the session.

        Only returns sessions in actionable states (quote_sent, awaiting_reply,
        revised) — ignores completed or failed sessions.
        """
        rows = self.execute_query(
            """
            SELECT *
            FROM email_quote_sessions
            WHERE quote_number = %s
            AND status IN ('quote_sent', 'awaiting_reply', 'revised', 'confirmed')
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (quote_number,),
            fetch=True,
        )
        if not rows:
            return None
        return self._parse_email_session_row(rows[0])

    def get_email_sessions(
        self,
        status: Optional[str] = None,
        sender_email: Optional[str] = None,
        limit: int = 50,
    ) -> List[Dict]:
        """
        List sessions with optional status / sender_email filters.
        Useful for admin dashboard or reporting.
        """
        query  = "SELECT * FROM email_quote_sessions WHERE 1=1"
        params = []
        if status:
            query  += " AND status = %s"
            params.append(status)
        if sender_email:
            query  += " AND sender_email = %s"
            params.append(sender_email)
        query += " ORDER BY created_at DESC LIMIT %s"
        params.append(limit)

        rows = self.execute_query(query, tuple(params), fetch=True) or []
        return [self._parse_email_session_row(r) for r in rows]

    # ─────────────────────────────────────────────────────────────────────────
    #  Update
    # ─────────────────────────────────────────────────────────────────────────

    def update_email_session(self, session_id: int, updates: Dict) -> bool:
        """
        Partial update of any columns in email_quote_sessions.
        Pass a dict of {column_name: value}.
        JSON-serialisable values in JSON_COLUMNS are auto-converted.

        Example:
            self.db.update_email_session(session_id, {
                "status":       "quote_sent",
                "quotation_id": 42,
                "total_price":  1245.00,
            })
        """
        if not updates:
            return True

        JSON_COLUMNS = {"parsed_data", "assumed_data", "missing_fields"}
        # Columns that accept None without complaint
        NULLABLE = {
            "quote_sent_at", "processed_at", "error_message",
            "pdf_path", "last_reply_message_id", "last_customer_message_id",
        }

        set_clauses = []
        values      = []

        for col, val in updates.items():
            if val is None and col not in NULLABLE:
                continue
            if col in JSON_COLUMNS and not isinstance(val, str):
                val = json.dumps(val, default=str)
            set_clauses.append(f"`{col}` = %s")
            values.append(val)

        if not set_clauses:
            return True

        values.append(session_id)
        query = (
            f"UPDATE email_quote_sessions "
            f"SET {', '.join(set_clauses)} "
            f"WHERE id = %s"
        )
        result = self.execute_query(query, tuple(values))
        return bool(result)

    # ─────────────────────────────────────────────────────────────────────────
    #  Maintenance
    # ─────────────────────────────────────────────────────────────────────────

    def expire_stale_email_sessions(self, ttl_days: int = 7) -> int:
        """
        Mark sessions stuck in 'awaiting_reply' for > ttl_days as 'confirmed'.
        Call once daily.  Returns number of rows updated.
        """
        result = self.execute_query(
            """
            UPDATE email_quote_sessions
            SET status = 'confirmed'
            WHERE status IN ('quote_sent', 'awaiting_reply')
            AND last_activity_at < DATE_SUB(NOW(), INTERVAL %s DAY)
            """,
            (ttl_days,),
        )
        count = result if isinstance(result, int) else 0
        if count:
            logger.info(f"✅ Expired {count} stale email_quote_session(s)")
        return count

    # ─────────────────────────────────────────────────────────────────────────
    #  Internal helper
    # ─────────────────────────────────────────────────────────────────────────

    def _parse_email_session_row(self, row: Dict) -> Dict:
        """Parse JSON string columns back to Python objects."""
        for col in ("parsed_data", "assumed_data", "missing_fields"):
            if row.get(col) and isinstance(row[col], str):
                try:
                    row[col] = json.loads(row[col])
                except Exception:
                    row[col] = {} if col == "parsed_data" else []
        return row
