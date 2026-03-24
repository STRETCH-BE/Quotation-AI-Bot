"""
services/mail/__init__.py
STRETCH Bot — Email Quote Flow package

Exposes all classes needed by bot.py and manager.py.

Files in this package:
    email_quote_processor.py  — AI parsing, assumption engine, cost calculation
    email_reply_builder.py    — Branded HTML reply builder (NL/FR/EN)
    email_listener.py         — Graph API inbox poller + orchestrator
    email_session_mixin.py    — DB methods mixed into EnhancedDatabaseManager
"""

from .email_quote_processor import EmailQuoteProcessor, EmailQuoteResult, Assumption
from .email_reply_builder import EmailReplyBuilder
from .email_listener import EmailListener
from .email_session_mixin import EmailSessionMixin

__all__ = [
    "EmailQuoteProcessor",
    "EmailQuoteResult",
    "Assumption",
    "EmailReplyBuilder",
    "EmailListener",
    "EmailSessionMixin",
]
