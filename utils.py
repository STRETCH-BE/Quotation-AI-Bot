"""
Utility functions for Stretch Ceiling Bot
ENHANCED: Better decimal and JSON serialization handling
"""
import re
import json
import logging
from decimal import Decimal
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Any, Optional  # example

logger = logging.getLogger(__name__)

def escape_markdown(text: str) -> str:
    """Escape special characters for Telegram Markdown parsing"""
    if text is None:
        return ""
    
    # Convert to string if not already
    text = str(text)
    
    # List of characters that need to be escaped in Markdown V1
    # Order matters - escape backslash first
    special_chars = ["\\", "*", "_", "[", "]", "(", ")", "~", "`", ">", "#", "+", "-", "=", "|", "{", "}", ".", "!"]
    
    # Escape each special character
    for char in special_chars:
        text = text.replace(char, f"\\{char}")
    
    return text

def serialize_for_json(obj):
    """Convert objects to JSON-serializable format with better decimal handling"""
    if isinstance(obj, Decimal):
        # Convert to float, handling None and special values
        return float(obj) if obj is not None else 0.0
    elif isinstance(obj, (datetime, timedelta)):
        return obj.isoformat()
    elif hasattr(obj, "__dict__"):
        return {k: serialize_for_json(v) for k, v in obj.__dict__.items()}
    elif isinstance(obj, dict):
        return {k: serialize_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, (list, tuple)):
        return [serialize_for_json(item) for item in obj]
    else:
        return obj

class DecimalEncoder(json.JSONEncoder):
    """JSON encoder that handles Decimal types"""
    def default(self, o):
        if isinstance(o, Decimal):
            return float(o)
        if isinstance(o, datetime):
            return o.isoformat()
        if isinstance(o, timedelta):
            return str(o)
        return super(DecimalEncoder, self).default(o)

def safe_json_dumps(obj: Any, **kwargs) -> str:
    """Safely dump object to JSON with decimal handling"""
    return json.dumps(obj, cls=DecimalEncoder, **kwargs)

def safe_json_loads(json_str: str) -> Any:
    """Safely load JSON with decimal handling"""
    if not json_str:
        return {}
    try:
        return json.loads(json_str)
    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error: {e}")
        return {}

def parse_dimensions(text: str) -> Tuple[float, float]:
    """Parse dimension input (e.g., '5.5 x 4.2' or '5.5m × 4.2m')"""
    pattern = r"(\d+(?:\.\d+)?)\s*[mM]?\s*[xX×]\s*(\d+(?:\.\d+)?)"
    match = re.search(pattern, text.replace(",", "."))
    if match:
        return float(match.group(1)), float(match.group(2))
    raise ValueError("Invalid dimension format")

def format_price(price: float) -> str:
    """Format price for display"""
    if isinstance(price, Decimal):
        price = float(price)
    return f"€{price:.2f}"

def clean_phone_number(phone: str) -> str:
    """Clean and normalize phone number"""
    # Remove all non-digit characters except +
    cleaned = re.sub(r'[^\d+]', '', phone)
    return cleaned

def validate_email(email: str) -> bool:
    """Validate email format"""
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return bool(re.match(pattern, email))

def validate_vat_number(vat: str) -> bool:
    """Basic VAT number validation"""
    # Remove spaces and convert to uppercase
    vat = vat.upper().replace(' ', '')
    
    # Basic check - at least 8 characters
    if len(vat) < 8:
        return False
    
    # Could add country-specific validation here
    return True