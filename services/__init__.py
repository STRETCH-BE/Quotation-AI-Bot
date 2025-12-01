"""
Services module initialization
Fixed imports for all service classes
"""
from .ai_chat import EnhancedAIChatManager
from .email_sender import EntraIDEmailSender  # Fixed: was EmailSender
from .pdf_generator import ImprovedStretchQuotePDFGenerator  # Fixed: was PDFGenerator
from .cost_calculator import CostCalculator  # Fixed: was from .calculator

# Removed WhatsApp as the file doesn't exist
# If you need WhatsApp, create a whatsapp.py file with WhatsAppIntegration class

__all__ = [
    'EnhancedAIChatManager',
    'EntraIDEmailSender',
    'ImprovedStretchQuotePDFGenerator',
    'CostCalculator'
]