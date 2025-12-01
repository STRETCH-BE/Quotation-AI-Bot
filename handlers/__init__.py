"""
Handlers module initialization
Fixed imports for all handler classes
Includes CustomerSelectionHandler for quote customer management
"""
from .quote_flow import EnhancedMultiCeilingQuoteFlow
from .quote_editor import QuoteEditor
from .conversational import ConversationalBotHandler
from .admin_messaging import AdminMessagingSystem
from .customer_selection import CustomerSelectionHandler, CustomerState

# Import new handlers if they exist
try:
    from .user_onboarding import UserOnboardingHandler
except ImportError:
    UserOnboardingHandler = None
    print("Warning: UserOnboardingHandler not found")

try:
    from .admin_user_management import AdminUserManagement
except ImportError:
    AdminUserManagement = None
    print("Warning: AdminUserManagement not found")

try:
    from .dynamics365_integration import Dynamics365IntegrationHandler
except ImportError:
    Dynamics365IntegrationHandler = None
    print("Warning: Dynamics365IntegrationHandler not found")

__all__ = [
    'EnhancedMultiCeilingQuoteFlow',
    'QuoteEditor',
    'ConversationalBotHandler',
    'AdminMessagingSystem',
    'CustomerSelectionHandler',
    'CustomerState',
    'UserOnboardingHandler',
    'AdminUserManagement',
    'Dynamics365IntegrationHandler'
]