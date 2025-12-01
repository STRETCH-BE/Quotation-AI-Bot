"""
Customer Selection Handler for Stretch Ceiling Bot
Handles new and existing customer selection for quotes
Integrates with Dynamics 365 for account/contact management

FIXED VERSION: Properly interfaces with quote_flow.py
- handle_message accepts CustomerState enum
- Returns dict with {"complete": True, "customer_data": ...} or {"next_state": CustomerState.XXX}
"""
import logging
import re
from enum import Enum
from typing import Dict, List, Optional, Any
from datetime import datetime

from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes, ConversationHandler

from config import Config

logger = logging.getLogger(__name__)


class CustomerState(Enum):
    """States for customer selection flow - MUST match quote_flow.py mapping"""
    CUSTOMER_TYPE = "customer_type"
    
    # New customer states - names match quote_flow.py expectations
    NEW_CUSTOMER_COMPANY = "new_customer_company"
    NEW_CUSTOMER_VAT = "new_customer_vat"
    NEW_CUSTOMER_CONTACT = "new_customer_contact"
    NEW_CUSTOMER_ADDRESS = "new_customer_address"
    NEW_CUSTOMER_PHONE = "new_customer_phone"
    NEW_CUSTOMER_EMAIL = "new_customer_email"
    NEW_CUSTOMER_LEAD_SOURCE = "new_customer_lead_source"
    NEW_CUSTOMER_CONFIRM = "new_customer_confirm"
    
    # Existing customer states
    EXISTING_CUSTOMER_SEARCH = "existing_customer_search"
    EXISTING_CUSTOMER_SELECT = "existing_customer_select"
    EXISTING_CONTACT_SELECT = "existing_contact_select"
    
    # New contact under existing account
    NEW_CONTACT_NAME = "new_contact_name"
    NEW_CONTACT_PHONE = "new_contact_phone"
    NEW_CONTACT_EMAIL = "new_contact_email"
    
    # Email selection
    EMAIL_SELECTION = "email_selection"
    CUSTOM_EMAIL_INPUT = "custom_email_input"


class CustomerSelectionHandler:
    """Handles customer selection for quotes with Dynamics 365 integration"""
    
    LEAD_SOURCES = [
        "Website", "Referral", "Trade Show", "Social Media",
        "Cold Call", "Partner", "Advertisement", "Other"
    ]
    
    def __init__(self, db_manager, dynamics_service=None):
        self.db = db_manager
        self.dynamics = dynamics_service
        self.customer_sessions = {}
    
    def set_dynamics_service(self, dynamics_service):
        """Set Dynamics 365 service (called after initialization)"""
        self.dynamics = dynamics_service
    
    def _ensure_session(self, user_id: int) -> dict:
        """Ensure customer session exists for user"""
        if user_id not in self.customer_sessions:
            logger.info(f"Creating new customer session for user {user_id}")
            self.customer_sessions[user_id] = {
                'type': None,
                'data': {},
                'selected_account': None,
                'selected_contact': None,
                'search_results': [],
                'new_contact': {}
            }
        return self.customer_sessions[user_id]
    
    def _make_result(self, next_state: CustomerState = None, complete: bool = False, 
                     customer_data: dict = None) -> dict:
        """Helper to create properly formatted return dict"""
        if complete:
            return {"complete": True, "customer_data": customer_data or {}}
        elif next_state:
            return {"next_state": next_state}
        return {}
    
    # ==================== MAIN MESSAGE HANDLER ====================
    
    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE,
                            state, message_text: str, session_data: dict) -> dict:
        """
        Main message handler - routes to appropriate handler based on current state.
        
        Args:
            update: Telegram update
            context: Telegram context  
            state: CustomerState enum (passed from quote_flow.py)
            message_text: The message text
            session_data: Full session data dict
            
        Returns:
            dict with either:
            - {"complete": True, "customer_data": {...}} when customer selection is done
            - {"next_state": CustomerState.XXX} for state transitions
        """
        user_id = update.effective_user.id
        self._ensure_session(user_id)
        
        # Handle both enum and string state (for flexibility)
        if isinstance(state, CustomerState):
            current_state = state
        elif isinstance(state, str):
            # Try to match by value
            current_state = None
            for s in CustomerState:
                if s.value == state or s.name == state:
                    current_state = s
                    break
            if not current_state:
                logger.warning(f"Could not match state string: {state}")
                return await self._restart_customer_selection(update, context)
        else:
            logger.warning(f"Invalid state type: {type(state)}")
            return await self._restart_customer_selection(update, context)
        
        logger.info(f"CustomerSelection handling state: {current_state.name} for user {user_id}")
        
        # Route to appropriate handler
        try:
            if current_state == CustomerState.CUSTOMER_TYPE:
                return await self._handle_customer_type(update, context, session_data)
            
            # New customer flow
            elif current_state == CustomerState.NEW_CUSTOMER_COMPANY:
                return await self._handle_company_name(update, context, session_data)
            elif current_state == CustomerState.NEW_CUSTOMER_VAT:
                return await self._handle_vat(update, context, session_data)
            elif current_state == CustomerState.NEW_CUSTOMER_CONTACT:
                return await self._handle_contact_name(update, context, session_data)
            elif current_state == CustomerState.NEW_CUSTOMER_ADDRESS:
                return await self._handle_address(update, context, session_data)
            elif current_state == CustomerState.NEW_CUSTOMER_PHONE:
                return await self._handle_phone(update, context, session_data)
            elif current_state == CustomerState.NEW_CUSTOMER_EMAIL:
                return await self._handle_email(update, context, session_data)
            elif current_state == CustomerState.NEW_CUSTOMER_LEAD_SOURCE:
                return await self._handle_lead_source(update, context, session_data)
            elif current_state == CustomerState.NEW_CUSTOMER_CONFIRM:
                return await self._handle_new_customer_confirmation(update, context, session_data)
            
            # Existing customer flow
            elif current_state == CustomerState.EXISTING_CUSTOMER_SEARCH:
                return await self._handle_search(update, context, session_data)
            elif current_state == CustomerState.EXISTING_CUSTOMER_SELECT:
                return await self._handle_account_selection(update, context, session_data)
            elif current_state == CustomerState.EXISTING_CONTACT_SELECT:
                return await self._handle_contact_selection(update, context, session_data)
            
            # New contact under existing account
            elif current_state == CustomerState.NEW_CONTACT_NAME:
                return await self._handle_new_contact_name(update, context, session_data)
            elif current_state == CustomerState.NEW_CONTACT_PHONE:
                return await self._handle_new_contact_phone(update, context, session_data)
            elif current_state == CustomerState.NEW_CONTACT_EMAIL:
                return await self._handle_new_contact_email(update, context, session_data)
            
            # Email selection
            elif current_state == CustomerState.EMAIL_SELECTION:
                return await self._handle_email_selection(update, context, session_data)
            elif current_state == CustomerState.CUSTOM_EMAIL_INPUT:
                return await self._handle_custom_email(update, context, session_data)
            
            else:
                logger.warning(f"Unhandled customer state: {current_state}")
                return await self._restart_customer_selection(update, context)
                
        except Exception as e:
            logger.error(f"Error in customer selection handler: {e}")
            import traceback
            logger.error(traceback.format_exc())
            await update.message.reply_text(
                "❌ An error occurred. Please try again.",
                reply_markup=ReplyKeyboardRemove()
            )
            return await self._restart_customer_selection(update, context)
    
    async def _restart_customer_selection(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> dict:
        """Restart customer selection flow"""
        await self.start_customer_selection(update, context)
        return self._make_result(next_state=CustomerState.CUSTOMER_TYPE)
    
    # ==================== START FLOW ====================
    
    async def start_customer_selection(self, update: Update, context: ContextTypes.DEFAULT_TYPE, 
                                       session_data: dict = None) -> str:
        """Start customer selection flow
        
        Args:
            update: Telegram update
            context: Bot context
            session_data: Quote session data (optional, for future use)
        """
        user_id = update.effective_user.id
        self._ensure_session(user_id)
        
        keyboard = [
            ["🆕 New Customer"],
            ["📋 Existing Customer"],
            ["⬅️ Back"]
        ]
        reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
        
        await update.message.reply_text(
            "👤 **Customer Selection**\n\n"
            "Is this quote for a new or existing customer?\n\n"
            "• **New Customer** - Create a new account/contact\n"
            "• **Existing Customer** - Search and select from CRM",
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )
        
        return CustomerState.CUSTOMER_TYPE.value
    
    # ==================== CUSTOMER TYPE HANDLER ====================
    
    async def _handle_customer_type(self, update: Update, context: ContextTypes.DEFAULT_TYPE, 
                                    session_data: dict) -> dict:
        """Handle new vs existing customer selection"""
        user_id = update.effective_user.id
        message_text = update.message.text
        
        if message_text == "🆕 New Customer":
            self.customer_sessions[user_id]['type'] = 'new'
            await self._ask_company_name(update, context)
            return self._make_result(next_state=CustomerState.NEW_CUSTOMER_COMPANY)
        
        elif message_text == "📋 Existing Customer":
            self.customer_sessions[user_id]['type'] = 'existing'
            await self._ask_search(update, context)
            return self._make_result(next_state=CustomerState.EXISTING_CUSTOMER_SEARCH)
        
        elif message_text == "⬅️ Back":
            return {"back_to_client_group": True}
        
        else:
            await update.message.reply_text("Please select a valid option.")
            return self._make_result(next_state=CustomerState.CUSTOMER_TYPE)
    
    # ==================== NEW CUSTOMER FLOW ====================
    
    async def _ask_company_name(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Ask for company name"""
        keyboard = [["Skip (Private Customer)"], ["⬅️ Back"]]
        reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
        
        await update.message.reply_text(
            "🏢 **Company Information**\n\n"
            "Enter the company name:\n\n"
            "_(Skip if this is a private customer)_",
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )
    
    async def _handle_company_name(self, update: Update, context: ContextTypes.DEFAULT_TYPE,
                                   session_data: dict) -> dict:
        """Handle company name input"""
        user_id = update.effective_user.id
        message_text = update.message.text.strip()
        session = self._ensure_session(user_id)
        
        if message_text == "⬅️ Back":
            await self.start_customer_selection(update, context)
            return self._make_result(next_state=CustomerState.CUSTOMER_TYPE)
        
        if message_text == "Skip (Private Customer)":
            session['data']['company_name'] = None
            session['data']['is_company'] = False
        else:
            session['data']['company_name'] = message_text
            session['data']['is_company'] = True
        
        await self._ask_vat(update, context, session['data'].get('is_company', False))
        return self._make_result(next_state=CustomerState.NEW_CUSTOMER_VAT)
    
    async def _ask_vat(self, update: Update, context: ContextTypes.DEFAULT_TYPE, is_company: bool):
        """Ask for VAT number"""
        keyboard = [["Skip VAT"], ["⬅️ Back"]]
        reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
        
        if is_company:
            prompt = "🔢 **VAT Number**\n\nEnter the VAT number (e.g., BE0123456789):"
        else:
            prompt = "🔢 **VAT Number** (Optional)\n\nEnter VAT number if applicable, or skip:"
        
        await update.message.reply_text(prompt, reply_markup=reply_markup, parse_mode="Markdown")
    
    async def _handle_vat(self, update: Update, context: ContextTypes.DEFAULT_TYPE,
                         session_data: dict) -> dict:
        """Handle VAT number input"""
        user_id = update.effective_user.id
        message_text = update.message.text.strip()
        session = self._ensure_session(user_id)
        
        if message_text == "⬅️ Back":
            await self._ask_company_name(update, context)
            return self._make_result(next_state=CustomerState.NEW_CUSTOMER_COMPANY)
        
        if message_text == "Skip VAT":
            session['data']['vat_number'] = None
        else:
            # Clean and validate VAT
            vat_cleaned = re.sub(r'[^A-Z0-9]', '', message_text.upper())
            session['data']['vat_number'] = vat_cleaned
        
        await self._ask_contact_name(update, context, session['data'].get('is_company', False))
        return self._make_result(next_state=CustomerState.NEW_CUSTOMER_CONTACT)
    
    async def _ask_contact_name(self, update: Update, context: ContextTypes.DEFAULT_TYPE, is_company: bool):
        """Ask for contact name"""
        keyboard = [["⬅️ Back"]]
        reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
        
        if is_company:
            prompt = "👤 **Contact Person**\n\nEnter the contact person's full name:"
        else:
            prompt = "👤 **Customer Name**\n\nEnter the customer's full name:"
        
        await update.message.reply_text(prompt, reply_markup=reply_markup, parse_mode="Markdown")
    
    async def _handle_contact_name(self, update: Update, context: ContextTypes.DEFAULT_TYPE,
                                   session_data: dict) -> dict:
        """Handle contact name input"""
        user_id = update.effective_user.id
        message_text = update.message.text.strip()
        session = self._ensure_session(user_id)
        
        if message_text == "⬅️ Back":
            await self._ask_vat(update, context, session['data'].get('is_company', False))
            return self._make_result(next_state=CustomerState.NEW_CUSTOMER_VAT)
        
        # Parse name
        name_parts = message_text.split(' ', 1)
        session['data']['first_name'] = name_parts[0]
        session['data']['last_name'] = name_parts[1] if len(name_parts) > 1 else ''
        session['data']['full_name'] = message_text
        
        await self._ask_address(update, context)
        return self._make_result(next_state=CustomerState.NEW_CUSTOMER_ADDRESS)
    
    async def _ask_address(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Ask for address"""
        keyboard = [["Skip Address"], ["⬅️ Back"]]
        reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
        
        await update.message.reply_text(
            "📍 **Address**\n\nEnter the customer's address:",
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )
    
    async def _handle_address(self, update: Update, context: ContextTypes.DEFAULT_TYPE,
                             session_data: dict) -> dict:
        """Handle address input"""
        user_id = update.effective_user.id
        message_text = update.message.text.strip()
        session = self._ensure_session(user_id)
        
        if message_text == "⬅️ Back":
            await self._ask_contact_name(update, context, session['data'].get('is_company', False))
            return self._make_result(next_state=CustomerState.NEW_CUSTOMER_CONTACT)
        
        if message_text == "Skip Address":
            session['data']['address'] = None
        else:
            session['data']['address'] = message_text
        
        await self._ask_phone(update, context)
        return self._make_result(next_state=CustomerState.NEW_CUSTOMER_PHONE)
    
    async def _ask_phone(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Ask for phone number"""
        keyboard = [["Skip Phone"], ["⬅️ Back"]]
        reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
        
        await update.message.reply_text(
            "📞 **Phone Number**\n\nEnter the customer's phone number:",
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )
    
    async def _handle_phone(self, update: Update, context: ContextTypes.DEFAULT_TYPE,
                           session_data: dict) -> dict:
        """Handle phone input"""
        user_id = update.effective_user.id
        message_text = update.message.text.strip()
        session = self._ensure_session(user_id)
        
        if message_text == "⬅️ Back":
            await self._ask_address(update, context)
            return self._make_result(next_state=CustomerState.NEW_CUSTOMER_ADDRESS)
        
        if message_text == "Skip Phone":
            session['data']['phone'] = None
        else:
            phone_cleaned = re.sub(r'[^\d+]', '', message_text)
            session['data']['phone'] = phone_cleaned
        
        await self._ask_email(update, context)
        return self._make_result(next_state=CustomerState.NEW_CUSTOMER_EMAIL)
    
    async def _ask_email(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Ask for email"""
        keyboard = [["Skip Email"], ["⬅️ Back"]]
        reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
        
        await update.message.reply_text(
            "📧 **Email Address**\n\nEnter the customer's email:",
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )
    
    async def _handle_email(self, update: Update, context: ContextTypes.DEFAULT_TYPE,
                           session_data: dict) -> dict:
        """Handle email input"""
        user_id = update.effective_user.id
        message_text = update.message.text.strip()
        session = self._ensure_session(user_id)
        
        if message_text == "⬅️ Back":
            await self._ask_phone(update, context)
            return self._make_result(next_state=CustomerState.NEW_CUSTOMER_PHONE)
        
        if message_text == "Skip Email":
            session['data']['email'] = None
        else:
            # Simple email validation
            if '@' in message_text and '.' in message_text:
                session['data']['email'] = message_text.lower()
            else:
                await update.message.reply_text("Please enter a valid email address.")
                return self._make_result(next_state=CustomerState.NEW_CUSTOMER_EMAIL)
        
        await self._ask_lead_source(update, context)
        return self._make_result(next_state=CustomerState.NEW_CUSTOMER_LEAD_SOURCE)
    
    async def _ask_lead_source(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Ask for lead source"""
        keyboard = [[src] for src in self.LEAD_SOURCES[:4]]
        keyboard.append(self.LEAD_SOURCES[4:])
        keyboard.append(["⬅️ Back"])
        reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
        
        await update.message.reply_text(
            "📊 **Lead Source**\n\nHow did this customer find you?",
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )
    
    async def _handle_lead_source(self, update: Update, context: ContextTypes.DEFAULT_TYPE,
                                  session_data: dict) -> dict:
        """Handle lead source selection"""
        user_id = update.effective_user.id
        message_text = update.message.text.strip()
        session = self._ensure_session(user_id)
        
        if message_text == "⬅️ Back":
            await self._ask_email(update, context)
            return self._make_result(next_state=CustomerState.NEW_CUSTOMER_EMAIL)
        
        if message_text in self.LEAD_SOURCES:
            session['data']['lead_source'] = message_text
        else:
            session['data']['lead_source'] = "Other"
        
        await self._ask_confirmation(update, context, session['data'])
        return self._make_result(next_state=CustomerState.NEW_CUSTOMER_CONFIRM)
    
    async def _ask_confirmation(self, update: Update, context: ContextTypes.DEFAULT_TYPE, data: dict):
        """Show customer summary and ask for confirmation"""
        keyboard = [["✅ Confirm & Continue"], ["✏️ Edit"], ["❌ Cancel"]]
        reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
        
        lines = ["📋 **New Customer Summary**\n"]
        
        if data.get('company_name'):
            lines.append(f"🏢 Company: {data['company_name']}")
        if data.get('vat_number'):
            lines.append(f"🔢 VAT: {data['vat_number']}")
        if data.get('full_name'):
            lines.append(f"👤 Contact: {data['full_name']}")
        if data.get('address'):
            lines.append(f"📍 Address: {data['address']}")
        if data.get('phone'):
            lines.append(f"📞 Phone: {data['phone']}")
        if data.get('email'):
            lines.append(f"📧 Email: {data['email']}")
        if data.get('lead_source'):
            lines.append(f"📊 Source: {data['lead_source']}")
        
        await update.message.reply_text(
            "\n".join(lines),
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )
    
    async def _handle_new_customer_confirmation(self, update: Update, context: ContextTypes.DEFAULT_TYPE,
                                                session_data: dict) -> dict:
        """Handle confirmation of new customer"""
        user_id = update.effective_user.id
        message_text = update.message.text.strip()
        session = self._ensure_session(user_id)
        
        if message_text == "✅ Confirm & Continue":
            customer_data = session['data']
            
            try:
                # Create in Dynamics 365
                dynamics_ids = await self._create_customer_in_dynamics(customer_data)
                
                # Build customer info for session
                customer_info = {
                    'type': 'new',
                    'data': customer_data,
                    'dynamics_account_id': dynamics_ids.get('account_id') if dynamics_ids else None,
                    'dynamics_contact_id': dynamics_ids.get('contact_id') if dynamics_ids else None,
                    'display_name': customer_data.get('company_name') or customer_data.get('full_name'),
                    'contact_name': customer_data.get('full_name'),
                    'email': customer_data.get('email'),
                    'phone': customer_data.get('phone'),
                    'address': customer_data.get('address'),
                    'vat_number': customer_data.get('vat_number'),
                    'is_company': customer_data.get('is_company', False)
                }
                
                if dynamics_ids:
                    await update.message.reply_text(
                        "✅ Customer created and synced to CRM!",
                        reply_markup=ReplyKeyboardRemove()
                    )
                else:
                    await update.message.reply_text(
                        "✅ Customer saved. CRM sync pending.",
                        reply_markup=ReplyKeyboardRemove()
                    )
                
                # Clean up session
                if user_id in self.customer_sessions:
                    del self.customer_sessions[user_id]
                
                # Return complete with customer data
                return self._make_result(complete=True, customer_data=customer_info)
                
            except Exception as e:
                logger.error(f"Error creating customer: {e}")
                
                # Still complete but without Dynamics sync
                customer_info = {
                    'type': 'new',
                    'data': customer_data,
                    'display_name': customer_data.get('company_name') or customer_data.get('full_name'),
                    'contact_name': customer_data.get('full_name'),
                    'email': customer_data.get('email'),
                    'sync_pending': True
                }
                
                await update.message.reply_text(
                    "⚠️ Customer saved locally. CRM sync will retry later.",
                    reply_markup=ReplyKeyboardRemove()
                )
                
                if user_id in self.customer_sessions:
                    del self.customer_sessions[user_id]
                return self._make_result(complete=True, customer_data=customer_info)
        
        elif message_text == "✏️ Edit":
            await self._ask_company_name(update, context)
            return self._make_result(next_state=CustomerState.NEW_CUSTOMER_COMPANY)
        
        elif message_text == "❌ Cancel":
            if user_id in self.customer_sessions:
                del self.customer_sessions[user_id]
            return {"cancel": True}
        
        return self._make_result(next_state=CustomerState.NEW_CUSTOMER_CONFIRM)
    
    async def _create_customer_in_dynamics(self, customer_data: dict) -> Optional[Dict[str, str]]:
        """Create customer in Dynamics 365"""
        if not self.dynamics:
            logger.warning("Dynamics 365 service not available")
            return None
        
        result = {}
        
        try:
            is_company = customer_data.get('is_company', False)
            company_name = customer_data.get('company_name')
            
            logger.info(f"📊 Creating customer in Dynamics - is_company: {is_company}, company_name: {company_name}")
            
            # If company, create Account first
            if is_company and company_name:
                logger.info(f"🏢 Creating Account for company: {company_name}")
                account_data = {
                    'name': company_name,
                    'address': customer_data.get('address'),
                    'phone': customer_data.get('phone'),
                    'email': customer_data.get('email'),
                    'vat_number': customer_data.get('vat_number')
                }
                logger.info(f"📝 Account data: {account_data}")
                
                account_id = await self.dynamics.create_account(account_data)
                if account_id:
                    result['account_id'] = account_id
                    logger.info(f"✅ Created Dynamics account: {account_id}")
                else:
                    logger.warning(f"⚠️ Failed to create account for {company_name}")
            else:
                logger.info(f"👤 Creating Contact only (private customer)")
            
            # Create Contact
            contact_data = {
                'firstname': customer_data.get('first_name', ''),
                'lastname': customer_data.get('last_name', ''),
                'email': customer_data.get('email'),
                'phone': customer_data.get('phone'),
                'address': customer_data.get('address')
            }
            logger.info(f"📝 Contact data: {contact_data}")
            
            # Link to account if created
            if result.get('account_id'):
                contact_data['account_id'] = result['account_id']
                logger.info(f"🔗 Linking contact to account: {result['account_id']}")
            
            contact_id = await self.dynamics.create_contact(contact_data)
            if contact_id:
                result['contact_id'] = contact_id
                logger.info(f"✅ Created Dynamics contact: {contact_id}")
            else:
                logger.warning("⚠️ Failed to create contact")
            
            return result if result else None
            
        except Exception as e:
            logger.error(f"Error creating customer in Dynamics: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None
    
    # ==================== EXISTING CUSTOMER FLOW ====================
    
    async def _ask_search(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Ask for search term"""
        keyboard = [["⬅️ Back"]]
        reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
        
        await update.message.reply_text(
            "🔍 **Search Customer**\n\n"
            "Enter company name, contact name, email, or phone to search:",
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )
    
    async def _handle_search(self, update: Update, context: ContextTypes.DEFAULT_TYPE,
                            session_data: dict) -> dict:
        """Handle search input"""
        user_id = update.effective_user.id
        message_text = update.message.text.strip()
        session = self._ensure_session(user_id)
        
        if message_text == "⬅️ Back":
            await self.start_customer_selection(update, context)
            return self._make_result(next_state=CustomerState.CUSTOMER_TYPE)
        
        # Handle button presses
        if message_text == "🔄 Search Again":
            await self._ask_search(update, context)
            return self._make_result(next_state=CustomerState.EXISTING_CUSTOMER_SEARCH)
        
        if message_text == "🆕 Create New Customer":
            self.customer_sessions[user_id]['type'] = 'new'
            await self._ask_company_name(update, context)
            return self._make_result(next_state=CustomerState.NEW_CUSTOMER_COMPANY)
        
        if len(message_text) < 2:
            await update.message.reply_text("Please enter at least 2 characters to search.")
            return self._make_result(next_state=CustomerState.EXISTING_CUSTOMER_SEARCH)
        
        # Search Dynamics 365
        accounts = []
        contacts = []
        
        logger.info(f"🔍 Starting customer search for: '{message_text}'")
        
        if self.dynamics:
            try:
                logger.info(f"🔍 Calling dynamics.search_accounts...")
                accounts = await self.dynamics.search_accounts(message_text) or []
                logger.info(f"🔍 Accounts found: {len(accounts)}")
                
                logger.info(f"🔍 Calling dynamics.search_contacts...")
                contacts = await self.dynamics.search_contacts(message_text) or []
                logger.info(f"🔍 Contacts found: {len(contacts)}")
            except Exception as e:
                logger.error(f"Error searching Dynamics: {e}")
                import traceback
                logger.error(traceback.format_exc())
        else:
            logger.warning("⚠️ Dynamics service not available for search")
        
        if not accounts and not contacts:
            keyboard = [
                ["🔄 Search Again"],
                ["🆕 Create New Customer"],
                ["⬅️ Back"]
            ]
            reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
            
            await update.message.reply_text(
                f"❌ No results found for '{message_text}'.\n\n"
                "Would you like to search again or create a new customer?",
                reply_markup=reply_markup
            )
            return self._make_result(next_state=CustomerState.EXISTING_CUSTOMER_SEARCH)
        
        # Store results
        session['search_results'] = {'accounts': accounts, 'contacts': contacts}
        
        # Show results
        await self._show_search_results(update, accounts, contacts)
        return self._make_result(next_state=CustomerState.EXISTING_CUSTOMER_SELECT)
    
    async def _show_search_results(self, update: Update, accounts: List[Dict], contacts: List[Dict]):
        """Show search results"""
        keyboard = []
        
        # Add accounts
        for acc in accounts[:5]:
            name = acc.get('name', 'Unknown')
            keyboard.append([f"🏢 {name}"])
        
        # Add standalone contacts
        for con in contacts[:5]:
            name = con.get('fullname') or con.get('name', 'Unknown')
            if not con.get('parent_account_id'):
                keyboard.append([f"👤 {name}"])
        
        keyboard.append(["🔄 Search Again"])
        keyboard.append(["🆕 Create New Customer"])
        keyboard.append(["⬅️ Back"])
        
        reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
        
        await update.message.reply_text(
            f"🔍 **Search Results**\n\n"
            f"Found {len(accounts)} companies and {len(contacts)} contacts.\n"
            f"Select a customer:",
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )
    
    async def _handle_account_selection(self, update: Update, context: ContextTypes.DEFAULT_TYPE,
                                        session_data: dict) -> dict:
        """Handle account/contact selection from search results"""
        user_id = update.effective_user.id
        message_text = update.message.text.strip()
        session = self._ensure_session(user_id)
        
        if message_text == "⬅️ Back":
            await self._ask_search(update, context)
            return self._make_result(next_state=CustomerState.EXISTING_CUSTOMER_SEARCH)
        
        if message_text == "🔄 Search Again":
            await self._ask_search(update, context)
            return self._make_result(next_state=CustomerState.EXISTING_CUSTOMER_SEARCH)
        
        if message_text == "🆕 Create New Customer":
            session['type'] = 'new'
            await self._ask_company_name(update, context)
            return self._make_result(next_state=CustomerState.NEW_CUSTOMER_COMPANY)
        
        # Find selected item
        results = session.get('search_results', {})
        
        if message_text.startswith("🏢 "):
            # Account selected
            name = message_text[2:].strip()
            account = next((a for a in results.get('accounts', []) if a.get('name') == name), None)
            
            if account:
                session['selected_account'] = account
                logger.info(f"📋 Selected account stored: {account}")
                logger.info(f"📋 Account address: '{account.get('address', 'NONE')}'")
                
                # Get contacts for this account
                contacts = []
                if self.dynamics and account.get('accountid'):
                    try:
                        contacts = await self.dynamics.get_account_contacts(account['accountid']) or []
                    except Exception as e:
                        logger.error(f"Error getting account contacts: {e}")
                
                session['account_contacts'] = contacts
                
                if contacts:
                    await self._show_account_contacts(update, account, contacts)
                    return self._make_result(next_state=CustomerState.EXISTING_CONTACT_SELECT)
                else:
                    # No contacts - offer to create one
                    keyboard = [
                        ["➕ Add New Contact"],
                        ["✅ Use Account Only"],
                        ["⬅️ Back"]
                    ]
                    reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
                    
                    await update.message.reply_text(
                        f"🏢 **{account.get('name')}**\n\n"
                        f"No contacts found for this account.\n"
                        f"Would you like to add a contact?",
                        reply_markup=reply_markup,
                        parse_mode="Markdown"
                    )
                    return self._make_result(next_state=CustomerState.EXISTING_CONTACT_SELECT)
        
        elif message_text.startswith("👤 "):
            # Contact selected directly
            name = message_text[2:].strip()
            contact = next((c for c in results.get('contacts', []) 
                           if (c.get('fullname') or c.get('name')) == name), None)
            
            if contact:
                customer_info = {
                    'type': 'existing',
                    'dynamics_contact_id': contact.get('contactid') or contact.get('id'),
                    'dynamics_account_id': contact.get('parent_account_id'),
                    'display_name': contact.get('fullname') or contact.get('name'),
                    'contact_name': contact.get('fullname') or contact.get('name'),
                    'email': contact.get('email', ''),
                    'phone': contact.get('phone', ''),
                    'address': contact.get('address', ''),
                    'is_company': False
                }
                
                await update.message.reply_text(
                    f"✅ Selected: {customer_info['display_name']}",
                    reply_markup=ReplyKeyboardRemove()
                )
                
                if user_id in self.customer_sessions:
                    del self.customer_sessions[user_id]
                return self._make_result(complete=True, customer_data=customer_info)
        
        await update.message.reply_text("Please select a valid option.")
        return self._make_result(next_state=CustomerState.EXISTING_CUSTOMER_SELECT)
    
    async def _show_account_contacts(self, update: Update, account: dict, contacts: List[Dict]):
        """Show contacts for selected account"""
        keyboard = []
        
        for con in contacts[:10]:
            name = con.get('fullname') or con.get('name', 'Unknown')
            keyboard.append([f"👤 {name}"])
        
        keyboard.append(["➕ Add New Contact"])
        keyboard.append(["⬅️ Back"])
        
        reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
        
        await update.message.reply_text(
            f"🏢 **{account.get('name')}**\n\n"
            f"Select a contact person or add a new one:",
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )
    
    async def _handle_contact_selection(self, update: Update, context: ContextTypes.DEFAULT_TYPE,
                                        session_data: dict) -> dict:
        """Handle contact selection for account"""
        user_id = update.effective_user.id
        message_text = update.message.text.strip()
        session = self._ensure_session(user_id)
        
        if message_text == "⬅️ Back":
            results = session.get('search_results', {})
            if results:
                await self._show_search_results(update, results.get('accounts', []), results.get('contacts', []))
                return self._make_result(next_state=CustomerState.EXISTING_CUSTOMER_SELECT)
            await self._ask_search(update, context)
            return self._make_result(next_state=CustomerState.EXISTING_CUSTOMER_SEARCH)
        
        if message_text == "➕ Add New Contact":
            session['new_contact'] = {}
            await self._ask_new_contact_name(update, context)
            return self._make_result(next_state=CustomerState.NEW_CONTACT_NAME)
        
        if message_text == "✅ Use Account Only":
            account = session.get('selected_account', {})
            customer_info = {
                'type': 'existing',
                'dynamics_account_id': account.get('accountid') or account.get('id'),
                'company_name': account.get('name'),  # PDF generator looks for this
                'display_name': account.get('name'),
                'contact_name': None,
                'email': account.get('email'),
                'phone': account.get('phone'),
                'address': account.get('address', ''),
                'vat_number': account.get('vat_number', ''),
                'is_company': True
            }
            
            await update.message.reply_text(
                f"✅ Selected: {account.get('name')}",
                reply_markup=ReplyKeyboardRemove()
            )
            
            if user_id in self.customer_sessions:
                del self.customer_sessions[user_id]
            return self._make_result(complete=True, customer_data=customer_info)
        
        # Contact selected
        if message_text.startswith("👤 "):
            name = message_text[2:].strip()
            contacts = session.get('account_contacts', [])
            contact = next((c for c in contacts if (c.get('fullname') or c.get('name')) == name), None)
            
            if contact:
                account = session.get('selected_account', {})
                
                # For B2B, prefer ACCOUNT address (company billing) over contact address
                # Contact address is usually personal/home address
                address = account.get('address', '') or contact.get('address', '')
                
                customer_info = {
                    'type': 'existing',
                    'dynamics_account_id': account.get('accountid') or account.get('id'),
                    'dynamics_contact_id': contact.get('contactid') or contact.get('id'),
                    'company_name': account.get('name'),  # PDF generator looks for this
                    'display_name': account.get('name'),
                    'contact_name': contact.get('fullname') or contact.get('name'),
                    'email': contact.get('email') or account.get('email'),
                    'phone': contact.get('phone') or account.get('phone'),
                    'address': address,  # Account address preferred for B2B
                    'vat_number': account.get('vat_number', ''),
                    'is_company': True
                }
                
                # Debug logging
                logger.info(f"📋 CUSTOMER_INFO created for PDF:")
                logger.info(f"  - company_name: {customer_info.get('company_name')}")
                logger.info(f"  - display_name: {customer_info.get('display_name')}")
                logger.info(f"  - contact_name: {customer_info.get('contact_name')}")
                logger.info(f"  - email: {customer_info.get('email')}")
                logger.info(f"  - phone: {customer_info.get('phone')}")
                logger.info(f"  - address: '{customer_info.get('address')}' (from account: '{account.get('address')}', from contact: '{contact.get('address')}')")
                logger.info(f"  - vat_number: {customer_info.get('vat_number')}")
                logger.info(f"  - is_company: {customer_info.get('is_company')}")
                
                await update.message.reply_text(
                    f"✅ Selected: {account.get('name')}\n"
                    f"👤 Contact: {contact.get('fullname') or contact.get('name')}",
                    reply_markup=ReplyKeyboardRemove()
                )
                
                if user_id in self.customer_sessions:
                    del self.customer_sessions[user_id]
                return self._make_result(complete=True, customer_data=customer_info)
        
        return self._make_result(next_state=CustomerState.EXISTING_CONTACT_SELECT)
    
    # ==================== NEW CONTACT UNDER EXISTING ACCOUNT ====================
    
    async def _ask_new_contact_name(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Ask for new contact name"""
        keyboard = [["⬅️ Back"]]
        reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
        
        await update.message.reply_text(
            "👤 **New Contact**\n\nEnter the contact person's full name:",
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )
    
    async def _handle_new_contact_name(self, update: Update, context: ContextTypes.DEFAULT_TYPE,
                                       session_data: dict) -> dict:
        """Handle new contact name"""
        user_id = update.effective_user.id
        message_text = update.message.text.strip()
        session = self._ensure_session(user_id)
        
        if message_text == "⬅️ Back":
            account = session.get('selected_account', {})
            contacts = session.get('account_contacts', [])
            if account:
                await self._show_account_contacts(update, account, contacts)
                return self._make_result(next_state=CustomerState.EXISTING_CONTACT_SELECT)
            return self._make_result(next_state=CustomerState.EXISTING_CUSTOMER_SELECT)
        
        name_parts = message_text.split(' ', 1)
        session['new_contact'] = {
            'firstname': name_parts[0],
            'lastname': name_parts[1] if len(name_parts) > 1 else '',
            'fullname': message_text
        }
        
        await self._ask_new_contact_phone(update, context)
        return self._make_result(next_state=CustomerState.NEW_CONTACT_PHONE)
    
    async def _ask_new_contact_phone(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Ask for new contact phone"""
        keyboard = [["Skip"], ["⬅️ Back"]]
        reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
        
        await update.message.reply_text(
            "📞 **Phone Number**\n\nEnter the contact's phone number:",
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )
    
    async def _handle_new_contact_phone(self, update: Update, context: ContextTypes.DEFAULT_TYPE,
                                        session_data: dict) -> dict:
        """Handle new contact phone"""
        user_id = update.effective_user.id
        message_text = update.message.text.strip()
        session = self._ensure_session(user_id)
        
        if message_text == "⬅️ Back":
            await self._ask_new_contact_name(update, context)
            return self._make_result(next_state=CustomerState.NEW_CONTACT_NAME)
        
        if message_text != "Skip":
            session['new_contact']['phone'] = re.sub(r'[^\d+]', '', message_text)
        
        await self._ask_new_contact_email(update, context)
        return self._make_result(next_state=CustomerState.NEW_CONTACT_EMAIL)
    
    async def _ask_new_contact_email(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Ask for new contact email"""
        keyboard = [["Skip"], ["⬅️ Back"]]
        reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
        
        await update.message.reply_text(
            "📧 **Email Address**\n\nEnter the contact's email:",
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )
    
    async def _handle_new_contact_email(self, update: Update, context: ContextTypes.DEFAULT_TYPE,
                                        session_data: dict) -> dict:
        """Handle new contact email and create contact"""
        user_id = update.effective_user.id
        message_text = update.message.text.strip()
        session = self._ensure_session(user_id)
        
        if message_text == "⬅️ Back":
            await self._ask_new_contact_phone(update, context)
            return self._make_result(next_state=CustomerState.NEW_CONTACT_PHONE)
        
        if message_text != "Skip":
            if '@' in message_text and '.' in message_text:
                session['new_contact']['email'] = message_text.lower()
            else:
                await update.message.reply_text("Please enter a valid email or Skip.")
                return self._make_result(next_state=CustomerState.NEW_CONTACT_EMAIL)
        
        # Create contact in Dynamics
        account = session.get('selected_account', {})
        new_contact = session['new_contact']
        
        contact_id = None
        if self.dynamics:
            try:
                contact_data = {
                    'firstname': new_contact.get('firstname'),
                    'lastname': new_contact.get('lastname'),
                    'email': new_contact.get('email'),
                    'phone': new_contact.get('phone'),
                    'account_id': account.get('accountid') or account.get('id')
                }
                contact_id = await self.dynamics.create_contact(contact_data)
                if contact_id:
                    logger.info(f"✅ Created Dynamics contact: {contact_id}")
            except Exception as e:
                logger.error(f"Error creating contact in Dynamics: {e}")
        
        # Build customer info
        customer_info = {
            'type': 'existing',
            'dynamics_account_id': account.get('accountid') or account.get('id'),
            'dynamics_contact_id': contact_id,
            'company_name': account.get('name'),  # PDF generator looks for this
            'display_name': account.get('name'),
            'contact_name': new_contact.get('fullname'),
            'email': new_contact.get('email') or account.get('email'),
            'phone': new_contact.get('phone') or account.get('phone'),
            'address': account.get('address', ''),
            'vat_number': account.get('vat_number', ''),
            'is_company': True
        }
        
        await update.message.reply_text(
            f"✅ Contact added!\n\n"
            f"🏢 {account.get('name')}\n"
            f"👤 {new_contact.get('fullname')}",
            reply_markup=ReplyKeyboardRemove()
        )
        
        if user_id in self.customer_sessions:
            del self.customer_sessions[user_id]
        return self._make_result(complete=True, customer_data=customer_info)
    
    # ==================== EMAIL SELECTION ====================
    
    async def ask_email_selection(self, update: Update, context: ContextTypes.DEFAULT_TYPE,
                                  session_data: dict):
        """
        Public method to ask which email to send the quote to.
        Called by quote_flow.py after quote generation.
        
        Shows options based on customer data if available.
        """
        customer = session_data.get('customer', {})
        user_id = update.effective_user.id
        
        # Collect available email options
        email_options = []
        
        # Customer email
        customer_email = customer.get('email')
        if customer_email:
            email_options.append(f"📧 {customer_email}")
        
        # User profile email (if different)
        user_profile = session_data.get('user_profile', {})
        if user_profile:
            profile_email = user_profile.get('email')
            if profile_email and profile_email != customer_email:
                email_options.append(f"👤 {profile_email}")
        
        # Always offer custom email option
        email_options.append("✏️ Enter different email")
        email_options.append("❌ Skip email")
        
        # Build keyboard
        keyboard = [[opt] for opt in email_options]
        reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
        
        # Build message
        customer_name = customer.get('display_name') or customer.get('contact_name') or 'the customer'
        
        await update.message.reply_text(
            f"📧 **Send Quote**\n\n"
            f"Where should we send the quote for {customer_name}?\n\n"
            f"Select an email address or enter a different one:",
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )
    
    async def _handle_email_selection(self, update: Update, context: ContextTypes.DEFAULT_TYPE,
                                      session_data: dict) -> dict:
        """Handle email selection for quote delivery"""
        message_text = update.message.text.strip()
        
        if message_text == "⬅️ Back":
            return {"back": True}
        
        if message_text == "❌ Skip email":
            return {"skip_email": True}
        
        if message_text == "✏️ Enter different email":
            # Ask for custom email
            keyboard = [["⬅️ Back"]]
            reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
            await update.message.reply_text(
                "📧 Enter the email address to send the quote to:",
                reply_markup=reply_markup
            )
            return self._make_result(next_state=CustomerState.CUSTOM_EMAIL_INPUT)
        
        # Extract email from button text (remove emoji prefix like "📧 " or "👤 ")
        email = message_text
        if message_text.startswith("📧 "):
            email = message_text[2:].strip()
        elif message_text.startswith("👤 "):
            email = message_text[2:].strip()
        
        # Validate email
        if '@' in email and '.' in email:
            logger.info(f"📧 Email selected: {email}")
            return {"email": email.lower()}
        
        await update.message.reply_text("Please select an option or enter a valid email address.")
        return self._make_result(next_state=CustomerState.EMAIL_SELECTION)
    
    async def _handle_custom_email(self, update: Update, context: ContextTypes.DEFAULT_TYPE,
                                   session_data: dict) -> dict:
        """Handle custom email input"""
        message_text = update.message.text.strip()
        
        if message_text == "⬅️ Back":
            # Go back to email selection
            await self.ask_email_selection(update, context, session_data)
            return self._make_result(next_state=CustomerState.EMAIL_SELECTION)
        
        if '@' in message_text and '.' in message_text:
            logger.info(f"📧 Custom email entered: {message_text}")
            return {"email": message_text.lower()}
        
        await update.message.reply_text("Please enter a valid email address.")
        return self._make_result(next_state=CustomerState.CUSTOM_EMAIL_INPUT)
    
    # ==================== UTILITIES ====================
    
    def get_customer_summary(self, session_data: dict) -> str:
        """Get formatted customer summary"""
        customer = session_data.get('customer', {})
        
        if not customer:
            return "No customer selected"
        
        lines = []
        if customer.get('display_name'):
            lines.append(f"🏢 {customer['display_name']}")
        if customer.get('contact_name'):
            lines.append(f"👤 {customer['contact_name']}")
        if customer.get('email'):
            lines.append(f"📧 {customer['email']}")
        if customer.get('phone'):
            lines.append(f"📞 {customer['phone']}")
        
        return "\n".join(lines) if lines else "Customer details not available"
    
    def cleanup_session(self, user_id: int):
        """Clean up customer session"""
        if user_id in self.customer_sessions:
            del self.customer_sessions[user_id]