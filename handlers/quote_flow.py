"""
Enhanced Quote Flow Handler for Stretch Ceiling Bot
Version 8.8 - With Customer Selection Integration and Dynamics 365
FIXED: Handle both Message and CallbackQuery updates
ENHANCED: Dynamics 365 integration for quote synchronization
NEW: Customer selection before quote creation
"""
import os
import json
import logging
import traceback
import asyncio
from datetime import datetime, timedelta
from typing import Dict, Optional, List
import uuid

from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import ContextTypes

from config import Config
from models import CeilingConfig, ConversationState, CeilingCost, is_customer_state
from utils import escape_markdown, parse_dimensions, format_price, serialize_for_json
from services import CostCalculator, EntraIDEmailSender, ImprovedStretchQuotePDFGenerator

# Import customer selection handler
from handlers.customer_selection import CustomerSelectionHandler, CustomerState

logger = logging.getLogger(__name__)


class EnhancedMultiCeilingQuoteFlow:
    """Enhanced quote flow with edit capabilities, perimeter editing, customer selection and email sending"""
    
    def __init__(self, db_manager):
        self.db = db_manager
        self.calculator = CostCalculator(db_manager)
        self.email_sender = EntraIDEmailSender()
        
        # Initialize customer selection handler
        self.customer_selection = CustomerSelectionHandler(db_manager)
    
    def _get_message_and_user(self, update: Update):
        """Extract message and user from update object - handles both Message and CallbackQuery"""
        if update.callback_query:
            return update.callback_query.message, update.callback_query.from_user
        elif update.message:
            return update.message, update.effective_user
        else:
            return None, None
    
    async def start_quote_flow(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Start the quote flow - FIXED to handle CallbackQuery"""
        try:
            message, user = self._get_message_and_user(update)
            if not message or not user:
                logger.error("Could not get message/user in start_quote_flow")
                return
            
            user_id = user.id
            
            logger.info(f"🚀 Starting multi-ceiling quote for user {user_id} ({user.first_name})")
            
            # Log conversation
            self.db.log_conversation(user_id, "user", "/create_quote")
            
            # Ensure user exists
            self.db.ensure_user_exists(
                user_id=user_id, 
                username=user.username, 
                first_name=user.first_name, 
                last_name=user.last_name
            )
            
            # Check if admin
            is_admin = user_id in Config.ADMIN_USER_IDS
            
            # Initialize session with enhanced edit capabilities
            session_data = {
                "user_id": user_id,
                "started_at": datetime.now().isoformat(),
                "is_admin": is_admin,
                "client_group": self.db.get_user_client_group(user_id) if not is_admin else None,
                "ceiling_count": 0,
                "current_ceiling_index": 0,
                "ceilings": [],
                "ceiling_costs": [],
                "customer": None,  # NEW: Customer data
                "state": ConversationState.CLIENT_GROUP.value if is_admin else ConversationState.CEILING_COUNT.value,
                "edit_history": [],
                "previous_steps": [],
            }
            
            self.db.save_quote_session(user_id, session_data, session_data["state"])
            
            if is_admin:
                await self.ask_client_group(update, context, session_data)
            else:
                await self.ask_ceiling_count(update, context, session_data)
        
        except Exception as e:
            logger.error(f"❌ Error starting quote flow: {e}")
            message, _ = self._get_message_and_user(update)
            if message:
                await message.reply_text("❌ Error starting quote. Please try again.")
    
    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle incoming messages during quote flow"""
        try:
            user_id = update.effective_user.id
            message_text = update.message.text.strip()
            
            # Get session
            session = self.db.get_quote_session(user_id)
            if not session:
                await update.message.reply_text("No active quote session. Use /create_quote to start a new quote.")
                return
            
            session_data = json.loads(session["session_data"])
            state = ConversationState(session_data["state"])
            
            logger.debug(f"Current state: {state}, Message: {message_text}")
            
            # Check for special commands
            if message_text.lower() in ["cancel", "stop", "quit"]:
                self.db.delete_quote_session(user_id)
                # Clean up customer selection session
                if user_id in self.customer_selection.customer_sessions:
                    del self.customer_selection.customer_sessions[user_id]
                await update.message.reply_text(
                    "❌ Quote cancelled. Use /create_quote to start again.", 
                    reply_markup=ReplyKeyboardRemove()
                )
                return
            
            # Check for back command
            if message_text.lower() in ["back", "previous", "⬅️ Back"]:
                await self.handle_back_navigation(update, context, session_data)
                return
            
            # Check if this is a customer selection state
            if is_customer_state(state):
                await self.route_customer_message(update, context, state, message_text, session_data)
                return
            
            # Route to appropriate handler
            await self.route_to_handler(update, context, state, message_text, session_data)
        
        except Exception as e:
            logger.error(f"❌ Error handling message: {e}")
            logger.error(traceback.format_exc())
            await update.message.reply_text("❌ Error processing message. Please try again.")
    
    async def route_customer_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE, 
                                     state: ConversationState, message_text: str, session_data: Dict):
        """Route messages to customer selection handler"""
        user_id = update.effective_user.id
        
        # Map ConversationState to CustomerState
        state_mapping = {
            ConversationState.CUSTOMER_TYPE: CustomerState.CUSTOMER_TYPE,
            ConversationState.NEW_CUSTOMER_COMPANY: CustomerState.NEW_CUSTOMER_COMPANY,
            ConversationState.NEW_CUSTOMER_VAT: CustomerState.NEW_CUSTOMER_VAT,
            ConversationState.NEW_CUSTOMER_CONTACT: CustomerState.NEW_CUSTOMER_CONTACT,
            ConversationState.NEW_CUSTOMER_ADDRESS: CustomerState.NEW_CUSTOMER_ADDRESS,
            ConversationState.NEW_CUSTOMER_PHONE: CustomerState.NEW_CUSTOMER_PHONE,
            ConversationState.NEW_CUSTOMER_EMAIL: CustomerState.NEW_CUSTOMER_EMAIL,
            ConversationState.NEW_CUSTOMER_LEAD_SOURCE: CustomerState.NEW_CUSTOMER_LEAD_SOURCE,
            ConversationState.NEW_CUSTOMER_CONFIRM: CustomerState.NEW_CUSTOMER_CONFIRM,
            ConversationState.EXISTING_CUSTOMER_SEARCH: CustomerState.EXISTING_CUSTOMER_SEARCH,
            ConversationState.EXISTING_CUSTOMER_SELECT: CustomerState.EXISTING_CUSTOMER_SELECT,
            ConversationState.EXISTING_CONTACT_SELECT: CustomerState.EXISTING_CONTACT_SELECT,
            ConversationState.NEW_CONTACT_NAME: CustomerState.NEW_CONTACT_NAME,
            ConversationState.NEW_CONTACT_EMAIL: CustomerState.NEW_CONTACT_EMAIL,
            ConversationState.NEW_CONTACT_PHONE: CustomerState.NEW_CONTACT_PHONE,
            ConversationState.EMAIL_SELECTION: CustomerState.EMAIL_SELECTION,
            ConversationState.CUSTOM_EMAIL_INPUT: CustomerState.CUSTOM_EMAIL_INPUT,
        }
        
        customer_state = state_mapping.get(state)
        if customer_state:
            result = await self.customer_selection.handle_message(
                update, context, customer_state, message_text, session_data
            )
            
            # Check if customer selection is complete
            if result and result.get("complete"):
                # Store customer data in session
                customer_data = result.get("customer_data")
                session_data["customer"] = customer_data
                
                # Log the customer data being stored
                logger.info(f"✅ STORING customer in session:")
                if customer_data:
                    logger.info(f"  - company_name: {customer_data.get('company_name')}")
                    logger.info(f"  - display_name: {customer_data.get('display_name')}")
                    logger.info(f"  - contact_name: {customer_data.get('contact_name')}")
                    logger.info(f"  - email: {customer_data.get('email')}")
                    logger.info(f"  - address: {customer_data.get('address')}")
                else:
                    logger.warning("  - customer_data is None!")
                
                # Move to ceiling count
                session_data["state"] = ConversationState.CEILING_COUNT.value
                self.db.save_quote_session(user_id, session_data, session_data["state"])
                await self.ask_ceiling_count(update, context, session_data)
            
            elif result and result.get("next_state"):
                # Update state and continue
                next_state = result["next_state"]
                # Convert CustomerState back to ConversationState
                reverse_mapping = {v: k for k, v in state_mapping.items()}
                conv_state = reverse_mapping.get(next_state)
                if conv_state:
                    session_data["state"] = conv_state.value
                    self.db.save_quote_session(user_id, session_data, session_data["state"])
            
            elif result and result.get("email"):
                # Email selected - send quote and complete
                email = result["email"]
                session_data["email"] = email
                context.user_data["pending_email"] = email
                
                await update.message.reply_text(f"✅ Quote will be sent to {email}")
                await self.complete_quote(update, context, session_data)
                return
            
            elif result and result.get("skip_email"):
                # User chose to skip email - just complete
                await self.complete_quote(update, context, session_data)
                return
            
            elif result and result.get("back"):
                # Handle back navigation
                await self.handle_back_navigation(update, context, session_data)
            
            elif result and result.get("cancel"):
                # User cancelled - end quote
                if user_id in self.customer_selection.customer_sessions:
                    del self.customer_selection.customer_sessions[user_id]
                session_data["state"] = ConversationState.CANCELLED.value if hasattr(ConversationState, 'CANCELLED') else "cancelled"
                self.db.delete_quote_session(user_id)
                await update.message.reply_text("❌ Quote cancelled.", reply_markup=ReplyKeyboardRemove())
    
    async def handle_back_navigation(self, update: Update, context: ContextTypes.DEFAULT_TYPE, session_data: Dict):
        """Handle back navigation in quote flow"""
        if session_data.get("previous_steps"):
            # Get previous state
            previous = session_data["previous_steps"].pop()
            session_data["state"] = previous["state"]
            
            # Restore previous data if needed
            if "data" in previous:
                for key, value in previous["data"].items():
                    session_data[key] = value
            
            # Save updated session
            self.db.save_quote_session(session_data["user_id"], session_data, session_data["state"])
            
            # Go to previous state
            await self.route_to_state(update, context, ConversationState(previous["state"]), session_data)
        else:
            await update.message.reply_text("Cannot go back further. Continue with the current step.")
    
    async def save_state_for_back_navigation(self, session_data: Dict, state: str, data: Dict = None):
        """Save current state for back navigation"""
        if "previous_steps" not in session_data:
            session_data["previous_steps"] = []
        
        step = {"state": state}
        if data:
            step["data"] = data
        
        session_data["previous_steps"].append(step)
        
        # Limit history to last 10 steps
        if len(session_data["previous_steps"]) > 10:
            session_data["previous_steps"] = session_data["previous_steps"][-10:]
    
    async def route_to_state(self, update: Update, context: ContextTypes.DEFAULT_TYPE, state: ConversationState, session_data: Dict):
        """Route to a specific state"""
        state_handlers = {
            ConversationState.CLIENT_GROUP: self.ask_client_group,
            ConversationState.CUSTOMER_TYPE: self.ask_customer_type,  # NEW
            ConversationState.CEILING_COUNT: self.ask_ceiling_count,
            ConversationState.CEILING_NAME: self.ask_ceiling_name,
            ConversationState.CEILING_SIZE: self.ask_ceiling_size,
            ConversationState.SIZE_CONFIRMATION: self.ask_size_confirmation,
            ConversationState.PERIMETER_EDIT: self.ask_perimeter_edit,
            ConversationState.CORNERS_COUNT: self.ask_corners_count,
            ConversationState.CEILING_TYPE: self.ask_ceiling_type,
            ConversationState.TYPE_CEILING: self.ask_type_ceiling,
            ConversationState.CEILING_COLOR: self.ask_ceiling_color,
            ConversationState.CEILING_ACOUSTIC: self.ask_ceiling_acoustic,
            ConversationState.ACOUSTIC_PERFORMANCE: self.ask_acoustic_performance,
            ConversationState.PERIMETER_PROFILE: self.ask_perimeter_profile,
            ConversationState.SEAM_QUESTION: self.ask_seam_question,
            ConversationState.SEAM_LENGTH: self.ask_seam_length,
            ConversationState.LIGHTS_QUESTION: self.ask_lights_question,
            ConversationState.LIGHT_SELECTION: self.ask_light_selection,
            ConversationState.LIGHT_QUANTITY: self.ask_light_quantity,
            ConversationState.MORE_LIGHTS: self.ask_more_lights,
            ConversationState.WOOD_QUESTION: self.ask_wood_question,
            ConversationState.WOOD_SELECTION: self.ask_wood_selection,
            ConversationState.WOOD_QUANTITY: self.ask_wood_quantity,
            ConversationState.MORE_WOOD: self.ask_more_wood,
            ConversationState.NEXT_CEILING: self.ask_next_ceiling,
            ConversationState.QUOTE_REFERENCE: self.ask_quote_reference,
            ConversationState.EMAIL_REQUEST: self.ask_email_request,
            ConversationState.EMAIL_INPUT: self.ask_email_input,
            ConversationState.EMAIL_SELECTION: self.ask_email_selection,  # NEW
        }
        
        handler = state_handlers.get(state)
        if handler:
            await handler(update, context, session_data)
    
    async def route_to_handler(self, update, context, state, message_text, session_data):
        """Route message to appropriate handler based on state"""
        handlers = {
            ConversationState.CLIENT_GROUP: self.handle_client_group,
            ConversationState.CEILING_COUNT: self.handle_ceiling_count,
            ConversationState.CEILING_NAME: self.handle_ceiling_name,
            ConversationState.CEILING_SIZE: self.handle_ceiling_size,
            ConversationState.SIZE_CONFIRMATION: self.handle_size_confirmation,
            ConversationState.PERIMETER_EDIT: self.handle_perimeter_edit,
            ConversationState.CORNERS_COUNT: self.handle_corners_count,
            ConversationState.CEILING_TYPE: self.handle_ceiling_type,
            ConversationState.TYPE_CEILING: self.handle_type_ceiling,
            ConversationState.CEILING_COLOR: self.handle_ceiling_color,
            ConversationState.CEILING_FINISH: self.handle_ceiling_finish,
            ConversationState.CEILING_ACOUSTIC: self.handle_ceiling_acoustic,
            ConversationState.ACOUSTIC_PERFORMANCE: self.handle_acoustic_performance,
            ConversationState.PERIMETER_PROFILE: self.handle_perimeter_profile,
            ConversationState.SEAM_QUESTION: self.handle_seam_question,
            ConversationState.SEAM_LENGTH: self.handle_seam_length,
            ConversationState.LIGHTS_QUESTION: self.handle_lights_question,
            ConversationState.LIGHT_SELECTION: self.handle_light_selection,
            ConversationState.LIGHT_QUANTITY: self.handle_light_quantity,
            ConversationState.MORE_LIGHTS: self.handle_more_lights,
            ConversationState.WOOD_QUESTION: self.handle_wood_question,
            ConversationState.WOOD_SELECTION: self.handle_wood_selection,
            ConversationState.WOOD_QUANTITY: self.handle_wood_quantity,
            ConversationState.MORE_WOOD: self.handle_more_wood,
            ConversationState.NEXT_CEILING: self.handle_next_ceiling,
            ConversationState.QUOTE_REFERENCE: self.handle_quote_reference,
            ConversationState.EMAIL_REQUEST: self.handle_email_request,
            ConversationState.EMAIL_INPUT: self.handle_email_input,
        }
        
        handler = handlers.get(state)
        if handler:
            await handler(update, context, message_text, session_data)
        else:
            logger.error(f"No handler for state: {state}")
            await update.message.reply_text("❌ Unexpected state. Please restart with /create_quote")
    
    # ==================== ASK METHODS (with back button) ====================
    
    async def ask_client_group(self, update, context, session_data):
        """Ask admin for client group selection - FIXED"""
        message, _ = self._get_message_and_user(update)
        if not message:
            return
        
        keyboard = [
            ["B2C - Consumer"],
            ["B2B - Reseller"],
            ["B2B - Hospitality"]
        ]
        reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
        
        await message.reply_text(
            "🔧 **Admin Mode - Client Group Selection**\n\n"
            "Select the client type for this quote:",
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )
    
    async def ask_customer_type(self, update, context, session_data):
        """Ask for new or existing customer - NEW"""
        await self.customer_selection.start_customer_selection(update, context, session_data)
    
    async def ask_ceiling_count(self, update, context, session_data):
        """Ask how many ceilings to quote - FIXED"""
        message, _ = self._get_message_and_user(update)
        if not message:
            return
        
        max_ceilings = Config.MAX_CEILINGS_PER_QUOTE
        
        keyboard = [
            ["1 ceiling"],
            ["2 ceilings"],
            ["3 ceilings"],
            ["4 ceilings"],
            [f"5+ ceilings (up to {max_ceilings})"]
        ]
        
        # Add back button if there are previous steps
        if session_data.get("previous_steps"):
            keyboard.append(["⬅️ Back"])
        
        reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
        
        client_group_display = {
            "price_b2c": "Consumer",
            "price_b2b_reseller": "B2B Reseller",
            "price_b2b_hospitality": "B2B Hospitality"
        }.get(session_data.get("client_group", "price_b2c"), "Consumer")
        
        # Show customer info if available
        customer_info = ""
        if session_data.get("customer"):
            customer = session_data["customer"]
            customer_name = customer.get("display_name", customer.get("contact_name", ""))
            if customer_name:
                customer_info = f"\n👤 Customer: {customer_name}"
        
        await message.reply_text(
            f"🏠 **Multi-Ceiling Quote System** 🏠\n\n"
            f"Client Type: {client_group_display}{customer_info}\n\n"
            f"How many ceilings would you like to quote?",
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )
    
    async def ask_ceiling_name(self, update, context, session_data):
        """Ask for ceiling name - FIXED"""
        message, _ = self._get_message_and_user(update)
        if not message:
            return
        
        ceiling_num = session_data["current_ceiling_index"] + 1
        total = session_data["ceiling_count"]
        
        # Simple keyboard with back option
        keyboard = [["⬅️ Back"]] if session_data.get("previous_steps") else []
        reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True) if keyboard else ReplyKeyboardRemove()
        
        await message.reply_text(
            f"**Ceiling {ceiling_num} of {total}**\n\n"
            f"What name would you like to give this ceiling?\n\n"
            f"Examples: Living Room, Master Bedroom, Kitchen",
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )
    
    async def ask_ceiling_size(self, update, context, session_data):
        """Ask for ceiling dimensions - FIXED"""
        message, _ = self._get_message_and_user(update)
        if not message:
            return
        
        ceiling = session_data["ceilings"][session_data["current_ceiling_index"]]
        
        keyboard = [["⬅️ Back"]] if session_data.get("previous_steps") else []
        reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True) if keyboard else ReplyKeyboardRemove()
        
        ceiling_name = escape_markdown(ceiling["name"])
        await message.reply_text(
            f"📐 **{ceiling_name} - Dimensions**\n\n"
            f"Please enter the dimensions (length × width) in meters.\n\n"
            f"Examples:\n"
            f"• 5.5 x 4.2\n"
            f"• 5.5m × 4.2m\n"
            f"• 5 x 4",
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )
    
    async def ask_size_confirmation(self, update, context, session_data):
        """Ask for size confirmation with perimeter info - FIXED"""
        message, _ = self._get_message_and_user(update)
        if not message:
            return
        
        ceiling = session_data["ceilings"][session_data["current_ceiling_index"]]
        
        keyboard = [
            ["✅ Correct"],
            ["❌ Re-enter dimensions"],
            ["✏️ Edit perimeter"],
            ["⬅️ Back"]
        ]
        reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
        
        ceiling_name = escape_markdown(ceiling["name"])
        await message.reply_text(
            f"📐 **{ceiling_name} - Confirmation**\n\n"
            f"• Length: {ceiling['length']}m\n"
            f"• Width: {ceiling['width']}m\n"
            f"• Area: {ceiling['area']:.2f} m²\n"
            f"• Perimeter: {ceiling['perimeter']:.2f} m\n\n"
            f"Is this correct?",
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )
    
    async def ask_perimeter_edit(self, update, context, session_data):
        """Ask for manual perimeter input - FIXED"""
        message, _ = self._get_message_and_user(update)
        if not message:
            return
        
        ceiling = session_data["ceilings"][session_data["current_ceiling_index"]]
        
        keyboard = [["⬅️ Back"]]
        reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        
        ceiling_name = escape_markdown(ceiling["name"])
        await message.reply_text(
            f"📏 **{ceiling_name} - Edit Perimeter**\n\n"
            f"Current calculated perimeter: {ceiling['perimeter']:.2f}m\n"
            f"(Based on: 2 × ({ceiling['length']}m + {ceiling['width']}m))\n\n"
            f"Enter the actual perimeter in meters if different:\n"
            f"(e.g., for complex shapes or special cases)\n\n"
            f"Examples: 18.5, 22.3, 25",
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )
    
    async def ask_corners_count(self, update, context, session_data):
        """Ask for number of corners - FIXED"""
        message, _ = self._get_message_and_user(update)
        if not message:
            return
        
        ceiling = session_data["ceilings"][session_data["current_ceiling_index"]]
        
        keyboard = [
            ["4"], ["5"], ["6"],
            ["7"], ["8"], ["More than 8"],
            ["⬅️ Back"]
        ]
        reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
        
        ceiling_name = escape_markdown(ceiling["name"])
        await message.reply_text(
            f"📲 **{ceiling_name} - Corners**\n\n"
            f"How many corners does this ceiling have?",
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )
    
    async def ask_ceiling_type(self, update, context, session_data):
        """Ask for ceiling type (product_type) - FIXED"""
        message, _ = self._get_message_and_user(update)
        if not message:
            return
        
        ceiling = session_data["ceilings"][session_data["current_ceiling_index"]]
        
        # Get available ceiling types from database
        ceiling_types = self.db.get_unique_values("ceiling", "product_type")
        
        if not ceiling_types:
            ceiling_types = ["fabric", "pvc"]  # Fallback
        
        keyboard = [[ct.upper()] for ct in ceiling_types]
        keyboard.append(["⬅️ Back"])
        reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
        
        ceiling_name = escape_markdown(ceiling["name"])
        await message.reply_text(
            f"🏗️ **{ceiling_name} - Ceiling Type**\n\n"
            f"Step 1/3: Select the main type of stretch ceiling:",
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )
    
    async def ask_type_ceiling(self, update, context, session_data):
        """Ask for specific type_ceiling based on product_type - FIXED"""
        message, _ = self._get_message_and_user(update)
        if not message:
            return
        
        ceiling = session_data["ceilings"][session_data["current_ceiling_index"]]
        product_type = ceiling["ceiling_type"]
        
        # Get available type_ceiling values for this product_type
        type_ceilings = self.db.get_type_ceilings_for_product_type(product_type)
        
        logger.info(f"📋 Asking type_ceiling for product_type '{product_type}'")
        logger.info(f"Available options: {type_ceilings}")
        
        keyboard = [[tc] for tc in type_ceilings]
        keyboard.append(["⬅️ Back"])
        reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
        
        ceiling_name = escape_markdown(ceiling["name"])
        await message.reply_text(
            f"📋 **{ceiling_name} - Specific Type**\n\n"
            f"Step 2/3: Select the specific type of {product_type.upper()} ceiling:\n\n"
            f"Available options for {product_type.upper()}:",
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )
    
    async def ask_ceiling_color(self, update, context, session_data):
        """Ask for ceiling color based on product_type and type_ceiling - FIXED"""
        message, _ = self._get_message_and_user(update)
        if not message:
            return
        
        ceiling = session_data["ceilings"][session_data["current_ceiling_index"]]
        
        # Get available colors for selected type_ceiling
        colors = self.db.get_colors_for_type_ceiling(
            ceiling["ceiling_type"], 
            ceiling.get("type_ceiling", "standard")
        )
        
        keyboard = [[color.capitalize()] for color in colors]
        keyboard.append(["⬅️ Back"])
        reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
        
        ceiling_name = escape_markdown(ceiling["name"])
        type_ceiling = escape_markdown(ceiling.get("type_ceiling", "standard"))
        
        await message.reply_text(
            f"🎨 **{ceiling_name} - Color**\n\n"
            f"Step 3/3: Select the color for your {ceiling['ceiling_type'].upper()} - {type_ceiling} ceiling:",
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )
    
    async def ask_ceiling_finish(self, update, context, session_data):
        """Ask for ceiling finish - FIXED"""
        message, _ = self._get_message_and_user(update)
        if not message:
            return
        
        ceiling = session_data["ceilings"][session_data["current_ceiling_index"]]
        
        # Get available finishes
        finishes = self.db.get_unique_values("ceiling", "finish", {
            "product_type": ceiling["ceiling_type"],
            "type_ceiling": ceiling.get("type_ceiling"),
            "color": ceiling["color"]
        })
        
        if not finishes:
            finishes = ["Mat"]  # Default from your data
        
        keyboard = [[finish] for finish in finishes]
        keyboard.append(["⬅️ Back"])
        reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
        
        ceiling_name = escape_markdown(ceiling["name"])
        await message.reply_text(
            f"✨ **{ceiling_name} - Finish**\n\n"
            f"Select the finish:",
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )
    
    async def ask_ceiling_acoustic(self, update, context, session_data):
        """Ask about acoustic requirements - Enhanced with auto-detection"""
        ceiling = session_data["ceilings"][session_data["current_ceiling_index"]]
        
        # Check if the selected ceiling product has acoustic in its description
        ceiling_product = self.db.get_ceiling_product(
            ceiling["ceiling_type"],
            ceiling.get("type_ceiling", "standard"),
            ceiling["color"]
        )
        
        if ceiling_product and "acoustic" in ceiling_product.get("description", "").lower():
            # This is already an acoustic ceiling, ask about acoustic performance enhancement
            session_data["ceilings"][session_data["current_ceiling_index"]]["acoustic"] = True
            session_data["state"] = ConversationState.ACOUSTIC_PERFORMANCE.value
            self.db.save_quote_session(session_data["user_id"], session_data, session_data["state"])
            await self.ask_acoustic_performance(update, context, session_data)
        else:
            # Standard ceiling, skip acoustic performance
            session_data["ceilings"][session_data["current_ceiling_index"]]["acoustic"] = False
            session_data["state"] = ConversationState.PERIMETER_PROFILE.value
            self.db.save_quote_session(session_data["user_id"], session_data, session_data["state"])
            await self.ask_perimeter_profile(update, context, session_data)
    
    async def ask_acoustic_performance(self, update, context, session_data):
        """Ask for acoustic performance enhancement - FIXED"""
        message, _ = self._get_message_and_user(update)
        if not message:
            return
        
        ceiling = session_data["ceilings"][session_data["current_ceiling_index"]]
        
        # Get acoustic performance products
        acoustic_products = self.db.get_acoustic_performance_products()
        
        if acoustic_products:
            keyboard = []
            # Group by acoustic_performance value
            performance_groups = {}
            for product in acoustic_products:
                perf = product.get("acoustic_performance", "")
                if perf not in performance_groups:
                    performance_groups[perf] = product
            
            for perf, product in performance_groups.items():
                keyboard.append([f"{perf} - {product.get('description', perf)[:40]}"])
            
            keyboard.append(["Skip acoustic enhancement"])
        else:
            keyboard = [["Skip acoustic enhancement"]]
        
        keyboard.append(["⬅️ Back"])
        
        reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
        
        ceiling_name = escape_markdown(ceiling["name"])
        await message.reply_text(
            f"🔊 **{ceiling_name} - Acoustic Enhancement**\n\n"
            f"This ceiling has acoustic properties.\n"
            f"Would you like to add acoustic enhancement?",
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )
    
    async def ask_perimeter_profile(self, update, context, session_data):
        """Ask for perimeter profile selection - FIXED"""
        message, _ = self._get_message_and_user(update)
        if not message:
            return
        
        ceiling = session_data["ceilings"][session_data["current_ceiling_index"]]
        
        # Get perimeter products
        perimeter_products = self.db.get_products_by_category("perimeter")
        
        if perimeter_products:
            keyboard = []
            for product in perimeter_products[:6]:  # Limit to 6 options
                keyboard.append([f"{product['product_code']} - {product['description'][:30]}"])
        else:
            keyboard = [["Standard Profile"]]
        
        keyboard.append(["⬅️ Back"])
        
        reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
        
        ceiling_name = escape_markdown(ceiling["name"])
        await message.reply_text(
            f"🔧 **{ceiling_name} - Perimeter Profile**\n\n"
            f"Select the perimeter profile type:",
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )
    
    async def ask_seam_question(self, update, context, session_data):
        """Ask about seams - FIXED"""
        message, _ = self._get_message_and_user(update)
        if not message:
            return
        
        ceiling = session_data["ceilings"][session_data["current_ceiling_index"]]
        
        keyboard = [
            ["Yes - There will be seams"],
            ["No - No seams needed"],
            ["⬅️ Back"]
        ]
        reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
        
        ceiling_name = escape_markdown(ceiling["name"])
        await message.reply_text(
            f"🔗 **{ceiling_name} - Seams**\n\n"
            f"Will there be any seams in this ceiling?\n\n"
            f"Seams are needed for large ceilings or complex shapes.",
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )
    
    async def ask_seam_length(self, update, context, session_data):
        """Ask for seam length - FIXED"""
        message, _ = self._get_message_and_user(update)
        if not message:
            return
        
        ceiling = session_data["ceilings"][session_data["current_ceiling_index"]]
        
        keyboard = [["⬅️ Back"]]
        reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        
        ceiling_name = escape_markdown(ceiling["name"])
        await message.reply_text(
            f"🔗 **{ceiling_name} - Seam Length**\n\n"
            f"How many meters of seams are needed?\n\n"
            f"Enter the total length in meters (e.g., 5.5):",
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )
    
    async def ask_lights_question(self, update, context, session_data):
        """Ask about lights - FIXED"""
        message, _ = self._get_message_and_user(update)
        if not message:
            return
        
        ceiling = session_data["ceilings"][session_data["current_ceiling_index"]]
        
        keyboard = [
            ["Yes - Add lights"],
            ["No - No lights needed"],
            ["⬅️ Back"]
        ]
        reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
        
        ceiling_name = escape_markdown(ceiling["name"])
        await message.reply_text(
            f"💡 **{ceiling_name} - Lighting**\n\n"
            f"Would you like to add lights?",
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )
    
    async def ask_light_selection(self, update, context, session_data):
        """Ask for light selection - FIXED"""
        message, _ = self._get_message_and_user(update)
        if not message:
            return
        
        ceiling = session_data["ceilings"][session_data["current_ceiling_index"]]
        
        # Get light products
        light_products = self.db.get_products_by_category("light")
        
        if light_products:
            keyboard = []
            for product in light_products[:8]:  # Limit to 8 options
                keyboard.append([f"{product['product_code']} - {product['description'][:25]}"])
        else:
            keyboard = [["No lights available"]]
        
        keyboard.append(["⬅️ Back"])
        
        reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
        
        ceiling_name = escape_markdown(ceiling["name"])
        await message.reply_text(
            f"💡 **{ceiling_name} - Light Selection**\n\n"
            f"Select the type of lights:",
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )
    
    async def ask_light_quantity(self, update, context, session_data):
        """Ask for light quantity - FIXED"""
        message, _ = self._get_message_and_user(update)
        if not message:
            return
        
        ceiling = session_data["ceilings"][session_data["current_ceiling_index"]]
        
        keyboard = [["⬅️ Back"]]
        reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        
        ceiling_name = escape_markdown(ceiling["name"])
        await message.reply_text(
            f"💡 **{ceiling_name} - Light Quantity**\n\n"
            f"How many lights do you need?\n\n"
            f"Enter a number (e.g., 6):",
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )
    
    async def ask_more_lights(self, update, context, session_data):
        """Ask if more lights are needed - FIXED"""
        message, _ = self._get_message_and_user(update)
        if not message:
            return
        
        keyboard = [
            ["Yes - Add more lights"],
            ["No - Continue"],
            ["⬅️ Back"]
        ]
        reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
        
        await message.reply_text(
            "Would you like to add more lights?",
            reply_markup=reply_markup
        )
    
    async def ask_wood_question(self, update, context, session_data):
        """Ask about wood structures - FIXED"""
        message, _ = self._get_message_and_user(update)
        if not message:
            return
        
        ceiling = session_data["ceilings"][session_data["current_ceiling_index"]]
        
        keyboard = [
            ["Yes - Add wood structures"],
            ["No - Continue"],
            ["⬅️ Back"]
        ]
        reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
        
        ceiling_name = escape_markdown(ceiling["name"])
        await message.reply_text(
            f"🪵 **{ceiling_name} - Wood Structures**\n\n"
            f"Do you need any wood structures?",
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )
    
    async def ask_wood_selection(self, update, context, session_data):
        """Ask for wood structure selection - FIXED"""
        message, _ = self._get_message_and_user(update)
        if not message:
            return
        
        ceiling = session_data["ceilings"][session_data["current_ceiling_index"]]
        
        # Get wood products
        wood_products = self.db.get_products_by_category("wood_structure")
        
        if wood_products:
            keyboard = []
            for product in wood_products[:8]:  # Limit to 8 options
                keyboard.append([f"{product['product_code']} - {product['description'][:25]}"])
        else:
            keyboard = [["No wood structures available"]]
        
        keyboard.append(["⬅️ Back"])
        
        reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
        
        ceiling_name = escape_markdown(ceiling["name"])
        await message.reply_text(
            f"🪵 **{ceiling_name} - Wood Selection**\n\n"
            f"Select the wood structure:",
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )
    
    async def ask_wood_quantity(self, update, context, session_data):
        """Ask for wood quantity - FIXED"""
        message, _ = self._get_message_and_user(update)
        if not message:
            return
        
        ceiling = session_data["ceilings"][session_data["current_ceiling_index"]]
        
        keyboard = [["⬅️ Back"]]
        reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        
        ceiling_name = escape_markdown(ceiling["name"])
        await message.reply_text(
            f"🪵 **{ceiling_name} - Wood Quantity**\n\n"
            f"How many meters do you need?\n\n"
            f"Enter the quantity (e.g., 10):",
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )
    
    async def ask_more_wood(self, update, context, session_data):
        """Ask if more wood structures are needed - FIXED"""
        message, _ = self._get_message_and_user(update)
        if not message:
            return
        
        keyboard = [
            ["Yes - Add more wood"],
            ["No - Continue"],
            ["⬅️ Back"]
        ]
        reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
        
        await message.reply_text(
            "Would you like to add more wood structures?",
            reply_markup=reply_markup
        )
    
    async def ask_next_ceiling(self, update, context, session_data):
        """Ask about next ceiling or finish - FIXED"""
        message, _ = self._get_message_and_user(update)
        if not message:
            return
        
        current = session_data["current_ceiling_index"] + 1
        total = session_data["ceiling_count"]
        
        if current < total:
            keyboard = [
                [f"Continue to ceiling {current + 1}"],
                ["View summary and finish"],
                ["⬅️ Back"]
            ]
        else:
            keyboard = [
                ["Finish quote"],
                ["⬅️ Back"]
            ]
        
        reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
        
        # Show ceiling summary
        ceiling_config = self.create_ceiling_config(session_data["ceilings"][session_data["current_ceiling_index"]])
        costs = self.calculator.calculate_ceiling_costs(ceiling_config, session_data["client_group"])
        
        # Convert CeilingCost to dict for consistency
        costs_dict = {
            "ceiling_cost": costs.ceiling_cost,
            "perimeter_structure_cost": costs.perimeter_structure_cost,
            "perimeter_profile_cost": costs.perimeter_profile_cost,
            "corners_cost": costs.corners_cost,
            "seam_cost": costs.seam_cost,
            "lights_cost": costs.lights_cost,
            "wood_structures_cost": costs.wood_structures_cost,
            "acoustic_absorber_cost": costs.acoustic_absorber_cost,
            "total": costs.total
        }
        
        summary = self.format_ceiling_summary(ceiling_config, costs_dict)
        
        await message.reply_text(
            f"{summary}\n\n"
            f"What would you like to do next?",
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )
    
    async def ask_quote_reference(self, update, context, session_data):
        """Ask for quote reference - FIXED"""
        message, _ = self._get_message_and_user(update)
        if not message:
            return
        
        keyboard = [["⬅️ Back"]]
        reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        
        await message.reply_text(
            "📋 **Quote Reference**\n\n"
            "Please provide a reference for this quote:\n\n"
            "Examples:\n"
            "• Johnson Residence\n"
            "• Office Building - Floor 2\n"
            "• Restaurant Project",
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )
    
    async def ask_email_request(self, update, context, session_data):
        """Ask if email is needed - FIXED"""
        message, _ = self._get_message_and_user(update)
        if not message:
            return
        
        keyboard = [
            ["📧 Send quote by email"],
            ["No thanks"],
            ["⬅️ Back"]
        ]
        reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
        
        await message.reply_text(
            "📧 **Email Delivery**\n\n"
            "Would you like to receive this quote by email?",
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )
    
    async def ask_email_input(self, update, context, session_data):
        """Ask for email address - FIXED"""
        message, _ = self._get_message_and_user(update)
        if not message:
            return
        
        keyboard = [["⬅️ Back"]]
        reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        
        await message.reply_text(
            "Please enter your email address:",
            reply_markup=reply_markup
        )
    
    async def ask_email_selection(self, update, context, session_data):
        """Ask which email to send the quote to - NEW"""
        await self.customer_selection.ask_email_selection(update, context, session_data)
    
    # ==================== HANDLER METHODS ====================
    
    async def handle_client_group(self, update, context, message_text, session_data):
        """Handle client group selection - MODIFIED to go to customer selection"""
        mapping = {
            "B2C - Consumer": "price_b2c",
            "B2B - Reseller": "price_b2b_reseller",
            "B2B - Hospitality": "price_b2b_hospitality"
        }
        
        client_group = mapping.get(message_text)
        if client_group:
            # Save previous state for back navigation
            await self.save_state_for_back_navigation(session_data, session_data["state"])
            
            session_data["client_group"] = client_group
            
            # NEW: Go to customer selection instead of ceiling count
            session_data["state"] = ConversationState.CUSTOMER_TYPE.value
            self.db.save_quote_session(session_data["user_id"], session_data, session_data["state"])
            await self.ask_customer_type(update, context, session_data)
        else:
            await update.message.reply_text("Please select a valid client group.")
    
    async def handle_ceiling_count(self, update, context, message_text, session_data):
        """Handle ceiling count input"""
        try:
            if "ceiling" in message_text:
                # Parse from button text
                if message_text.startswith("1"):
                    count = 1
                elif message_text.startswith("2"):
                    count = 2
                elif message_text.startswith("3"):
                    count = 3
                elif message_text.startswith("4"):
                    count = 4
                elif message_text.startswith("5+"):
                    # User selected 5+ ceilings - ask for exact count
                    session_data["awaiting_5plus_count"] = True
                    self.db.save_quote_session(session_data["user_id"], session_data, session_data["state"])
                    
                    await update.message.reply_text(
                        f"🔢 **How many ceilings do you need?**\n\n"
                        f"Please enter a number between 5 and {Config.MAX_CEILINGS_PER_QUOTE}:",
                        reply_markup=ReplyKeyboardRemove(),
                        parse_mode="Markdown"
                    )
                    return
                else:
                    count = 5  # Default fallback
            else:
                # Direct number input
                count = int(message_text)
            
            if 1 <= count <= Config.MAX_CEILINGS_PER_QUOTE:
                # Save previous state
                await self.save_state_for_back_navigation(session_data, session_data["state"])
                
                session_data["ceiling_count"] = count
                session_data["current_ceiling_index"] = 0
                session_data["ceilings"] = []
                session_data["state"] = ConversationState.CEILING_NAME.value
                self.db.save_quote_session(session_data["user_id"], session_data, session_data["state"])
                await self.ask_ceiling_name(update, context, session_data)
            else:
                await update.message.reply_text(
                    f"Please enter a number between 1 and {Config.MAX_CEILINGS_PER_QUOTE}."
                )
        
        except ValueError:
            await update.message.reply_text("Please enter a valid number.")
    
    async def handle_ceiling_name(self, update, context, message_text, session_data):
        """Handle ceiling name input"""
        # Save previous state
        await self.save_state_for_back_navigation(session_data, session_data["state"])
        
        # Initialize new ceiling
        ceiling = {
            "name": message_text,
            "length": 0,
            "width": 0,
            "area": 0,
            "perimeter": 0,
            "perimeter_edited": False,
            "corners": 4,
            "ceiling_type": "",
            "type_ceiling": "",
            "color": "",
            "finish": "",
            "acoustic": False,
            "acoustic_performance": None,
            "acoustic_product": None,
            "perimeter_profile": None,
            "has_seams": False,
            "seam_length": 0,
            "lights": [],
            "wood_structures": [],
            "acoustic_absorber": None
        }
        
        session_data["ceilings"].append(ceiling)
        session_data["state"] = ConversationState.CEILING_SIZE.value
        self.db.save_quote_session(session_data["user_id"], session_data, session_data["state"])
        await self.ask_ceiling_size(update, context, session_data)
    
    async def handle_ceiling_size(self, update, context, message_text, session_data):
        """Handle ceiling size input"""
        try:
            length, width = parse_dimensions(message_text)
            
            if 0.1 <= length <= 100 and 0.1 <= width <= 100:
                # Save previous state
                await self.save_state_for_back_navigation(
                    session_data, 
                    session_data["state"], 
                    {"ceiling_index": session_data["current_ceiling_index"]}
                )
                
                ceiling = session_data["ceilings"][session_data["current_ceiling_index"]]
                ceiling["length"] = length
                ceiling["width"] = width
                ceiling["area"] = length * width
                ceiling["perimeter"] = 2 * (length + width)
                
                session_data["state"] = ConversationState.SIZE_CONFIRMATION.value
                self.db.save_quote_session(session_data["user_id"], session_data, session_data["state"])
                await self.ask_size_confirmation(update, context, session_data)
            else:
                await update.message.reply_text(
                    "Please enter dimensions between 0.1 and 100 meters."
                )
        
        except (ValueError, TypeError):
            await update.message.reply_text(
                "Invalid format. Please enter dimensions like '5.5 x 4.2' or '5 x 4'"
            )
    
    async def handle_size_confirmation(self, update, context, message_text, session_data):
        """Handle size confirmation"""
        if message_text == "✅ Correct":
            # Save previous state
            await self.save_state_for_back_navigation(session_data, session_data["state"])
            
            session_data["state"] = ConversationState.CORNERS_COUNT.value
            self.db.save_quote_session(session_data["user_id"], session_data, session_data["state"])
            await self.ask_corners_count(update, context, session_data)
        
        elif message_text == "❌ Re-enter dimensions":
            session_data["state"] = ConversationState.CEILING_SIZE.value
            self.db.save_quote_session(session_data["user_id"], session_data, session_data["state"])
            await self.ask_ceiling_size(update, context, session_data)
        
        elif message_text == "✏️ Edit perimeter":
            session_data["state"] = ConversationState.PERIMETER_EDIT.value
            self.db.save_quote_session(session_data["user_id"], session_data, session_data["state"])
            await self.ask_perimeter_edit(update, context, session_data)
    
    async def handle_perimeter_edit(self, update, context, message_text, session_data):
        """Handle manual perimeter input"""
        try:
            perimeter = float(message_text.replace(",", ".").replace("m", "").strip())
            
            if 0.1 <= perimeter <= 500:
                # Save previous state
                await self.save_state_for_back_navigation(session_data, session_data["state"])
                
                ceiling = session_data["ceilings"][session_data["current_ceiling_index"]]
                ceiling["perimeter"] = perimeter
                ceiling["perimeter_edited"] = True
                
                session_data["state"] = ConversationState.CORNERS_COUNT.value
                self.db.save_quote_session(session_data["user_id"], session_data, session_data["state"])
                await self.ask_corners_count(update, context, session_data)
            else:
                await update.message.reply_text(
                    "Please enter a valid perimeter between 0.1 and 500 meters."
                )
        
        except ValueError:
            await update.message.reply_text(
                "Invalid format. Please enter a number (e.g., 18.5)"
            )
    
    async def handle_corners_count(self, update, context, message_text, session_data):
        """Handle corners count input"""
        try:
            if message_text == "More than 8":
                corners = 8
            else:
                corners = int(message_text)
            
            if 3 <= corners <= 20:
                # Save previous state
                await self.save_state_for_back_navigation(session_data, session_data["state"])
                
                ceiling = session_data["ceilings"][session_data["current_ceiling_index"]]
                ceiling["corners"] = corners
                
                session_data["state"] = ConversationState.CEILING_TYPE.value
                self.db.save_quote_session(session_data["user_id"], session_data, session_data["state"])
                await self.ask_ceiling_type(update, context, session_data)
            else:
                await update.message.reply_text("Please enter a number between 3 and 20.")
        
        except ValueError:
            await update.message.reply_text("Please enter a valid number.")
    
    async def handle_ceiling_type(self, update, context, message_text, session_data):
        """Handle ceiling type selection"""
        # Save previous state
        await self.save_state_for_back_navigation(session_data, session_data["state"])
        
        ceiling = session_data["ceilings"][session_data["current_ceiling_index"]]
        
        # Store the ceiling type in lowercase for consistency
        ceiling["ceiling_type"] = message_text.lower()
        ceiling["product_type"] = message_text.lower()  # Store in both fields
        
        logger.info(f"✅ Ceiling type set to '{message_text.lower()}' for ceiling '{ceiling['name']}'")
        
        # IMPORTANT: Always go to type_ceiling state
        session_data["state"] = ConversationState.TYPE_CEILING.value
        self.db.save_quote_session(session_data["user_id"], session_data, session_data["state"])
        await self.ask_type_ceiling(update, context, session_data)
    
    async def handle_type_ceiling(self, update, context, message_text, session_data):
        """Handle type_ceiling selection"""
        # Save previous state
        await self.save_state_for_back_navigation(session_data, session_data["state"])
        
        ceiling = session_data["ceilings"][session_data["current_ceiling_index"]]
        ceiling["type_ceiling"] = message_text
        
        logger.info(f"✅ Type ceiling set to '{message_text}' for ceiling '{ceiling['name']}'")
        logger.info(f"Current ceiling config: type={ceiling['ceiling_type']}, type_ceiling={ceiling['type_ceiling']}")
        
        session_data["state"] = ConversationState.CEILING_COLOR.value
        self.db.save_quote_session(session_data["user_id"], session_data, session_data["state"])
        await self.ask_ceiling_color(update, context, session_data)
    
    async def handle_ceiling_color(self, update, context, message_text, session_data):
        """Handle ceiling color selection"""
        # Save previous state
        await self.save_state_for_back_navigation(session_data, session_data["state"])
        
        ceiling = session_data["ceilings"][session_data["current_ceiling_index"]]
        
        # Store color in lowercase for consistency
        ceiling["color"] = message_text.lower()
        
        logger.info(f"✅ Ceiling color set to '{message_text.lower()}' for ceiling '{ceiling['name']}'")
        logger.info(f"Final ceiling config: type={ceiling['ceiling_type']}, type_ceiling={ceiling['type_ceiling']}, color={ceiling['color']}")
        
        # Skip finish and acoustic questions, go directly to check if acoustic
        session_data["state"] = ConversationState.CEILING_ACOUSTIC.value
        self.db.save_quote_session(session_data["user_id"], session_data, session_data["state"])
        await self.ask_ceiling_acoustic(update, context, session_data)
    
    async def handle_ceiling_finish(self, update, context, message_text, session_data):
        """Handle ceiling finish selection"""
        ceiling = session_data["ceilings"][session_data["current_ceiling_index"]]
        ceiling["finish"] = message_text
        
        session_data["state"] = ConversationState.CEILING_ACOUSTIC.value
        self.db.save_quote_session(session_data["user_id"], session_data, session_data["state"])
        await self.ask_ceiling_acoustic(update, context, session_data)
    
    async def handle_ceiling_acoustic(self, update, context, message_text, session_data):
        """Handle acoustic selection"""
        # This is handled automatically in ask_ceiling_acoustic
        pass
    
    async def handle_acoustic_performance(self, update, context, message_text, session_data):
        """Handle acoustic performance selection"""
        # Save previous state
        await self.save_state_for_back_navigation(session_data, session_data["state"])
        
        ceiling = session_data["ceilings"][session_data["current_ceiling_index"]]
        
        if message_text != "Skip acoustic enhancement":
            # Extract the acoustic performance value
            parts = message_text.split(" - ")
            acoustic_perf = parts[0] if parts else message_text
            
            # Find the acoustic product
            acoustic_products = self.db.get_acoustic_performance_products()
            for product in acoustic_products:
                if product.get("acoustic_performance") == acoustic_perf:
                    ceiling["acoustic_product"] = product
                    ceiling["acoustic_performance"] = acoustic_perf
                    break
        
        session_data["state"] = ConversationState.PERIMETER_PROFILE.value
        self.db.save_quote_session(session_data["user_id"], session_data, session_data["state"])
        await self.ask_perimeter_profile(update, context, session_data)
    
    async def handle_perimeter_profile(self, update, context, message_text, session_data):
        """Handle perimeter profile selection"""
        # Save previous state
        await self.save_state_for_back_navigation(session_data, session_data["state"])
        
        ceiling = session_data["ceilings"][session_data["current_ceiling_index"]]
        
        # Extract product code
        if " - " in message_text:
            code = message_text.split(" - ")[0]
            product = self.db.get_product_by_code(code)
            if product:
                # Convert Decimal values to float before storing
                for key, value in product.items():
                    if hasattr(value, 'quantize'):  # Check if it's a Decimal
                        product[key] = float(value)
                ceiling["perimeter_profile"] = product
        
        session_data["state"] = ConversationState.SEAM_QUESTION.value
        self.db.save_quote_session(session_data["user_id"], session_data, session_data["state"])
        await self.ask_seam_question(update, context, session_data)
    
    async def handle_seam_question(self, update, context, message_text, session_data):
        """Handle seam question"""
        # Save previous state
        await self.save_state_for_back_navigation(session_data, session_data["state"])
        
        ceiling = session_data["ceilings"][session_data["current_ceiling_index"]]
        ceiling["has_seams"] = message_text.startswith("Yes")
        
        if ceiling["has_seams"]:
            session_data["state"] = ConversationState.SEAM_LENGTH.value
            self.db.save_quote_session(session_data["user_id"], session_data, session_data["state"])
            await self.ask_seam_length(update, context, session_data)
        else:
            session_data["state"] = ConversationState.LIGHTS_QUESTION.value
            self.db.save_quote_session(session_data["user_id"], session_data, session_data["state"])
            await self.ask_lights_question(update, context, session_data)
    
    async def handle_seam_length(self, update, context, message_text, session_data):
        """Handle seam length input"""
        try:
            length = float(message_text.replace(",", "."))
            if length > 0:
                # Save previous state
                await self.save_state_for_back_navigation(session_data, session_data["state"])
                
                ceiling = session_data["ceilings"][session_data["current_ceiling_index"]]
                ceiling["seam_length"] = length
                
                session_data["state"] = ConversationState.LIGHTS_QUESTION.value
                self.db.save_quote_session(session_data["user_id"], session_data, session_data["state"])
                await self.ask_lights_question(update, context, session_data)
            else:
                await update.message.reply_text("Please enter a positive number.")
        
        except ValueError:
            await update.message.reply_text("Please enter a valid number.")
    
    async def handle_lights_question(self, update, context, message_text, session_data):
        """Handle lights question"""
        # Save previous state
        await self.save_state_for_back_navigation(session_data, session_data["state"])
        
        if message_text.startswith("Yes"):
            session_data["state"] = ConversationState.LIGHT_SELECTION.value
            self.db.save_quote_session(session_data["user_id"], session_data, session_data["state"])
            await self.ask_light_selection(update, context, session_data)
        else:
            session_data["state"] = ConversationState.WOOD_QUESTION.value
            self.db.save_quote_session(session_data["user_id"], session_data, session_data["state"])
            await self.ask_wood_question(update, context, session_data)
    
    async def handle_light_selection(self, update, context, message_text, session_data):
        """Handle light selection"""
        if message_text == "No lights available":
            session_data["state"] = ConversationState.WOOD_QUESTION.value
            self.db.save_quote_session(session_data["user_id"], session_data, session_data["state"])
            await self.ask_wood_question(update, context, session_data)
        else:
            # Extract product code
            code = message_text.split(" - ")[0]
            product = self.db.get_product_by_code(code)
            if product:
                session_data["temp_light"] = product
                session_data["state"] = ConversationState.LIGHT_QUANTITY.value
                self.db.save_quote_session(session_data["user_id"], session_data, session_data["state"])
                await self.ask_light_quantity(update, context, session_data)
            else:
                await update.message.reply_text("Invalid selection. Please try again.")
    
    async def handle_light_quantity(self, update, context, message_text, session_data):
        """Handle light quantity input"""
        try:
            quantity = int(message_text)
            if quantity > 0:
                # Save previous state
                await self.save_state_for_back_navigation(session_data, session_data["state"])
                
                ceiling = session_data["ceilings"][session_data["current_ceiling_index"]]
                light = session_data["temp_light"].copy()
                light["quantity"] = quantity
                light["price"] = light[session_data["client_group"]]
                
                # Ensure we have product_code field for backward compatibility
                if "product_code" not in light and "code" in light:
                    light["product_code"] = light["code"]
                
                ceiling["lights"].append(light)
                
                session_data["state"] = ConversationState.MORE_LIGHTS.value
                self.db.save_quote_session(session_data["user_id"], session_data, session_data["state"])
                await self.ask_more_lights(update, context, session_data)
            else:
                await update.message.reply_text("Please enter a positive number.")
        
        except ValueError:
            await update.message.reply_text("Please enter a valid number.")
    
    async def handle_more_lights(self, update, context, message_text, session_data):
        """Handle more lights question"""
        if message_text.startswith("Yes"):
            session_data["state"] = ConversationState.LIGHT_SELECTION.value
            self.db.save_quote_session(session_data["user_id"], session_data, session_data["state"])
            await self.ask_light_selection(update, context, session_data)
        else:
            session_data["state"] = ConversationState.WOOD_QUESTION.value
            self.db.save_quote_session(session_data["user_id"], session_data, session_data["state"])
            await self.ask_wood_question(update, context, session_data)
    
    async def handle_wood_question(self, update, context, message_text, session_data):
        """Handle wood structures question"""
        # Save previous state
        await self.save_state_for_back_navigation(session_data, session_data["state"])
        
        if message_text.startswith("Yes"):
            session_data["state"] = ConversationState.WOOD_SELECTION.value
            self.db.save_quote_session(session_data["user_id"], session_data, session_data["state"])
            await self.ask_wood_selection(update, context, session_data)
        else:
            # Ceiling complete - calculate costs with debug info
            ceiling_index = session_data["current_ceiling_index"]
            ceiling_data = session_data["ceilings"][ceiling_index]
            
            logger.info(f"📝 Calculating costs for ceiling: {ceiling_data['name']}")
            logger.info(f"  Config: type={ceiling_data.get('ceiling_type')}, "
                       f"type_ceiling={ceiling_data.get('type_ceiling')}, "
                       f"color={ceiling_data.get('color')}")
            
            ceiling_config = self.create_ceiling_config(ceiling_data)
            
            # Fix any lights/wood that have 'code' instead of 'product_code'
            for light in ceiling_config.lights:
                if 'product_code' not in light and 'code' in light:
                    light['product_code'] = light['code']
            
            for wood in ceiling_config.wood_structures:
                if 'product_code' not in wood and 'code' in wood:
                    wood['product_code'] = wood['code']
            
            costs = self.calculator.calculate_ceiling_costs(ceiling_config, session_data["client_group"])
            
            # Convert CeilingCost to dict for JSON serialization
            costs_dict = {
                "ceiling_cost": costs.ceiling_cost,
                "perimeter_structure_cost": costs.perimeter_structure_cost,
                "perimeter_profile_cost": costs.perimeter_profile_cost,
                "corners_cost": costs.corners_cost,
                "seam_cost": costs.seam_cost,
                "lights_cost": costs.lights_cost,
                "wood_structures_cost": costs.wood_structures_cost,
                "acoustic_absorber_cost": costs.acoustic_absorber_cost,
                "total": costs.total
            }
            
            logger.info(f"💰 Calculated total cost for ceiling '{ceiling_data['name']}': €{costs.total:.2f}")
            
            session_data["ceiling_costs"].append(costs_dict)
            
            session_data["state"] = ConversationState.NEXT_CEILING.value
            self.db.save_quote_session(session_data["user_id"], session_data, session_data["state"])
            await self.ask_next_ceiling(update, context, session_data)
    
    async def handle_wood_selection(self, update, context, message_text, session_data):
        """Handle wood structure selection"""
        if message_text == "No wood structures available":
            # Ceiling complete
            ceiling_config = self.create_ceiling_config(
                session_data["ceilings"][session_data["current_ceiling_index"]]
            )
            
            # Fix any lights/wood that have 'code' instead of 'product_code'
            for light in ceiling_config.lights:
                if 'product_code' not in light and 'code' in light:
                    light['product_code'] = light['code']
            
            for wood in ceiling_config.wood_structures:
                if 'product_code' not in wood and 'code' in wood:
                    wood['product_code'] = wood['code']
            
            costs = self.calculator.calculate_ceiling_costs(ceiling_config, session_data["client_group"])
            
            # Convert CeilingCost to dict for JSON serialization
            costs_dict = {
                "ceiling_cost": costs.ceiling_cost,
                "perimeter_structure_cost": costs.perimeter_structure_cost,
                "perimeter_profile_cost": costs.perimeter_profile_cost,
                "corners_cost": costs.corners_cost,
                "seam_cost": costs.seam_cost,
                "lights_cost": costs.lights_cost,
                "wood_structures_cost": costs.wood_structures_cost,
                "acoustic_absorber_cost": costs.acoustic_absorber_cost,
                "total": costs.total
            }
            
            session_data["ceiling_costs"].append(costs_dict)
            
            session_data["state"] = ConversationState.NEXT_CEILING.value
            self.db.save_quote_session(session_data["user_id"], session_data, session_data["state"])
            await self.ask_next_ceiling(update, context, session_data)
        else:
            # Extract product code
            code = message_text.split(" - ")[0]
            product = self.db.get_product_by_code(code)
            if product:
                session_data["temp_wood"] = product
                session_data["state"] = ConversationState.WOOD_QUANTITY.value
                self.db.save_quote_session(session_data["user_id"], session_data, session_data["state"])
                await self.ask_wood_quantity(update, context, session_data)
            else:
                await update.message.reply_text("Invalid selection. Please try again.")
    
    async def handle_wood_quantity(self, update, context, message_text, session_data):
        """Handle wood quantity input"""
        try:
            quantity = float(message_text.replace(",", "."))
            if quantity > 0:
                # Save previous state
                await self.save_state_for_back_navigation(session_data, session_data["state"])
                
                ceiling = session_data["ceilings"][session_data["current_ceiling_index"]]
                wood = session_data["temp_wood"].copy()
                wood["quantity"] = quantity
                wood["price"] = wood[session_data["client_group"]]
                
                # Ensure we have product_code field for backward compatibility
                if "product_code" not in wood and "code" in wood:
                    wood["product_code"] = wood["code"]
                
                ceiling["wood_structures"].append(wood)
                
                session_data["state"] = ConversationState.MORE_WOOD.value
                self.db.save_quote_session(session_data["user_id"], session_data, session_data["state"])
                await self.ask_more_wood(update, context, session_data)
            else:
                await update.message.reply_text("Please enter a positive number.")
        
        except ValueError:
            await update.message.reply_text("Please enter a valid number.")
    
    async def handle_more_wood(self, update, context, message_text, session_data):
        """Handle more wood structures question"""
        if message_text.startswith("Yes"):
            session_data["state"] = ConversationState.WOOD_SELECTION.value
            self.db.save_quote_session(session_data["user_id"], session_data, session_data["state"])
            await self.ask_wood_selection(update, context, session_data)
        else:
            # Ceiling complete
            ceiling_config = self.create_ceiling_config(
                session_data["ceilings"][session_data["current_ceiling_index"]]
            )
            
            # Fix any lights/wood that have 'code' instead of 'product_code'
            for light in ceiling_config.lights:
                if 'product_code' not in light and 'code' in light:
                    light['product_code'] = light['code']
            
            for wood in ceiling_config.wood_structures:
                if 'product_code' not in wood and 'code' in wood:
                    wood['product_code'] = wood['code']
            
            costs = self.calculator.calculate_ceiling_costs(ceiling_config, session_data["client_group"])
            
            # Convert CeilingCost to dict for JSON serialization
            costs_dict = {
                "ceiling_cost": costs.ceiling_cost,
                "perimeter_structure_cost": costs.perimeter_structure_cost,
                "perimeter_profile_cost": costs.perimeter_profile_cost,
                "corners_cost": costs.corners_cost,
                "seam_cost": costs.seam_cost,
                "lights_cost": costs.lights_cost,
                "wood_structures_cost": costs.wood_structures_cost,
                "acoustic_absorber_cost": costs.acoustic_absorber_cost,
                "total": costs.total
            }
            
            session_data["ceiling_costs"].append(costs_dict)
            
            session_data["state"] = ConversationState.NEXT_CEILING.value
            self.db.save_quote_session(session_data["user_id"], session_data, session_data["state"])
            await self.ask_next_ceiling(update, context, session_data)
    
    async def handle_next_ceiling(self, update, context, message_text, session_data):
        """Handle next ceiling decision"""
        # Save previous state
        await self.save_state_for_back_navigation(session_data, session_data["state"])
        
        if message_text.startswith("Continue"):
            # Move to next ceiling
            session_data["current_ceiling_index"] += 1
            session_data["state"] = ConversationState.CEILING_NAME.value
            self.db.save_quote_session(session_data["user_id"], session_data, session_data["state"])
            await self.ask_ceiling_name(update, context, session_data)
        
        elif message_text == "View summary and finish":
            # Check if there are more ceilings
            current = session_data["current_ceiling_index"] + 1
            total = session_data["ceiling_count"]
            
            if current < total:
                # Still more ceilings, ask to continue or finish
                session_data["current_ceiling_index"] += 1
                session_data["state"] = ConversationState.CEILING_NAME.value
                self.db.save_quote_session(session_data["user_id"], session_data, session_data["state"])
                await self.ask_ceiling_name(update, context, session_data)
            else:
                # All ceilings done
                session_data["state"] = ConversationState.QUOTE_REFERENCE.value
                self.db.save_quote_session(session_data["user_id"], session_data, session_data["state"])
                await self.ask_quote_reference(update, context, session_data)
        else:
            # Finish quote
            session_data["state"] = ConversationState.QUOTE_REFERENCE.value
            self.db.save_quote_session(session_data["user_id"], session_data, session_data["state"])
            await self.ask_quote_reference(update, context, session_data)
    
    async def handle_quote_reference(self, update, context, message_text, session_data):
        """Handle quote reference input"""
        # Save previous state
        await self.save_state_for_back_navigation(session_data, session_data["state"])
        
        session_data["quote_reference"] = message_text
        
        # Generate final quote
        await self.generate_final_quote(update, context, session_data)
        
        # Check if customer has email(s) for email selection
        if session_data.get("customer"):
            session_data["state"] = ConversationState.EMAIL_SELECTION.value
            self.db.save_quote_session(session_data["user_id"], session_data, session_data["state"])
            await self.ask_email_selection(update, context, session_data)
        else:
            session_data["state"] = ConversationState.EMAIL_REQUEST.value
            self.db.save_quote_session(session_data["user_id"], session_data, session_data["state"])
            await self.ask_email_request(update, context, session_data)
    
    async def handle_email_request(self, update, context, message_text, session_data):
        """Handle email request"""
        if message_text.startswith("📧"):
            session_data["state"] = ConversationState.EMAIL_INPUT.value
            self.db.save_quote_session(session_data["user_id"], session_data, session_data["state"])
            await self.ask_email_input(update, context, session_data)
        else:
            await self.complete_quote(update, context, session_data)
    
    async def handle_email_input(self, update, context, message_text, session_data):
        """Handle email input - UPDATED WITH ENTRA ID EMAIL SENDING"""
        if "@" in message_text and "." in message_text:
            session_data["email"] = message_text
            await update.message.reply_text(f"✅ Quote will be sent to {message_text}")
            
            # Store email for sending after quote is saved
            context.user_data["pending_email"] = message_text
            
            await self.complete_quote(update, context, session_data)
        else:
            await update.message.reply_text("Please enter a valid email address.")
    
    # ==================== HELPER METHODS ====================
    
    def create_ceiling_config(self, ceiling_data: Dict) -> CeilingConfig:
        """Create CeilingConfig from ceiling data"""
        config = CeilingConfig(
            name=ceiling_data["name"],
            length=ceiling_data["length"],
            width=ceiling_data["width"],
            area=ceiling_data["area"],
            perimeter=ceiling_data["perimeter"],
            perimeter_edited=ceiling_data.get("perimeter_edited", False),
            corners=ceiling_data["corners"],
            ceiling_type=ceiling_data.get("ceiling_type", ""),
            type_ceiling=ceiling_data.get("type_ceiling", "standard"),
            color=ceiling_data.get("color", "white"),
            acoustic=ceiling_data.get("acoustic", False),
            finish=ceiling_data.get("finish", "Mat"),
            perimeter_profile=ceiling_data.get("perimeter_profile"),
            has_seams=ceiling_data.get("has_seams", False),
            seam_length=ceiling_data.get("seam_length", 0),
            lights=ceiling_data.get("lights", []),
            wood_structures=ceiling_data.get("wood_structures", []),
            acoustic_product=ceiling_data.get("acoustic_product")
        )
        
        # Ensure area and perimeter are calculated
        if config.area == 0 and config.length > 0 and config.width > 0:
            config.calculate_dimensions()
        
        # Set acoustic performance if available
        if ceiling_data.get("acoustic_product"):
            config.acoustic_performance = ceiling_data["acoustic_product"].get("acoustic_performance")
        
        return config
    
    def format_ceiling_summary(self, config: CeilingConfig, costs) -> str:
        """Format ceiling summary with proper escaping"""
        # Escape user-provided name
        ceiling_name = escape_markdown(config.name)
        summary = f"✅ **{ceiling_name} \\- Complete**\n\n"
        
        summary += f"📐 **Specifications:**\n"
        summary += f"• Dimensions: {config.length}m × {config.width}m\n"
        summary += f"• Area: {config.area:.2f} m²\n"
        summary += f"• Perimeter: {config.perimeter:.2f} m"
        if config.perimeter_edited:
            summary += " \\(manually edited\\)"
        summary += f"\n• Corners: {config.corners}\n"
        summary += f"• Type: {config.ceiling_type.upper()} \\- {escape_markdown(config.type_ceiling)}\n"
        summary += f"• Color: {escape_markdown(config.color.capitalize())}\n"
        summary += f"• Acoustic: {'Yes' if config.acoustic else 'No'}\n"
        
        if config.acoustic_performance:
            summary += f"• Acoustic Enhancement: {escape_markdown(config.acoustic_performance)}\n"
        
        if config.has_seams:
            summary += f"• Seams: {config.seam_length} m\n"
        
        # Handle both CeilingCost objects and dicts
        if isinstance(costs, dict):
            ceiling_cost = costs.get("ceiling_cost", 0)
            perimeter_structure_cost = costs.get("perimeter_structure_cost", 0)
            perimeter_profile_cost = costs.get("perimeter_profile_cost", 0)
            corners_cost = costs.get("corners_cost", 0)
            seam_cost = costs.get("seam_cost", 0)
            acoustic_absorber_cost = costs.get("acoustic_absorber_cost", 0)
            lights_cost = costs.get("lights_cost", 0)
            wood_structures_cost = costs.get("wood_structures_cost", 0)
            total_cost = costs.get("total", 0)
        else:
            ceiling_cost = costs.ceiling_cost
            perimeter_structure_cost = costs.perimeter_structure_cost
            perimeter_profile_cost = costs.perimeter_profile_cost
            corners_cost = costs.corners_cost
            seam_cost = costs.seam_cost
            acoustic_absorber_cost = costs.acoustic_absorber_cost
            lights_cost = costs.lights_cost
            wood_structures_cost = costs.wood_structures_cost
            total_cost = costs.total
        
        summary += f"\n💰 **Cost Breakdown:**\n"
        if ceiling_cost > 0:
            summary += f"• Ceiling Material: {format_price(ceiling_cost)}\n"
        if perimeter_structure_cost > 0:
            summary += f"• Perimeter Structure: {format_price(perimeter_structure_cost)}\n"
        if perimeter_profile_cost > 0:
            summary += f"• Perimeter Profile: {format_price(perimeter_profile_cost)}\n"
        if corners_cost > 0:
            summary += f"• Corners: {format_price(corners_cost)}\n"
        if seam_cost > 0:
            summary += f"• Seams: {format_price(seam_cost)}\n"
        if acoustic_absorber_cost > 0:
            summary += f"• Acoustic Enhancement: {format_price(acoustic_absorber_cost)}\n"
        if lights_cost > 0:
            summary += f"• Lights: {format_price(lights_cost)}\n"
            for light in config.lights:
                # Use product_code instead of code, with fallback
                light_code = escape_markdown(light.get("product_code", light.get("code", "UNKNOWN")))
                summary += f"  \\- {light_code}: {light['quantity']} pcs\n"
        if wood_structures_cost > 0:
            summary += f"• Wood Structures: {format_price(wood_structures_cost)}\n"
            for wood in config.wood_structures:
                # Use product_code instead of code, with fallback
                wood_code = escape_markdown(wood.get("product_code", wood.get("code", "UNKNOWN")))
                summary += f"  \\- {wood_code}: {wood['quantity']} m\n"
        
        summary += f"\n🎯 **Total: {format_price(total_cost)}**"
        
        return summary
    
    def format_quote_summary_with_customer(self, session_data: Dict) -> str:
        """Format complete quote summary including customer information"""
        summary = "📋 **QUOTE SUMMARY**\n\n"
        
        # Customer information
        customer = session_data.get("customer")
        if customer:
            summary += "👤 **Customer:**\n"
            if customer.get("display_name"):
                summary += f"• Name: {escape_markdown(customer['display_name'])}\n"
            if customer.get("contact_name") and customer.get("contact_name") != customer.get("display_name"):
                summary += f"• Contact: {escape_markdown(customer['contact_name'])}\n"
            if customer.get("email"):
                summary += f"• Email: {escape_markdown(customer['email'])}\n"
            summary += "\n"
        
        # Ceilings summary
        grand_total = 0
        for i, (ceiling_data, costs) in enumerate(zip(session_data["ceilings"], session_data["ceiling_costs"])):
            config = self.create_ceiling_config(ceiling_data)
            
            if isinstance(costs, dict):
                total_cost = costs.get("total", 0)
            else:
                total_cost = costs.total
            
            summary += f"**{i + 1}\\. {escape_markdown(config.name)}**\n"
            summary += f"   {config.length}m × {config.width}m = {config.area:.2f} m²\n"
            summary += f"   {config.ceiling_type.upper()} \\- {escape_markdown(config.type_ceiling)}\n"
            summary += f"   Total: {format_price(total_cost)}\n\n"
            
            grand_total += total_cost
        
        summary += f"➖➖➖➖➖➖➖➖➖➖\n"
        summary += f"🎯 **GRAND TOTAL: {format_price(grand_total)}**"
        
        return summary
    
    async def generate_final_quote(self, update, context, session_data):
        """Generate the final quote with proper escaping"""
        quote = f"🏗️ **MULTI\\-CEILING STRETCH CEILING QUOTE** 🏗️\n\n"
        quote += f"📋 Reference: {escape_markdown(session_data['quote_reference'])}\n"
        quote += f"📅 Date: {datetime.now().strftime('%Y-%m-%d')}\n"
        
        # Fix client group display
        client_group = session_data["client_group"]
        if client_group.startswith("price_"):
            client_group_display = client_group.replace("price_", "").replace("_", " ").title()
        else:
            client_group_display = client_group.replace("_", " ").title()
        
        quote += f"👤 Client Type: {escape_markdown(client_group_display)}\n"
        
        # Add customer info if available
        customer = session_data.get("customer")
        if customer:
            if customer.get("display_name"):
                quote += f"🏢 Customer: {escape_markdown(customer['display_name'])}\n"
            if customer.get("contact_name") and customer.get("contact_name") != customer.get("display_name"):
                quote += f"👤 Contact: {escape_markdown(customer['contact_name'])}\n"
        
        quote += "\n"
        
        grand_total = 0
        has_estimates = False
        
        # Individual ceiling summaries
        for i, (ceiling_data, costs) in enumerate(zip(session_data["ceilings"], session_data["ceiling_costs"])):
            config = self.create_ceiling_config(ceiling_data)
            
            # Handle both CeilingCost objects and dicts
            if isinstance(costs, dict):
                total_cost = costs.get("total", 0)
                ceiling_cost = costs.get("ceiling_cost", 0)
            else:
                total_cost = costs.total
                ceiling_cost = costs.ceiling_cost
            
            ceiling_name = escape_markdown(config.name)
            quote += f"**{i + 1}\\. {ceiling_name}**\n"
            quote += f"   📐 {config.length}m × {config.width}m = {config.area:.2f} m²\n"
            quote += f"   📏 Perimeter: {config.perimeter:.2f} m"
            if config.perimeter_edited:
                quote += " \\(manually adjusted\\)"
            quote += f"\n   🎨 {config.ceiling_type.upper()} \\- {escape_markdown(config.type_ceiling)} \\- {escape_markdown(config.color.capitalize())}\n"
            
            if ceiling_cost > 0:
                quote += f"   💰 Total: {format_price(total_cost)}\n\n"
            else:
                quote += f"   💰 Total: {format_price(total_cost)} \\*\n\n"
                has_estimates = True
            
            grand_total += total_cost
        
        quote += f"➖➖➖➖➖➖➖➖➖➖➖➖\n"
        quote += f"🎯 **GRAND TOTAL: {format_price(grand_total)}**\n\n"
        quote += f"✅ This quote is valid for {Config.QUOTE_VALIDITY_DAYS} days\n"
        
        if has_estimates:
            quote += f"\n⚠️ \\*Some prices are estimated\\. Contact us for exact pricing\\."
        
        await update.message.reply_text(quote, parse_mode="Markdown")
    
    async def complete_quote(self, update, context, session_data):
        """Complete the quote process - UPDATED WITH CUSTOMER DATA AND DYNAMICS 365 SYNC"""
        try:
            # Calculate grand total
            grand_total = 0
            for costs in session_data["ceiling_costs"]:
                if isinstance(costs, dict):
                    total_cost = costs.get("total", 0)
                else:
                    total_cost = costs.total
                grand_total += total_cost
            
            # Save to database
            quote_id = self.db.save_quotation(
                user_id=session_data["user_id"],
                quote_data=session_data,
                total_price=grand_total,
                client_group=session_data["client_group"]
            )
            
            if quote_id:
                # Generate PDF automatically
                try:
                    quote = self.db.get_quote_by_id(quote_id)
                    if quote and Config.ENABLE_PDF_GENERATION:
                        # Define paths HERE
                        output_dir = "/tmp/quotes"
                        logo_path = "/home/STRETCH/stretch_logo.png"
                        os.makedirs(output_dir, exist_ok=True)
                        
                        pdf_generator = ImprovedStretchQuotePDFGenerator(output_dir, logo_path)
                        quote_data = json.loads(quote["quote_data"])
                        
                        # CRITICAL FIX: Merge ceiling_costs into ceilings for PDF
                        # The PDF generator expects 'total_price' and 'cost_breakdown' in each ceiling
                        if "ceilings" in quote_data and "ceiling_costs" in quote_data:
                            for i, ceiling in enumerate(quote_data["ceilings"]):
                                if i < len(quote_data["ceiling_costs"]):
                                    costs = quote_data["ceiling_costs"][i]
                                    ceiling["total_price"] = costs.get("total", 0)
                                    # Map from ceiling_costs keys to PDF expected keys
                                    ceiling["cost_breakdown"] = {
                                        "ceiling_material": costs.get("ceiling_cost", 0),
                                        "perimeter_structure": costs.get("perimeter_structure_cost", 0),
                                        "perimeter_profile": costs.get("perimeter_profile_cost", 0),
                                        "corners": costs.get("corners_cost", 0),
                                        "acoustic_absorber": costs.get("acoustic_absorber_cost", 0),
                                        "seams": costs.get("seam_cost", 0),
                                        "lights": costs.get("lights_cost", 0),
                                        "wood_structures": costs.get("wood_structures_cost", 0),
                                    }
                                    logger.info(f"📋 Ceiling {i+1} price for PDF: €{ceiling['total_price']:.2f}")
                                    logger.info(f"📋 Cost breakdown: {ceiling['cost_breakdown']}")
                        
                        # CRITICAL FIX: Add customer data from session to quote_data for PDF
                        # The customer selection data is in session_data, not in the stored quote_data
                        if session_data.get("customer"):
                            quote_data["customer"] = session_data["customer"]
                            customer = session_data["customer"]
                            logger.info(f"📋 Customer data for PDF:")
                            logger.info(f"  - display_name: {customer.get('display_name')}")
                            logger.info(f"  - company_name: {customer.get('company_name')}")
                            logger.info(f"  - contact_name: {customer.get('contact_name')}")
                            logger.info(f"  - email: {customer.get('email')}")
                            logger.info(f"  - phone: {customer.get('phone')}")
                            logger.info(f"  - address: {customer.get('address')}")
                            logger.info(f"  - vat_number: {customer.get('vat_number')}")
                        else:
                            logger.warning(f"⚠️ NO customer data in session_data for PDF!")
                            logger.warning(f"  - session_data keys: {list(session_data.keys())}")
                        
                        # FIXED: Get user profile for billing info in PDF (may be None)
                        user_profile = self.db.get_user_profile(session_data["user_id"])
                        
                        # Ensure user_profile is a dict, not None
                        if user_profile is None:
                            user_profile = {}
                            logger.warning(f"User profile not found for user {session_data['user_id']}, using empty profile")
                        
                        # FIXED: Pass user_profile to PDF generator for billing info
                        pdf_path = pdf_generator.build_pdf(
                            quote["quote_number"], 
                            quote_data,
                            user_profile if user_profile else None
                        )
                        
                        # Send PDF via Telegram
                        with open(pdf_path, "rb") as pdf_file:
                            await context.bot.send_document(
                                chat_id=session_data["user_id"],
                                document=pdf_file,
                                filename=f"Quote_{quote['quote_number']}.pdf",
                                caption=f"📄 Your quote has been generated!\n"
                                        f"Quote ID: #{quote_id}\n"
                                        f"Total: {format_price(grand_total)}"
                            )
                        
                        # Check if email was requested and send if needed
                        if context.user_data.get("pending_email"):
                            email = context.user_data["pending_email"]
                            email_sent = await self.email_sender.send_quote_email(
                                recipient_email=email,
                                quote_number=quote["quote_number"],
                                pdf_path=pdf_path,
                                quote_data=quote_data,
                                total_price=grand_total
                            )
                            
                            if email_sent:
                                await update.message.reply_text(
                                    f"✅ Quote sent successfully to {email}!\n"
                                    f"A copy was also sent to {Config.COMPANY_EMAIL}"
                                )
                            else:
                                await update.message.reply_text(
                                    "⚠️ Failed to send email. Please contact support."
                                )
                            
                            # Clear pending email
                            del context.user_data["pending_email"]
                
                except Exception as e:
                    logger.error(f"❌ Error generating/sending PDF: {e}")
                
                # Sync to Dynamics 365 if enabled
                try:
                    # Get dynamics integration from application context
                    if hasattr(context, 'application') and hasattr(context.application, 'bot_data'):
                        dynamics_obj = context.application.bot_data.get('dynamics_integration')
                        
                        if dynamics_obj:
                            # Check what type of object we have
                            if hasattr(dynamics_obj, 'sync_quote_to_dynamics'):
                                # It's the Dynamics365IntegrationHandler
                                asyncio.create_task(dynamics_obj.sync_quote_to_dynamics(quote_id))
                                logger.info(f"🔄 Queued quote {quote_id} for Dynamics 365 sync via integration handler")
                            elif hasattr(dynamics_obj, 'create_or_update_quote'):
                                # It's Dynamics365Service directly - sync inline
                                try:
                                    # Get customer IDs from session
                                    customer = session_data.get("customer", {})
                                    contact_id = customer.get("dynamics_contact_id")
                                    account_id = customer.get("dynamics_account_id")
                                    
                                    if contact_id or account_id:
                                        quote_data_for_sync = json.loads(quote["quote_data"])
                                        quote_data_for_sync["customer"] = customer
                                        
                                        dynamics_quote_id = await dynamics_obj.create_or_update_quote(
                                            quote_data=quote_data_for_sync,
                                            contact_id=contact_id,
                                            account_id=account_id
                                        )
                                        if dynamics_quote_id:
                                            logger.info(f"✅ Quote {quote_id} synced to Dynamics 365: {dynamics_quote_id}")
                                        else:
                                            logger.warning(f"⚠️ Quote {quote_id} sync returned no ID")
                                    else:
                                        logger.warning(f"No Dynamics customer IDs found for quote {quote_id}")
                                except Exception as sync_err:
                                    logger.error(f"Error syncing quote directly: {sync_err}")
                            else:
                                logger.warning(f"Dynamics object found but no sync method available: {type(dynamics_obj)}")
                except Exception as e:
                    logger.error(f"Could not queue quote to Dynamics sync: {e}")
                    import traceback
                    logger.error(traceback.format_exc())
                    # Don't fail the quote save if Dynamics sync fails
                
                # Show customer info in completion message if available
                customer_info = ""
                if session_data.get("customer"):
                    customer = session_data["customer"]
                    if customer.get("display_name"):
                        customer_info = f"\n👤 Customer: {customer['display_name']}"
                
                await update.message.reply_text(
                    f"✅ **Quote Saved Successfully!**\n\n"
                    f"Quote ID: #{quote_id}{customer_info}\n"
                    f"Total: {format_price(grand_total)}\n\n"
                    f"Thank you for using {Config.COMPANY_NAME}!\n\n"
                    f"Use /quotes to view and manage your quotes.",
                    reply_markup=ReplyKeyboardRemove(),
                    parse_mode="Markdown"
                )
            else:
                await update.message.reply_text(
                    "❌ Error saving quote. Please contact support.",
                    reply_markup=ReplyKeyboardRemove()
                )
            
            # Clean up session
            self.db.delete_quote_session(session_data["user_id"])
            
            # Clean up customer selection session
            user_id = session_data["user_id"]
            if user_id in self.customer_selection.customer_sessions:
                del self.customer_selection.customer_sessions[user_id]
        
        except Exception as e:
            logger.error(f"❌ Error completing quote: {e}")
            await update.message.reply_text(
                "❌ Error completing quote. Please contact support.",
                reply_markup=ReplyKeyboardRemove()
            )