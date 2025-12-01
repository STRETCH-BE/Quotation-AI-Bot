"""
User Onboarding Handler for Stretch Ceiling Bot
Complete implementation for user registration and profile management with Dynamics 365 sync
"""
import logging
import re
import asyncio
from typing import Dict, Optional
from datetime import datetime
from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes, ConversationHandler

logger = logging.getLogger(__name__)

class UserOnboardingHandler:
    """Handles user onboarding, profile management, and data collection with Dynamics 365 sync"""
    
    # Conversation states
    ASK_FIRST_NAME = 1
    ASK_LAST_NAME = 2
    ASK_IS_COMPANY = 3
    ASK_COMPANY_NAME = 4
    ASK_VAT_NUMBER = 5
    ASK_ADDRESS = 6
    ASK_EMAIL = 7
    ASK_PHONE = 8
    CONFIRM_DATA = 9
    EDIT_FIELD = 10
    EDIT_VALUE = 11
    
    def __init__(self, db_manager):
        self.db = db_manager
        self.user_sessions = {}
    
    async def start_onboarding(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Start the onboarding process for new users"""
        user = update.effective_user
        user_id = user.id
        
        logger.info(f"🚀 Starting onboarding for user {user_id}")
        
        # Check if user is already onboarded
        user_data = self.db.get_user_profile(user_id)
        if user_data and user_data.get('onboarding_completed'):
            await update.message.reply_text(
                "👋 Welcome back! You've already completed onboarding.\n\n"
                "Use /profile to view or edit your information."
            )
            return ConversationHandler.END
        
        # Initialize onboarding session
        self.user_sessions[user_id] = {
            'user_id': user_id,
            'telegram_username': user.username,
            'telegram_first_name': user.first_name,
            'telegram_last_name': user.last_name,
            'started_at': datetime.now().isoformat(),
            'is_company': False,
            'data': {}
        }
        
        # Store in context for persistence
        context.user_data['onboarding_session'] = self.user_sessions[user_id]
        context.user_data['onboarding_active'] = True
        
        await update.message.reply_text(
            "👋 Welcome to STRETCH Ceiling Bot!\n\n"
            "I need to collect some information to personalize your experience.\n"
            "This will only take a few minutes.\n\n"
            "💡 You can type /cancel at any time to stop.\n\n"
            "Let's start with your first name:",
            reply_markup=ReplyKeyboardRemove()
        )
        
        logger.info(f"Onboarding started for user {user_id}, returning ASK_FIRST_NAME state")
        return self.ASK_FIRST_NAME
    
    async def ask_first_name(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Handle first name input"""
        user_id = update.effective_user.id
        first_name = update.message.text.strip()
        
        logger.info(f"User {user_id} entered first name: {first_name}")
        
        # Check if session exists in memory or context
        if user_id not in self.user_sessions:
            if 'onboarding_session' in context.user_data:
                self.user_sessions[user_id] = context.user_data['onboarding_session']
            else:
                await update.message.reply_text(
                    "Session expired. Please start again with /start"
                )
                return ConversationHandler.END
        
        if len(first_name) < 2:
            await update.message.reply_text("Please enter a valid first name (at least 2 characters):")
            return self.ASK_FIRST_NAME
        
        self.user_sessions[user_id]['data']['first_name'] = first_name
        context.user_data['onboarding_session'] = self.user_sessions[user_id]
        
        await update.message.reply_text(
            f"Nice to meet you, {first_name}! 👍\n\n"
            "Now, please enter your last name:"
        )
        
        return self.ASK_LAST_NAME
    
    async def ask_last_name(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Handle last name input"""
        user_id = update.effective_user.id
        last_name = update.message.text.strip()
        
        logger.info(f"User {user_id} entered last name: {last_name}")
        
        # Check session
        if user_id not in self.user_sessions:
            if 'onboarding_session' in context.user_data:
                self.user_sessions[user_id] = context.user_data['onboarding_session']
            else:
                await update.message.reply_text(
                    "Session expired. Please start again with /start"
                )
                return ConversationHandler.END
        
        if len(last_name) < 2:
            await update.message.reply_text("Please enter a valid last name (at least 2 characters):")
            return self.ASK_LAST_NAME
        
        self.user_sessions[user_id]['data']['last_name'] = last_name
        context.user_data['onboarding_session'] = self.user_sessions[user_id]
        
        keyboard = [
            ["Yes - I represent a company"],
            ["No - I'm a private individual"]
        ]
        reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
        
        await update.message.reply_text(
            "Are you registering as a company? 🏢",
            reply_markup=reply_markup
        )
        
        return self.ASK_IS_COMPANY
    
    async def ask_is_company(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Handle company status selection"""
        user_id = update.effective_user.id
        message_text = update.message.text
        
        logger.info(f"User {user_id} company status: {message_text}")
        
        # Check session
        if user_id not in self.user_sessions:
            if 'onboarding_session' in context.user_data:
                self.user_sessions[user_id] = context.user_data['onboarding_session']
            else:
                await update.message.reply_text(
                    "Session expired. Please start again with /start"
                )
                return ConversationHandler.END
        
        if message_text.startswith("Yes"):
            self.user_sessions[user_id]['is_company'] = True
            await update.message.reply_text(
                "Please enter your company name:",
                reply_markup=ReplyKeyboardRemove()
            )
            return self.ASK_COMPANY_NAME
        else:
            self.user_sessions[user_id]['is_company'] = False
            await update.message.reply_text(
                "Please enter your address:\n"
                "(Street, Number, Postal Code, City)",
                reply_markup=ReplyKeyboardRemove()
            )
            return self.ASK_ADDRESS
    
    async def ask_company_name(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Handle company name input"""
        user_id = update.effective_user.id
        company_name = update.message.text.strip()
        
        # Check session
        if user_id not in self.user_sessions:
            if 'onboarding_session' in context.user_data:
                self.user_sessions[user_id] = context.user_data['onboarding_session']
            else:
                await update.message.reply_text(
                    "Session expired. Please start again with /start"
                )
                return ConversationHandler.END
        
        if len(company_name) < 2:
            await update.message.reply_text("Please enter a valid company name:")
            return self.ASK_COMPANY_NAME
        
        self.user_sessions[user_id]['data']['company_name'] = company_name
        context.user_data['onboarding_session'] = self.user_sessions[user_id]
        
        await update.message.reply_text(
            "Please enter your company's VAT number:\n"
            "(Format: BE0123456789 or similar)"
        )
        
        return self.ASK_VAT_NUMBER
    
    async def ask_vat_number(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Handle VAT number input"""
        user_id = update.effective_user.id
        vat_number = update.message.text.strip().upper()
        
        # Check session
        if user_id not in self.user_sessions:
            if 'onboarding_session' in context.user_data:
                self.user_sessions[user_id] = context.user_data['onboarding_session']
            else:
                await update.message.reply_text(
                    "Session expired. Please start again with /start"
                )
                return ConversationHandler.END
        
        # Basic VAT number validation (can be enhanced based on country)
        if len(vat_number) < 8:
            await update.message.reply_text(
                "Please enter a valid VAT number:\n"
                "Examples: BE0123456789, NL123456789B01"
            )
            return self.ASK_VAT_NUMBER
        
        self.user_sessions[user_id]['data']['vat_number'] = vat_number
        context.user_data['onboarding_session'] = self.user_sessions[user_id]
        
        await update.message.reply_text(
            "Please enter your company address:\n"
            "(Street, Number, Postal Code, City)"
        )
        
        return self.ASK_ADDRESS
    
    async def ask_address(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Handle address input"""
        user_id = update.effective_user.id
        address = update.message.text.strip()
        
        # Check session
        if user_id not in self.user_sessions:
            if 'onboarding_session' in context.user_data:
                self.user_sessions[user_id] = context.user_data['onboarding_session']
            else:
                await update.message.reply_text(
                    "Session expired. Please start again with /start"
                )
                return ConversationHandler.END
        
        if len(address) < 10:
            await update.message.reply_text(
                "Please enter a complete address:\n"
                "(Street, Number, Postal Code, City)"
            )
            return self.ASK_ADDRESS
        
        self.user_sessions[user_id]['data']['address'] = address
        context.user_data['onboarding_session'] = self.user_sessions[user_id]
        
        await update.message.reply_text(
            "Please enter your email address: 📧"
        )
        
        return self.ASK_EMAIL
    
    async def ask_email(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Handle email input"""
        user_id = update.effective_user.id
        email = update.message.text.strip().lower()
        
        # Check session
        if user_id not in self.user_sessions:
            if 'onboarding_session' in context.user_data:
                self.user_sessions[user_id] = context.user_data['onboarding_session']
            else:
                await update.message.reply_text(
                    "Session expired. Please start again with /start"
                )
                return ConversationHandler.END
        
        # Email validation
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        if not re.match(email_pattern, email):
            await update.message.reply_text(
                "Please enter a valid email address:"
            )
            return self.ASK_EMAIL
        
        self.user_sessions[user_id]['data']['email'] = email
        context.user_data['onboarding_session'] = self.user_sessions[user_id]
        
        await update.message.reply_text(
            "Please enter your phone number: 📱\n"
            "(Include country code, e.g., +32 123 456 789)"
        )
        
        return self.ASK_PHONE
    
    async def ask_phone(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Handle phone number input"""
        user_id = update.effective_user.id
        phone = update.message.text.strip()
        
        # Check session
        if user_id not in self.user_sessions:
            if 'onboarding_session' in context.user_data:
                self.user_sessions[user_id] = context.user_data['onboarding_session']
            else:
                await update.message.reply_text(
                    "Session expired. Please start again with /start"
                )
                return ConversationHandler.END
        
        # Basic phone validation (remove spaces and check length)
        phone_digits = re.sub(r'\D', '', phone)
        if len(phone_digits) < 8:
            await update.message.reply_text(
                "Please enter a valid phone number with country code:\n"
                "Examples: +32 123 456 789, +31 6 12345678"
            )
            return self.ASK_PHONE
        
        self.user_sessions[user_id]['data']['phone'] = phone
        context.user_data['onboarding_session'] = self.user_sessions[user_id]
        
        # Show confirmation
        await self.show_confirmation(update, context)
        
        return self.CONFIRM_DATA
    
    async def show_confirmation(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show data confirmation screen"""
        user_id = update.effective_user.id
        
        # Check session
        if user_id not in self.user_sessions:
            if 'onboarding_session' in context.user_data:
                self.user_sessions[user_id] = context.user_data['onboarding_session']
            else:
                await update.message.reply_text(
                    "Session expired. Please start again with /start"
                )
                return
        
        session = self.user_sessions[user_id]
        data = session['data']
        
        confirmation_text = "📋 **Please confirm your information:**\n\n"
        confirmation_text += f"**Personal Information**\n"
        confirmation_text += f"• First Name: {data['first_name']}\n"
        confirmation_text += f"• Last Name: {data['last_name']}\n\n"
        
        if session['is_company']:
            confirmation_text += f"**Company Information**\n"
            confirmation_text += f"• Company Name: {data['company_name']}\n"
            confirmation_text += f"• VAT Number: {data['vat_number']}\n\n"
        
        confirmation_text += f"**Contact Information**\n"
        confirmation_text += f"• Address: {data['address']}\n"
        confirmation_text += f"• Email: {data['email']}\n"
        confirmation_text += f"• Phone: {data['phone']}\n\n"
        confirmation_text += "Is this information correct?"
        
        keyboard = [
            ["✅ Yes, save my information"],
            ["✏️ Edit information"],
            ["❌ Cancel registration"]
        ]
        reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
        
        await update.message.reply_text(
            confirmation_text,
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )
    
    async def handle_confirmation(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Handle confirmation response"""
        user_id = update.effective_user.id
        message_text = update.message.text
        
        logger.info(f"User {user_id} confirmation response: {message_text}")
        
        # Check session
        if user_id not in self.user_sessions:
            if 'onboarding_session' in context.user_data:
                self.user_sessions[user_id] = context.user_data['onboarding_session']
            else:
                await update.message.reply_text(
                    "Session expired. Please start again with /start"
                )
                return ConversationHandler.END
        
        if "Yes, save my information" in message_text:
            # Save user data
            success = await self.save_user_data(user_id)
            
            if success:
                await update.message.reply_text(
                    "✅ **Registration Complete!**\n\n"
                    "Your information has been saved successfully.\n"
                    "You can now use all features of the bot.\n\n"
                    "• Use /profile to view or edit your information anytime.\n"
                    "• Use /create_quote to start creating quotes.\n"
                    "• Use /ask_a_question to chat with our AI assistant.",
                    reply_markup=ReplyKeyboardRemove(),
                    parse_mode="Markdown"
                )
                
                # Log activity
                self.db.log_user_activity(user_id, 'onboarding_completed', {
                    'is_company': self.user_sessions[user_id]['is_company']
                })
                
                # Clean up session
                del self.user_sessions[user_id]
                if 'onboarding_session' in context.user_data:
                    del context.user_data['onboarding_session']
                if 'onboarding_active' in context.user_data:
                    del context.user_data['onboarding_active']
                
                logger.info(f"✅ Onboarding completed successfully for user {user_id}")
                return ConversationHandler.END
            else:
                await update.message.reply_text(
                    "❌ Error saving your information. Please try again or contact support.",
                    reply_markup=ReplyKeyboardRemove()
                )
                return ConversationHandler.END
        
        elif "Edit information" in message_text:
            # Show edit menu
            await self.show_edit_menu(update, context)
            return self.EDIT_FIELD
        
        elif "Cancel registration" in message_text:
            await update.message.reply_text(
                "Registration cancelled. You can start again with /start",
                reply_markup=ReplyKeyboardRemove()
            )
            del self.user_sessions[user_id]
            if 'onboarding_session' in context.user_data:
                del context.user_data['onboarding_session']
            if 'onboarding_active' in context.user_data:
                del context.user_data['onboarding_active']
            return ConversationHandler.END
        
        else:
            # Unknown response
            await update.message.reply_text(
                "Please select one of the options from the keyboard."
            )
            return self.CONFIRM_DATA
    
    async def show_edit_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show field selection for editing"""
        user_id = update.effective_user.id
        
        # Check session
        if user_id not in self.user_sessions:
            if 'onboarding_session' in context.user_data:
                self.user_sessions[user_id] = context.user_data['onboarding_session']
            else:
                await update.message.reply_text(
                    "Session expired. Please start again with /start"
                )
                return
        
        session = self.user_sessions[user_id]
        
        keyboard = [
            ["First Name", "Last Name"],
            ["Address"],
            ["Email", "Phone"]
        ]
        
        if session['is_company']:
            keyboard.insert(1, ["Company Name", "VAT Number"])
        
        keyboard.append(["⬅️ Back to confirmation"])
        
        reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
        
        await update.message.reply_text(
            "Which field would you like to edit?",
            reply_markup=reply_markup
        )
    
    async def handle_edit_field(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Handle field editing selection"""
        user_id = update.effective_user.id
        field = update.message.text
        
        # Check session
        if user_id not in self.user_sessions:
            if 'onboarding_session' in context.user_data:
                self.user_sessions[user_id] = context.user_data['onboarding_session']
            else:
                await update.message.reply_text(
                    "Session expired. Please start again with /start"
                )
                return ConversationHandler.END
        
        if field == "⬅️ Back to confirmation":
            await self.show_confirmation(update, context)
            return self.CONFIRM_DATA
        
        # Store which field we're editing
        context.user_data['editing_field'] = field
        
        field_prompts = {
            "First Name": "Enter new first name:",
            "Last Name": "Enter new last name:",
            "Company Name": "Enter new company name:",
            "VAT Number": "Enter new VAT number:",
            "Address": "Enter new address:",
            "Email": "Enter new email address:",
            "Phone": "Enter new phone number:"
        }
        
        prompt = field_prompts.get(field, "Enter new value:")
        
        await update.message.reply_text(prompt, reply_markup=ReplyKeyboardRemove())
        
        return self.EDIT_VALUE
    
    async def handle_edit_value(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Handle the new value for edited field"""
        user_id = update.effective_user.id
        new_value = update.message.text.strip()
        field = context.user_data.get('editing_field')
        
        # Check if user has an active session
        if user_id not in self.user_sessions:
            if 'onboarding_session' in context.user_data:
                self.user_sessions[user_id] = context.user_data['onboarding_session']
            else:
                await update.message.reply_text(
                    "Session expired. Please use /profile to try again.",
                    reply_markup=ReplyKeyboardRemove()
                )
                return ConversationHandler.END
        
        if not field:
            await self.show_edit_menu(update, context)
            return self.EDIT_FIELD
        
        # Map display field names to data keys
        field_mapping = {
            "First Name": "first_name",
            "Last Name": "last_name",
            "Company Name": "company_name",
            "VAT Number": "vat_number",
            "Address": "address",
            "Email": "email",
            "Phone": "phone"
        }
        
        data_key = field_mapping.get(field)
        if data_key:
            # Validate based on field type
            if data_key == "email":
                email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
                if not re.match(email_pattern, new_value.lower()):
                    await update.message.reply_text("Please enter a valid email address:")
                    return self.EDIT_VALUE
                new_value = new_value.lower()
            
            elif data_key == "phone":
                phone_digits = re.sub(r'\D', '', new_value)
                if len(phone_digits) < 8:
                    await update.message.reply_text("Please enter a valid phone number:")
                    return self.EDIT_VALUE
            
            elif data_key in ["first_name", "last_name", "company_name"]:
                if len(new_value) < 2:
                    await update.message.reply_text(f"Please enter a valid {field} (at least 2 characters):")
                    return self.EDIT_VALUE
            
            elif data_key == "vat_number":
                if len(new_value) < 8:
                    await update.message.reply_text("Please enter a valid VAT number:")
                    return self.EDIT_VALUE
                new_value = new_value.upper()
            
            elif data_key == "address":
                if len(new_value) < 10:
                    await update.message.reply_text("Please enter a complete address:")
                    return self.EDIT_VALUE
            
            # Update the value and show success message
            self.user_sessions[user_id]['data'][data_key] = new_value
            context.user_data['onboarding_session'] = self.user_sessions[user_id]
            await update.message.reply_text(f"✅ {field} updated successfully!")
        
        # Handle next steps
        return await self.handle_edit_completion(update, context, user_id)

    async def handle_edit_completion(self, update, context, user_id):
        """Handle completion of edit operation"""
        # Clear editing field
        if 'editing_field' in context.user_data:
            del context.user_data['editing_field']
        
        # Show appropriate edit menu based on context
        if self.user_sessions[user_id].get('editing_existing'):
            return await self.show_profile_edit_menu(update, context, user_id)
        else:
            await self.show_edit_menu(update, context)
            return self.EDIT_FIELD

    async def show_profile_edit_menu(self, update, context, user_id):
        """Show edit menu for profile editing"""
        keyboard = [
            ["First Name", "Last Name"],
            ["Address"],
            ["Email", "Phone"]
        ]
        
        if self.user_sessions[user_id].get('is_company'):
            keyboard.insert(1, ["Company Name", "VAT Number"])
        
        keyboard.append(["✅ Save Changes", "❌ Cancel"])
        
        reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
        
        await update.message.reply_text(
            "What would you like to edit next?",
            reply_markup=reply_markup
        )
        return self.EDIT_FIELD

    async def save_user_data(self, user_id: int) -> bool:
        """Save user data to database and sync to Dynamics 365"""
        try:
            session = self.user_sessions[user_id]
            data = session['data']
            
            # Determine client group based on company status
            if session['is_company']:
                client_group = 'price_b2b_reseller'  # Default B2B group for companies
            else:
                client_group = 'price_b2c'
            
            # Prepare user data for database
            user_data = {
                'user_id': user_id,
                'telegram_username': session.get('telegram_username'),
                'first_name': data['first_name'],
                'last_name': data['last_name'],
                'full_name': f"{data['first_name']} {data['last_name']}",
                'is_company': session['is_company'],
                'company_name': data.get('company_name'),
                'vat_number': data.get('vat_number'),
                'address': data['address'],
                'email': data['email'],
                'phone': data['phone'],
                'client_group': client_group,
                'onboarding_completed': True,
                'onboarding_date': datetime.now(),
                'source': 'telegram'
            }
            
            # Save to database
            success = self.db.save_user_profile(user_data)
            
            if success:
                logger.info(f"✅ User {user_id} onboarding completed and saved")
                
                # Sync to Dynamics 365 if enabled
                try:
                    # Try to get dynamics integration from context or create new
                    from dynamics365_integration import Dynamics365IntegrationHandler
                    from config import Config
                    
                    if Config.ENABLE_DYNAMICS_SYNC:
                        dynamics_integration = Dynamics365IntegrationHandler(self.db)
                        if dynamics_integration and dynamics_integration.dynamics_service:
                            asyncio.create_task(dynamics_integration.sync_user_to_dynamics(user_id))
                            logger.info(f"🔄 Queued user {user_id} for Dynamics 365 sync")
                except Exception as e:
                    logger.error(f"Could not queue Dynamics sync: {e}")
                    # Don't fail the save if Dynamics sync fails
            
            return success
            
        except Exception as e:
            logger.error(f"Error saving user data: {e}")
            return False
    
    async def show_user_profile(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show user profile with edit options and Dynamics sync status"""
        # Handle both regular messages and callback queries
        if update.callback_query:
            user_id = update.callback_query.from_user.id
            message = update.callback_query.message
        else:
            user_id = update.effective_user.id
            message = update.message
        
        user_data = self.db.get_user_profile(user_id)
        if not user_data:
            await message.reply_text(
                "You haven't completed onboarding yet.\n"
                "Please use /start to begin the registration process."
            )
            return
        
        profile_text = "👤 **Your Profile**\n\n"
        profile_text += f"**Personal Information**\n"
        profile_text += f"• Name: {user_data['first_name']} {user_data['last_name']}\n"
        
        if user_data.get('is_company'):
            profile_text += f"\n**Company Information**\n"
            profile_text += f"• Company: {user_data['company_name']}\n"
            profile_text += f"• VAT: {user_data['vat_number']}\n"
        
        profile_text += f"\n**Contact Information**\n"
        profile_text += f"• Address: {user_data['address']}\n"
        profile_text += f"• Email: {user_data['email']}\n"
        profile_text += f"• Phone: {user_data['phone']}\n"
        
        profile_text += f"\n**Account Details**\n"
        profile_text += f"• User Type: {user_data.get('client_group', 'B2C').replace('price_', '').upper()}\n"
        
        # Add Dynamics 365 sync status
        try:
            from config import Config
            if Config.ENABLE_DYNAMICS_SYNC:
                dynamics_ids = self.db.get_user_dynamics_ids(user_id)
                if dynamics_ids.get('contact_id'):
                    profile_text += f"• Dynamics 365: ✅ Synced\n"
                elif dynamics_ids.get('sync_status') == 'error':
                    profile_text += f"• Dynamics 365: ❌ Sync Error\n"
                else:
                    profile_text += f"• Dynamics 365: ⏳ Pending Sync\n"
        except:
            pass
        
        if user_data.get('onboarding_date'):
            reg_date = user_data['onboarding_date']
            if isinstance(reg_date, str):
                reg_date = datetime.fromisoformat(reg_date)
            profile_text += f"• Registered: {reg_date.strftime('%Y-%m-%d')}\n"
        
        keyboard = [
            [InlineKeyboardButton("✏️ Edit Profile", callback_data="edit_profile")],
            [InlineKeyboardButton("📊 View My Quotes", callback_data="view_quotes")],
            [InlineKeyboardButton("❌ Close", callback_data="close_profile")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await message.reply_text(
            profile_text,
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )
    
    async def handle_profile_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle profile-related callbacks"""
        query = update.callback_query
        await query.answer()
        
        if query.data == "edit_profile":
            # Start profile editing
            user_id = query.from_user.id
            user_data = self.db.get_user_profile(user_id)
            
            if not user_data:
                await query.edit_message_text("Profile not found. Please complete onboarding first.")
                return ConversationHandler.END
            
            # Initialize edit session with current data
            self.user_sessions[user_id] = {
                'user_id': user_id,
                'is_company': user_data.get('is_company', False),
                'data': {
                    'first_name': user_data['first_name'],
                    'last_name': user_data['last_name'],
                    'company_name': user_data.get('company_name'),
                    'vat_number': user_data.get('vat_number'),
                    'address': user_data['address'],
                    'email': user_data['email'],
                    'phone': user_data['phone']
                },
                'editing_existing': True
            }
            
            # Store in context
            context.user_data['onboarding_session'] = self.user_sessions[user_id]
            context.user_data['editing_profile'] = True
            
            await query.edit_message_text("Loading edit menu...")
            
            # Send edit menu as new message
            keyboard = [
                ["First Name", "Last Name"],
                ["Address"],
                ["Email", "Phone"]
            ]
            
            if user_data.get('is_company'):
                keyboard.insert(1, ["Company Name", "VAT Number"])
            
            keyboard.append(["✅ Save Changes", "❌ Cancel"])
            
            reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
            
            await query.message.reply_text(
                "What would you like to edit?",
                reply_markup=reply_markup
            )
            
            # Return the state to enter the conversation handler
            return self.EDIT_FIELD
            
        elif query.data == "view_quotes":
            await query.edit_message_text("Redirecting to quotes...")
            # This would be handled by the main bot to show quotes
            
        elif query.data == "close_profile":
            await query.edit_message_text("Profile closed. Use /profile to view again.")
    
    async def handle_profile_edit(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Handle profile editing for existing users with Dynamics sync"""
        user_id = update.effective_user.id
        message_text = update.message.text
        
        logger.info(f"Profile edit handler: User {user_id}, text: {message_text}")
        
        # Check if user has an active session
        if user_id not in self.user_sessions:
            if 'onboarding_session' in context.user_data:
                self.user_sessions[user_id] = context.user_data['onboarding_session']
            else:
                await update.message.reply_text(
                    "Session expired. Please use /profile to try again.",
                    reply_markup=ReplyKeyboardRemove()
                )
                return ConversationHandler.END
        
        if message_text == "✅ Save Changes":
            # Save updated data
            success = await self.save_user_data(user_id)
            
            if success:
                await update.message.reply_text(
                    "✅ Profile updated successfully!",
                    reply_markup=ReplyKeyboardRemove()
                )
                
                # Log activity
                self.db.log_user_activity(user_id, 'profile_edited', {
                    'fields_edited': list(self.user_sessions[user_id]['data'].keys())
                })
                
                # Sync updated profile to Dynamics 365
                try:
                    from dynamics365_integration import Dynamics365IntegrationHandler
                    from config import Config
                    
                    if Config.ENABLE_DYNAMICS_SYNC:
                        dynamics_integration = Dynamics365IntegrationHandler(self.db)
                        if dynamics_integration and dynamics_integration.dynamics_service:
                            asyncio.create_task(dynamics_integration.sync_user_to_dynamics(user_id))
                            logger.info(f"🔄 Queued updated profile for user {user_id} to Dynamics sync")
                except Exception as e:
                    logger.error(f"Could not queue profile update to Dynamics: {e}")
            else:
                await update.message.reply_text(
                    "❌ Error updating profile. Please try again.",
                    reply_markup=ReplyKeyboardRemove()
                )
            
            # Clean up session
            del self.user_sessions[user_id]
            if 'onboarding_session' in context.user_data:
                del context.user_data['onboarding_session']
            if 'editing_profile' in context.user_data:
                del context.user_data['editing_profile']
            
            return ConversationHandler.END
        
        elif message_text == "❌ Cancel":
            await update.message.reply_text(
                "Edit cancelled.",
                reply_markup=ReplyKeyboardRemove()
            )
            del self.user_sessions[user_id]
            if 'onboarding_session' in context.user_data:
                del context.user_data['onboarding_session']
            if 'editing_profile' in context.user_data:
                del context.user_data['editing_profile']
            return ConversationHandler.END
        
        elif message_text in ["First Name", "Last Name", "Company Name", "VAT Number", "Address", "Email", "Phone"]:
            context.user_data['editing_field'] = message_text
            
            field_prompts = {
                "First Name": "Enter new first name:",
                "Last Name": "Enter new last name:",
                "Company Name": "Enter new company name:",
                "VAT Number": "Enter new VAT number:",
                "Address": "Enter new address:",
                "Email": "Enter new email address:",
                "Phone": "Enter new phone number:"
            }
            
            prompt = field_prompts.get(message_text, "Enter new value:")
            await update.message.reply_text(prompt, reply_markup=ReplyKeyboardRemove())
            
            return self.EDIT_VALUE
        
        # If we get here, show the edit menu again
        await self.show_edit_menu(update, context)
        return self.EDIT_FIELD
    
    async def cancel_onboarding(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Cancel the onboarding process"""
        user_id = update.effective_user.id
        
        if user_id in self.user_sessions:
            del self.user_sessions[user_id]
        
        # Clean up context data
        if 'onboarding_session' in context.user_data:
            del context.user_data['onboarding_session']
        if 'onboarding_active' in context.user_data:
            del context.user_data['onboarding_active']
        if 'editing_profile' in context.user_data:
            del context.user_data['editing_profile']
        if 'editing_field' in context.user_data:
            del context.user_data['editing_field']
        
        await update.message.reply_text(
            "Process cancelled. You can start again with /start",
            reply_markup=ReplyKeyboardRemove()
        )
        
        logger.info(f"Onboarding/editing cancelled for user {user_id}")
        return ConversationHandler.END