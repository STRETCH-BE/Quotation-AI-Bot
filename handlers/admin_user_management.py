# -*- coding: utf-8 -*-
"""
Admin User Management System for Stretch Ceiling Bot
Provides comprehensive user management capabilities for administrators
"""
import logging
import json
import io
from typing import Dict, List, Optional
from datetime import datetime, timedelta
from decimal import Decimal
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import ContextTypes

from config import Config

logger = logging.getLogger(__name__)


class AdminUserManagement:
    """Handles admin user management functionality"""
    
    def __init__(self, db_manager):
        self.db = db_manager
        self.admin_sessions = {}  # Store admin navigation state
    
    async def handle_user_management(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Main entry point for user management"""
        user_id = update.effective_user.id
        
        # Check if admin
        if user_id not in Config.ADMIN_USER_IDS:
            await update.message.reply_text("This command is for administrators only.")
            return
        
        # Initialize admin session
        self.admin_sessions[user_id] = {
            'current_page': 1,
            'filters': {},
            'selected_user': None
        }
        
        keyboard = [
            ["List All Users", "Search Users"],
            ["User Statistics", "User Groups"],
            ["Export User Data", "Manage Tags"],
            ["Back to Admin Menu"]
        ]
        reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
        
        await update.message.reply_text(
            "USER MANAGEMENT\n\nSelect an option:",
            reply_markup=reply_markup
        )
    
    async def handle_user_list(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Display paginated user list"""
        user_id = update.effective_user.id
        session = self.admin_sessions.get(user_id, {})
        page = session.get('current_page', 1)
        filters = session.get('filters', {})
        
        # Get users from database
        result = self.db.get_users_for_admin(page=page, per_page=10, filters=filters)
        users = result.get('users', [])
        
        if not users:
            message = "No users found."
            if update.callback_query:
                await update.callback_query.edit_message_text(message)
            else:
                await update.message.reply_text(message)
            return
        
        # Build user list message
        message = "USER LIST\n\n"
        
        for user in users:
            user_name = f"{user.get('first_name', '')} {user.get('last_name', '')}".strip()
            if user.get('is_company'):
                user_name += f" ({user.get('company_name', 'Company')})"
            
            message += f"- {user_name}\n"
            message += f"  ID: {user['user_id']}\n"
            
            client_group = user.get('client_group', 'price_b2c')
            message += f"  Type: {client_group.replace('price_', '').upper()}\n"
            message += f"  Quotes: {user.get('quote_count', 0)}\n"
            
            if user.get('total_revenue'):
                revenue = float(user['total_revenue']) if isinstance(user['total_revenue'], Decimal) else user['total_revenue']
                message += f"  Revenue: EUR {revenue:.2f}\n"
            
            last_activity = user.get('last_activity')
            if last_activity:
                if isinstance(last_activity, datetime):
                    message += f"  Last active: {last_activity.strftime('%Y-%m-%d')}\n"
                else:
                    message += f"  Last active: {str(last_activity)[:10]}\n"
            else:
                message += f"  Last active: Never\n"
            message += "\n"
        
        total_pages = result.get('total_pages', 1)
        total_users = result.get('total', len(users))
        message += f"Page {page} of {total_pages} ({total_users} users)"
        
        # Add active filters info
        if filters:
            message += "\n\nActive filters: "
            filter_info = []
            if filters.get('search'):
                filter_info.append(f"search='{filters['search']}'")
            if filters.get('client_group'):
                filter_info.append(f"type={filters['client_group']}")
            if filters.get('is_company') is not None:
                filter_info.append("companies only" if filters['is_company'] else "individuals only")
            message += ", ".join(filter_info)
        
        # Navigation buttons
        keyboard = []
        nav_row = []
        
        if page > 1:
            nav_row.append(InlineKeyboardButton("Previous", callback_data="admin_users_prev"))
        if page < total_pages:
            nav_row.append(InlineKeyboardButton("Next", callback_data="admin_users_next"))
        
        if nav_row:
            keyboard.append(nav_row)
        
        keyboard.append([
            InlineKeyboardButton("Refresh", callback_data="admin_users_refresh"),
            InlineKeyboardButton("Clear Filters", callback_data="admin_users_clear_filters")
        ])
        
        # User selection buttons (show first 5 users)
        for user in users[:5]:
            first_name = user.get('first_name', 'Unknown')
            last_name = user.get('last_name', '')[:10]
            keyboard.append([
                InlineKeyboardButton(
                    f"{first_name} {last_name}",
                    callback_data=f"admin_user_{user['user_id']}"
                )
            ])
        
        keyboard.append([InlineKeyboardButton("Close", callback_data="admin_users_close")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        if update.callback_query:
            await update.callback_query.edit_message_text(
                message,
                reply_markup=reply_markup
            )
        else:
            await update.message.reply_text(
                message,
                reply_markup=reply_markup
            )
    
    async def handle_user_detail(self, update: Update, context: ContextTypes.DEFAULT_TYPE, selected_user_id: int):
        """Show detailed user information"""
        # Get user data
        user = self.db.get_user_profile(selected_user_id)
        if not user:
            await update.callback_query.answer("User not found", show_alert=True)
            return
        
        # Get user statistics
        stats = self.db.get_user_statistics(selected_user_id)
        
        # Get conversation memory
        memory = self.db.get_user_conversation_memory(selected_user_id)
        
        # Build detailed message
        message = "USER DETAILS\n\n"
        
        # Personal Information
        message += "Personal Information\n"
        message += f"  Name: {user.get('first_name', '')} {user.get('last_name', '')}\n"
        message += f"  ID: {user['user_id']}\n"
        message += f"  Username: @{user.get('telegram_username', 'Not set')}\n"
        
        # Company Information (if applicable)
        if user.get('is_company'):
            message += f"\nCompany Information\n"
            message += f"  Company: {user.get('company_name', 'N/A')}\n"
            message += f"  VAT: {user.get('vat_number', 'N/A')}\n"
        
        # Contact Information
        message += f"\nContact Information\n"
        message += f"  Email: {user.get('email', 'Not set')}\n"
        message += f"  Phone: {user.get('phone', 'Not set')}\n"
        message += f"  Address: {user.get('address', 'Not set')}\n"
        
        # Account Information
        message += f"\nAccount Information\n"
        client_group = user.get('client_group', 'price_b2c')
        message += f"  Type: {client_group.replace('price_', '').upper()}\n"
        message += f"  Onboarded: {'Yes' if user.get('onboarding_completed') else 'No'}\n"
        
        created_at = user.get('created_at')
        if created_at:
            if isinstance(created_at, datetime):
                message += f"  Registered: {created_at.strftime('%Y-%m-%d')}\n"
            else:
                message += f"  Registered: {str(created_at)[:10]}\n"
        
        last_activity = user.get('last_activity')
        if last_activity:
            if isinstance(last_activity, datetime):
                message += f"  Last Active: {last_activity.strftime('%Y-%m-%d %H:%M')}\n"
            else:
                message += f"  Last Active: {str(last_activity)[:16]}\n"
        
        # Statistics
        message += f"\nStatistics\n"
        message += f"  Total Quotes: {stats.get('total_quotes', 0)}\n"
        message += f"  Accepted Quotes: {stats.get('accepted_quotes', 0)}\n"
        total_value = stats.get('total_value', 0) or 0
        if isinstance(total_value, Decimal):
            total_value = float(total_value)
        message += f"  Total Value: EUR {total_value:.2f}\n"
        message += f"  Messages: {stats.get('total_messages', 0)}\n"
        
        # Conversation Memory
        if memory and memory.get('interaction_count', 0) > 0:
            message += f"\nAI Memory\n"
            message += f"  Interactions: {memory['interaction_count']}\n"
            if memory.get('last_topics'):
                topics = memory['last_topics']
                if isinstance(topics, list):
                    message += f"  Recent Topics: {', '.join(topics[:3])}\n"
        
        # Notes
        if user.get('notes'):
            notes_preview = str(user['notes'])[:200]
            message += f"\nNotes\n  {notes_preview}{'...' if len(str(user['notes'])) > 200 else ''}\n"
        
        # Tags
        if user.get('tags'):
            tags = user['tags']
            if isinstance(tags, str):
                try:
                    tags = json.loads(tags)
                except:
                    tags = []
            if tags:
                message += f"\nTags: {', '.join(tags)}\n"
        
        # Create action buttons
        keyboard = [
            [
                InlineKeyboardButton("Edit", callback_data=f"admin_edit_user_{selected_user_id}"),
                InlineKeyboardButton("Change Type", callback_data=f"admin_change_type_{selected_user_id}")
            ],
            [
                InlineKeyboardButton("Add Note", callback_data=f"admin_add_note_{selected_user_id}"),
                InlineKeyboardButton("Tags", callback_data=f"admin_manage_tags_{selected_user_id}")
            ],
            [
                InlineKeyboardButton("Quotes", callback_data=f"admin_view_quotes_{selected_user_id}"),
                InlineKeyboardButton("Activity", callback_data=f"admin_view_activity_{selected_user_id}")
            ],
            [
                InlineKeyboardButton("Message", callback_data=f"admin_send_msg_{selected_user_id}"),
                InlineKeyboardButton("Export", callback_data=f"admin_export_user_{selected_user_id}")
            ],
            [InlineKeyboardButton("Back to List", callback_data="admin_users_back")]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.callback_query.edit_message_text(
            message,
            reply_markup=reply_markup
        )
    
    async def handle_change_user_type(self, update: Update, context: ContextTypes.DEFAULT_TYPE, target_user_id: int):
        """Handle changing user client group"""
        keyboard = [
            [InlineKeyboardButton("B2C - Consumer", callback_data=f"admin_set_type_{target_user_id}_price_b2c")],
            [InlineKeyboardButton("B2B - Reseller", callback_data=f"admin_set_type_{target_user_id}_price_b2b_reseller")],
            [InlineKeyboardButton("B2B - Hospitality", callback_data=f"admin_set_type_{target_user_id}_price_b2b_hospitality")],
            [InlineKeyboardButton("Cancel", callback_data=f"admin_user_{target_user_id}")]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.callback_query.edit_message_text(
            "Select new user type:",
            reply_markup=reply_markup
        )
    
    async def handle_user_search(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle user search interface"""
        keyboard = [
            ["Search by Name", "Search by Email"],
            ["Search by Company", "Search by Phone"],
            ["Show Companies Only", "Show B2B Users"],
            ["Show Active Users", "Show With Quotes"],
            ["Back"]
        ]
        reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
        
        await update.message.reply_text(
            "SEARCH USERS\n\nSelect search criteria or type your search term:",
            reply_markup=reply_markup
        )
        
        # Set search mode
        context.user_data['admin_search_mode'] = True
    
    async def handle_search_criteria(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle search criteria selection"""
        user_id = update.effective_user.id
        message_text = update.message.text
        
        if message_text == "Back":
            context.user_data.pop('admin_search_mode', None)
            await self.handle_user_management(update, context)
            return
        
        # Initialize session if needed
        if user_id not in self.admin_sessions:
            self.admin_sessions[user_id] = {'filters': {}, 'current_page': 1}
        
        filters = self.admin_sessions[user_id]['filters']
        
        # Handle quick filters
        if message_text == "Show Companies Only":
            filters['is_company'] = True
            self.admin_sessions[user_id]['current_page'] = 1
            await update.message.reply_text("Showing companies only.", reply_markup=ReplyKeyboardRemove())
            await self.handle_user_list(update, context)
        
        elif message_text == "Show B2B Users":
            filters['client_group'] = 'price_b2b%'
            self.admin_sessions[user_id]['current_page'] = 1
            await update.message.reply_text("Showing B2B users only.", reply_markup=ReplyKeyboardRemove())
            await self.handle_user_list(update, context)
        
        elif message_text == "Show Active Users":
            filters['active_days'] = 30
            self.admin_sessions[user_id]['current_page'] = 1
            await update.message.reply_text("Showing users active in last 30 days.", reply_markup=ReplyKeyboardRemove())
            await self.handle_user_list(update, context)
        
        elif message_text == "Show With Quotes":
            filters['has_quotes'] = True
            self.admin_sessions[user_id]['current_page'] = 1
            await update.message.reply_text("Showing users with quotes.", reply_markup=ReplyKeyboardRemove())
            await self.handle_user_list(update, context)
        
        else:
            # Text search options
            search_field_map = {
                "Search by Name": "name",
                "Search by Email": "email",
                "Search by Company": "company",
                "Search by Phone": "phone"
            }
            
            if message_text in search_field_map:
                context.user_data['admin_search_field'] = search_field_map[message_text]
                field_name = message_text.replace("Search by ", "").lower()
                await update.message.reply_text(
                    f"Enter {field_name} to search:",
                    reply_markup=ReplyKeyboardRemove()
                )
    
    async def handle_search_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle search term input"""
        user_id = update.effective_user.id
        search_term = update.message.text.strip()
        
        # Initialize session if needed
        if user_id not in self.admin_sessions:
            self.admin_sessions[user_id] = {'filters': {}, 'current_page': 1}
        
        # Set search filter
        self.admin_sessions[user_id]['filters']['search'] = search_term
        self.admin_sessions[user_id]['current_page'] = 1
        
        # Clear search mode
        context.user_data.pop('admin_search_mode', None)
        context.user_data.pop('admin_search_field', None)
        
        await self.handle_user_list(update, context)
    
    async def handle_user_statistics(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show overall user statistics"""
        stats = self.db.get_system_statistics()
        
        message = "USER STATISTICS\n\n"
        
        message += "User Totals\n"
        message += f"  Total Users: {stats.get('total_users', 0)}\n"
        message += f"  Active (7 days): {stats.get('active_users_week', 0)}\n"
        message += f"  Active (30 days): {stats.get('active_users_month', 0)}\n"
        
        message += "\nQuote Statistics\n"
        message += f"  Total Quotes: {stats.get('total_quotes', 0)}\n"
        message += f"  Quotes (7 days): {stats.get('quotes_week', 0)}\n"
        message += f"  Accepted Quotes: {stats.get('accepted_quotes', 0)}\n"
        
        total_revenue = stats.get('total_revenue', 0) or 0
        if isinstance(total_revenue, Decimal):
            total_revenue = float(total_revenue)
        message += f"  Total Revenue: EUR {total_revenue:.2f}\n"
        
        message += "\nSystem\n"
        message += f"  Messages (24h): {stats.get('messages_24h', 0)}\n"
        message += f"  Active Sessions: {stats.get('active_sessions', 0)}\n"
        message += f"  Products: {stats.get('total_products', 0)}\n"
        
        await update.message.reply_text(message, reply_markup=ReplyKeyboardRemove())
    
    async def handle_callback_query(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle admin user management callbacks"""
        query = update.callback_query
        await query.answer()
        
        data = query.data
        admin_id = query.from_user.id
        
        # Check admin status
        if admin_id not in Config.ADMIN_USER_IDS:
            await query.answer("Unauthorized", show_alert=True)
            return
        
        # Initialize session if needed
        if admin_id not in self.admin_sessions:
            self.admin_sessions[admin_id] = {'current_page': 1, 'filters': {}}
        
        # User list navigation
        if data == "admin_users_prev":
            self.admin_sessions[admin_id]['current_page'] = max(1, self.admin_sessions[admin_id].get('current_page', 1) - 1)
            await self.handle_user_list(update, context)
        
        elif data == "admin_users_next":
            self.admin_sessions[admin_id]['current_page'] = self.admin_sessions[admin_id].get('current_page', 1) + 1
            await self.handle_user_list(update, context)
        
        elif data == "admin_users_refresh":
            await self.handle_user_list(update, context)
        
        elif data == "admin_users_back":
            await self.handle_user_list(update, context)
        
        elif data == "admin_users_close":
            await query.edit_message_text("User management closed.")
        
        elif data == "admin_users_clear_filters":
            self.admin_sessions[admin_id]['filters'] = {}
            self.admin_sessions[admin_id]['current_page'] = 1
            await self.handle_user_list(update, context)
        
        # User selection
        elif data.startswith("admin_user_"):
            user_id = int(data.replace("admin_user_", ""))
            await self.handle_user_detail(update, context, user_id)
        
        # Change user type
        elif data.startswith("admin_change_type_"):
            user_id = int(data.replace("admin_change_type_", ""))
            await self.handle_change_user_type(update, context, user_id)
        
        elif data.startswith("admin_set_type_"):
            parts = data.split("_")
            user_id = int(parts[3])
            new_type = "_".join(parts[4:])
            
            success = self.db.update_user_client_group(user_id, new_type, admin_id)
            
            if success:
                await query.answer("User type updated!", show_alert=True)
                await self.handle_user_detail(update, context, user_id)
            else:
                await query.answer("Failed to update user type", show_alert=True)
        
        # Edit user
        elif data.startswith("admin_edit_user_"):
            user_id = int(data.replace("admin_edit_user_", ""))
            await query.edit_message_text(
                f"To edit user profile, use:\n/admin_edit_user {user_id}\n\n"
                "This will start an interactive editing session."
            )
        
        # Add note
        elif data.startswith("admin_add_note_"):
            user_id = int(data.replace("admin_add_note_", ""))
            context.user_data['admin_adding_note_for'] = user_id
            await query.edit_message_text(
                "Please type the note you want to add for this user:\n\n"
                "Type /cancel to cancel."
            )
        
        # Manage tags
        elif data.startswith("admin_manage_tags_"):
            user_id = int(data.replace("admin_manage_tags_", ""))
            await self.show_tag_management(update, context, user_id)
        
        # Add tag
        elif data.startswith("admin_add_tag_"):
            parts = data.split("_")
            user_id = int(parts[3])
            tag = "_".join(parts[4:])
            
            success = self.db.add_user_tag(user_id, tag)
            if success:
                await query.answer(f"Tag '{tag}' added!", show_alert=True)
            await self.show_tag_management(update, context, user_id)
        
        # Remove tag
        elif data.startswith("admin_remove_tag_"):
            parts = data.split("_")
            user_id = int(parts[3])
            tag = "_".join(parts[4:])
            
            success = self.db.remove_user_tag(user_id, tag)
            if success:
                await query.answer(f"Tag '{tag}' removed!", show_alert=True)
            await self.show_tag_management(update, context, user_id)
        
        # View quotes
        elif data.startswith("admin_view_quotes_"):
            user_id = int(data.replace("admin_view_quotes_", ""))
            await self.show_user_quotes(update, context, user_id)
        
        # View activity
        elif data.startswith("admin_view_activity_"):
            user_id = int(data.replace("admin_view_activity_", ""))
            await self.show_user_activity(update, context, user_id)
        
        # Send message
        elif data.startswith("admin_send_msg_"):
            user_id = int(data.replace("admin_send_msg_", ""))
            await query.edit_message_text(
                f"To send a message to this user, use:\n/send_message\n\n"
                f"Then select 'Individual User' and choose user ID: {user_id}"
            )
        
        # Export user data
        elif data.startswith("admin_export_user_"):
            user_id = int(data.replace("admin_export_user_", ""))
            await self.export_user_data(update, context, user_id)
    
    async def show_tag_management(self, update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int):
        """Show tag management interface"""
        user = self.db.get_user_profile(user_id)
        if not user:
            await update.callback_query.answer("User not found", show_alert=True)
            return
        
        current_tags = []
        if user.get('tags'):
            tags = user['tags']
            if isinstance(tags, str):
                try:
                    current_tags = json.loads(tags)
                except:
                    current_tags = []
            elif isinstance(tags, list):
                current_tags = tags
        
        message = f"TAG MANAGEMENT\n\n"
        message += f"User: {user.get('first_name', '')} {user.get('last_name', '')}\n"
        message += f"ID: {user_id}\n\n"
        
        if current_tags:
            message += "Current Tags:\n"
            for tag in current_tags:
                message += f"  - {tag}\n"
        else:
            message += "No tags assigned.\n"
        
        # Create buttons
        keyboard = []
        
        # Available tags to add
        available_tags = ["VIP", "Priority", "Installer", "Architect", "Dealer", "Wholesale", "Pending", "New"]
        tags_to_show = [t for t in available_tags if t not in current_tags]
        
        if tags_to_show:
            message += "\nAvailable Tags to Add:"
            for tag in tags_to_show[:4]:
                keyboard.append([InlineKeyboardButton(f"+ Add {tag}", callback_data=f"admin_add_tag_{user_id}_{tag}")])
        
        # Current tags to remove
        if current_tags:
            message += "\n\nClick to remove:"
            for tag in current_tags[:4]:
                keyboard.append([InlineKeyboardButton(f"- Remove {tag}", callback_data=f"admin_remove_tag_{user_id}_{tag}")])
        
        keyboard.append([InlineKeyboardButton("Back", callback_data=f"admin_user_{user_id}")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.callback_query.edit_message_text(
            message,
            reply_markup=reply_markup
        )
    
    async def show_user_quotes(self, update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int):
        """Show user's quotes"""
        quotes = self.db.get_user_quotes(user_id)
        
        if not quotes:
            keyboard = [[InlineKeyboardButton("Back", callback_data=f"admin_user_{user_id}")]]
            await update.callback_query.edit_message_text(
                "No quotes found for this user.",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return
        
        message = f"QUOTES FOR USER {user_id}\n\n"
        
        for quote in quotes[:10]:
            status = quote.get('status', 'draft')
            status_indicator = {
                'draft': '[Draft]',
                'sent': '[Sent]',
                'accepted': '[Accepted]',
                'rejected': '[Rejected]',
                'expired': '[Expired]'
            }.get(status, '[?]')
            
            message += f"{status_indicator} Quote #{quote.get('quote_number', 'N/A')}\n"
            
            total_price = quote.get('total_price', 0)
            if isinstance(total_price, Decimal):
                total_price = float(total_price)
            message += f"  Amount: EUR {total_price:.2f}\n"
            message += f"  Status: {status.capitalize()}\n"
            
            created_at = quote.get('created_at')
            if created_at:
                if isinstance(created_at, datetime):
                    message += f"  Date: {created_at.strftime('%Y-%m-%d')}\n"
                else:
                    message += f"  Date: {str(created_at)[:10]}\n"
            message += "\n"
        
        if len(quotes) > 10:
            message += f"... and {len(quotes) - 10} more quotes"
        
        keyboard = [[InlineKeyboardButton("Back", callback_data=f"admin_user_{user_id}")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.callback_query.edit_message_text(message, reply_markup=reply_markup)
    
    async def show_user_activity(self, update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int):
        """Show user's recent activity"""
        activities = self.db.get_user_activity_log(user_id, limit=20)
        
        if not activities:
            keyboard = [[InlineKeyboardButton("Back", callback_data=f"admin_user_{user_id}")]]
            await update.callback_query.edit_message_text(
                "No activity found for this user.",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return
        
        message = f"ACTIVITY FOR USER {user_id}\n\n"
        
        for activity in activities:
            activity_type = activity.get('activity_type', 'unknown')
            created_at = activity.get('created_at')
            
            if created_at:
                if isinstance(created_at, datetime):
                    time_str = created_at.strftime('%Y-%m-%d %H:%M')
                else:
                    time_str = str(created_at)[:16]
            else:
                time_str = 'N/A'
            
            message += f"[{time_str}] {activity_type}\n"
        
        keyboard = [[InlineKeyboardButton("Back", callback_data=f"admin_user_{user_id}")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.callback_query.edit_message_text(message, reply_markup=reply_markup)
    
    async def export_user_data(self, update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int):
        """Export user data to JSON file"""
        try:
            # Get all user data
            user_profile = self.db.get_user_profile(user_id)
            if not user_profile:
                await update.callback_query.answer("User not found", show_alert=True)
                return
            
            user_quotes = self.db.get_user_quotes(user_id)
            user_memory = self.db.get_user_conversation_memory(user_id)
            user_conversations = self.db.get_conversation_history(user_id, limit=50)
            user_activities = self.db.get_user_activity_log(user_id)
            user_stats = self.db.get_user_statistics(user_id)
            
            # Convert datetime and Decimal objects to serializable types
            def convert_for_json(obj):
                if isinstance(obj, datetime):
                    return obj.isoformat()
                elif isinstance(obj, Decimal):
                    return float(obj)
                elif isinstance(obj, dict):
                    return {k: convert_for_json(v) for k, v in obj.items()}
                elif isinstance(obj, list):
                    return [convert_for_json(item) for item in obj]
                return obj
            
            # Compile data
            export_data = {
                'export_info': {
                    'exported_by': update.effective_user.id,
                    'export_date': datetime.now().isoformat(),
                    'bot_name': 'STRETCH Ceiling Bot'
                },
                'profile': convert_for_json(user_profile),
                'statistics': convert_for_json(user_stats),
                'quotes': convert_for_json(user_quotes),
                'conversation_memory': convert_for_json(user_memory),
                'recent_conversations': convert_for_json(user_conversations),
                'activities': convert_for_json(user_activities)
            }
            
            # Create JSON file
            json_str = json.dumps(export_data, indent=2, ensure_ascii=False)
            json_bytes = json_str.encode('utf-8')
            json_file = io.BytesIO(json_bytes)
            json_file.name = f"user_{user_id}_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            
            # Send file
            await context.bot.send_document(
                chat_id=update.effective_chat.id,
                document=json_file,
                filename=json_file.name,
                caption=f"User data export\n\nUser ID: {user_id}\nName: {user_profile.get('first_name', '')} {user_profile.get('last_name', '')}"
            )
            
            # Log the export
            self.db.log_user_activity(
                user_id,
                'data_exported',
                {'exported_by': update.effective_user.id}
            )
            
        except Exception as e:
            logger.error(f"Error exporting user data: {e}")
            await update.callback_query.answer("Error exporting data", show_alert=True)
    
    async def handle_add_note(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle adding a note to user profile"""
        admin_id = update.effective_user.id
        note_text = update.message.text
        
        if 'admin_adding_note_for' not in context.user_data:
            return
        
        target_user_id = context.user_data['admin_adding_note_for']
        
        # Add note to database
        success = self.db.add_user_note(target_user_id, note_text, admin_id)
        
        if success:
            await update.message.reply_text("Note added successfully!")
            
            # Log activity
            self.db.log_user_activity(
                target_user_id,
                'admin_note_added',
                {'note': note_text[:100], 'admin_id': admin_id}
            )
        else:
            await update.message.reply_text("Failed to add note.")
        
        # Clear the state
        del context.user_data['admin_adding_note_for']