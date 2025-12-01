"""
Admin Messaging System for Stretch Ceiling Bot
"""
import logging
import asyncio
from datetime import datetime
from typing import List
from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes, ConversationHandler
from telegram.error import TelegramError

from config import Config

logger = logging.getLogger(__name__)

class AdminMessagingSystem:
    """Handles admin messaging functionality for broadcasting and direct messages"""
    
    # Conversation states
    MESSAGE_TYPE = 1
    USER_SELECTION = 2
    MESSAGE_INPUT = 3
    CONFIRMATION = 4
    
    def __init__(self, db_manager):
        self.db = db_manager
        self.admin_message_session = {}  # Store admin messaging sessions
    
    async def admin_message_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Start admin messaging flow"""
        user_id = update.effective_user.id
        
        # Check if admin
        if user_id not in Config.ADMIN_USER_IDS:
            await update.message.reply_text("⛔ This command is for administrators only.")
            return ConversationHandler.END
        
        # Initialize session
        self.admin_message_session[user_id] = {
            "type": None,
            "target_users": [],
            "message": None,
            "started_at": datetime.now(),
        }
        
        # Show options
        keyboard = [
            ["📤 Send to Individual User"],
            ["📢 Broadcast to All Users"],
            ["👥 Send to User Group"],
            ["📊 View Message History"],
            ["❌ Cancel"],
        ]
        reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
        
        await update.message.reply_text(
            "📨 **Admin Messaging System**\n\n" "Select messaging option:",
            reply_markup=reply_markup,
            parse_mode="Markdown",
        )
        
        return self.MESSAGE_TYPE
    
    async def handle_message_type(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle message type selection"""
        user_id = update.effective_user.id
        message_text = update.message.text
        
        if message_text == "❌ Cancel":
            await update.message.reply_text("Messaging cancelled.", reply_markup=ReplyKeyboardRemove())
            return ConversationHandler.END
        
        session = self.admin_message_session.get(user_id)
        if not session:
            return ConversationHandler.END
        
        if message_text == "📤 Send to Individual User":
            session["type"] = "individual"
            return await self.show_user_selection(update, context)
        
        elif message_text == "📢 Broadcast to All Users":
            session["type"] = "broadcast"
            session["target_users"] = await self.get_all_user_ids()
            return await self.ask_for_message(update, context)
        
        elif message_text == "👥 Send to User Group":
            session["type"] = "group"
            return await self.show_group_selection(update, context)
        
        elif message_text == "📊 View Message History":
            await self.show_message_history(update, context)
            return ConversationHandler.END
        
        return self.MESSAGE_TYPE
    
    async def show_user_selection(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show user selection for individual messaging"""
        # Get recent active users
        users = self.db.execute_query(
            """
            SELECT DISTINCT u.user_id, u.username, u.first_name, u.last_name,
            COUNT(q.id) as quote_count, MAX(u.last_activity) as last_seen
            FROM users u
            LEFT JOIN quotations q ON u.user_id = q.user_id
            WHERE u.last_activity > DATE_SUB(NOW(), INTERVAL 30 DAY)
            GROUP BY u.user_id
            ORDER BY u.last_activity DESC
            LIMIT 20
            """,
            fetch=True,
        )
        
        if not users:
            await update.message.reply_text("No active users found.", reply_markup=ReplyKeyboardRemove())
            return ConversationHandler.END
        
        # Create inline keyboard with users
        keyboard = []
        for user in users:
            display_name = f"{user['first_name'] or ''} {user['last_name'] or ''}".strip()
            if not display_name:
                display_name = user["username"] or f"User {user['user_id']}"
            
            last_seen = user["last_seen"].strftime("%Y-%m-%d") if user["last_seen"] else "Never"
            button_text = f"{display_name} (Quotes: {user['quote_count']}) - {last_seen}"
            
            keyboard.append([InlineKeyboardButton(button_text, callback_data=f"msg_user_{user['user_id']}")])
        
        # Add search option
        keyboard.append([InlineKeyboardButton("🔍 Search by User ID", callback_data="msg_search")])
        keyboard.append([InlineKeyboardButton("❌ Cancel", callback_data="msg_cancel")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(
            "👤 **Select User**\n\n" "Choose a user to send message to:",
            reply_markup=reply_markup,
            parse_mode="Markdown",
        )
        
        return self.USER_SELECTION
    
    async def show_group_selection(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show group selection options"""
        keyboard = [
            ["B2C Customers"],
            ["B2B Resellers"],
            ["B2B Hospitality"],
            ["Active Users (30 days)"],
            ["Users with Quotes"],
            ["❌ Cancel"],
        ]
        reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
        
        await update.message.reply_text(
            "👥 **Select User Group**\n\n" "Choose which group to message:",
            reply_markup=reply_markup,
            parse_mode="Markdown",
        )
        
        return self.USER_SELECTION
    
    async def handle_user_selection(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle user/group selection"""
        user_id = update.effective_user.id
        session = self.admin_message_session.get(user_id)
        
        if not session:
            return ConversationHandler.END
        
        if update.callback_query:
            # Handle inline keyboard selection
            query = update.callback_query
            await query.answer()
            
            if query.data == "msg_cancel":
                await query.edit_message_text("Messaging cancelled.")
                return ConversationHandler.END
            
            elif query.data == "msg_search":
                await query.edit_message_text("Please enter the User ID to message:", parse_mode="Markdown")
                return self.USER_SELECTION
            
            elif query.data.startswith("msg_user_"):
                target_user_id = int(query.data.replace("msg_user_", ""))
                session["target_users"] = [target_user_id]
                
                # Get user info for confirmation
                user_info = self.db.execute_query(
                    "SELECT first_name, last_name, username FROM users WHERE user_id = %s",
                    (target_user_id,),
                    fetch=True,
                )
                
                if user_info:
                    user = user_info[0]
                    name = f"{user['first_name'] or ''} {user['last_name'] or ''}".strip()
                    session["target_display"] = name or user["username"] or f"User {target_user_id}"
                
                await query.edit_message_text("User selected. Please type your message:")
                return self.MESSAGE_INPUT
        
        else:
            # Handle text input
            message_text = update.message.text
            
            if message_text == "❌ Cancel":
                await update.message.reply_text("Messaging cancelled.", reply_markup=ReplyKeyboardRemove())
                return ConversationHandler.END
            
            # Handle group selection
            group_users = []
            group_name = message_text
            
            if message_text == "B2C Customers":
                group_users = self.db.execute_query(
                    "SELECT user_id FROM users WHERE client_group = 'price_b2c'", fetch=True
                )
            elif message_text == "B2B Resellers":
                group_users = self.db.execute_query(
                    "SELECT user_id FROM users WHERE client_group = 'price_b2b_reseller'", fetch=True
                )
            elif message_text == "B2B Hospitality":
                group_users = self.db.execute_query(
                    "SELECT user_id FROM users WHERE client_group = 'price_b2b_hospitality'", fetch=True
                )
            elif message_text == "Active Users (30 days)":
                group_users = self.db.execute_query(
                    "SELECT user_id FROM users WHERE last_activity > DATE_SUB(NOW(), INTERVAL 30 DAY)", fetch=True
                )
            elif message_text == "Users with Quotes":
                group_users = self.db.execute_query("SELECT DISTINCT user_id FROM quotations", fetch=True)
            elif message_text.isdigit():
                # Direct user ID input
                target_user_id = int(message_text)
                # Verify user exists
                user_exists = self.db.execute_query(
                    "SELECT user_id FROM users WHERE user_id = %s", (target_user_id,), fetch=True
                )
                if user_exists:
                    session["target_users"] = [target_user_id]
                    session["target_display"] = f"User {target_user_id}"
                    await update.message.reply_text(
                        "User found. Please type your message:", reply_markup=ReplyKeyboardRemove()
                    )
                    return self.MESSAGE_INPUT
                else:
                    await update.message.reply_text("User not found. Please try again:")
                    return self.USER_SELECTION
            
            if group_users:
                session["target_users"] = [user["user_id"] for user in group_users]
                session["target_display"] = f"{group_name} ({len(session['target_users'])} users)"
                await update.message.reply_text(
                    f"Selected: {session['target_display']}\n\n" "Please type your message:",
                    reply_markup=ReplyKeyboardRemove(),
                )
                return self.MESSAGE_INPUT
        
        return self.USER_SELECTION
    
    async def ask_for_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Ask for message content"""
        user_id = update.effective_user.id
        session = self.admin_message_session.get(user_id)
        
        if session["type"] == "broadcast":
            user_count = len(session["target_users"])
            await update.message.reply_text(
                f"📢 **Broadcast Message**\n\n"
                f"This will be sent to {user_count} users.\n\n"
                "Please type your message (you can use Markdown formatting):",
                reply_markup=ReplyKeyboardRemove(),
                parse_mode="Markdown",
            )
        
        return self.MESSAGE_INPUT
    
    async def handle_message_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle message input"""
        user_id = update.effective_user.id
        session = self.admin_message_session.get(user_id)
        
        if not session:
            return ConversationHandler.END
        
        # Store message
        session["message"] = update.message.text
        
        # Show confirmation
        keyboard = [["✅ Send Message"], ["✏️ Edit Message"], ["❌ Cancel"]]
        reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
        
        preview = f"**Message Preview:**\n\n{session['message']}\n\n"
        
        if session["type"] == "individual":
            preview += f"**To:** {session.get('target_display', 'Selected user')}"
        elif session["type"] == "broadcast":
            preview += f"**To:** All users ({len(session['target_users'])} recipients)"
        else:
            preview += f"**To:** {session.get('target_display', 'Selected group')}"
        
        await update.message.reply_text(preview, reply_markup=reply_markup, parse_mode="Markdown")
        
        return self.CONFIRMATION
    
    async def handle_confirmation(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle sending confirmation"""
        user_id = update.effective_user.id
        message_text = update.message.text
        session = self.admin_message_session.get(user_id)
        
        if not session:
            return ConversationHandler.END
        
        if message_text == "❌ Cancel":
            await update.message.reply_text("Message cancelled.", reply_markup=ReplyKeyboardRemove())
            return ConversationHandler.END
        
        elif message_text == "✏️ Edit Message":
            await update.message.reply_text("Please type your message again:", reply_markup=ReplyKeyboardRemove())
            return self.MESSAGE_INPUT
        
        elif message_text == "✅ Send Message":
            # Send messages
            await update.message.reply_text("📤 Sending messages...", reply_markup=ReplyKeyboardRemove())
            
            # Add admin signature
            final_message = session["message"] + "\n\n_- STRETCH Admin Team_"
            
            # Send messages with progress updates
            sent_count = 0
            failed_count = 0
            failed_users = []
            
            total_users = len(session["target_users"])
            progress_message = await update.message.reply_text(f"Progress: 0/{total_users}")
            
            for i, target_user_id in enumerate(session["target_users"]):
                try:
                    await context.bot.send_message(chat_id=target_user_id, text=final_message, parse_mode="Markdown")
                    sent_count += 1
                    
                    # Log the message
                    self.log_admin_message(
                        admin_id=user_id,
                        target_user_id=target_user_id,
                        message=session["message"],
                        message_type=session["type"],
                    )
                
                except TelegramError as e:
                    logger.error(f"Failed to send message to {target_user_id}: {e}")
                    failed_count += 1
                    failed_users.append(target_user_id)
                
                # Update progress every 10 users
                if (i + 1) % 10 == 0 or (i + 1) == total_users:
                    try:
                        await progress_message.edit_text(f"Progress: {i + 1}/{total_users}")
                    except:
                        pass
                
                # Small delay to avoid rate limits
                if total_users > 10:
                    await asyncio.sleep(0.1)
            
            # Final report
            report = f"✅ **Message Delivery Report**\n\n"
            report += f"Successfully sent: {sent_count}\n"
            if failed_count > 0:
                report += f"Failed: {failed_count}\n"
                if len(failed_users) <= 5:
                    report += f"Failed users: {', '.join(map(str, failed_users))}"
            
            await update.message.reply_text(report, parse_mode="Markdown")
            
            # Clean up session
            del self.admin_message_session[user_id]
        
        return ConversationHandler.END
    
    async def show_message_history(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show admin message history"""
        # Get recent admin messages
        history = self.db.execute_query(
            """
            SELECT am.*, u.first_name, u.last_name, u.username,
            admin.first_name as admin_first_name, admin.last_name as admin_last_name
            FROM admin_messages am
            LEFT JOIN users u ON am.target_user_id = u.user_id
            LEFT JOIN users admin ON am.admin_id = admin.user_id
            WHERE am.sent_at > DATE_SUB(NOW(), INTERVAL 7 DAY)
            ORDER BY am.sent_at DESC
            LIMIT 20
            """,
            fetch=True,
        )
        
        if not history:
            await update.message.reply_text("No messages sent in the last 7 days.")
            return
        
        report = "📊 **Message History (Last 7 Days)**\n\n"
        
        for msg in history:
            admin_name = f"{msg['admin_first_name'] or ''} {msg['admin_last_name'] or ''}".strip()
            target_name = f"{msg['first_name'] or ''} {msg['last_name'] or ''}".strip()
            if not target_name:
                target_name = msg["username"] or f"User {msg['target_user_id']}"
            
            sent_time = msg["sent_at"].strftime("%Y-%m-%d %H:%M")
            msg_preview = msg["message"][:50] + "..." if len(msg["message"]) > 50 else msg["message"]
            
            report += f"**{sent_time}**\n"
            report += f"From: {admin_name}\n"
            report += f"To: {target_name} ({msg['message_type']})\n"
            report += f"Message: {msg_preview}\n\n"
        
        await update.message.reply_text(report, parse_mode="Markdown")
    
    def log_admin_message(self, admin_id: int, target_user_id: int, message: str, message_type: str):
        """Log admin message to database"""
        try:
            self.db.execute_query(
                """
                INSERT INTO admin_messages (admin_id, target_user_id, message, message_type, sent_at)
                VALUES (%s, %s, %s, %s, NOW())
                """,
                (admin_id, target_user_id, message, message_type),
            )
        except Exception as e:
            logger.error(f"Failed to log admin message: {e}")
    
    async def get_all_user_ids(self) -> List[int]:
        """Get all user IDs for broadcasting"""
        users = self.db.execute_query("SELECT user_id FROM users", fetch=True)
        return [user["user_id"] for user in users] if users else []
    
    async def cancel_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Cancel the conversation"""
        user_id = update.effective_user.id
        if user_id in self.admin_message_session:
            del self.admin_message_session[user_id]
        
        await update.message.reply_text("Admin messaging cancelled.", reply_markup=ReplyKeyboardRemove())
        return ConversationHandler.END