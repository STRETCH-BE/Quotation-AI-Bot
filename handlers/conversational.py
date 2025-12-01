"""
Conversational Bot Handler for Stretch Ceiling Bot
FIXED: Handle both Message and CallbackQuery updates
FIXED: Use self.ai instead of self.ai_manager
"""
import logging
from telegram import Update
from telegram.ext import ContextTypes

from config import Config

logger = logging.getLogger(__name__)

class ConversationalBotHandler:
    """Handles conversational AI interactions"""
    
    def __init__(self, db_manager, ai_manager):
        self.db = db_manager
        self.ai = ai_manager
    
    async def handle_ai_question(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /ask_a_question command - FIXED to handle CallbackQuery"""
        # Get user and message based on update type
        if update.callback_query:
            user = update.callback_query.from_user
            message = update.callback_query.message
        else:
            user = update.effective_user
            message = update.message
        
        if not user or not message:
            logger.error("Could not determine user or message in handle_ai_question")
            return
        
        # Set AI chat mode
        context.user_data["ai_chat_mode"] = True
        
        # Log the interaction - FIXED: use self.ai instead of self.ai_manager
        self.ai.log_conversation(user.id, "user", "/ask_a_question")
        
        response = (
            "🤖 **AI Assistant Mode**\n\n"
            f"Hello {user.first_name}! I'm here to help answer your questions about:\n\n"
            "• Stretch ceilings and their benefits\n"
            "• Installation process and requirements\n"
            "• Pricing and quote information\n"
            "• Product types and colors\n"
            "• Maintenance and warranty\n"
            "• Technical specifications\n\n"
            "Type your question below, and I'll provide detailed information.\n"
            "Type 'exit' to leave AI chat mode."
        )
        
        await message.reply_text(response, parse_mode="Markdown")
        
        # Log bot response - FIXED: use self.ai instead of self.ai_manager
        self.ai.log_conversation(user.id, "bot", response)
    
    async def handle_ai_response(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle AI chat messages - FIXED to only process when in AI chat mode"""
        # Only process if explicitly in AI chat mode
        if not context.user_data.get("ai_chat_mode", False):
            return
        
        user_id = update.effective_user.id
        user_message = update.message.text
        
        # Exit AI chat mode with specific commands
        if user_message.lower() in ["/exit", "/stop", "exit", "stop", "/create_quote", "/quotes", "/help", "/status"]:
            context.user_data["ai_chat_mode"] = False
            
            # If it's a command, let it be processed normally
            if user_message.startswith("/"):
                return
            
            await update.message.reply_text("🤖 Exited AI chat mode. Use /help to see available commands.")
            return
        
        # Show typing indicator
        await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")
        
        # Get AI response
        ai_response = await self.ai.get_ai_response(user_id, user_message)
        
        # Send response
        await update.message.reply_text(ai_response)