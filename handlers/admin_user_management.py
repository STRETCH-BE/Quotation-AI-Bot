# -*- coding: utf-8 -*-
"""
Admin User Management System for Stretch Ceiling Bot
Version 2.0 - Fixed encoding, emoji display, and Markdown escaping
Provides comprehensive user management capabilities for administrators
"""
import logging
import json
import io
from typing import Dict, List, Optional
from datetime import datetime, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import ContextTypes

from config import Config

logger = logging.getLogger(__name__)

def escape_markdown_v2(text: str) -> str:
    """Escape special characters for Telegram MarkdownV2"""
    if not text:
        return ""
    special_chars = ['_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!']
    for char in special_chars:
        text = str(text).replace(char, f'\\{char}')
    return text

class AdminUserManagement:
    """Handles admin user management functionality"""
    
    def __init__(self, db_manager):
        self.db = db_manager
        self.admin_sessions = {}
    
    async def handle_user_management(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Main entry point for user management"""
        user_id = update.effective_user.id
        
        if user_id not in Config.ADMIN_USER_IDS:
            await update.message.reply_text("Alleen voor administrators.")
            return
        
        self.admin_sessions[user_id] = {
            'current_page': 1,
            'filters': {},
            'selected_user': None
        }
        
        keyboard = [
            ["📋 List All Users", "🔍 Search Users"],
            ["📊 User Statistics", "👥 User Groups"],
            ["📤 Export User Data", "🏷️ Manage Tags"],
            ["⬅️ Back to Admin Menu"]
        ]
        reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
        
        await update.message.reply_text(
            "Gebruikersbeheer\n\nSelecteer een optie:",
            reply_markup=reply_markup
        )
    
    async def handle_user_list(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Display paginated user list"""
        user_id = update.effective_user.id
        session = self.admin_sessions.get(user_id, {})
        page = session.get('current_page', 1)
        filters = session.get('filters', {})
        
        result = self.db.get_users_for_admin(page=page, per_page=10, filters=filters)
        users = result['users']
        
        if not users:
            message = "Geen gebruikers gevonden."
            if update.callback_query:
                await update.callback_query.edit_message_text(message)
            else:
                await update.message.reply_text(message)
            return
        
        # Build user list message - NO MARKDOWN to avoid escaping issues
        message = "GEBRUIKERSLIJST\n\n"
        
        for user in users:
            user_name = f"{user['first_name']} {user.get('last_name', '')}".strip()
            if user.get('is_company'):
                user_name += f" ({user.get('company_name', 'Bedrijf')})"
            
            message += f"- {user_name}\n"
            message += f"  ID: {user['user_id']}\n"
            message += f"  Type: {user['client_group'].replace('price_', '').upper()}\n"
            message += f"  Offertes: {user.get('quote_count', 0)}\n"
            if user.get('total_revenue'):
                message += f"  Omzet: EUR {user['total_revenue']:.2f}\n"
            last_activity = user.get('last_activity')
            if last_activity:
                if isinstance(last_activity, str):
                    message += f"  Laatst actief: {last_activity[:10]}\n\n"
                else:
                    message += f"  Laatst actief: {last_activity.strftime('%Y-%m-%d')}\n\n"
            else:
                message += f"  Laatst actief: Nooit\n\n"
        
        message += f"\nPagina {result['page']} van {result['total_pages']} ({result['total']} gebruikers)"
        
        if filters:
            message += "\n\nActieve filters: "
            filter_info = []
            if filters.get('search'):
                filter_info.append(f"zoeken='{filters['search']}'")
            if filters.get('client_group'):
                filter_info.append(f"type={filters['client_group']}")
            if filters.get('is_company') is not None:
                filter_info.append("alleen bedrijven" if filters['is_company'] else "alleen particulieren")
            message += ", ".join(filter_info)
        
        # Navigation buttons
        keyboard = []
        nav_row = []
        
        if result['page'] > 1:
            nav_row.append(InlineKeyboardButton("Vorige", callback_data=f"admin_users_page_{result['page']-1}"))
        if result['page'] < result['total_pages']:
            nav_row.append(InlineKeyboardButton("Volgende", callback_data=f"admin_users_page_{result['page']+1}"))
        
        if nav_row:
            keyboard.append(nav_row)
        
        # User selection buttons
        for user in users[:5]:
            keyboard.append([
                InlineKeyboardButton(
                    f"{user['first_name']} {user.get('last_name', '')[:10]}",
                    callback_data=f"admin_user_{user['user_id']}"
                )
            ])
        
        keyboard.append([InlineKeyboardButton("Filters Wissen", callback_data="admin_clear_filters")])
        keyboard.append([InlineKeyboardButton("⬅️ Back", callback_data="admin_users_back")])
        
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
    
    async def handle_callback_query(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle user management callbacks - main entry point"""
        return await self.handle_user_callback(update, context)
    
    async def handle_user_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle user management callbacks"""
        query = update.callback_query
        await query.answer()
        
        user_id = query.from_user.id
        data = query.data
        
        if user_id not in Config.ADMIN_USER_IDS:
            await query.edit_message_text("Geen toegang.")
            return
        
        if data.startswith("admin_users_page_"):
            page = int(data.replace("admin_users_page_", ""))
            if user_id in self.admin_sessions:
                self.admin_sessions[user_id]['current_page'] = page
            await self.handle_user_list(update, context)
        
        elif data.startswith("admin_user_"):
            target_user_id = int(data.replace("admin_user_", ""))
            await self.show_user_profile(update, context, target_user_id)
        
        elif data.startswith("admin_change_type_"):
            target_user_id = int(data.replace("admin_change_type_", ""))
            await self.handle_change_user_type(update, context, target_user_id)
        
        elif data.startswith("admin_set_type_"):
            parts = data.split("_")
            target_user_id = int(parts[3])
            new_type = "_".join(parts[4:])
            await self.set_user_type(update, context, target_user_id, new_type)
        
        elif data.startswith("admin_view_quotes_"):
            target_user_id = int(data.replace("admin_view_quotes_", ""))
            await self.show_user_quotes(update, context, target_user_id)
        
        elif data.startswith("admin_view_activity_"):
            target_user_id = int(data.replace("admin_view_activity_", ""))
            await self.show_user_activity(update, context, target_user_id)
        
        elif data == "admin_clear_filters":
            if user_id in self.admin_sessions:
                self.admin_sessions[user_id]['filters'] = {}
                self.admin_sessions[user_id]['current_page'] = 1
            await self.handle_user_list(update, context)
        
        elif data == "admin_users_back":
            await query.message.delete()
    
    async def show_user_profile(self, update: Update, context: ContextTypes.DEFAULT_TYPE, selected_user_id: int):
        """Show detailed user profile"""
        user = self.db.get_user_profile(selected_user_id)
        
        if not user:
            await update.callback_query.edit_message_text("Gebruiker niet gevonden.")
            return
        
        stats = self.db.get_user_stats(selected_user_id)
        memory = self.db.get_conversation_memory(selected_user_id)
        
        # Build profile message - NO MARKDOWN
        message = "GEBRUIKERSPROFIEL\n\n"
        
        # Basic info
        message += "Basisinformatie\n"
        message += f"- Naam: {user.get('first_name', '')} {user.get('last_name', '')}\n"
        message += f"- Telegram ID: {user['user_id']}\n"
        message += f"- Gebruikerstype: {user.get('client_group', 'price_b2c').replace('price_', '').upper()}\n"
        
        if user.get('is_company'):
            message += f"\nBedrijfsinfo\n"
            message += f"- Bedrijf: {user.get('company_name', 'N/B')}\n"
            message += f"- BTW: {user.get('vat_number', 'N/B')}\n"
        
        # Contact info
        message += f"\nContactgegevens\n"
        message += f"- Email: {user.get('email', 'N/B')}\n"
        message += f"- Telefoon: {user.get('phone', 'N/B')}\n"
        message += f"- Adres: {user.get('address', 'N/B')}\n"
        
        # Statistics
        message += f"\nStatistieken\n"
        message += f"- Totaal Offertes: {stats.get('total_quotes', 0)}\n"
        message += f"- Geaccepteerde Offertes: {stats.get('accepted_quotes', 0)}\n"
        message += f"- Totale Waarde: EUR {float(stats.get('total_value', 0) or 0):.2f}\n"
        message += f"- Berichten: {stats.get('total_messages', 0)}\n"
        
        # AI Memory
        if memory and memory.get('interaction_count', 0) > 0:
            message += f"\nAI Geheugen\n"
            message += f"- Interacties: {memory['interaction_count']}\n"
            if memory.get('last_topics'):
                topics = memory['last_topics'][:3] if isinstance(memory['last_topics'], list) else []
                message += f"- Recente Onderwerpen: {', '.join(topics)}\n"
        
        # Notes
        if user.get('notes'):
            notes_preview = str(user['notes'])[:200]
            message += f"\nNotities\n{notes_preview}{'...' if len(str(user['notes'])) > 200 else ''}\n"
        
        # Tags
        if user.get('tags'):
            tags = json.loads(user['tags']) if isinstance(user['tags'], str) else user['tags']
            if tags:
                message += f"\nTags: {', '.join(tags)}\n"
        
        # Action buttons
        keyboard = [
            [
                InlineKeyboardButton("Profiel Bewerken", callback_data=f"admin_edit_user_{selected_user_id}"),
                InlineKeyboardButton("Type Wijzigen", callback_data=f"admin_change_type_{selected_user_id}")
            ],
            [
                InlineKeyboardButton("Notitie Toevoegen", callback_data=f"admin_add_note_{selected_user_id}"),
                InlineKeyboardButton("Tags Beheren", callback_data=f"admin_manage_tags_{selected_user_id}")
            ],
            [
                InlineKeyboardButton("Offertes Bekijken", callback_data=f"admin_view_quotes_{selected_user_id}"),
                InlineKeyboardButton("Activiteit Bekijken", callback_data=f"admin_view_activity_{selected_user_id}")
            ],
            [
                InlineKeyboardButton("Bericht Sturen", callback_data=f"admin_send_msg_{selected_user_id}"),
                InlineKeyboardButton("Data Exporteren", callback_data=f"admin_export_user_{selected_user_id}")
            ],
            [InlineKeyboardButton("Terug naar Lijst", callback_data="admin_users_back")]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.callback_query.edit_message_text(
            message,
            reply_markup=reply_markup
        )
    
    async def handle_change_user_type(self, update: Update, context: ContextTypes.DEFAULT_TYPE, target_user_id: int):
        """Handle changing user client group"""
        keyboard = [
            [InlineKeyboardButton("B2C - Particulier", callback_data=f"admin_set_type_{target_user_id}_price_b2c")],
            [InlineKeyboardButton("B2B - Reseller", callback_data=f"admin_set_type_{target_user_id}_price_b2b_reseller")],
            [InlineKeyboardButton("B2B - Hospitality", callback_data=f"admin_set_type_{target_user_id}_price_b2b_hospitality")],
            [InlineKeyboardButton("Annuleren", callback_data=f"admin_user_{target_user_id}")]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.callback_query.edit_message_text(
            "Selecteer nieuw gebruikerstype:",
            reply_markup=reply_markup
        )
    
    async def set_user_type(self, update: Update, context: ContextTypes.DEFAULT_TYPE, target_user_id: int, new_type: str):
        """Set user client group"""
        success = self.db.update_user_client_group(target_user_id, new_type)
        
        if success:
            await update.callback_query.answer("Gebruikerstype bijgewerkt!")
            await self.show_user_profile(update, context, target_user_id)
        else:
            await update.callback_query.answer("Fout bij bijwerken", show_alert=True)
    
    async def handle_user_search(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle user search interface"""
        keyboard = [
            ["Search by Name", "Search by Email"],
            ["Search by Company", "Search by Phone"],
            ["Show Companies Only", "Show B2B Users"],
            ["Show Active Users", "Show With Quotes"],
            ["⬅️ Back"]
        ]
        reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
        
        await update.message.reply_text(
            "ZOEK GEBRUIKERS\n\nSelecteer zoekcriteria of typ je zoekterm:",
            reply_markup=reply_markup
        )
        
        context.user_data['admin_search_mode'] = True
    
    async def handle_search_criteria(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle search criteria selection"""
        user_id = update.effective_user.id
        message_text = update.message.text
        
        if message_text == "⬅️ Back":
            await self.handle_user_management(update, context)
            return
        
        if user_id not in self.admin_sessions:
            self.admin_sessions[user_id] = {'filters': {}}
        
        filters = self.admin_sessions[user_id]['filters']
        
        if message_text == "Show Companies Only":
            filters['is_company'] = True
            await update.message.reply_text("Alleen bedrijven worden getoond.", reply_markup=ReplyKeyboardRemove())
            await self.handle_user_list(update, context)
        
        elif message_text == "Show B2B Users":
            filters['client_group'] = 'price_b2b%'
            await update.message.reply_text("Alleen B2B gebruikers worden getoond.", reply_markup=ReplyKeyboardRemove())
            await self.handle_user_list(update, context)
        
        elif message_text == "Show Active Users":
            filters['active_days'] = 30
            await update.message.reply_text("Gebruikers actief in laatste 30 dagen.", reply_markup=ReplyKeyboardRemove())
            await self.handle_user_list(update, context)
        
        elif message_text == "Show With Quotes":
            filters['has_quotes'] = True
            await update.message.reply_text("Gebruikers met offertes worden getoond.", reply_markup=ReplyKeyboardRemove())
            await self.handle_user_list(update, context)
        
        else:
            search_field_map = {
                "Search by Name": "name",
                "Search by Email": "email",
                "Search by Company": "company",
                "Search by Phone": "phone"
            }
            
            if message_text in search_field_map:
                context.user_data['admin_search_field'] = search_field_map[message_text]
                field_name = message_text.replace("Zoek op ", "").lower()
                await update.message.reply_text(
                    f"Voer {field_name} in om te zoeken:",
                    reply_markup=ReplyKeyboardRemove()
                )
    
    async def handle_search_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle search term input"""
        user_id = update.effective_user.id
        search_term = update.message.text.strip()
        
        if user_id not in self.admin_sessions:
            self.admin_sessions[user_id] = {'filters': {}}
        
        self.admin_sessions[user_id]['filters']['search'] = search_term
        self.admin_sessions[user_id]['current_page'] = 1
        
        context.user_data.pop('admin_search_mode', None)
        context.user_data.pop('admin_search_field', None)
        
        await self.handle_user_list(update, context)
    
    async def show_user_quotes(self, update: Update, context: ContextTypes.DEFAULT_TYPE, target_user_id: int):
        """Show user's quotes"""
        quotes = self.db.get_user_quotes(target_user_id, limit=10)
        
        if not quotes:
            await update.callback_query.edit_message_text(
                "Geen offertes gevonden voor deze gebruiker.",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("⬅️ Back", callback_data=f"admin_user_{target_user_id}")
                ]])
            )
            return
        
        message = f"OFFERTES VOOR GEBRUIKER {target_user_id}\n\n"
        
        for quote in quotes:
            status_emoji = {
                'draft': '[Concept]',
                'sent': '[Verzonden]',
                'accepted': '[Geaccepteerd]',
                'rejected': '[Afgewezen]',
                'expired': '[Verlopen]'
            }.get(quote.get('status', 'draft'), '[?]')
            
            message += f"{status_emoji} Offerte #{quote.get('quote_number', 'N/B')}\n"
            message += f"  Totaal: EUR {quote.get('total_price', 0):.2f}\n"
            
            created_at = quote.get('created_at')
            if created_at:
                if isinstance(created_at, str):
                    message += f"  Datum: {created_at[:10]}\n"
                else:
                    message += f"  Datum: {created_at.strftime('%Y-%m-%d')}\n"
            message += "\n"
        
        keyboard = [[InlineKeyboardButton("⬅️ Back", callback_data=f"admin_user_{target_user_id}")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.callback_query.edit_message_text(message, reply_markup=reply_markup)
    
    async def show_user_activity(self, update: Update, context: ContextTypes.DEFAULT_TYPE, target_user_id: int):
        """Show user's recent activity"""
        activities = self.db.get_user_activity_log(target_user_id, limit=20)
        
        if not activities:
            await update.callback_query.edit_message_text(
                "Geen activiteiten gevonden voor deze gebruiker.",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("⬅️ Back", callback_data=f"admin_user_{target_user_id}")
                ]])
            )
            return
        
        message = f"ACTIVITEITEN VOOR GEBRUIKER {target_user_id}\n\n"
        
        for activity in activities:
            action = activity.get('action', 'unknown')
            timestamp = activity.get('timestamp')
            
            if timestamp:
                if isinstance(timestamp, str):
                    time_str = timestamp[:16]
                else:
                    time_str = timestamp.strftime('%Y-%m-%d %H:%M')
            else:
                time_str = 'N/B'
            
            message += f"[{time_str}] {action}\n"
        
        keyboard = [[InlineKeyboardButton("⬅️ Back", callback_data=f"admin_user_{target_user_id}")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.callback_query.edit_message_text(message, reply_markup=reply_markup)
    
    async def handle_user_statistics(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show overall user statistics"""
        stats = self.db.get_admin_statistics()
        
        message = "GEBRUIKERSSTATISTIEKEN\n\n"
        
        message += "Totalen\n"
        message += f"- Totaal Gebruikers: {stats.get('total_users', 0)}\n"
        message += f"- Actief (30 dagen): {stats.get('active_users_30d', 0)}\n"
        message += f"- Nieuwe (7 dagen): {stats.get('new_users_7d', 0)}\n"
        
        message += "\nPer Type\n"
        message += f"- B2C: {stats.get('b2c_users', 0)}\n"
        message += f"- B2B Reseller: {stats.get('b2b_reseller_users', 0)}\n"
        message += f"- B2B Hospitality: {stats.get('b2b_hospitality_users', 0)}\n"
        message += f"- Bedrijven: {stats.get('company_users', 0)}\n"
        
        message += "\nOffertes\n"
        message += f"- Totaal Offertes: {stats.get('total_quotes', 0)}\n"
        message += f"- Totale Waarde: EUR {float(stats.get('total_quote_value', 0) or 0):.2f}\n"
        message += f"- Geaccepteerd: {stats.get('accepted_quotes', 0)}\n"
        
        await update.message.reply_text(message, reply_markup=ReplyKeyboardRemove())
    
    async def handle_export_users(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Export user data to CSV"""
        user_id = update.effective_user.id
        
        if user_id not in Config.ADMIN_USER_IDS:
            await update.message.reply_text("Alleen voor administrators.")
            return
        
        await update.message.reply_text("Export wordt voorbereid...")
        
        try:
            users = self.db.get_all_users_for_export()
            
            csv_content = "user_id,first_name,last_name,email,phone,is_company,company_name,vat_number,client_group,quote_count,total_value,created_at,last_activity\n"
            
            for user in users:
                row = [
                    str(user.get('user_id', '')),
                    str(user.get('first_name', '')).replace(',', ';'),
                    str(user.get('last_name', '')).replace(',', ';'),
                    str(user.get('email', '')),
                    str(user.get('phone', '')),
                    '1' if user.get('is_company') else '0',
                    str(user.get('company_name', '')).replace(',', ';'),
                    str(user.get('vat_number', '')),
                    str(user.get('client_group', '')),
                    str(user.get('quote_count', 0)),
                    str(user.get('total_value', 0)),
                    str(user.get('created_at', ''))[:10] if user.get('created_at') else '',
                    str(user.get('last_activity', ''))[:10] if user.get('last_activity') else ''
                ]
                csv_content += ','.join(row) + '\n'
            
            csv_bytes = csv_content.encode('utf-8-sig')
            csv_file = io.BytesIO(csv_bytes)
            csv_file.name = f"users_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            
            await update.message.reply_document(
                document=csv_file,
                filename=csv_file.name,
                caption=f"Gebruikersexport - {len(users)} gebruikers"
            )
            
        except Exception as e:
            logger.error(f"Error exporting users: {e}")
            await update.message.reply_text(f"Fout bij exporteren: {str(e)}")