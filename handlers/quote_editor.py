"""
Quote Editor Handler for Stretch Ceiling Bot
Complete implementation for viewing and editing quotes
ENHANCED: Full add ceiling wizard matching quote_flow.py + Dynamics 365 integration
Version 3.0 - Complete add ceiling wizard with type, type_ceiling, color, lights, wood, seams, perimeter profile
"""
import logging
import json
import asyncio
import os
from datetime import datetime
from decimal import Decimal
from typing import Dict, Optional, List
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes, ConversationHandler

from config import Config
from utils import format_price, escape_markdown
from models import CeilingConfig

logger = logging.getLogger(__name__)


def decimal_default(obj):
    """JSON encoder helper for Decimal and other non-serializable types"""
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f'Object of type {obj.__class__.__name__} is not JSON serializable')


class QuoteEditor:
    """Handles quote viewing and editing with conversation handler support and Dynamics 365 sync"""
    
    # Conversation states - Basic edit
    EDIT_MENU = 1
    EDIT_DIMENSIONS = 2
    EDIT_PERIMETER = 3
    EDIT_COLOR = 4
    EDIT_CORNERS = 5
    
    # Add ceiling states - matching quote_flow.py flow
    ADD_CEILING_NAME = 6
    ADD_CEILING_SIZE = 7
    ADD_CEILING_SIZE_CONFIRM = 8
    ADD_CEILING_PERIMETER_EDIT = 9
    ADD_CEILING_CORNERS = 10
    ADD_CEILING_TYPE = 11          # fabric/pvc from database
    ADD_CEILING_TYPE_CEILING = 12  # Standard/Acoustic/Light/print from database
    ADD_CEILING_COLOR = 13         # from database
    ADD_CEILING_ACOUSTIC_PERF = 14
    ADD_CEILING_PERIMETER_PROFILE = 15
    ADD_CEILING_SEAM_QUESTION = 16
    ADD_CEILING_SEAM_LENGTH = 17
    ADD_CEILING_LIGHTS_QUESTION = 18
    ADD_CEILING_LIGHT_SELECTION = 19
    ADD_CEILING_LIGHT_QUANTITY = 20
    ADD_CEILING_MORE_LIGHTS = 21
    ADD_CEILING_WOOD_QUESTION = 22
    ADD_CEILING_WOOD_SELECTION = 23
    ADD_CEILING_WOOD_QUANTITY = 24
    ADD_CEILING_MORE_WOOD = 25
    
    # Remove ceiling state
    REMOVE_CEILING = 26
    
    def __init__(self, db, calculator):
        self.db = db
        self.calculator = calculator
        self.edit_sessions = {}  # Store active edit sessions
        self.pdf_generator = None  # Will be set by bot if PDF generation is enabled
        self.email_sender = None  # Will be set by bot if email is enabled
    
    async def show_user_quotes(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show all quotes for the user"""
        user_id = update.effective_user.id
        
        # Determine if this is a callback query or regular message
        if update.callback_query:
            message_func = update.callback_query.message.reply_text
            await update.callback_query.answer()
        else:
            message_func = update.message.reply_text
        
        # Get user's quotes from database
        quotes = self.db.get_user_quotes(user_id)
        
        if not quotes:
            await message_func(
                "📋 Je hebt nog geen offertes.\n\n"
                "Gebruik /create_quote om je eerste offerte te maken!"
            )
            return
        
        # Create message with quotes list - LIMIT LENGTH
        message = "📋 **Jouw Offertes**\n\n"
        
        # Show only first 10 quotes in the message
        quotes_to_show = quotes[:10]
        
        for quote in quotes_to_show:
            status_emoji = {
                'draft': '📝',
                'sent': '📤',
                'accepted': '✅',
                'rejected': '❌',
                'expired': '⏰'
            }.get(quote.get('status', 'draft'), '📄')
            
            message += f"{status_emoji} **Offerte #{quote.get('quote_number', 'N/B')}**\n"
            
            created_at = quote.get('created_at')
            if created_at:
                if isinstance(created_at, str):
                    message += f"Aangemaakt: {created_at[:10]}\n"
                else:
                    message += f"Aangemaakt: {created_at.strftime('%d/%m/%Y')}\n"
            
            quote_data = quote.get('quote_data', {})
            if isinstance(quote_data, str):
                try:
                    quote_data = json.loads(quote_data)
                except:
                    quote_data = {}
            
            ceilings = quote_data.get('ceilings', [])
            message += f"Plafonds: {len(ceilings)}\n"
            message += f"Totaal: {format_price(quote.get('total_price', 0))}\n"
            message += f"Status: {quote.get('status', 'draft').capitalize()}\n\n"
        
        if len(quotes) > 10:
            message += f"_Toont 10 van {len(quotes)} offertes._\n\n"
        
        if len(message) > 3500:
            message = "📋 **Jouw Offertes**\n\n"
            message += f"Je hebt {len(quotes)} offertes. Gebruik de knoppen hieronder om ze te bekijken:\n\n"
        
        keyboard = []
        for quote in quotes[:10]:
            quote_id = quote.get('quotation_id')
            if quote_id:
                keyboard.append([
                    InlineKeyboardButton(
                        f"📄 {quote.get('quote_number', f'Offerte {quote_id}')}",
                        callback_data=f"quote_view_{quote_id}"
                    )
                ])
        
        keyboard.append([InlineKeyboardButton("🔙 Terug", callback_data="quote_list_back")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await message_func(
            message,
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )
    
    async def handle_quote_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle callback queries for quotes"""
        query = update.callback_query
        await query.answer()
        
        user_id = query.from_user.id
        data = query.data
        
        if data.startswith("quote_view_"):
            quote_id = int(data.replace("quote_view_", ""))
            await self.show_quote_details(query, quote_id)
        
        elif data.startswith("quote_status_"):
            parts = data.split("_")
            quote_id = int(parts[2])
            await self.show_status_menu(query, quote_id)
        
        elif data.startswith("status_change_"):
            parts = data.split("_")
            quote_id = int(parts[2])
            new_status = parts[3]
            await self.change_quote_status(query, quote_id, new_status)
        
        elif data.startswith("quote_pdf_"):
            quote_id = int(data.replace("quote_pdf_", ""))
            await self.generate_pdf(query, quote_id)
        
        elif data.startswith("quote_email_"):
            quote_id = int(data.replace("quote_email_", ""))
            await self.send_quote_email(query, quote_id)
        
        elif data.startswith("quote_delete_"):
            quote_id = int(data.replace("quote_delete_", ""))
            await self.confirm_delete_quote(query, quote_id)
        
        elif data.startswith("confirm_delete_"):
            quote_id = int(data.replace("confirm_delete_", ""))
            await self.delete_quote(query, quote_id)
        
        elif data == "quote_list_back":
            await query.message.delete()
    
    async def show_quote_details(self, query, quote_id: int):
        """Show detailed quote information"""
        user_id = query.from_user.id
        
        quote = self.db.get_quote_by_id(quote_id)
        
        if not quote:
            await query.edit_message_text("❌ Offerte niet gevonden.")
            return
        
        if quote['user_id'] != user_id:
            await query.edit_message_text("❌ Je kunt alleen je eigen offertes bekijken.")
            return
        
        quote_data = quote.get('quote_data', {})
        if isinstance(quote_data, str):
            try:
                quote_data = json.loads(quote_data)
            except:
                quote_data = {}
        
        # Build message
        status_emoji = {
            'draft': '📝',
            'sent': '📤',
            'accepted': '✅',
            'rejected': '❌',
            'expired': '⏰'
        }.get(quote.get('status', 'draft'), '📄')
        
        message = f"{status_emoji} **Offerte #{quote.get('quote_number', 'N/B')}**\n\n"
        
        # Get user profile or customer info
        user_profile = self.db.get_user_profile(user_id)
        customer = quote_data.get('customer')
        
        # Show customer info (PRIORITY: customer > user_profile)
        if customer:
            if customer.get('company_name') or customer.get('dynamics_account_id'):
                message += f"**Bedrijf:** {customer.get('company_name', customer.get('display_name', 'N/B'))}\n"
                if customer.get('vat_number') or customer.get('vat'):
                    message += f"**BTW:** {customer.get('vat_number', customer.get('vat', 'N/B'))}\n"
                if customer.get('contact_name'):
                    message += f"**Contact:** {customer.get('contact_name')}\n"
            else:
                message += f"**Klant:** {customer.get('contact_name', customer.get('display_name', 'N/B'))}\n"
            if customer.get('email'):
                message += f"**Email:** {customer.get('email')}\n"
        elif user_profile:
            if user_profile.get('is_company'):
                message += f"**Bedrijf:** {user_profile.get('company_name', 'N/B')}\n"
                message += f"**BTW:** {user_profile.get('vat_number', 'N/B')}\n"
            else:
                message += f"**Klant:** {user_profile.get('first_name', '')} {user_profile.get('last_name', '')}\n"
            message += f"**Email:** {user_profile.get('email', 'N/B')}\n\n"
        
        # Quote details
        message += f"\n**Status:** {quote.get('status', 'draft').capitalize()}\n"
        message += f"**Totaal:** {format_price(quote.get('total_price', 0))}\n"
        
        created_at = quote.get('created_at')
        if created_at:
            if isinstance(created_at, str):
                message += f"**Aangemaakt:** {created_at[:10]}\n"
            else:
                message += f"**Aangemaakt:** {created_at.strftime('%d/%m/%Y %H:%M')}\n"
        
        # Ceiling summary
        ceilings = quote_data.get('ceilings', [])
        if ceilings:
            message += f"\n**Plafonds:** {len(ceilings)}\n"
            for i, ceiling in enumerate(ceilings):
                ceiling_name = ceiling.get('name', f'Plafond {i+1}')
                area = ceiling.get('area', 0)
                message += f"  • {ceiling_name}: {area:.2f}m²\n"
        
        # Create action buttons
        keyboard = [
            [
                InlineKeyboardButton("📄 PDF Genereren", callback_data=f"quote_pdf_{quote_id}"),
                InlineKeyboardButton("📧 E-mail Verzenden", callback_data=f"quote_email_{quote_id}")
            ],
            [
                InlineKeyboardButton("✏️ Bewerken", callback_data=f"quote_edit_{quote_id}"),
                InlineKeyboardButton("🔄 Status", callback_data=f"quote_status_{quote_id}")
            ],
            [
                InlineKeyboardButton("🗑️ Verwijderen", callback_data=f"quote_delete_{quote_id}")
            ],
            [InlineKeyboardButton("🔙 Terug naar Lijst", callback_data="quote_list_back")]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(
            message,
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )
    
    async def show_status_menu(self, query, quote_id: int):
        """Show status change menu"""
        statuses = [
            ('draft', '📝 Concept'),
            ('sent', '📤 Verzonden'),
            ('accepted', '✅ Geaccepteerd'),
            ('rejected', '❌ Afgewezen'),
            ('expired', '⏰ Verlopen')
        ]
        
        keyboard = []
        for status, label in statuses:
            keyboard.append([
                InlineKeyboardButton(label, callback_data=f"status_change_{quote_id}_{status}")
            ])
        keyboard.append([InlineKeyboardButton("🔙 Terug", callback_data=f"quote_view_{quote_id}")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(
            "🔄 **Status Wijzigen**\n\nSelecteer de nieuwe status:",
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )
    
    async def change_quote_status(self, query, quote_id: int, new_status: str):
        """Change quote status"""
        user_id = query.from_user.id
        
        success = self.db.update_quote_status(quote_id, new_status, user_id)
        
        if success:
            await query.answer(f"✅ Status gewijzigd naar {new_status}", show_alert=True)
            
            # Sync to Dynamics 365 if available
            try:
                dynamics_integration = query.message.bot.bot_data.get('dynamics_integration')
                if dynamics_integration and dynamics_integration.dynamics_service:
                    asyncio.create_task(dynamics_integration.sync_quote_to_dynamics(quote_id))
                    logger.info(f"🔄 Offerte {quote_id} status wijziging naar Dynamics sync gequeued")
            except Exception as e:
                logger.error(f"Could not queue quote status to Dynamics: {e}")
            
            await self.show_quote_details(query, quote_id)
        else:
            await query.answer("Fout bij wijzigen status", show_alert=True)
    
    async def generate_pdf(self, query, quote_id: int):
        """Generate PDF for a quote - WITH CUSTOMER DATA PRIORITY FIX"""
        if not Config.ENABLE_PDF_GENERATION or not self.pdf_generator:
            await query.answer("PDF genereren is niet beschikbaar", show_alert=True)
            return
        
        user_id = query.from_user.id
        
        quote = self.db.get_quote_by_id(quote_id)
        if not quote or quote['user_id'] != user_id:
            await query.answer("Toegang geweigerd", show_alert=True)
            return
        
        await query.answer("PDF wordt gegenereerd...")
        
        try:
            # Get user profile for PDF
            user_profile = self.db.get_user_profile(user_id)
            
            # Parse quote_data if it's a JSON string
            quote_data = quote.get('quote_data', {})
            if isinstance(quote_data, str):
                try:
                    quote_data = json.loads(quote_data)
                except:
                    logger.error("Failed to parse quote_data JSON")
                    quote_data = {}
            
            # Prepare quote data for PDF generator - FIXED: Include customer data
            pdf_quote_data = {
                'quote_number': quote.get('quote_number'),
                'ceilings': quote_data.get('ceilings', []),
                'ceiling_costs': quote_data.get('ceiling_costs', []),
                'total_price': quote.get('total_price', 0),
                'quote_reference': quote_data.get('quote_reference', ''),
                'user_profile': user_profile,
                'client_group': user_profile.get('client_group', 'price_b2c') if user_profile else 'price_b2c',
                # === CUSTOMER DATA PRIORITY FIX ===
                'customer': quote_data.get('customer'),  # Include Dynamics 365 customer data
            }
            
            logger.info(f"📋 PDF genereren - klant data: {pdf_quote_data.get('customer')}")
            
            # Generate PDF
            pdf_path = self.pdf_generator.generate_quote(pdf_quote_data)
            
            # Send PDF file
            with open(pdf_path, 'rb') as pdf_file:
                await query.message.reply_document(
                    document=pdf_file,
                    filename=f"Offerte_{quote['quote_number']}.pdf",
                    caption=f"📄 Offerte #{quote['quote_number']}\n"
                           f"Totaal: {format_price(quote['total_price'])}"
                )
            
            await query.answer("PDF verzonden!", show_alert=True)
            
            # Log activity
            self.db.log_user_activity(user_id, 'pdf_generated', {
                'quote_id': quote_id,
                'quote_number': quote['quote_number']
            })
                
        except Exception as e:
            logger.error(f"❌ Fout bij PDF genereren: {e}")
            import traceback
            logger.error(traceback.format_exc())
            await query.answer("Fout bij PDF genereren", show_alert=True)
    
    async def send_quote_email(self, query, quote_id: int):
        """Send quote via email - WITH CUSTOMER DATA PRIORITY FIX"""
        user_id = query.from_user.id
        
        quote = self.db.get_quote_by_id(quote_id)
        if not quote or quote['user_id'] != user_id:
            await query.answer("Toegang geweigerd", show_alert=True)
            return
        
        # Get user profile
        user_profile = self.db.get_user_profile(user_id)
        
        # Parse quote_data
        quote_data = quote.get('quote_data', {})
        if isinstance(quote_data, str):
            try:
                quote_data = json.loads(quote_data)
            except:
                quote_data = {}
        
        # Get email - PRIORITY: customer > user_profile
        customer = quote_data.get('customer')
        recipient_email = None
        
        if customer and customer.get('email'):
            recipient_email = customer.get('email')
            logger.info(f"📧 Email naar klant: {recipient_email}")
        elif user_profile and user_profile.get('email'):
            recipient_email = user_profile.get('email')
            logger.info(f"📧 Email naar gebruiker: {recipient_email}")
        
        if not recipient_email:
            await query.answer("Geen e-mailadres gevonden", show_alert=True)
            return
        
        if not Config.ENABLE_EMAIL_SENDING or not self.email_sender:
            await query.answer("E-mail service is niet beschikbaar", show_alert=True)
            return
        
        await query.answer("E-mail wordt verzonden...")
        
        try:
            # Generate PDF first if available
            pdf_path = None
            if self.pdf_generator and Config.ENABLE_PDF_GENERATION:
                try:
                    pdf_quote_data = {
                        'quote_number': quote.get('quote_number'),
                        'ceilings': quote_data.get('ceilings', []),
                        'ceiling_costs': quote_data.get('ceiling_costs', []),
                        'total_price': quote.get('total_price', 0),
                        'quote_reference': quote_data.get('quote_reference', ''),
                        'user_profile': user_profile,
                        'client_group': user_profile.get('client_group', 'price_b2c') if user_profile else 'price_b2c',
                        # === CUSTOMER DATA PRIORITY FIX ===
                        'customer': customer,  # Include Dynamics 365 customer data
                    }
                    pdf_path = self.pdf_generator.generate_quote(pdf_quote_data)
                except Exception as e:
                    logger.error(f"Error generating PDF for email: {e}")
            
            # Prepare quote_data with customer for email
            email_quote_data = quote_data.copy()
            email_quote_data['customer'] = customer
            
            # Send email
            success = await self.email_sender.send_quote_email(
                recipient_email=recipient_email,
                quote_number=quote['quote_number'],
                pdf_path=pdf_path,
                quote_data=email_quote_data,  # Pass quote_data with customer
                total_price=quote.get('total_price', 0),
                user_profile=user_profile
            )
            
            if success:
                await query.answer("✅ E-mail succesvol verzonden!", show_alert=True)
                await query.message.reply_text(
                    f"📧 Offerte #{quote['quote_number']} is verzonden naar {recipient_email}"
                )
                
                # Update quote status to 'sent' if it was draft
                if quote.get('status') == 'draft':
                    self.db.update_quote_status(quote_id, 'sent', user_id)
                
                # Log activity
                self.db.log_user_activity(user_id, 'quote_emailed', {
                    'quote_id': quote_id,
                    'quote_number': quote['quote_number'],
                    'email': recipient_email
                })
            else:
                await query.answer("❌ E-mail verzenden mislukt", show_alert=True)
                
        except Exception as e:
            logger.error(f"Fout bij verzenden offerte e-mail: {e}")
            import traceback
            logger.error(traceback.format_exc())
            await query.answer("Fout bij verzenden e-mail", show_alert=True)
    
    async def confirm_delete_quote(self, query, quote_id: int):
        """Show delete confirmation"""
        keyboard = [
            [
                InlineKeyboardButton("✅ Ja, Verwijderen", callback_data=f"confirm_delete_{quote_id}"),
                InlineKeyboardButton("❌ Annuleren", callback_data=f"quote_view_{quote_id}")
            ]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(
            "⚠️ **Offerte Verwijderen**\n\n"
            "Weet je zeker dat je deze offerte wilt verwijderen?\n"
            "Deze actie kan niet ongedaan worden gemaakt.",
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )
    
    async def delete_quote(self, query, quote_id: int):
        """Delete a quote"""
        user_id = query.from_user.id
        
        success = self.db.delete_quote(quote_id, user_id)
        
        if success:
            await query.answer("✅ Offerte verwijderd", show_alert=True)
            await query.edit_message_text("✅ Offerte is verwijderd.")
            
            self.db.log_user_activity(user_id, 'quote_deleted', {'quote_id': quote_id})
        else:
            await query.answer("Fout bij verwijderen offerte", show_alert=True)
    
    # ==================== EDITING METHODS ====================
    
    async def start_quote_edit(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Start editing a quote"""
        query = update.callback_query
        await query.answer()
        
        quote_id = int(query.data.replace("quote_edit_", ""))
        user_id = query.from_user.id
        
        quote = self.db.get_quote_by_id(quote_id)
        
        if not quote:
            await query.edit_message_text("❌ Offerte niet gevonden.")
            return ConversationHandler.END
        
        if quote['user_id'] != user_id:
            await query.edit_message_text("❌ Je kunt alleen je eigen offertes bewerken.")
            return ConversationHandler.END
        
        quote_data = quote.get('quote_data', {})
        if isinstance(quote_data, str):
            try:
                quote_data = json.loads(quote_data)
                quote['quote_data'] = quote_data
            except:
                await query.edit_message_text("❌ Fout bij laden offerte data.")
                return ConversationHandler.END
        
        ceilings = quote_data.get('ceilings', [])
        ceiling_costs = quote_data.get('ceiling_costs', [])
        
        # Convert Decimal values to float for total_price
        total_price = quote.get('total_price', 0)
        if isinstance(total_price, Decimal):
            total_price = float(total_price)
        
        structured_quote = {
            'id': quote.get('quotation_id', quote_id),
            'quote_number': quote.get('quote_number'),
            'status': quote.get('status', 'draft'),
            'total_price': total_price,
            'ceilings': ceilings,
            'ceiling_costs': ceiling_costs,
            'quote_reference': quote_data.get('quote_reference', ''),
            'customer': quote_data.get('customer'),  # Include customer data
        }
        
        # Use custom encoder to handle Decimal types
        self.edit_sessions[user_id] = {
            'quote': structured_quote,
            'original_quote': json.loads(json.dumps(structured_quote, default=decimal_default)),
            'current_ceiling_index': 0
        }
        
        return await self.show_edit_menu(update, context)
    
    async def show_edit_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Show the edit menu for a quote"""
        user_id = update.effective_user.id
        
        if user_id not in self.edit_sessions:
            if update.callback_query:
                await update.callback_query.edit_message_text("❌ Bewerksessie verlopen.")
            else:
                await update.message.reply_text("❌ Bewerksessie verlopen.")
            return ConversationHandler.END
        
        session = self.edit_sessions[user_id]
        quote = session['quote']
        ceilings = quote.get('ceilings', [])
        
        message = f"✏️ **Offerte Bewerken: #{quote.get('quote_number', 'N/B')}**\n\n"
        
        if ceilings:
            current_index = session.get('current_ceiling_index', 0)
            if current_index >= len(ceilings):
                current_index = 0
                session['current_ceiling_index'] = 0
            
            ceiling = ceilings[current_index]
            message += f"**Plafond {current_index + 1} van {len(ceilings)}**\n"
            message += f"Naam: {ceiling.get('name', f'Plafond {current_index + 1}')}\n"
            message += f"Afmetingen: {ceiling.get('length', 0)}m × {ceiling.get('width', 0)}m\n"
            message += f"Oppervlakte: {ceiling.get('area', 0):.2f}m²\n"
            message += f"Omtrek: {ceiling.get('perimeter', 0)}m\n"
            message += f"Hoeken: {ceiling.get('corners', 4)}\n"
            message += f"Kleur: {ceiling.get('color', 'Wit')}\n"
        else:
            message += "Geen plafonds gevonden in deze offerte.\n"
        
        message += f"\n**Totaal:** {format_price(quote.get('total_price', 0))}"
        
        # Build keyboard
        keyboard = []
        
        if ceilings:
            keyboard.append([
                InlineKeyboardButton("📐 Afmetingen", callback_data="edit_dimensions"),
                InlineKeyboardButton("📏 Omtrek", callback_data="edit_perimeter")
            ])
            keyboard.append([
                InlineKeyboardButton("🎨 Kleur", callback_data="edit_color"),
                InlineKeyboardButton("📐 Hoeken", callback_data="edit_corners")
            ])
            
            if len(ceilings) > 1:
                keyboard.append([
                    InlineKeyboardButton("⬅️ Vorige", callback_data="edit_prev_ceiling"),
                    InlineKeyboardButton("➡️ Volgende", callback_data="edit_next_ceiling")
                ])
        
        # Add/Remove ceiling buttons
        keyboard.append([
            InlineKeyboardButton("➕ Plafond Toevoegen", callback_data="add_ceiling"),
        ])
        if ceilings and len(ceilings) > 0:
            keyboard.append([
                InlineKeyboardButton("🗑️ Plafond Verwijderen", callback_data="remove_ceiling"),
            ])
        
        keyboard.append([
            InlineKeyboardButton("💾 Opslaan", callback_data="edit_save"),
            InlineKeyboardButton("❌ Annuleren", callback_data="edit_cancel")
        ])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        if update.callback_query:
            await update.callback_query.edit_message_text(
                message,
                reply_markup=reply_markup,
                parse_mode="Markdown"
            )
        else:
            await update.message.reply_text(
                message,
                reply_markup=reply_markup,
                parse_mode="Markdown"
            )
        
        return self.EDIT_MENU
    
    async def show_edit_menu_after_add(self, message, user_id: int) -> int:
        """Show edit menu after adding a ceiling (from message context)"""
        if user_id not in self.edit_sessions:
            return ConversationHandler.END
        
        session = self.edit_sessions[user_id]
        quote = session['quote']
        ceilings = quote.get('ceilings', [])
        
        msg = f"✏️ **Offerte Bewerken: #{quote.get('quote_number', 'N/B')}**\n\n"
        
        if ceilings:
            current_index = session.get('current_ceiling_index', 0)
            if current_index >= len(ceilings):
                current_index = 0
                session['current_ceiling_index'] = 0
            
            ceiling = ceilings[current_index]
            msg += f"**Plafond {current_index + 1} van {len(ceilings)}**\n"
            msg += f"Naam: {ceiling.get('name', f'Plafond {current_index + 1}')}\n"
            msg += f"Afmetingen: {ceiling.get('length', 0)}m × {ceiling.get('width', 0)}m\n"
            msg += f"Oppervlakte: {ceiling.get('area', 0):.2f}m²\n"
            msg += f"Omtrek: {ceiling.get('perimeter', 0)}m\n"
            msg += f"Hoeken: {ceiling.get('corners', 4)}\n"
            msg += f"Kleur: {ceiling.get('color', 'Wit')}\n"
        else:
            msg += "Geen plafonds gevonden in deze offerte.\n"
        
        # Calculate new total
        total = 0
        for costs in quote.get('ceiling_costs', []):
            if isinstance(costs, dict):
                total += costs.get('total', 0)
            else:
                total += getattr(costs, 'total', 0)
        
        msg += f"\n**Totaal:** {format_price(total)}"
        
        # Build keyboard
        keyboard = []
        
        if ceilings:
            keyboard.append([
                InlineKeyboardButton("📐 Afmetingen", callback_data="edit_dimensions"),
                InlineKeyboardButton("📏 Omtrek", callback_data="edit_perimeter")
            ])
            keyboard.append([
                InlineKeyboardButton("🎨 Kleur", callback_data="edit_color"),
                InlineKeyboardButton("📐 Hoeken", callback_data="edit_corners")
            ])
            
            if len(ceilings) > 1:
                keyboard.append([
                    InlineKeyboardButton("⬅️ Vorige", callback_data="edit_prev_ceiling"),
                    InlineKeyboardButton("➡️ Volgende", callback_data="edit_next_ceiling")
                ])
        
        # Add/Remove ceiling buttons
        keyboard.append([
            InlineKeyboardButton("➕ Plafond Toevoegen", callback_data="add_ceiling"),
        ])
        if ceilings and len(ceilings) > 0:
            keyboard.append([
                InlineKeyboardButton("🗑️ Plafond Verwijderen", callback_data="remove_ceiling"),
            ])
        
        keyboard.append([
            InlineKeyboardButton("💾 Opslaan", callback_data="edit_save"),
            InlineKeyboardButton("❌ Annuleren", callback_data="edit_cancel")
        ])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # Send as new message
        await message.reply_text(msg, reply_markup=reply_markup, parse_mode="Markdown")
        
        return self.EDIT_MENU
    
    async def handle_edit_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Handle edit menu callbacks"""
        query = update.callback_query
        await query.answer()
        
        user_id = query.from_user.id
        data = query.data
        
        if user_id not in self.edit_sessions:
            await query.edit_message_text("❌ Bewerksessie verlopen.")
            return ConversationHandler.END
        
        session = self.edit_sessions[user_id]
        
        if data == "edit_dimensions":
            context.user_data['edit_state'] = 'dimensions'
            await query.edit_message_text(
                "📐 **Afmetingen Bewerken**\n\n"
                "Voer de nieuwe afmetingen in als: lengte,breedte\n"
                "Voorbeeld: 5.5,4.2"
            )
            return self.EDIT_DIMENSIONS
        
        elif data == "edit_perimeter":
            context.user_data['edit_state'] = 'perimeter'
            await query.edit_message_text(
                "📏 **Omtrek Bewerken**\n\n"
                "Voer de nieuwe omtrek in meters in:\n"
                "Voorbeeld: 18.5"
            )
            return self.EDIT_PERIMETER
        
        elif data == "edit_color":
            context.user_data['edit_state'] = 'color'
            await query.edit_message_text(
                "🎨 **Kleur Bewerken**\n\n"
                "Voer de nieuwe kleur in:\n"
                "Voorbeeld: Wit, Zwart, RAL9010"
            )
            return self.EDIT_COLOR
        
        elif data == "edit_corners":
            context.user_data['edit_state'] = 'corners'
            await query.edit_message_text(
                "📐 **Hoeken Bewerken**\n\n"
                "Voer het aantal hoeken in:\n"
                "Voorbeeld: 4"
            )
            return self.EDIT_CORNERS
        
        elif data == "edit_prev_ceiling":
            ceilings = session['quote'].get('ceilings', [])
            current = session.get('current_ceiling_index', 0)
            session['current_ceiling_index'] = (current - 1) % len(ceilings)
            return await self.show_edit_menu(update, context)
        
        elif data == "edit_next_ceiling":
            ceilings = session['quote'].get('ceilings', [])
            current = session.get('current_ceiling_index', 0)
            session['current_ceiling_index'] = (current + 1) % len(ceilings)
            return await self.show_edit_menu(update, context)
        
        elif data == "add_ceiling":
            return await self.start_add_ceiling(update, context)
        
        elif data == "remove_ceiling":
            return await self.show_remove_ceiling_menu(update, context)
        
        elif data.startswith("confirm_remove_ceiling_"):
            ceiling_index = int(data.replace("confirm_remove_ceiling_", ""))
            return await self.confirm_remove_ceiling(update, context, ceiling_index)
        
        elif data == "cancel_remove_ceiling":
            return await self.show_edit_menu(update, context)
        
        elif data == "edit_save":
            return await self.save_quote_changes(update, context)
        
        elif data == "edit_cancel":
            return await self.cancel_edit(update, context)
        
        return self.EDIT_MENU
    
    async def save_quote_changes(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Save quote changes to database"""
        query = update.callback_query
        user_id = query.from_user.id
        
        if user_id not in self.edit_sessions:
            await query.edit_message_text("❌ Bewerksessie verlopen.")
            return ConversationHandler.END
        
        session = self.edit_sessions[user_id]
        quote = session['quote']
        
        try:
            # Get client group from quote or user profile
            client_group = quote.get('client_group', 'price_b2c')
            if not client_group or client_group == 'price_b2c':
                user_profile = self.db.get_user_profile(user_id)
                if user_profile:
                    client_group = user_profile.get('client_group', 'price_b2c')
            
            new_ceiling_costs = []
            total_price = 0
            
            for ceiling in quote.get('ceilings', []):
                # Create CeilingConfig with correct parameters matching quote_flow.py
                config = CeilingConfig(
                    name=ceiling.get('name', 'Plafond'),
                    length=ceiling.get('length', 0),
                    width=ceiling.get('width', 0),
                    area=ceiling.get('area', 0),
                    perimeter=ceiling.get('perimeter', 0),
                    perimeter_edited=ceiling.get('perimeter_edited', False),
                    corners=ceiling.get('corners', 4),
                    ceiling_type=ceiling.get('ceiling_type', ceiling.get('product_type', 'fabric')),
                    type_ceiling=ceiling.get('type_ceiling', 'Standard'),
                    color=ceiling.get('color', 'wit'),
                    acoustic=ceiling.get('acoustic', ceiling.get('is_acoustic', False)),
                    finish=ceiling.get('finish', 'Mat'),
                    perimeter_profile=ceiling.get('perimeter_profile'),
                    has_seams=ceiling.get('has_seams', False),
                    seam_length=ceiling.get('seam_length', 0),
                    lights=ceiling.get('lights', []),
                    wood_structures=ceiling.get('wood_structures', []),
                    acoustic_product=ceiling.get('acoustic_product')
                )
                
                # Calculate dimensions if needed
                if config.area == 0 and config.length > 0 and config.width > 0:
                    config.calculate_dimensions()
                
                costs = self.calculator.calculate_ceiling_costs(config, client_group)
                
                # Convert CeilingCost to dict
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
                
                new_ceiling_costs.append(costs_dict)
                total_price += costs.total
            
            quote['ceiling_costs'] = new_ceiling_costs
            quote['total_price'] = total_price
            
            # Save to database
            quote_data = {
                'ceilings': quote.get('ceilings', []),
                'ceiling_costs': new_ceiling_costs,
                'quote_reference': quote.get('quote_reference', ''),
                'customer': quote.get('customer'),  # Preserve customer data
                'client_group': client_group,  # Preserve client group
            }
            
            success = self.db.update_quote_data(
                quote_id=quote['id'],
                quote_data=quote_data,
                total_price=total_price
            )
            
            if success:
                # Sync to Dynamics
                try:
                    dynamics_integration = context.application.bot_data.get('dynamics_integration')
                    if dynamics_integration and dynamics_integration.dynamics_service:
                        asyncio.create_task(dynamics_integration.sync_quote_to_dynamics(quote['id']))
                except Exception as e:
                    logger.error(f"Could not queue quote to Dynamics: {e}")
                
                quote_id = quote['id']
                quote_number = quote.get('quote_number', '')
                customer_name = quote.get('customer', {}).get('name', 'Klant') if quote.get('customer') else 'Klant'
                
                # Build action buttons
                keyboard = [
                    [
                        InlineKeyboardButton("📄 PDF Genereren", callback_data=f"quote_action_pdf_{quote_id}"),
                        InlineKeyboardButton("📧 Versturen", callback_data=f"quote_action_email_{quote_id}")
                    ],
                    [
                        InlineKeyboardButton("✏️ Verder Bewerken", callback_data=f"quote_edit_{quote_id}"),
                        InlineKeyboardButton("🏠 Hoofdmenu", callback_data="back_to_main")
                    ]
                ]
                
                await query.edit_message_text(
                    f"✅ **Offerte Opgeslagen!**\n\n"
                    f"📋 Offerte: `{quote_number}`\n"
                    f"👤 Klant: {customer_name}\n"
                    f"💰 Totaal: **{format_price(total_price)}**\n\n"
                    f"Wat wil je nu doen?",
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode='Markdown'
                )
                
                del self.edit_sessions[user_id]
                return ConversationHandler.END
            else:
                await query.edit_message_text("❌ Fout bij opslaan offerte.")
                return ConversationHandler.END
                
        except Exception as e:
            logger.error(f"Error saving quote: {e}")
            import traceback
            logger.error(traceback.format_exc())
            await query.edit_message_text("❌ Fout bij opslaan wijzigingen.")
            return ConversationHandler.END
    
    async def handle_edit_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Handle text input during editing"""
        user_id = update.effective_user.id
        
        if user_id not in self.edit_sessions:
            await update.message.reply_text("❌ Bewerksessie verlopen.")
            return ConversationHandler.END
        
        session = self.edit_sessions[user_id]
        edit_state = context.user_data.get('edit_state')
        text = update.message.text.strip()
        
        current_index = session.get('current_ceiling_index', 0)
        ceilings = session['quote'].get('ceilings', [])
        
        if not ceilings or current_index >= len(ceilings):
            await update.message.reply_text("❌ Geen plafond geselecteerd.")
            return await self.show_edit_menu(update, context)
        
        ceiling = ceilings[current_index]
        
        if edit_state == 'dimensions':
            try:
                parts = text.replace(' ', '').split(',')
                if len(parts) != 2:
                    await update.message.reply_text("Voer afmetingen in als: lengte,breedte")
                    return self.EDIT_DIMENSIONS
                
                length = float(parts[0])
                width = float(parts[1])
                
                if length <= 0 or width <= 0:
                    await update.message.reply_text("Afmetingen moeten groter zijn dan 0.")
                    return self.EDIT_DIMENSIONS
                
                ceiling['length'] = length
                ceiling['width'] = width
                ceiling['area'] = length * width
                ceiling['perimeter'] = 2 * (length + width)
                
                await update.message.reply_text(f"✅ Afmetingen bijgewerkt: {length}m × {width}m")
                return await self.show_edit_menu(update, context)
                
            except ValueError:
                await update.message.reply_text("Ongeldige invoer. Gebruik: 5.5,4.2")
                return self.EDIT_DIMENSIONS
        
        elif edit_state == 'perimeter':
            try:
                perimeter = float(text)
                if perimeter <= 0:
                    await update.message.reply_text("Omtrek moet groter zijn dan 0.")
                    return self.EDIT_PERIMETER
                
                ceiling['perimeter'] = perimeter
                
                await update.message.reply_text(f"✅ Omtrek bijgewerkt naar: {perimeter}m")
                return await self.show_edit_menu(update, context)
                
            except ValueError:
                await update.message.reply_text("Voer een geldig getal in.")
                return self.EDIT_PERIMETER
        
        elif edit_state == 'color':
            ceiling['color'] = text
            await update.message.reply_text(f"✅ Kleur bijgewerkt naar: {text}")
            return await self.show_edit_menu(update, context)
        
        elif edit_state == 'corners':
            try:
                corners = int(text)
                if corners < 3:
                    await update.message.reply_text("Minimum aantal hoeken is 3.")
                    return self.EDIT_CORNERS
                
                ceiling['corners'] = corners
                
                await update.message.reply_text(f"✅ Hoeken bijgewerkt naar: {corners}")
                return await self.show_edit_menu(update, context)
                
            except ValueError:
                await update.message.reply_text("Voer een geldig getal in.")
                return self.EDIT_CORNERS
        
        context.user_data.pop('edit_state', None)
        return await self.show_edit_menu(update, context)
    
    async def handle_cancel_edit(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Handle /cancel command during editing"""
        user_id = update.effective_user.id
        
        if user_id in self.edit_sessions:
            del self.edit_sessions[user_id]
        
        await update.message.reply_text(
            "❌ Bewerken offerte geannuleerd.\n"
            "Gebruik /quotes om je offertes te bekijken."
        )
        
        return ConversationHandler.END
    
    # ============== ADD CEILING METHODS - MATCHING QUOTE_FLOW.PY ==============
    
    async def start_add_ceiling(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Start the add ceiling wizard - matches quote_flow.py"""
        query = update.callback_query
        await query.answer()
        user_id = query.from_user.id
        
        if user_id not in self.edit_sessions:
            await query.edit_message_text("❌ Bewerksessie verlopen.")
            return ConversationHandler.END
        
        # Initialize new ceiling data in session - matches quote_flow structure
        self.edit_sessions[user_id]['new_ceiling'] = {
            'name': '',
            'length': 0,
            'width': 0,
            'area': 0,
            'perimeter': 0,
            'perimeter_edited': False,
            'corners': 4,
            'ceiling_type': '',
            'type_ceiling': '',
            'color': '',
            'finish': '',
            'acoustic': False,
            'acoustic_performance': None,
            'acoustic_product': None,
            'perimeter_profile': None,
            'has_seams': False,
            'seam_length': 0,
            'lights': [],
            'wood_structures': [],
            'acoustic_absorber': None
        }
        
        # Get client_group from the quote session
        session = self.edit_sessions[user_id]
        client_group = session['quote'].get('client_group', 'price_b2c')
        session['new_ceiling']['client_group'] = client_group
        
        await query.edit_message_text(
            "➕ **Nieuw Plafond Toevoegen**\n\n"
            "Stap 1: Voer een naam in voor dit plafond:\n"
            "Bijvoorbeeld: Woonkamer, Badkamer, Keuken\n\n"
            "Type /cancel om te annuleren."
        )
        return self.ADD_CEILING_NAME
    
    async def handle_ceiling_name(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Handle ceiling name input"""
        user_id = update.effective_user.id
        
        if user_id not in self.edit_sessions:
            await update.message.reply_text("❌ Bewerksessie verlopen.")
            return ConversationHandler.END
        
        name = update.message.text.strip()
        if not name:
            await update.message.reply_text("❌ Voer een geldige naam in.")
            return self.ADD_CEILING_NAME
        
        self.edit_sessions[user_id]['new_ceiling']['name'] = name
        
        await update.message.reply_text(
            f"✅ Naam: **{name}**\n\n"
            "📐 **Stap 2: Afmetingen**\n"
            "Voer de afmetingen in (lengte × breedte) in meters:\n\n"
            "Voorbeelden:\n"
            "• 5.5 x 4.2\n"
            "• 5.5m × 4.2m\n"
            "• 5,4"
        )
        return self.ADD_CEILING_SIZE
    
    async def handle_ceiling_size(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Handle ceiling size input"""
        user_id = update.effective_user.id
        
        if user_id not in self.edit_sessions:
            await update.message.reply_text("❌ Bewerksessie verlopen.")
            return ConversationHandler.END
        
        text = update.message.text.strip()
        # Parse dimensions - support multiple formats
        text = text.replace('×', 'x').replace('X', 'x').replace('m', '').replace(' ', '')
        
        try:
            if 'x' in text:
                parts = text.split('x')
            elif ',' in text:
                parts = text.split(',')
            else:
                raise ValueError("Invalid format")
            
            if len(parts) != 2:
                raise ValueError("Need 2 values")
            
            length = float(parts[0].replace(',', '.'))
            width = float(parts[1].replace(',', '.'))
            
            if length <= 0 or width <= 0:
                raise ValueError("Values must be positive")
            
            if length > 100 or width > 100:
                raise ValueError("Values too large")
            
            area = length * width
            perimeter = 2 * (length + width)
            
            new_ceiling = self.edit_sessions[user_id]['new_ceiling']
            new_ceiling['length'] = length
            new_ceiling['width'] = width
            new_ceiling['area'] = area
            new_ceiling['perimeter'] = perimeter
            
            # Show confirmation with inline buttons
            keyboard = [
                [InlineKeyboardButton("✅ Correct", callback_data="add_ceiling_size_ok")],
                [InlineKeyboardButton("❌ Opnieuw invoeren", callback_data="add_ceiling_size_redo")],
                [InlineKeyboardButton("✏️ Omtrek aanpassen", callback_data="add_ceiling_edit_perimeter")],
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await update.message.reply_text(
                f"📐 **{new_ceiling['name']} - Bevestiging**\n\n"
                f"• Lengte: {length}m\n"
                f"• Breedte: {width}m\n"
                f"• Oppervlakte: {area:.2f} m²\n"
                f"• Omtrek: {perimeter:.2f} m\n\n"
                f"Is dit correct?",
                reply_markup=reply_markup
            )
            return self.ADD_CEILING_SIZE_CONFIRM
            
        except Exception:
            await update.message.reply_text(
                "❌ Ongeldige invoer.\n"
                "Voer afmetingen in als: 5.5 x 4.2\n"
                "Of: 5.5,4.2"
            )
            return self.ADD_CEILING_SIZE
    
    async def handle_size_confirmation_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Handle size confirmation callback"""
        query = update.callback_query
        await query.answer()
        user_id = query.from_user.id
        
        if user_id not in self.edit_sessions:
            await query.edit_message_text("❌ Bewerksessie verlopen.")
            return ConversationHandler.END
        
        data = query.data
        new_ceiling = self.edit_sessions[user_id]['new_ceiling']
        
        if data == "add_ceiling_size_ok":
            # Continue to corners
            keyboard = [
                [InlineKeyboardButton("4", callback_data="add_ceiling_corners_4")],
                [InlineKeyboardButton("5", callback_data="add_ceiling_corners_5")],
                [InlineKeyboardButton("6", callback_data="add_ceiling_corners_6")],
                [InlineKeyboardButton("7", callback_data="add_ceiling_corners_7")],
                [InlineKeyboardButton("8+", callback_data="add_ceiling_corners_8")],
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await query.edit_message_text(
                f"📐 **{new_ceiling['name']} - Hoeken**\n\n"
                f"Hoeveel hoeken heeft dit plafond/wand?",
                reply_markup=reply_markup
            )
            return self.ADD_CEILING_CORNERS
        
        elif data == "add_ceiling_size_redo":
            await query.edit_message_text(
                f"📐 **{new_ceiling['name']} - Afmetingen**\n\n"
                "Voer de afmetingen opnieuw in (lengte × breedte):"
            )
            return self.ADD_CEILING_SIZE
        
        elif data == "add_ceiling_edit_perimeter":
            await query.edit_message_text(
                f"📏 **{new_ceiling['name']} - Omtrek Aanpassen**\n\n"
                f"Huidige berekende omtrek: {new_ceiling['perimeter']:.2f}m\n"
                f"(Gebaseerd op: 2 × ({new_ceiling['length']}m + {new_ceiling['width']}m))\n\n"
                f"Voer de werkelijke omtrek in meters in:\n"
                f"(voor complexe vormen of speciale gevallen)"
            )
            return self.ADD_CEILING_PERIMETER_EDIT
        
        return self.ADD_CEILING_SIZE_CONFIRM
    
    async def handle_perimeter_edit(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Handle manual perimeter input"""
        user_id = update.effective_user.id
        
        if user_id not in self.edit_sessions:
            await update.message.reply_text("❌ Bewerksessie verlopen.")
            return ConversationHandler.END
        
        try:
            perimeter = float(update.message.text.strip().replace(',', '.').replace('m', ''))
            if perimeter <= 0 or perimeter > 500:
                raise ValueError("Invalid perimeter")
            
            new_ceiling = self.edit_sessions[user_id]['new_ceiling']
            new_ceiling['perimeter'] = perimeter
            new_ceiling['perimeter_edited'] = True
            
            # Continue to corners
            keyboard = [
                [InlineKeyboardButton("4", callback_data="add_ceiling_corners_4")],
                [InlineKeyboardButton("5", callback_data="add_ceiling_corners_5")],
                [InlineKeyboardButton("6", callback_data="add_ceiling_corners_6")],
                [InlineKeyboardButton("7", callback_data="add_ceiling_corners_7")],
                [InlineKeyboardButton("8+", callback_data="add_ceiling_corners_8")],
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await update.message.reply_text(
                f"✅ Omtrek aangepast naar: {perimeter:.2f}m\n\n"
                f"📐 **{new_ceiling['name']} - Hoeken**\n\n"
                f"Hoeveel hoeken heeft dit plafond/wand?",
                reply_markup=reply_markup
            )
            return self.ADD_CEILING_CORNERS
            
        except ValueError:
            await update.message.reply_text(
                "❌ Ongeldige invoer.\n"
                "Voer een geldige omtrek in meters in (bijv. 18.5)"
            )
            return self.ADD_CEILING_PERIMETER_EDIT
    
    async def handle_corners_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Handle corners selection callback"""
        query = update.callback_query
        await query.answer()
        user_id = query.from_user.id
        
        if user_id not in self.edit_sessions:
            await query.edit_message_text("❌ Bewerksessie verlopen.")
            return ConversationHandler.END
        
        data = query.data
        corners = int(data.replace("add_ceiling_corners_", ""))
        
        new_ceiling = self.edit_sessions[user_id]['new_ceiling']
        new_ceiling['corners'] = corners
        
        # Get ceiling types from database
        ceiling_types = self.db.get_unique_values("ceiling", "product_type")
        if not ceiling_types:
            ceiling_types = ["fabric", "pvc"]  # Fallback
        
        keyboard = []
        for ct in ceiling_types:
            keyboard.append([InlineKeyboardButton(ct.upper(), callback_data=f"add_ceiling_type_{ct.lower()}")])
        
        await query.edit_message_text(
            f"✅ Hoeken: {corners}\n\n"
            f"🏗️ **{new_ceiling['name']} - Type Plafond**\n\n"
            f"Stap 3: Selecteer het type stretch plafond:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return self.ADD_CEILING_TYPE
    
    async def handle_type_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Handle ceiling type (product_type) selection"""
        query = update.callback_query
        await query.answer()
        user_id = query.from_user.id
        
        if user_id not in self.edit_sessions:
            await query.edit_message_text("❌ Bewerksessie verlopen.")
            return ConversationHandler.END
        
        data = query.data
        ceiling_type = data.replace("add_ceiling_type_", "")
        
        new_ceiling = self.edit_sessions[user_id]['new_ceiling']
        new_ceiling['ceiling_type'] = ceiling_type
        new_ceiling['product_type'] = ceiling_type
        
        logger.info(f"✅ Add ceiling: type set to '{ceiling_type}'")
        
        # Get type_ceiling options from database based on product_type
        type_ceilings = self.db.get_type_ceilings_for_product_type(ceiling_type)
        
        if not type_ceilings:
            type_ceilings = ["Standard"]  # Fallback
        
        keyboard = []
        for tc in type_ceilings:
            keyboard.append([InlineKeyboardButton(tc, callback_data=f"add_ceiling_tc_{tc}")])
        
        await query.edit_message_text(
            f"✅ Type: {ceiling_type.upper()}\n\n"
            f"📋 **{new_ceiling['name']} - Specifiek Type**\n\n"
            f"Stap 4: Selecteer het specifieke type {ceiling_type.upper()} plafond:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return self.ADD_CEILING_TYPE_CEILING
    
    async def handle_type_ceiling_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Handle type_ceiling selection"""
        query = update.callback_query
        await query.answer()
        user_id = query.from_user.id
        
        if user_id not in self.edit_sessions:
            await query.edit_message_text("❌ Bewerksessie verlopen.")
            return ConversationHandler.END
        
        data = query.data
        type_ceiling = data.replace("add_ceiling_tc_", "")
        
        new_ceiling = self.edit_sessions[user_id]['new_ceiling']
        new_ceiling['type_ceiling'] = type_ceiling
        
        logger.info(f"✅ Add ceiling: type_ceiling set to '{type_ceiling}'")
        
        # Check if this is acoustic
        is_acoustic = "acoustic" in type_ceiling.lower()
        new_ceiling['acoustic'] = is_acoustic
        
        # Get available colors from database
        colors = self.db.get_colors_for_type_ceiling(
            new_ceiling['ceiling_type'],
            type_ceiling
        )
        
        if not colors:
            colors = ["wit", "zwart"]  # Fallback
        
        keyboard = []
        # Show first 8 colors as buttons
        for color in colors[:8]:
            keyboard.append([InlineKeyboardButton(color.capitalize(), callback_data=f"add_ceiling_color_{color}")])
        
        if len(colors) > 8:
            keyboard.append([InlineKeyboardButton("Andere kleur...", callback_data="add_ceiling_color_other")])
        
        await query.edit_message_text(
            f"✅ Type: {new_ceiling['ceiling_type'].upper()} - {type_ceiling}\n\n"
            f"🎨 **{new_ceiling['name']} - Kleur**\n\n"
            f"Stap 5: Selecteer de kleur:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return self.ADD_CEILING_COLOR
    
    async def handle_color_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Handle color selection callback"""
        query = update.callback_query
        await query.answer()
        user_id = query.from_user.id
        
        if user_id not in self.edit_sessions:
            await query.edit_message_text("❌ Bewerksessie verlopen.")
            return ConversationHandler.END
        
        data = query.data
        
        if data == "add_ceiling_color_other":
            new_ceiling = self.edit_sessions[user_id]['new_ceiling']
            await query.edit_message_text(
                f"🎨 **{new_ceiling['name']} - Kleur**\n\n"
                "Voer de gewenste kleur in:\n"
                "Bijvoorbeeld: wit, zwart, RAL 9010"
            )
            # Stay in same state but wait for text input
            return self.ADD_CEILING_COLOR
        
        color = data.replace("add_ceiling_color_", "")
        return await self._process_color_selection(query.message, user_id, color, is_callback=True)
    
    async def handle_color_text(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Handle color text input"""
        user_id = update.effective_user.id
        
        if user_id not in self.edit_sessions:
            await update.message.reply_text("❌ Bewerksessie verlopen.")
            return ConversationHandler.END
        
        color = update.message.text.strip().lower()
        if not color:
            await update.message.reply_text("❌ Voer een geldige kleur in.")
            return self.ADD_CEILING_COLOR
        
        return await self._process_color_selection(update.message, user_id, color, is_callback=False)
    
    async def _process_color_selection(self, message, user_id: int, color: str, is_callback: bool = False) -> int:
        """Process color selection and continue to next step"""
        new_ceiling = self.edit_sessions[user_id]['new_ceiling']
        new_ceiling['color'] = color
        
        logger.info(f"✅ Add ceiling: color set to '{color}'")
        
        # Check if acoustic - if so, ask for acoustic performance
        if new_ceiling.get('acoustic'):
            # Get acoustic performance products
            acoustic_products = self.db.get_acoustic_performance_products()
            
            keyboard = []
            if acoustic_products:
                performance_groups = {}
                for product in acoustic_products:
                    perf = product.get("acoustic_performance", "")
                    if perf and perf not in performance_groups:
                        performance_groups[perf] = product
                
                for perf, product in performance_groups.items():
                    desc = product.get('description', perf)[:30]
                    keyboard.append([InlineKeyboardButton(f"{perf}", callback_data=f"add_ceiling_acoustic_{perf}")])
            
            keyboard.append([InlineKeyboardButton("Overslaan", callback_data="add_ceiling_acoustic_skip")])
            
            msg = (
                f"✅ Kleur: {color.capitalize()}\n\n"
                f"🔊 **{new_ceiling['name']} - Akoestische Verbetering**\n\n"
                f"Dit is een akoestisch plafond.\n"
                f"Wilt u akoestische absorbers toevoegen?"
            )
            
            if is_callback:
                await message.edit_text(msg, reply_markup=InlineKeyboardMarkup(keyboard))
            else:
                await message.reply_text(msg, reply_markup=InlineKeyboardMarkup(keyboard))
            
            return self.ADD_CEILING_ACOUSTIC_PERF
        
        # Not acoustic - go to perimeter profile
        return await self._ask_perimeter_profile(message, user_id, color, is_callback)
    
    async def _ask_perimeter_profile(self, message, user_id: int, color: str, is_callback: bool = False) -> int:
        """Ask for perimeter profile selection"""
        new_ceiling = self.edit_sessions[user_id]['new_ceiling']
        
        # Get perimeter products
        perimeter_products = self.db.get_products_by_category("perimeter")
        
        keyboard = []
        if perimeter_products:
            for product in perimeter_products[:6]:
                code = product.get('product_code', product.get('code', ''))
                desc = product.get('description', '')[:25]
                keyboard.append([InlineKeyboardButton(f"{code} - {desc}", callback_data=f"add_ceiling_profile_{code}")])
        
        keyboard.append([InlineKeyboardButton("Standaard profiel", callback_data="add_ceiling_profile_standard")])
        
        msg = (
            f"✅ Kleur: {color.capitalize()}\n\n"
            f"🔧 **{new_ceiling['name']} - Omtrek Profiel**\n\n"
            f"Selecteer het type omtrekprofiel:"
        )
        
        if is_callback:
            await message.edit_text(msg, reply_markup=InlineKeyboardMarkup(keyboard))
        else:
            await message.reply_text(msg, reply_markup=InlineKeyboardMarkup(keyboard))
        
        return self.ADD_CEILING_PERIMETER_PROFILE
    
    async def handle_acoustic_perf_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Handle acoustic performance selection"""
        query = update.callback_query
        await query.answer()
        user_id = query.from_user.id
        
        if user_id not in self.edit_sessions:
            await query.edit_message_text("❌ Bewerksessie verlopen.")
            return ConversationHandler.END
        
        data = query.data
        new_ceiling = self.edit_sessions[user_id]['new_ceiling']
        
        if data != "add_ceiling_acoustic_skip":
            acoustic_perf = data.replace("add_ceiling_acoustic_", "")
            new_ceiling['acoustic_performance'] = acoustic_perf
            
            # Find and store the acoustic product
            acoustic_products = self.db.get_acoustic_performance_products()
            for product in acoustic_products:
                if product.get("acoustic_performance") == acoustic_perf:
                    new_ceiling['acoustic_product'] = product
                    break
        
        # Continue to perimeter profile
        return await self._ask_perimeter_profile(query.message, user_id, new_ceiling['color'], is_callback=True)
    
    async def handle_perimeter_profile_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Handle perimeter profile selection"""
        query = update.callback_query
        await query.answer()
        user_id = query.from_user.id
        
        if user_id not in self.edit_sessions:
            await query.edit_message_text("❌ Bewerksessie verlopen.")
            return ConversationHandler.END
        
        data = query.data
        new_ceiling = self.edit_sessions[user_id]['new_ceiling']
        
        if data != "add_ceiling_profile_standard":
            code = data.replace("add_ceiling_profile_", "")
            product = self.db.get_product_by_code(code)
            if product:
                # Convert Decimal values to float
                for key, value in product.items():
                    if hasattr(value, 'quantize'):
                        product[key] = float(value)
                new_ceiling['perimeter_profile'] = product
        
        # Ask about seams
        keyboard = [
            [InlineKeyboardButton("Ja - Er zijn naden nodig", callback_data="add_ceiling_seams_yes")],
            [InlineKeyboardButton("Nee - Geen naden", callback_data="add_ceiling_seams_no")],
        ]
        
        await query.edit_message_text(
            f"🔗 **{new_ceiling['name']} - Naden**\n\n"
            f"Zijn er naden nodig in dit plafond?\n\n"
            f"(Naden zijn nodig voor grote plafonds of complexe vormen)",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return self.ADD_CEILING_SEAM_QUESTION
    
    async def handle_seam_question_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Handle seam question"""
        query = update.callback_query
        await query.answer()
        user_id = query.from_user.id
        
        if user_id not in self.edit_sessions:
            await query.edit_message_text("❌ Bewerksessie verlopen.")
            return ConversationHandler.END
        
        data = query.data
        new_ceiling = self.edit_sessions[user_id]['new_ceiling']
        
        if data == "add_ceiling_seams_yes":
            new_ceiling['has_seams'] = True
            await query.edit_message_text(
                f"🔗 **{new_ceiling['name']} - Naadlengte**\n\n"
                f"Hoeveel meter naden zijn er nodig?\n\n"
                f"Voer het totale aantal meters in (bijv. 5.5):"
            )
            return self.ADD_CEILING_SEAM_LENGTH
        else:
            new_ceiling['has_seams'] = False
            new_ceiling['seam_length'] = 0
            return await self._ask_lights_question(query.message, user_id, is_callback=True)
    
    async def handle_seam_length(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Handle seam length input"""
        user_id = update.effective_user.id
        
        if user_id not in self.edit_sessions:
            await update.message.reply_text("❌ Bewerksessie verlopen.")
            return ConversationHandler.END
        
        try:
            length = float(update.message.text.strip().replace(',', '.'))
            if length <= 0:
                raise ValueError("Must be positive")
            
            new_ceiling = self.edit_sessions[user_id]['new_ceiling']
            new_ceiling['seam_length'] = length
            
            return await self._ask_lights_question(update.message, user_id, is_callback=False)
            
        except ValueError:
            await update.message.reply_text(
                "❌ Voer een geldig aantal meters in (bijv. 5.5)"
            )
            return self.ADD_CEILING_SEAM_LENGTH
    
    async def _ask_lights_question(self, message, user_id: int, is_callback: bool = False) -> int:
        """Ask about lights"""
        new_ceiling = self.edit_sessions[user_id]['new_ceiling']
        
        keyboard = [
            [InlineKeyboardButton("Ja - Verlichting toevoegen", callback_data="add_ceiling_lights_yes")],
            [InlineKeyboardButton("Nee - Geen verlichting", callback_data="add_ceiling_lights_no")],
        ]
        
        msg = (
            f"💡 **{new_ceiling['name']} - Verlichting**\n\n"
            f"Wilt u verlichting toevoegen aan dit plafond?"
        )
        
        if is_callback:
            await message.edit_text(msg, reply_markup=InlineKeyboardMarkup(keyboard))
        else:
            await message.reply_text(msg, reply_markup=InlineKeyboardMarkup(keyboard))
        
        return self.ADD_CEILING_LIGHTS_QUESTION
    
    async def handle_lights_question_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Handle lights question"""
        query = update.callback_query
        await query.answer()
        user_id = query.from_user.id
        
        if user_id not in self.edit_sessions:
            await query.edit_message_text("❌ Bewerksessie verlopen.")
            return ConversationHandler.END
        
        data = query.data
        
        if data == "add_ceiling_lights_yes":
            return await self._show_light_selection(query.message, user_id, is_callback=True)
        else:
            return await self._ask_wood_question(query.message, user_id, is_callback=True)
    
    async def _show_light_selection(self, message, user_id: int, is_callback: bool = False) -> int:
        """Show light selection"""
        new_ceiling = self.edit_sessions[user_id]['new_ceiling']
        
        # Get light products
        light_products = self.db.get_products_by_category("light")
        
        keyboard = []
        if light_products:
            for product in light_products[:8]:
                code = product.get('product_code', product.get('code', ''))
                desc = product.get('description', '')[:20]
                keyboard.append([InlineKeyboardButton(f"{code} - {desc}", callback_data=f"add_ceiling_light_{code}")])
        
        keyboard.append([InlineKeyboardButton("Annuleren", callback_data="add_ceiling_light_cancel")])
        
        msg = (
            f"💡 **{new_ceiling['name']} - Verlichting Selectie**\n\n"
            f"Selecteer het type verlichting:"
        )
        
        if is_callback:
            await message.edit_text(msg, reply_markup=InlineKeyboardMarkup(keyboard))
        else:
            await message.reply_text(msg, reply_markup=InlineKeyboardMarkup(keyboard))
        
        return self.ADD_CEILING_LIGHT_SELECTION
    
    async def handle_light_selection_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Handle light selection"""
        query = update.callback_query
        await query.answer()
        user_id = query.from_user.id
        
        if user_id not in self.edit_sessions:
            await query.edit_message_text("❌ Bewerksessie verlopen.")
            return ConversationHandler.END
        
        data = query.data
        
        if data == "add_ceiling_light_cancel":
            return await self._ask_wood_question(query.message, user_id, is_callback=True)
        
        code = data.replace("add_ceiling_light_", "")
        product = self.db.get_product_by_code(code)
        
        if product:
            # Store temporarily
            self.edit_sessions[user_id]['temp_light'] = product
            new_ceiling = self.edit_sessions[user_id]['new_ceiling']
            
            await query.edit_message_text(
                f"💡 **{new_ceiling['name']} - Aantal Lampen**\n\n"
                f"Geselecteerd: {code}\n\n"
                f"Hoeveel stuks heeft u nodig?"
            )
            return self.ADD_CEILING_LIGHT_QUANTITY
        else:
            await query.edit_message_text("❌ Product niet gevonden.")
            return await self._ask_wood_question(query.message, user_id, is_callback=True)
    
    async def handle_light_quantity(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Handle light quantity input"""
        user_id = update.effective_user.id
        
        if user_id not in self.edit_sessions:
            await update.message.reply_text("❌ Bewerksessie verlopen.")
            return ConversationHandler.END
        
        try:
            quantity = int(update.message.text.strip())
            if quantity <= 0:
                raise ValueError("Must be positive")
            
            session = self.edit_sessions[user_id]
            new_ceiling = session['new_ceiling']
            light = session['temp_light'].copy()
            
            # Get price based on client group
            client_group = new_ceiling.get('client_group', 'price_b2c')
            light['quantity'] = quantity
            light['price'] = light.get(client_group, light.get('price_b2c', 0))
            
            # Ensure product_code field exists
            if 'product_code' not in light and 'code' in light:
                light['product_code'] = light['code']
            
            new_ceiling['lights'].append(light)
            
            # Ask for more lights
            keyboard = [
                [InlineKeyboardButton("Ja - Meer verlichting", callback_data="add_ceiling_more_lights_yes")],
                [InlineKeyboardButton("Nee - Doorgaan", callback_data="add_ceiling_more_lights_no")],
            ]
            
            await update.message.reply_text(
                f"✅ {quantity}x {light.get('product_code', 'lamp')} toegevoegd\n\n"
                f"Wilt u meer verlichting toevoegen?",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return self.ADD_CEILING_MORE_LIGHTS
            
        except ValueError:
            await update.message.reply_text(
                "❌ Voer een geldig aantal in (bijv. 6)"
            )
            return self.ADD_CEILING_LIGHT_QUANTITY
    
    async def handle_more_lights_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Handle more lights question"""
        query = update.callback_query
        await query.answer()
        user_id = query.from_user.id
        
        if user_id not in self.edit_sessions:
            await query.edit_message_text("❌ Bewerksessie verlopen.")
            return ConversationHandler.END
        
        data = query.data
        
        if data == "add_ceiling_more_lights_yes":
            return await self._show_light_selection(query.message, user_id, is_callback=True)
        else:
            return await self._ask_wood_question(query.message, user_id, is_callback=True)
    
    async def _ask_wood_question(self, message, user_id: int, is_callback: bool = False) -> int:
        """Ask about wood structures"""
        new_ceiling = self.edit_sessions[user_id]['new_ceiling']
        
        keyboard = [
            [InlineKeyboardButton("Ja - Houtstructuren toevoegen", callback_data="add_ceiling_wood_yes")],
            [InlineKeyboardButton("Nee - Afronden", callback_data="add_ceiling_wood_no")],
        ]
        
        msg = (
            f"🪵 **{new_ceiling['name']} - Houtstructuren**\n\n"
            f"Wilt u houtstructuren toevoegen aan dit plafond?"
        )
        
        if is_callback:
            await message.edit_text(msg, reply_markup=InlineKeyboardMarkup(keyboard))
        else:
            await message.reply_text(msg, reply_markup=InlineKeyboardMarkup(keyboard))
        
        return self.ADD_CEILING_WOOD_QUESTION
    
    async def handle_wood_question_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Handle wood question"""
        query = update.callback_query
        await query.answer()
        user_id = query.from_user.id
        
        if user_id not in self.edit_sessions:
            await query.edit_message_text("❌ Bewerksessie verlopen.")
            return ConversationHandler.END
        
        data = query.data
        
        if data == "add_ceiling_wood_yes":
            return await self._show_wood_selection(query.message, user_id, is_callback=True)
        else:
            # Finalize ceiling
            return await self._finalize_ceiling(query.message, user_id, is_callback=True)
    
    async def _show_wood_selection(self, message, user_id: int, is_callback: bool = False) -> int:
        """Show wood selection"""
        new_ceiling = self.edit_sessions[user_id]['new_ceiling']
        
        # Get wood products
        wood_products = self.db.get_products_by_category("wood_structure")
        
        keyboard = []
        if wood_products:
            for product in wood_products[:8]:
                code = product.get('product_code', product.get('code', ''))
                desc = product.get('description', '')[:20]
                keyboard.append([InlineKeyboardButton(f"{code} - {desc}", callback_data=f"add_ceiling_wood_{code}")])
        
        keyboard.append([InlineKeyboardButton("Annuleren", callback_data="add_ceiling_wood_cancel")])
        
        msg = (
            f"🪵 **{new_ceiling['name']} - Houtstructuur Selectie**\n\n"
            f"Selecteer het type houtstructuur:"
        )
        
        if is_callback:
            await message.edit_text(msg, reply_markup=InlineKeyboardMarkup(keyboard))
        else:
            await message.reply_text(msg, reply_markup=InlineKeyboardMarkup(keyboard))
        
        return self.ADD_CEILING_WOOD_SELECTION
    
    async def handle_wood_selection_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Handle wood selection"""
        query = update.callback_query
        await query.answer()
        user_id = query.from_user.id
        
        if user_id not in self.edit_sessions:
            await query.edit_message_text("❌ Bewerksessie verlopen.")
            return ConversationHandler.END
        
        data = query.data
        
        if data == "add_ceiling_wood_cancel":
            return await self._finalize_ceiling(query.message, user_id, is_callback=True)
        
        code = data.replace("add_ceiling_wood_", "")
        product = self.db.get_product_by_code(code)
        
        if product:
            self.edit_sessions[user_id]['temp_wood'] = product
            new_ceiling = self.edit_sessions[user_id]['new_ceiling']
            
            await query.edit_message_text(
                f"🪵 **{new_ceiling['name']} - Aantal Meters**\n\n"
                f"Geselecteerd: {code}\n\n"
                f"Hoeveel meter heeft u nodig?"
            )
            return self.ADD_CEILING_WOOD_QUANTITY
        else:
            return await self._finalize_ceiling(query.message, user_id, is_callback=True)
    
    async def handle_wood_quantity(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Handle wood quantity input"""
        user_id = update.effective_user.id
        
        if user_id not in self.edit_sessions:
            await update.message.reply_text("❌ Bewerksessie verlopen.")
            return ConversationHandler.END
        
        try:
            quantity = float(update.message.text.strip().replace(',', '.'))
            if quantity <= 0:
                raise ValueError("Must be positive")
            
            session = self.edit_sessions[user_id]
            new_ceiling = session['new_ceiling']
            wood = session['temp_wood'].copy()
            
            # Get price based on client group
            client_group = new_ceiling.get('client_group', 'price_b2c')
            wood['quantity'] = quantity
            wood['price'] = wood.get(client_group, wood.get('price_b2c', 0))
            
            # Ensure product_code field exists
            if 'product_code' not in wood and 'code' in wood:
                wood['product_code'] = wood['code']
            
            new_ceiling['wood_structures'].append(wood)
            
            # Ask for more wood
            keyboard = [
                [InlineKeyboardButton("Ja - Meer houtstructuren", callback_data="add_ceiling_more_wood_yes")],
                [InlineKeyboardButton("Nee - Afronden", callback_data="add_ceiling_more_wood_no")],
            ]
            
            await update.message.reply_text(
                f"✅ {quantity}m {wood.get('product_code', 'hout')} toegevoegd\n\n"
                f"Wilt u meer houtstructuren toevoegen?",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return self.ADD_CEILING_MORE_WOOD
            
        except ValueError:
            await update.message.reply_text(
                "❌ Voer een geldig aantal meters in (bijv. 10)"
            )
            return self.ADD_CEILING_WOOD_QUANTITY
    
    async def handle_more_wood_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Handle more wood question"""
        query = update.callback_query
        await query.answer()
        user_id = query.from_user.id
        
        if user_id not in self.edit_sessions:
            await query.edit_message_text("❌ Bewerksessie verlopen.")
            return ConversationHandler.END
        
        data = query.data
        
        if data == "add_ceiling_more_wood_yes":
            return await self._show_wood_selection(query.message, user_id, is_callback=True)
        else:
            return await self._finalize_ceiling(query.message, user_id, is_callback=True)
    
    async def _finalize_ceiling(self, message, user_id: int, is_callback: bool = False) -> int:
        """Finalize and add the ceiling to the quote"""
        session = self.edit_sessions[user_id]
        new_ceiling = session['new_ceiling']
        
        # Create the complete ceiling object matching quote_flow.py structure
        ceiling = {
            'name': new_ceiling.get('name', 'Nieuw plafond'),
            'length': new_ceiling.get('length', 0),
            'width': new_ceiling.get('width', 0),
            'area': new_ceiling.get('area', 0),
            'perimeter': new_ceiling.get('perimeter', 0),
            'perimeter_edited': new_ceiling.get('perimeter_edited', False),
            'corners': new_ceiling.get('corners', 4),
            'ceiling_type': new_ceiling.get('ceiling_type', 'fabric'),
            'product_type': new_ceiling.get('ceiling_type', 'fabric'),
            'type_ceiling': new_ceiling.get('type_ceiling', 'Standard'),
            'color': new_ceiling.get('color', 'wit'),
            'finish': new_ceiling.get('finish', 'Mat'),
            'acoustic': new_ceiling.get('acoustic', False),
            'acoustic_performance': new_ceiling.get('acoustic_performance'),
            'acoustic_product': new_ceiling.get('acoustic_product'),
            'perimeter_profile': new_ceiling.get('perimeter_profile'),
            'has_seams': new_ceiling.get('has_seams', False),
            'seam_length': new_ceiling.get('seam_length', 0),
            'lights': new_ceiling.get('lights', []),
            'wood_structures': new_ceiling.get('wood_structures', []),
            'acoustic_absorber': new_ceiling.get('acoustic_absorber'),
        }
        
        # Add to quote
        if 'ceilings' not in session['quote']:
            session['quote']['ceilings'] = []
        session['quote']['ceilings'].append(ceiling)
        
        # Calculate costs for the new ceiling
        ceiling_config = CeilingConfig(
            name=ceiling['name'],
            length=ceiling['length'],
            width=ceiling['width'],
            area=ceiling['area'],
            perimeter=ceiling['perimeter'],
            perimeter_edited=ceiling['perimeter_edited'],
            corners=ceiling['corners'],
            ceiling_type=ceiling['ceiling_type'],
            type_ceiling=ceiling['type_ceiling'],
            color=ceiling['color'],
            acoustic=ceiling['acoustic'],
            finish=ceiling['finish'],
            perimeter_profile=ceiling['perimeter_profile'],
            has_seams=ceiling['has_seams'],
            seam_length=ceiling['seam_length'],
            lights=ceiling['lights'],
            wood_structures=ceiling['wood_structures'],
            acoustic_product=ceiling['acoustic_product']
        )
        
        if ceiling_config.area == 0 and ceiling_config.length > 0 and ceiling_config.width > 0:
            ceiling_config.calculate_dimensions()
        
        client_group = session['quote'].get('client_group', 'price_b2c')
        costs = self.calculator.calculate_ceiling_costs(ceiling_config, client_group)
        
        # Convert CeilingCost to dict
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
        
        # Add to ceiling_costs
        if 'ceiling_costs' not in session['quote']:
            session['quote']['ceiling_costs'] = []
        session['quote']['ceiling_costs'].append(costs_dict)
        
        # Set the new ceiling as current
        session['current_ceiling_index'] = len(session['quote']['ceilings']) - 1
        
        # Clear the temp new_ceiling data
        del session['new_ceiling']
        if 'temp_light' in session:
            del session['temp_light']
        if 'temp_wood' in session:
            del session['temp_wood']
        
        # Build summary
        summary = (
            f"✅ **Plafond Toegevoegd!**\n\n"
            f"📋 **{ceiling['name']}**\n"
            f"• Afmetingen: {ceiling['length']}m × {ceiling['width']}m\n"
            f"• Oppervlakte: {ceiling['area']:.2f} m²\n"
            f"• Omtrek: {ceiling['perimeter']:.2f} m"
        )
        
        if ceiling['perimeter_edited']:
            summary += " (aangepast)"
        
        summary += f"\n• Hoeken: {ceiling['corners']}\n"
        summary += f"• Type: {ceiling['ceiling_type'].upper()} - {ceiling['type_ceiling']}\n"
        summary += f"• Kleur: {ceiling['color'].capitalize()}\n"
        
        if ceiling['acoustic']:
            summary += f"• Akoestisch: Ja"
            if ceiling['acoustic_performance']:
                summary += f" ({ceiling['acoustic_performance']})"
            summary += "\n"
        
        if ceiling['has_seams']:
            summary += f"• Naden: {ceiling['seam_length']}m\n"
        
        if ceiling['lights']:
            summary += f"• Verlichting: {len(ceiling['lights'])} type(s)\n"
        
        if ceiling['wood_structures']:
            summary += f"• Houtstructuren: {len(ceiling['wood_structures'])} type(s)\n"
        
        summary += f"\n💰 **Totaal: {format_price(costs.total)}**\n\n"
        summary += "Terugkeren naar bewerkingsmenu..."
        
        if is_callback:
            await message.edit_text(summary)
        else:
            await message.reply_text(summary)
        
        logger.info(f"✅ Ceiling '{ceiling['name']}' added to quote for user {user_id}")
        
        # Return to edit menu
        return await self.show_edit_menu_after_add(message, user_id)
    
    # ============== REMOVE CEILING METHODS ==============
    
    async def show_remove_ceiling_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Show menu to select which ceiling to remove"""
        query = update.callback_query
        await query.answer()
        user_id = query.from_user.id
        
        if user_id not in self.edit_sessions:
            await query.edit_message_text("❌ Bewerksessie verlopen.")
            return ConversationHandler.END
        
        session = self.edit_sessions[user_id]
        ceilings = session['quote'].get('ceilings', [])
        
        if not ceilings:
            await query.edit_message_text("❌ Geen plafonds om te verwijderen.")
            return await self.show_edit_menu(update, context)
        
        if len(ceilings) == 1:
            await query.edit_message_text(
                "⚠️ Je kunt het laatste plafond niet verwijderen.\n"
                "Een offerte moet minimaal één plafond bevatten."
            )
            return await self.show_edit_menu(update, context)
        
        keyboard = []
        for i, ceiling in enumerate(ceilings):
            name = ceiling.get('name', f'Plafond {i+1}')
            area = ceiling.get('area', 0)
            keyboard.append([
                InlineKeyboardButton(
                    f"🗑️ {name} ({area:.2f}m²)",
                    callback_data=f"confirm_remove_ceiling_{i}"
                )
            ])
        
        keyboard.append([
            InlineKeyboardButton("❌ Annuleren", callback_data="cancel_remove_ceiling")
        ])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(
            "🗑️ **Plafond Verwijderen**\n\n"
            "Selecteer het plafond dat je wilt verwijderen:",
            reply_markup=reply_markup
        )
        return self.EDIT_MENU
    
    async def confirm_remove_ceiling(self, update: Update, context: ContextTypes.DEFAULT_TYPE, ceiling_index: int) -> int:
        """Remove a ceiling from the quote"""
        query = update.callback_query
        await query.answer()
        user_id = query.from_user.id
        
        if user_id not in self.edit_sessions:
            await query.edit_message_text("❌ Bewerksessie verlopen.")
            return ConversationHandler.END
        
        session = self.edit_sessions[user_id]
        ceilings = session['quote'].get('ceilings', [])
        
        if ceiling_index < 0 or ceiling_index >= len(ceilings):
            await query.edit_message_text("❌ Ongeldige selectie.")
            return await self.show_edit_menu(update, context)
        
        removed_ceiling = ceilings.pop(ceiling_index)
        
        # Update current ceiling index if needed
        if session.get('current_ceiling_index', 0) >= len(ceilings):
            session['current_ceiling_index'] = max(0, len(ceilings) - 1)
        
        await query.edit_message_text(
            f"✅ **Plafond Verwijderd**\n\n"
            f"'{removed_ceiling.get('name', 'Plafond')}' is verwijderd.\n\n"
            "Terugkeren naar bewerkingsmenu..."
        )
        
        return await self.show_edit_menu(update, context)
    
    async def cancel_edit(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Handle cancel button during editing"""
        query = update.callback_query
        await query.answer()
        
        user_id = query.from_user.id
        
        if user_id in self.edit_sessions:
            del self.edit_sessions[user_id]
        
        await query.edit_message_text(
            "❌ Bewerken offerte geannuleerd.\n"
            "Je wijzigingen zijn niet opgeslagen."
        )
        
        return ConversationHandler.END
    
    async def handle_quote_action(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle quote action callbacks (PDF, Email) - outside conversation handler"""
        query = update.callback_query
        await query.answer()
        
        data = query.data
        user_id = query.from_user.id
        
        # Parse action and quote_id
        if data.startswith("quote_action_pdf_"):
            quote_id = int(data.replace("quote_action_pdf_", ""))
            await self._generate_and_send_pdf(query, context, quote_id, user_id)
        elif data.startswith("quote_action_email_"):
            quote_id = int(data.replace("quote_action_email_", ""))
            await self._send_quote_email(query, context, quote_id, user_id)
        elif data == "back_to_main":
            await query.edit_message_text(
                "🏠 **Hoofdmenu**\n\n"
                "Gebruik /start om terug te keren naar het hoofdmenu."
            )
    
    async def _generate_and_send_pdf(self, query, context: ContextTypes.DEFAULT_TYPE, quote_id: int, user_id: int) -> None:
        """Generate and send PDF for a quote"""
        try:
            # Show loading message
            await query.edit_message_text("⏳ PDF wordt gegenereerd...")
            
            # Get quote from database
            quote_record = self.db.get_quote_by_id(quote_id)
            if not quote_record:
                await query.edit_message_text("❌ Offerte niet gevonden.")
                return
            
            # Parse quote data
            quote_data = quote_record.get('quote_data')
            if isinstance(quote_data, str):
                import json
                quote_data = json.loads(quote_data)
            
            # Get user profile for client_group
            user_profile = self.db.get_user_profile(user_id)
            client_group = 'price_b2c'
            if user_profile:
                client_group = user_profile.get('client_group', 'price_b2c')
            
            # Generate PDF
            pdf_generator = context.application.bot_data.get('pdf_generator')
            if not pdf_generator:
                await query.edit_message_text("❌ PDF generator niet beschikbaar.")
                return
            
            pdf_path = pdf_generator.generate_quote(
                quote_data=quote_data,
                quote_number=quote_record.get('quote_number', ''),
                user_profile=user_profile,
                client_group=client_group
            )
            
            if pdf_path and os.path.exists(pdf_path):
                # Send PDF
                with open(pdf_path, 'rb') as pdf_file:
                    quote_number = quote_record.get('quote_number', 'offerte')
                    await context.bot.send_document(
                        chat_id=query.message.chat_id,
                        document=pdf_file,
                        filename=f"Offerte_{quote_number}.pdf",
                        caption=f"📄 **Offerte {quote_number}**\n\nHier is je offerte als PDF."
                    )
                
                # Clean up
                try:
                    os.remove(pdf_path)
                except:
                    pass
                
                # Show success with action buttons
                customer_name = quote_data.get('customer', {}).get('name', 'Klant') if quote_data.get('customer') else 'Klant'
                total_price = quote_record.get('total_price', 0)
                
                keyboard = [
                    [
                        InlineKeyboardButton("📧 Versturen naar Klant", callback_data=f"quote_action_email_{quote_id}")
                    ],
                    [
                        InlineKeyboardButton("✏️ Bewerken", callback_data=f"quote_edit_{quote_id}"),
                        InlineKeyboardButton("🏠 Hoofdmenu", callback_data="back_to_main")
                    ]
                ]
                
                await query.edit_message_text(
                    f"✅ **PDF Gegenereerd!**\n\n"
                    f"📋 Offerte: `{quote_record.get('quote_number', '')}`\n"
                    f"👤 Klant: {customer_name}\n"
                    f"💰 Totaal: **{format_price(total_price)}**",
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode='Markdown'
                )
            else:
                await query.edit_message_text("❌ Fout bij genereren PDF.")
                
        except Exception as e:
            logger.error(f"Error generating PDF: {e}")
            import traceback
            logger.error(traceback.format_exc())
            await query.edit_message_text(f"❌ Fout bij genereren PDF: {str(e)}")
    
    async def _send_quote_email(self, query, context: ContextTypes.DEFAULT_TYPE, quote_id: int, user_id: int) -> None:
        """Send quote by email to customer"""
        try:
            # Show loading message
            await query.edit_message_text("⏳ Offerte wordt verstuurd...")
            
            # Get quote from database
            quote_record = self.db.get_quote_by_id(quote_id)
            if not quote_record:
                await query.edit_message_text("❌ Offerte niet gevonden.")
                return
            
            # Parse quote data
            quote_data = quote_record.get('quote_data')
            if isinstance(quote_data, str):
                import json
                quote_data = json.loads(quote_data)
            
            # Get customer email
            customer = quote_data.get('customer', {})
            customer_email = customer.get('email', '') if customer else ''
            customer_name = customer.get('name', 'Klant') if customer else 'Klant'
            
            if not customer_email:
                # Ask for email
                keyboard = [
                    [InlineKeyboardButton("🔙 Terug", callback_data=f"quote_action_pdf_{quote_id}")]
                ]
                await query.edit_message_text(
                    "⚠️ **Geen e-mailadres gevonden**\n\n"
                    "De klant heeft geen e-mailadres in de offerte.\n"
                    "Voeg eerst een e-mailadres toe aan de klantgegevens.",
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode='Markdown'
                )
                return
            
            # Get user profile
            user_profile = self.db.get_user_profile(user_id)
            client_group = 'price_b2c'
            if user_profile:
                client_group = user_profile.get('client_group', 'price_b2c')
            
            # Generate PDF first
            pdf_generator = context.application.bot_data.get('pdf_generator')
            email_service = context.application.bot_data.get('email_service')
            
            if not pdf_generator or not email_service:
                await query.edit_message_text("❌ Email service niet beschikbaar.")
                return
            
            pdf_path = pdf_generator.generate_quote(
                quote_data=quote_data,
                quote_number=quote_record.get('quote_number', ''),
                user_profile=user_profile,
                client_group=client_group
            )
            
            if not pdf_path or not os.path.exists(pdf_path):
                await query.edit_message_text("❌ Fout bij genereren PDF voor email.")
                return
            
            # Send email
            quote_number = quote_record.get('quote_number', '')
            total_price = quote_record.get('total_price', 0)
            
            subject = f"Uw offerte {quote_number} van STRETCH"
            body = f"""Beste {customer_name},

Hierbij ontvangt u uw offerte {quote_number} van STRETCH.

Offerte details:
- Offertenummer: {quote_number}
- Totaalbedrag: {format_price(total_price)}

De offerte is bijgevoegd als PDF.

Met vriendelijke groet,
STRETCH Team

---
Dit bericht is automatisch verzonden via de STRETCH Offerte Assistant.
"""
            
            success = await email_service.send_email_with_attachment(
                to_email=customer_email,
                subject=subject,
                body=body,
                attachment_path=pdf_path,
                attachment_name=f"Offerte_{quote_number}.pdf"
            )
            
            # Clean up PDF
            try:
                os.remove(pdf_path)
            except:
                pass
            
            if success:
                # Update quote status to 'sent'
                self.db.update_quote_status(quote_id, 'sent', user_id)
                
                keyboard = [
                    [
                        InlineKeyboardButton("📄 PDF Downloaden", callback_data=f"quote_action_pdf_{quote_id}"),
                        InlineKeyboardButton("✏️ Bewerken", callback_data=f"quote_edit_{quote_id}")
                    ],
                    [
                        InlineKeyboardButton("🏠 Hoofdmenu", callback_data="back_to_main")
                    ]
                ]
                
                await query.edit_message_text(
                    f"✅ **Offerte Verstuurd!**\n\n"
                    f"📋 Offerte: `{quote_number}`\n"
                    f"📧 Verzonden naar: {customer_email}\n"
                    f"👤 Klant: {customer_name}\n"
                    f"💰 Totaal: **{format_price(total_price)}**\n\n"
                    f"✅ Status bijgewerkt naar 'Verstuurd'",
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode='Markdown'
                )
            else:
                await query.edit_message_text(
                    f"❌ **Fout bij verzenden email**\n\n"
                    f"De offerte kon niet worden verzonden naar {customer_email}.\n"
                    f"Controleer het e-mailadres en probeer het opnieuw."
                )
                
        except Exception as e:
            logger.error(f"Error sending quote email: {e}")
            import traceback
            logger.error(traceback.format_exc())
            await query.edit_message_text(f"❌ Fout bij verzenden: {str(e)}")