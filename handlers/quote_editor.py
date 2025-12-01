"""
Quote Editor Handler for Stretch Ceiling Bot
Complete implementation for viewing and editing quotes
ENHANCED: Dynamics 365 integration for quote synchronization
Version 2.0 - Fixed customer data priority for PDF/email generation
"""
import logging
import json
import asyncio
from datetime import datetime
from typing import Dict, Optional, List
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes, ConversationHandler

from config import Config
from utils import format_price, escape_markdown
from models import CeilingConfig

logger = logging.getLogger(__name__)

class QuoteEditor:
    """Handles quote viewing and editing with conversation handler support and Dynamics 365 sync"""
    
    # Conversation states
    EDIT_MENU = 1
    EDIT_DIMENSIONS = 2
    EDIT_PERIMETER = 3
    EDIT_COLOR = 4
    EDIT_CORNERS = 5
    
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
        
        structured_quote = {
            'id': quote.get('quotation_id', quote_id),
            'quote_number': quote.get('quote_number'),
            'status': quote.get('status', 'draft'),
            'total_price': quote.get('total_price', 0),
            'ceilings': ceilings,
            'ceiling_costs': ceiling_costs,
            'quote_reference': quote_data.get('quote_reference', ''),
            'customer': quote_data.get('customer'),  # Include customer data
        }
        
        self.edit_sessions[user_id] = {
            'quote': structured_quote,
            'original_quote': json.loads(json.dumps(structured_quote)),
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
            # Recalculate costs
            user_profile = self.db.get_user_profile(user_id)
            client_group = user_profile.get('client_group', 'price_b2c') if user_profile else 'price_b2c'
            
            new_ceiling_costs = []
            total_price = 0
            
            for ceiling in quote.get('ceilings', []):
                config = CeilingConfig(
                    length=ceiling.get('length', 0),
                    width=ceiling.get('width', 0),
                    perimeter=ceiling.get('perimeter', 0),
                    corners=ceiling.get('corners', 4),
                    ceiling_type=ceiling.get('ceiling_type', 'mat'),
                    color=ceiling.get('color', 'wit'),
                    spots=ceiling.get('spots', 0),
                    is_acoustic=ceiling.get('is_acoustic', False),
                    is_backlit=ceiling.get('is_backlit', False)
                )
                
                costs = self.calculator.calculate_detailed_ceiling_cost(config, client_group)
                new_ceiling_costs.append(costs)
                total_price += costs.get('total', 0)
            
            quote['ceiling_costs'] = new_ceiling_costs
            quote['total_price'] = total_price
            
            # Save to database
            quote_data = {
                'ceilings': quote.get('ceilings', []),
                'ceiling_costs': new_ceiling_costs,
                'quote_reference': quote.get('quote_reference', ''),
                'customer': quote.get('customer'),  # Preserve customer data
            }
            
            success = self.db.update_quote(
                quote_id=quote['id'],
                user_id=user_id,
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
                
                await query.edit_message_text(
                    f"✅ **Offerte Opgeslagen!**\n\n"
                    f"Offerte #{quote.get('quote_number')}\n"
                    f"Nieuw totaal: {format_price(total_price)}"
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