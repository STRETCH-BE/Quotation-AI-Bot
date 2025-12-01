# -*- coding: utf-8 -*-
"""
Email Sender Service for Stretch Ceiling Bot
Version 2.1 - Dutch Language - Matching STRETCH BV PDF branding
Fixed customer data priority: quote_data['customer'] > user_profile > fallback
"""
import logging
import aiohttp
import base64
from typing import Optional, Dict
from datetime import datetime, timedelta

from config import Config

logger = logging.getLogger(__name__)

class EntraIDEmailSender:
    """Entra ID (Azure AD) email sender using Microsoft Graph API"""
    
    # Brand colors matching PDF
    STRETCH_RED = "#E30613"
    STRETCH_BLACK = "#1a1a1a"
    STRETCH_DARK_GRAY = "#333333"
    STRETCH_GRAY = "#555555"
    STRETCH_LIGHT_GRAY = "#f8f8f8"
    STRETCH_WHITE = "#FFFFFF"
    
    # Company info matching PDF
    COMPANY_NAME = "STRETCH BV"
    COMPANY_ADDRESS = "Gentseweg 309 A3, 9120 Beveren-Waas, België"
    COMPANY_PHONE = "+32 3 284 68 18"
    COMPANY_EMAIL = "info@stretchgroup.be"
    COMPANY_WEBSITE = "www.stretchplafond.be"
    COMPANY_VAT = "BE0675875709"
    IBAN = "BE63001882761108"
    BIC = "GEBABEBB"
    
    def __init__(self):
        self.tenant_id = Config.AZURE_TENANT_ID
        self.client_id = Config.AZURE_CLIENT_ID
        self.client_secret = Config.AZURE_CLIENT_SECRET
        self.from_email = Config.EMAIL_FROM
        self.access_token = None
    
    async def send_quote_email(
        self, 
        recipient_email: str, 
        quote_number: str, 
        pdf_path: str, 
        quote_data: dict, 
        total_price: float,
        user_profile: dict = None
    ) -> bool:
        """Send quote email with user information"""
        try:
            # Get access token
            if not await self._get_access_token():
                logger.error("Failed to get access token")
                return False
            
            # Create email HTML with user information
            email_html = self.create_quote_email_html(
                quote_number, 
                quote_data, 
                total_price,
                user_profile
            )
            
            # Read PDF file
            with open(pdf_path, 'rb') as f:
                pdf_content = base64.b64encode(f.read()).decode()
            
            # Prepare email message (Dutch subject)
            message = {
                "message": {
                    "subject": f"Uw Spanplafond Offerte #{quote_number} - {self.COMPANY_NAME}",
                    "body": {
                        "contentType": "HTML",
                        "content": email_html
                    },
                    "toRecipients": [
                        {
                            "emailAddress": {
                                "address": recipient_email
                            }
                        }
                    ],
                    "ccRecipients": [
                        {
                            "emailAddress": {
                                "address": self.COMPANY_EMAIL
                            }
                        }
                    ],
                    "attachments": [
                        {
                            "@odata.type": "#microsoft.graph.fileAttachment",
                            "name": f"Offerte_{quote_number}.pdf",
                            "contentType": "application/pdf",
                            "contentBytes": pdf_content
                        }
                    ]
                },
                "saveToSentItems": "true"
            }
            
            # Send email via Microsoft Graph API
            async with aiohttp.ClientSession() as session:
                headers = {
                    'Authorization': f'Bearer {self.access_token}',
                    'Content-Type': 'application/json'
                }
                
                url = f"https://graph.microsoft.com/v1.0/users/{self.from_email}/sendMail"
                
                async with session.post(url, json=message, headers=headers) as response:
                    if response.status == 202:
                        logger.info(f"✅ E-mail succesvol verzonden naar {recipient_email}")
                        return True
                    else:
                        error_text = await response.text()
                        logger.error(f"❌ E-mail verzenden mislukt: {response.status} - {error_text}")
                        return False
                        
        except Exception as e:
            logger.error(f"❌ Fout bij verzenden e-mail: {e}")
            return False
    
    def create_quote_email_html(
        self, 
        quote_number: str, 
        quote_data: dict, 
        total_price: float,
        user_profile: dict = None
    ) -> str:
        """Create HTML email matching STRETCH BV PDF branding - Dutch version"""
        
        # Extract recipient information
        # PRIORITY: quote_data['customer'] > user_profile > fallback
        recipient_name = "Geachte klant"
        company_name = ""
        
        # PRIORITY 1: Check for customer data from Dynamics 365 first
        customer = quote_data.get('customer', {})
        if customer:
            logger.info(f"📧 E-mail gebruikt klantgegevens van Dynamics 365")
            if customer.get('company_name') or customer.get('display_name'):
                company_name = customer.get('company_name') or customer.get('display_name', '')
                recipient_name = customer.get('contact_name') or company_name
            else:
                recipient_name = customer.get('contact_name') or customer.get('display_name', 'Geachte klant')
        # PRIORITY 2: Fall back to user profile
        elif user_profile:
            logger.info(f"📧 E-mail gebruikt gebruikersprofiel")
            if user_profile.get('is_company'):
                company_name = user_profile.get('company_name', '')
                recipient_name = f"{user_profile.get('first_name', '')} {user_profile.get('last_name', '')}".strip() or company_name
            else:
                recipient_name = f"{user_profile.get('first_name', '')} {user_profile.get('last_name', '')}".strip() or 'Geachte klant'
        else:
            logger.info(f"📧 E-mail gebruikt fallback gegevens")
        
        # Determine if B2B or B2C
        is_b2b = quote_data.get('client_group', '').startswith('price_b2b')
        
        # Build ceiling details HTML (Dutch)
        ceilings_html = ""
        ceilings = quote_data.get('ceilings', [])
        ceiling_costs = quote_data.get('ceiling_costs', [])
        
        for i, ceiling in enumerate(ceilings):
            ceiling_name = ceiling.get('name', f'Plafond {i+1}')
            
            # Get cost from ceiling_costs array - convert to float to handle Decimal
            subtotal = 0.0
            if i < len(ceiling_costs):
                costs = ceiling_costs[i]
                subtotal = float(costs.get('total', 0)) if isinstance(costs, dict) else 0.0
            
            # Determine material type display (Dutch)
            material_type = ceiling.get('material_type', ceiling.get('ceiling_type', 'STOF')).upper()
            material_subtype = ceiling.get('material_subtype', ceiling.get('type_ceiling', 'Standaard'))
            if ceiling.get('acoustic') or ceiling.get('is_acoustic'):
                material_subtype = 'Akoestisch'
            
            color = ceiling.get('color', 'Wit')
            if isinstance(color, str):
                color = color.capitalize()
            
            ceilings_html += f"""
            <tr>
                <td style="padding: 15px; border-bottom: 1px solid #e0e0e0;">
                    <strong style="color: {self.STRETCH_BLACK};">{ceiling_name}</strong><br/>
                    <span style="color: {self.STRETCH_GRAY}; font-size: 13px;">
                        {ceiling.get('length', 0)}m × {ceiling.get('width', 0)}m = {float(ceiling.get('area', 0)):.2f} m²<br/>
                        {material_type} - {material_subtype} - {color}
                    </span>
                </td>
                <td style="padding: 15px; border-bottom: 1px solid #e0e0e0; text-align: right; font-weight: bold; color: {self.STRETCH_BLACK};">
                    €{subtotal:,.2f}
                </td>
            </tr>
            """
        
        # Calculate VAT - convert to float to avoid Decimal multiplication issues
        total_price_float = float(total_price) if total_price else 0.0
        vat_amount = total_price_float * 0.21
        total_incl_vat = total_price_float + vat_amount  # Always add VAT for total
        
        # Valid until date
        valid_until = (datetime.now() + timedelta(days=30)).strftime('%d/%m/%Y')
        today = datetime.now().strftime('%d/%m/%Y')
        
        # Create complete HTML email with STRETCH branding (Dutch)
        html = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Uw Spanplafond Offerte - {self.COMPANY_NAME}</title>
</head>
<body style="font-family: 'Segoe UI', Arial, sans-serif; line-height: 1.6; color: {self.STRETCH_DARK_GRAY}; margin: 0; padding: 0; background-color: #f4f4f4;">
    
    <!-- Main Container -->
    <table width="100%" cellpadding="0" cellspacing="0" style="background-color: #f4f4f4;">
        <tr>
            <td align="center" style="padding: 20px;">
                <table width="600" cellpadding="0" cellspacing="0" style="background-color: {self.STRETCH_WHITE}; border-radius: 8px; overflow: hidden; box-shadow: 0 2px 10px rgba(0,0,0,0.1);">
                    
                    <!-- Red Header Banner -->
                    <tr>
                        <td style="background-color: {self.STRETCH_RED}; padding: 30px; text-align: center;">
                            <h1 style="color: {self.STRETCH_WHITE}; margin: 0; font-size: 28px; font-weight: bold; letter-spacing: 1px;">
                                {self.COMPANY_NAME}
                            </h1>
                            <p style="color: {self.STRETCH_WHITE}; margin: 8px 0 0 0; font-size: 14px; opacity: 0.9;">
                                Professionele Spanplafond Oplossingen
                            </p>
                        </td>
                    </tr>
                    
                    <!-- Quote Badge -->
                    <tr>
                        <td style="padding: 30px 40px 20px 40px;">
                            <table width="100%" cellpadding="0" cellspacing="0">
                                <tr>
                                    <td>
                                        <span style="background-color: {self.STRETCH_RED}; color: {self.STRETCH_WHITE}; padding: 8px 16px; border-radius: 4px; font-size: 12px; font-weight: bold; text-transform: uppercase;">
                                            Offerte
                                        </span>
                                    </td>
                                    <td style="text-align: right;">
                                        <span style="color: {self.STRETCH_GRAY}; font-size: 13px;">#{quote_number}</span>
                                    </td>
                                </tr>
                            </table>
                        </td>
                    </tr>
                    
                    <!-- Greeting -->
                    <tr>
                        <td style="padding: 0 40px 20px 40px;">
                            <p style="font-size: 16px; margin: 0 0 15px 0;">
                                Beste <strong>{recipient_name}</strong>{f' ({company_name})' if company_name and company_name != recipient_name else ''},
                            </p>
                            <p style="margin: 0; color: {self.STRETCH_GRAY};">
                                Hartelijk dank voor uw interesse in {self.COMPANY_NAME}! Hieronder vindt u uw persoonlijke offerte voor spanplafond installatie.
                            </p>
                        </td>
                    </tr>
                    
                    <!-- Quote Info Box -->
                    <tr>
                        <td style="padding: 0 40px 25px 40px;">
                            <table width="100%" cellpadding="0" cellspacing="0" style="background-color: {self.STRETCH_LIGHT_GRAY}; border-radius: 6px; border: 1px solid #e0e0e0;">
                                <tr>
                                    <td style="padding: 15px 20px; border-right: 1px solid #e0e0e0;" width="50%">
                                        <span style="color: {self.STRETCH_GRAY}; font-size: 12px; text-transform: uppercase;">Offertedatum</span><br/>
                                        <strong style="color: {self.STRETCH_BLACK};">{today}</strong>
                                    </td>
                                    <td style="padding: 15px 20px;" width="50%">
                                        <span style="color: {self.STRETCH_GRAY}; font-size: 12px; text-transform: uppercase;">Geldig tot</span><br/>
                                        <strong style="color: {self.STRETCH_BLACK};">{valid_until}</strong>
                                    </td>
                                </tr>
                                {f'''<tr>
                                    <td colspan="2" style="padding: 15px 20px; border-top: 1px solid #e0e0e0;">
                                        <span style="color: {self.STRETCH_GRAY}; font-size: 12px; text-transform: uppercase;">Referentie</span><br/>
                                        <strong style="color: {self.STRETCH_BLACK};">{quote_data.get("quote_reference")}</strong>
                                    </td>
                                </tr>''' if quote_data.get('quote_reference') else ''}
                            </table>
                        </td>
                    </tr>
                    
                    <!-- Ceiling Details Section -->
                    <tr>
                        <td style="padding: 0 40px 25px 40px;">
                            <h2 style="color: {self.STRETCH_RED}; font-size: 16px; margin: 0 0 15px 0; border-bottom: 2px solid {self.STRETCH_RED}; padding-bottom: 8px;">
                                Offerte Details
                            </h2>
                            <table width="100%" cellpadding="0" cellspacing="0" style="border: 1px solid #e0e0e0; border-radius: 6px; overflow: hidden;">
                                <tr style="background-color: {self.STRETCH_BLACK};">
                                    <td style="padding: 12px 15px; color: {self.STRETCH_WHITE}; font-weight: bold; font-size: 13px;">
                                        Omschrijving
                                    </td>
                                    <td style="padding: 12px 15px; color: {self.STRETCH_WHITE}; font-weight: bold; font-size: 13px; text-align: right;">
                                        Bedrag
                                    </td>
                                </tr>
                                {ceilings_html}
                            </table>
                        </td>
                    </tr>
                    
                    <!-- Totals Section -->
                    <tr>
                        <td style="padding: 0 40px 30px 40px;">
                            <table width="100%" cellpadding="0" cellspacing="0">
                                <tr>
                                    <td width="60%"></td>
                                    <td width="40%">
                                        <table width="100%" cellpadding="0" cellspacing="0">
                                            <tr>
                                                <td style="padding: 8px 0; color: {self.STRETCH_GRAY};">Subtotaal (excl. BTW)</td>
                                                <td style="padding: 8px 0; text-align: right;">€{total_price_float:,.2f}</td>
                                            </tr>
                                            <tr>
                                                <td style="padding: 8px 0; color: {self.STRETCH_GRAY};">BTW (21%)</td>
                                                <td style="padding: 8px 0; text-align: right;">€{vat_amount:,.2f}</td>
                                            </tr>
                                            <tr>
                                                <td colspan="2" style="padding-top: 10px;">
                                                    <table width="100%" cellpadding="0" cellspacing="0" style="background-color: {self.STRETCH_RED}; border-radius: 4px;">
                                                        <tr>
                                                            <td style="padding: 12px 15px; color: {self.STRETCH_WHITE}; font-weight: bold;">
                                                                TOTAAL (incl. BTW)
                                                            </td>
                                                            <td style="padding: 12px 15px; color: {self.STRETCH_WHITE}; font-weight: bold; text-align: right; font-size: 18px;">
                                                                €{total_incl_vat:,.2f}
                                                            </td>
                                                        </tr>
                                                    </table>
                                                </td>
                                            </tr>
                                        </table>
                                    </td>
                                </tr>
                            </table>
                        </td>
                    </tr>
                    
                    <!-- Why Choose Us -->
                    <tr>
                        <td style="padding: 0 40px 30px 40px;">
                            <h2 style="color: {self.STRETCH_RED}; font-size: 16px; margin: 0 0 15px 0;">
                                Waarom {self.COMPANY_NAME}?
                            </h2>
                            <table width="100%" cellpadding="0" cellspacing="0">
                                <tr>
                                    <td style="padding: 8px 0; color: {self.STRETCH_DARK_GRAY};">
                                        <span style="color: {self.STRETCH_RED};">✓</span> 10 jaar garantie op alle materialen
                                    </td>
                                </tr>
                                <tr>
                                    <td style="padding: 8px 0; color: {self.STRETCH_DARK_GRAY};">
                                        <span style="color: {self.STRETCH_RED};">✓</span> Professionele installatie door gecertificeerde technici
                                    </td>
                                </tr>
                                <tr>
                                    <td style="padding: 8px 0; color: {self.STRETCH_DARK_GRAY};">
                                        <span style="color: {self.STRETCH_RED};">✓</span> Breed assortiment kleuren en afwerkingen
                                    </td>
                                </tr>
                                <tr>
                                    <td style="padding: 8px 0; color: {self.STRETCH_DARK_GRAY};">
                                        <span style="color: {self.STRETCH_RED};">✓</span> Vochtbestendig en onderhoudsvriendelijk
                                    </td>
                                </tr>
                                <tr>
                                    <td style="padding: 8px 0; color: {self.STRETCH_DARK_GRAY};">
                                        <span style="color: {self.STRETCH_RED};">✓</span> {'Volumekortingen voor B2B klanten' if is_b2b else 'Concurrerende prijzen'}
                                    </td>
                                </tr>
                            </table>
                        </td>
                    </tr>
                    
                    <!-- Next Steps -->
                    <tr>
                        <td style="padding: 0 40px 30px 40px;">
                            <table width="100%" cellpadding="0" cellspacing="0" style="background-color: {self.STRETCH_LIGHT_GRAY}; border-radius: 6px; border-left: 4px solid {self.STRETCH_RED};">
                                <tr>
                                    <td style="padding: 20px;">
                                        <h3 style="color: {self.STRETCH_BLACK}; margin: 0 0 12px 0; font-size: 14px;">
                                            Volgende Stappen
                                        </h3>
                                        <ol style="margin: 0; padding-left: 20px; color: {self.STRETCH_DARK_GRAY}; font-size: 14px;">
                                            <li style="margin-bottom: 8px;">Bekijk de bijgevoegde PDF voor volledige details</li>
                                            <li style="margin-bottom: 8px;">Neem contact met ons op bij vragen</li>
                                            <li style="margin-bottom: 0;">Bevestig uw bestelling om de installatie in te plannen</li>
                                        </ol>
                                    </td>
                                </tr>
                            </table>
                        </td>
                    </tr>
                    
                    <!-- Contact Section -->
                    <tr>
                        <td style="padding: 0 40px 30px 40px;">
                            <table width="100%" cellpadding="0" cellspacing="0" style="background-color: {self.STRETCH_BLACK}; border-radius: 6px;">
                                <tr>
                                    <td style="padding: 25px; text-align: center;">
                                        <h3 style="color: {self.STRETCH_WHITE}; margin: 0 0 15px 0; font-size: 16px;">
                                            Vragen? Neem contact op
                                        </h3>
                                        <p style="margin: 8px 0; color: {self.STRETCH_WHITE}; font-size: 14px;">
                                            📞 <a href="tel:{self.COMPANY_PHONE}" style="color: {self.STRETCH_WHITE}; text-decoration: none;">{self.COMPANY_PHONE}</a>
                                        </p>
                                        <p style="margin: 8px 0; color: {self.STRETCH_WHITE}; font-size: 14px;">
                                            📧 <a href="mailto:{self.COMPANY_EMAIL}" style="color: {self.STRETCH_WHITE}; text-decoration: none;">{self.COMPANY_EMAIL}</a>
                                        </p>
                                        <p style="margin: 8px 0; color: {self.STRETCH_WHITE}; font-size: 14px;">
                                            🌐 <a href="https://{self.COMPANY_WEBSITE}" style="color: {self.STRETCH_WHITE}; text-decoration: none;">{self.COMPANY_WEBSITE}</a>
                                        </p>
                                    </td>
                                </tr>
                            </table>
                        </td>
                    </tr>
                    
                    <!-- Footer -->
                    <tr>
                        <td style="background-color: {self.STRETCH_LIGHT_GRAY}; padding: 20px 40px; border-top: 2px solid {self.STRETCH_RED};">
                            <table width="100%" cellpadding="0" cellspacing="0">
                                <tr>
                                    <td style="text-align: center;">
                                        <p style="margin: 0 0 8px 0; color: {self.STRETCH_DARK_GRAY}; font-size: 12px; font-weight: bold;">
                                            {self.COMPANY_NAME}
                                        </p>
                                        <p style="margin: 0 0 5px 0; color: {self.STRETCH_GRAY}; font-size: 11px;">
                                            {self.COMPANY_ADDRESS}
                                        </p>
                                        <p style="margin: 0 0 5px 0; color: {self.STRETCH_GRAY}; font-size: 11px;">
                                            BTW: {self.COMPANY_VAT} • IBAN: {self.IBAN} • BIC: {self.BIC}
                                        </p>
                                        <p style="margin: 15px 0 0 0; color: {self.STRETCH_GRAY}; font-size: 11px;">
                                            Deze offerte is 30 dagen geldig vanaf de uitgiftedatum.
                                        </p>
                                    </td>
                                </tr>
                            </table>
                        </td>
                    </tr>
                    
                </table>
            </td>
        </tr>
    </table>
    
</body>
</html>
"""
        
        return html
    
    async def _get_access_token(self) -> bool:
        """Get access token from Azure AD"""
        try:
            url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
            
            data = {
                'client_id': self.client_id,
                'client_secret': self.client_secret,
                'scope': 'https://graph.microsoft.com/.default',
                'grant_type': 'client_credentials'
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.post(url, data=data) as response:
                    if response.status == 200:
                        result = await response.json()
                        self.access_token = result['access_token']
                        return True
                    else:
                        error_text = await response.text()
                        logger.error(f"Failed to get access token: {error_text}")
                        return False
                        
        except Exception as e:
            logger.error(f"Error getting access token: {e}")
            return False
    
    def test_connection(self) -> bool:
        """Test the Entra ID connection synchronously"""
        import asyncio
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        result = loop.run_until_complete(self._test_connection_async())
        loop.close()
        return result
    
    async def test_connection_async(self) -> bool:
        """Test the Entra ID connection asynchronously"""
        return await self._get_access_token()
    
    async def _test_connection_async(self) -> bool:
        """Test the Entra ID connection"""
        return await self._get_access_token()