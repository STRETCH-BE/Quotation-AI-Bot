# -*- coding: utf-8 -*-
"""
PDF Generator for Stretch Ceiling Bot with User Profile Integration
Version 3.0 - Dutch Language Version - Fixed customer data priority
"""
import os
import logging
import json
from datetime import datetime, timedelta
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter, A4
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image, PageBreak, KeepTogether
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch, cm, mm
from reportlab.lib.enums import TA_CENTER, TA_RIGHT, TA_LEFT, TA_JUSTIFY
from reportlab.pdfgen import canvas
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.lib.colors import HexColor
from reportlab.platypus import PageTemplate, Frame
from io import BytesIO

logger = logging.getLogger(__name__)

class ImprovedStretchQuotePDFGenerator:
    """Generates professional PDF quotes with STRETCH branding and user profile integration - Dutch version"""
    
    # Brand colors
    STRETCH_RED = HexColor("#E30613")
    STRETCH_DARK_RED = HexColor("#B8050F")
    STRETCH_BLACK = HexColor("#1a1a1a")
    STRETCH_WHITE = HexColor("#FFFFFF")
    STRETCH_GRAY = HexColor("#555555")
    STRETCH_LIGHT_GRAY = HexColor("#f8f8f8")
    STRETCH_MEDIUM_GRAY = HexColor("#e0e0e0")
    STRETCH_DARK_GRAY = HexColor("#333333")
    
    # Fixed company data
    COMPANY_NAME = "STRETCH BV"
    COMPANY_ADDRESS = "Gentseweg 309 A3, 9120 Beveren-Waas, België"
    COMPANY_PHONE = "+32 3 284 68 18"
    COMPANY_EMAIL = "info@stretchgroup.be"
    COMPANY_WEBSITE = "www.stretchplafond.be"
    COMPANY_VAT = "BE0675875709"
    
    # Bank details
    BANK_ACCOUNT = "001-8827611-08"
    IBAN = "BE63001882761108"
    BIC = "GEBABEBB"
    TERMS_VALIDITY_DAYS = 30
    
    def __init__(self, output_dir: str = "quotes", logo_path: str = None):
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)
        self.logo_path = logo_path
        
        # Register DejaVu fonts for Unicode support
        self._register_fonts()
        
        self.styles = getSampleStyleSheet()
        self.setup_styles()
    
    def _register_fonts(self):
        """Register DejaVu fonts for Unicode character support"""
        font_paths = [
            '/usr/share/fonts/truetype/dejavu/',
            '/usr/share/fonts/TTF/',
            '/usr/share/fonts/dejavu/',
            '/usr/local/share/fonts/truetype/dejavu/',
        ]
        
        dejavu_path = None
        for path in font_paths:
            test_file = os.path.join(path, 'DejaVuSans.ttf')
            if os.path.exists(test_file):
                dejavu_path = path
                logger.info(f"✅ DejaVu fonts gevonden: {path}")
                break
        
        if dejavu_path:
            try:
                regular_path = os.path.join(dejavu_path, 'DejaVuSans.ttf')
                bold_path = os.path.join(dejavu_path, 'DejaVuSans-Bold.ttf')
                
                if not os.path.isfile(regular_path):
                    raise FileNotFoundError(f"Regular font niet gevonden: {regular_path}")
                if not os.path.isfile(bold_path):
                    raise FileNotFoundError(f"Bold font niet gevonden: {bold_path}")
                
                with open(regular_path, 'rb') as f:
                    f.read(100)
                with open(bold_path, 'rb') as f:
                    f.read(100)
                
                pdfmetrics.registerFont(TTFont('DejaVuSans', regular_path))
                pdfmetrics.registerFont(TTFont('DejaVuSans-Bold', bold_path))
                
                from reportlab.pdfbase.pdfmetrics import registerFontFamily
                registerFontFamily('DejaVuSans',
                    normal='DejaVuSans',
                    bold='DejaVuSans-Bold',
                    italic='DejaVuSans',
                    boldItalic='DejaVuSans-Bold')
                
                self.default_font = 'DejaVuSans'
                self.bold_font = 'DejaVuSans-Bold'
                logger.info("✅ DejaVu font family geregistreerd voor Unicode ondersteuning")
                return
            except Exception as e:
                logger.error(f"❌ Fout bij registreren DejaVu fonts: {e}")
                import traceback
                logger.error(traceback.format_exc())
        else:
            logger.warning(f"⚠️ DejaVu fonts niet gevonden in: {font_paths}")
        
        logger.warning("⚠️ Helvetica fallback wordt gebruikt")
        self.default_font = 'Helvetica'
        self.bold_font = 'Helvetica-Bold'
    
    def setup_styles(self):
        """Setup custom PDF styles matching STRETCH branding"""
        # Quote title - large, bold, red
        self.styles.add(
            ParagraphStyle(
                name="QuoteTitle",
                parent=self.styles["Title"],
                fontSize=24,
                textColor=self.STRETCH_RED,
                alignment=TA_LEFT,
                spaceAfter=15,
                spaceBefore=5,
                fontName=self.bold_font,
            )
        )
        
        # Section headers - red, bold
        self.styles.add(
            ParagraphStyle(
                name="SectionHeader",
                parent=self.styles["Heading2"],
                fontSize=13,
                textColor=self.STRETCH_RED,
                spaceAfter=8,
                spaceBefore=12,
                fontName=self.bold_font,
            )
        )
        
        # Normal text - left aligned
        self.styles.add(
            ParagraphStyle(
                name="NormalLeft",
                parent=self.styles["Normal"],
                fontSize=9,
                textColor=self.STRETCH_DARK_GRAY,
                leading=13,
                alignment=TA_LEFT,
                fontName=self.default_font,
            )
        )
        
        # Company info - slightly smaller
        self.styles.add(
            ParagraphStyle(
                name="CompanyInfo",
                parent=self.styles["Normal"],
                fontSize=9,
                textColor=self.STRETCH_DARK_GRAY,
                leading=12,
                alignment=TA_LEFT,
                fontName=self.default_font,
            )
        )
        
        # Client info - standard size
        self.styles.add(
            ParagraphStyle(
                name="ClientInfo",
                parent=self.styles["Normal"],
                fontSize=9,
                textColor=self.STRETCH_DARK_GRAY,
                leading=12,
                alignment=TA_LEFT,
                fontName=self.default_font,
            )
        )
        
        # Table header - white on dark
        self.styles.add(
            ParagraphStyle(
                name="TableHeader",
                parent=self.styles["Normal"],
                fontSize=9,
                textColor=self.STRETCH_WHITE,
                alignment=TA_CENTER,
                fontName=self.bold_font,
            )
        )
        
        # Table cell - readable size
        self.styles.add(
            ParagraphStyle(
                name="TableCell",
                parent=self.styles["Normal"],
                fontSize=9,
                textColor=self.STRETCH_DARK_GRAY,
                alignment=TA_LEFT,
                leading=11,
                fontName=self.default_font,
            )
        )
        
        # Table cell description - slightly smaller for long text
        self.styles.add(
            ParagraphStyle(
                name="TableCellDesc",
                parent=self.styles["Normal"],
                fontSize=8,
                textColor=self.STRETCH_DARK_GRAY,
                alignment=TA_LEFT,
                leading=10,
                fontName=self.default_font,
            )
        )
        
        # Footer style
        self.styles.add(
            ParagraphStyle(
                name="Footer",
                parent=self.styles["Normal"],
                fontSize=8,
                textColor=self.STRETCH_GRAY,
                alignment=TA_CENTER,
                leading=10,
                fontName=self.default_font,
            )
        )
        
        # Terms text - smaller
        self.styles.add(
            ParagraphStyle(
                name="TermsText",
                parent=self.styles["Normal"],
                fontSize=8,
                textColor=self.STRETCH_GRAY,
                alignment=TA_LEFT,
                leading=11,
                fontName=self.default_font,
            )
        )
        
        # Terms header - bold
        self.styles.add(
            ParagraphStyle(
                name="TermsHeader",
                parent=self.styles["Normal"],
                fontSize=9,
                textColor=self.STRETCH_DARK_GRAY,
                alignment=TA_CENTER,
                fontName=self.bold_font,
                spaceBefore=10,
                spaceAfter=5,
            )
        )
    
    def add_page_template(self, canvas, doc):
        """Draws branded header and footer on each page - Dutch version"""
        width, height = A4
        canvas.saveState()
        
        # Header background - red banner
        header_height = 60
        canvas.setFillColor(self.STRETCH_RED)
        canvas.rect(0, height - header_height, width, header_height, fill=True, stroke=False)
        
        # Header text - WHITE on red
        canvas.setFillColor(self.STRETCH_WHITE)
        canvas.setFont(self.bold_font, 24)
        canvas.drawCentredString(width / 2, height - 32, self.COMPANY_NAME.upper())
        canvas.setFont(self.default_font, 10)
        canvas.drawCentredString(width / 2, height - 48, "Professionele Spanplafond Oplossingen")
        
        # Footer - subtle gray background
        footer_height = 45
        canvas.setFillColor(self.STRETCH_LIGHT_GRAY)
        canvas.rect(0, 0, width, footer_height, fill=True, stroke=False)
        
        # Red accent line at top of footer
        canvas.setStrokeColor(self.STRETCH_RED)
        canvas.setLineWidth(2)
        canvas.line(0, footer_height, width, footer_height)
        
        # Footer text - dark gray for readability
        canvas.setFillColor(self.STRETCH_DARK_GRAY)
        canvas.setFont(self.default_font, 7)
        y = 32
        canvas.drawCentredString(width / 2, y, f"{self.COMPANY_NAME} • {self.COMPANY_ADDRESS}")
        canvas.drawCentredString(width / 2, y - 10, f"Tel: {self.COMPANY_PHONE} • E-mail: {self.COMPANY_EMAIL} • {self.COMPANY_WEBSITE}")
        canvas.drawCentredString(width / 2, y - 20, f"BTW: {self.COMPANY_VAT} • IBAN: {self.IBAN} • BIC: {self.BIC}")
        
        # Page number - right aligned (Dutch)
        canvas.setFont(self.default_font, 8)
        canvas.setFillColor(self.STRETCH_GRAY)
        canvas.drawRightString(width - 25, 8, f"Pagina {doc.page}")
        
        canvas.restoreState()
    
    def build_pdf(self, quote_number: str, quote_data: dict, user_profile: dict = None) -> str:
        """Build PDF with user profile integration"""
        filename = f"offerte_{quote_number}_{datetime.now():%Y%m%d_%H%M%S}.pdf"
        filepath = os.path.join(self.output_dir, filename)
        
        doc = SimpleDocTemplate(
            filepath,
            pagesize=A4,
            leftMargin=20 * mm,
            rightMargin=20 * mm,
            topMargin=70,
            bottomMargin=55
        )
        
        elements = []
        
        # Prepare client data - PRIORITY: customer > user_profile > fallback
        client_data = self._prepare_client_data(quote_data, user_profile)
        
        # Header with company and client info
        elements.append(self._make_header(client_data))
        elements.append(Spacer(1, 20))
        
        # Quote title (Dutch)
        elements.append(Paragraph(f"OFFERTE #{quote_number}", self.styles["QuoteTitle"]))
        
        # Quote metadata table
        quote_date = datetime.now()
        validity_days = self.TERMS_VALIDITY_DAYS
        reference = quote_data.get("quote_reference", "")
        project_name = quote_data.get("project_name", "")
        
        elements.extend(self._quote_info_table(quote_number, quote_date, validity_days, reference, project_name))
        elements.append(Spacer(1, 20))
        
        # Convert ceiling data to items
        items = self._convert_ceilings_to_items(quote_data)
        
        # Items section (Dutch)
        elements.append(Paragraph("Offerte Details", self.styles["SectionHeader"]))
        elements.append(self._items_table(items))
        elements.append(Spacer(1, 15))
        
        # Totals section
        elements.append(self._totals_table(items))
        elements.append(Spacer(1, 20))
        
        # Terms and conditions
        elements.extend(self._terms_section(validity_days, user_profile))
        
        # Build with custom page template
        doc.build(elements, onFirstPage=self.add_page_template, onLaterPages=self.add_page_template)
        
        logger.info(f"✅ PDF offerte gegenereerd: {filepath}")
        return filepath
    
    def _prepare_client_data(self, quote_data: dict, user_profile: dict = None):
        """
        Prepare client data with priority: customer selection > user_profile > quote data
        
        PRIORITY ORDER:
        1. quote_data['customer'] - Customer selected during quote flow (Dynamics 365)
        2. user_profile - Logged-in user's profile (fallback)
        3. quote_data - Basic quote info (last resort)
        """
        client = {
            "name": "",
            "company": "",
            "address": "",
            "postal_code": "",
            "city": "",
            "country": "",
            "phone": "",
            "email": "",
            "vat_number": ""
        }
        
        # PRIORITY 1: Customer selection data from Dynamics 365
        if quote_data.get("customer"):
            customer = quote_data["customer"]
            logger.info(f"📋 PDF gebruikt klantgegevens van Dynamics 365: {customer.get('display_name', 'Onbekend')}")
            
            company_name = customer.get("company_name") or customer.get("display_name", "")
            contact_name = customer.get("contact_name", "")
            
            # Check if B2B (has account ID or is_company flag)
            if customer.get("dynamics_account_id") or customer.get("is_company"):
                client["company"] = company_name
                client["name"] = contact_name
            else:
                # B2C - contact only
                client["name"] = contact_name or company_name
            
            client["email"] = customer.get("email", "")
            client["phone"] = customer.get("phone", "")
            client["address"] = customer.get("address", "")
            client["vat_number"] = customer.get("vat_number", customer.get("vat", ""))
            
            logger.info(f"📋 PDF klant - bedrijf: {client['company']}, naam: {client['name']}")
        
        # PRIORITY 2: User profile (fallback)
        elif user_profile:
            logger.info(f"📋 PDF gebruikt gebruikersprofiel: {user_profile.get('first_name', '')} {user_profile.get('last_name', '')}")
            client["name"] = f"{user_profile.get('first_name', '')} {user_profile.get('last_name', '')}".strip()
            client["email"] = user_profile.get('email', '')
            client["phone"] = user_profile.get('phone', '')
            client["address"] = user_profile.get('address', '')
            
            if user_profile.get('is_company'):
                client["company"] = user_profile.get('company_name', '')
                client["vat_number"] = user_profile.get('vat_number', '')
        
        # PRIORITY 3: Fallback to basic quote data
        else:
            logger.info(f"📋 PDF gebruikt fallback gegevens")
            client["name"] = quote_data.get("quote_reference", "Klant")
            client["email"] = quote_data.get("email", "")
        
        return client
    
    def _make_header(self, client: dict):
        """Create header with company and client information - Dutch version"""
        # Logo handling
        logo_element = None
        possible_logo_paths = [
            self.logo_path,
            "/home/STRETCH/stretch_logo.png",
            "/home/STRETCH/STRETCH_NEW/stretch_logo.png",
            "stretch_logo.png"
        ]
        
        for path in possible_logo_paths:
            if path and os.path.exists(path):
                try:
                    logo = Image(path)
                    aspect = logo.imageWidth / float(logo.imageHeight)
                    logo_width = 4 * cm
                    logo_height = logo_width / aspect
                    if logo_height > 2.2 * cm:
                        logo_height = 2.2 * cm
                        logo_width = logo_height * aspect
                    logo.drawWidth = logo_width
                    logo.drawHeight = logo_height
                    logo.hAlign = "LEFT"
                    logo_element = logo
                    break
                except Exception as e:
                    logger.warning(f"⚠️ Kon logo niet laden: {e}")
        
        if not logo_element:
            logo_element = Paragraph(
                f'<font face="{self.bold_font}" size="14">STR<font color="#E30613">≡</font>TCH</font><br/>'
                f'<font face="{self.default_font}" size="7" color="#666666">PLAFONDS &amp; WANDEN</font>',
                self.styles["CompanyInfo"]
            )
        
        # Company info column
        f = self.default_font
        fb = self.bold_font
        comp_text = (
            f"<font face='{fb}' size='10'>{self.COMPANY_NAME}</font><br/>"
            f"<font face='{f}' size='8'>{self.COMPANY_ADDRESS}</font><br/>"
            f"<font face='{f}' size='8'>BTW: {self.COMPANY_VAT}</font><br/>"
            f"<font face='{f}' size='8'>Tel: {self.COMPANY_PHONE}</font><br/>"
            f"<font face='{f}' size='8'>E-mail: {self.COMPANY_EMAIL}</font>"
        )
        comp_para = Paragraph(comp_text, self.styles["CompanyInfo"])
        
        # Client info column (Dutch)
        client_parts = []
        
        if client.get("company"):
            client_parts.append(f"<font face='{fb}' size='10'>{client['company']}</font>")
        
        if client.get("name"):
            if client.get("company"):
                client_parts.append(f"<font face='{f}' size='8'>T.a.v.: {client['name']}</font>")
            else:
                client_parts.append(f"<font face='{fb}' size='10'>{client['name']}</font>")
        
        if client.get("address"):
            client_parts.append(f"<font face='{f}' size='8'>{client['address']}</font>")
        
        if client.get("postal_code") or client.get("city"):
            city_line = " ".join(filter(None, [client.get("postal_code"), client.get("city")]))
            if city_line:
                client_parts.append(f"<font face='{f}' size='8'>{city_line}</font>")
        
        if client.get("phone"):
            client_parts.append(f"<font face='{f}' size='8'>Tel: {client['phone']}</font>")
        
        if client.get("email"):
            client_parts.append(f"<font face='{f}' size='8'>E-mail: {client['email']}</font>")
        
        if client.get("vat_number"):
            client_parts.append(f"<font face='{f}' size='8'>BTW: {client['vat_number']}</font>")
        
        # Build client text with "FACTURATIE:" header (Dutch for "BILL TO:")
        client_text = f"<font face='{fb}' size='9' color='#E30613'>KLANT:</font><br/>"
        client_text += "<br/>".join(client_parts)
        
        client_para = Paragraph(client_text, self.styles["ClientInfo"])
        
        # Create 3-column table (17cm total width)
        tbl = Table(
            [[logo_element, comp_para, client_para]],
            colWidths=[3.5 * cm, 6.5 * cm, 7 * cm]
        )
        tbl.setStyle(TableStyle([
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("LEFTPADDING", (0, 0), (-1, -1), 5),
            ("RIGHTPADDING", (0, 0), (-1, -1), 5),
            ("TOPPADDING", (0, 0), (-1, -1), 0),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
        ]))
        return tbl
    
    def _quote_info_table(self, quote_number: str, quote_date: datetime, validity_days: int, reference: str = "", project_name: str = ""):
        """Create quote information table with clean styling - Dutch version"""
        valid_until = quote_date + timedelta(days=validity_days)
        
        f = self.default_font
        fb = self.bold_font
        
        # Build data rows (Dutch labels)
        data = [
            [
                Paragraph(f"<font face='{fb}' size='8'>Offertenummer:</font>", self.styles["TableCell"]),
                Paragraph(f"<font face='{f}' size='8'>{quote_number}</font>", self.styles["TableCell"]),
                Paragraph(f"<font face='{fb}' size='8'>Datum:</font>", self.styles["TableCell"]),
                Paragraph(f"<font face='{f}' size='8'>{quote_date.strftime('%d/%m/%Y')}</font>", self.styles["TableCell"]),
            ],
            [
                Paragraph(f"<font face='{fb}' size='8'>Geldig tot:</font>", self.styles["TableCell"]),
                Paragraph(f"<font face='{f}' size='8'>{valid_until.strftime('%d/%m/%Y')}</font>", self.styles["TableCell"]),
                Paragraph(f"<font face='{fb}' size='8'>Referentie:</font>", self.styles["TableCell"]),
                Paragraph(f"<font face='{f}' size='8'>{reference or '-'}</font>", self.styles["TableCell"]),
            ],
        ]
        
        tbl = Table(data, colWidths=[3.5 * cm, 5 * cm, 3 * cm, 5.5 * cm])
        tbl.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, -1), self.STRETCH_LIGHT_GRAY),
            ("BOX", (0, 0), (-1, -1), 1, self.STRETCH_MEDIUM_GRAY),
            ("INNERGRID", (0, 0), (-1, -1), 0.5, self.STRETCH_MEDIUM_GRAY),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("TOPPADDING", (0, 0), (-1, -1), 6),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
            ("LEFTPADDING", (0, 0), (-1, -1), 8),
            ("RIGHTPADDING", (0, 0), (-1, -1), 8),
        ]))
        
        return [tbl]
    
    def _convert_ceilings_to_items(self, quote_data: dict) -> list:
        """Convert ceiling data to line items - Dutch version"""
        items = []
        ceilings = quote_data.get("ceilings", [])
        
        for i, ceiling in enumerate(ceilings, 1):
            # Determine material subtype - check for acoustic
            material_subtype = ceiling.get('material_subtype', 'Standaard')
            if ceiling.get('is_acoustic') or ceiling.get('acoustic'):
                material_subtype = 'Akoestisch'
            
            # Build description (Dutch)
            desc_parts = [
                f"<b>{ceiling.get('name', f'Plafond {i}')}</b> - {ceiling.get('length', 0)}m × {ceiling.get('width', 0)}m ({ceiling.get('area', 0):.2f}m²)<br/>",
                f"Type: {ceiling.get('material_type', 'STOF').upper()} - {material_subtype}<br/>",
                f"Kleur: {ceiling.get('color', 'Wit')}<br/>",
                f"Omtrek: {ceiling.get('perimeter', 0)}m<br/>",
                f"Hoeken: {ceiling.get('corners', 4)}",
            ]
            
            if ceiling.get('is_acoustic') or ceiling.get('acoustic'):
                acoustic_value = ceiling.get('acoustic_value') or ceiling.get('acoustic_performance') or 0.4
                desc_parts.append(f"<br/>Akoestisch: Ja - αw {acoustic_value}")
            
            if ceiling.get('is_backlit') or ceiling.get('backlit'):
                desc_parts.append(f"<br/>Doorlicht: Ja")
            
            # Cost breakdown (Dutch)
            breakdown = ceiling.get('cost_breakdown', {})
            if breakdown:
                desc_parts.append("<br/><br/><b>Kostenoverzicht:</b>")
                if breakdown.get('ceiling_material'):
                    desc_parts.append(f"<br/>• Plafondmateriaal: €{breakdown['ceiling_material']:,.2f}")
                if breakdown.get('perimeter_structure'):
                    desc_parts.append(f"<br/>• Omtrekstructuur: €{breakdown['perimeter_structure']:,.2f}")
                if breakdown.get('perimeter_profile'):
                    desc_parts.append(f"<br/>• Omtrekprofiel: €{breakdown['perimeter_profile']:,.2f}")
                if breakdown.get('corners'):
                    desc_parts.append(f"<br/>• Hoeken: €{breakdown['corners']:,.2f}")
                if breakdown.get('acoustic_absorber'):
                    desc_parts.append(f"<br/>• Akoestische absorber: €{breakdown['acoustic_absorber']:,.2f}")
                if breakdown.get('seams'):
                    desc_parts.append(f"<br/>• Naden: €{breakdown['seams']:,.2f}")
                if breakdown.get('lights'):
                    desc_parts.append(f"<br/>• Verlichting: €{breakdown['lights']:,.2f}")
                if breakdown.get('wood_structures'):
                    desc_parts.append(f"<br/>• Houtconstructies: €{breakdown['wood_structures']:,.2f}")
                if breakdown.get('backlit_system'):
                    desc_parts.append(f"<br/>• Doorlichtsysteem: €{breakdown['backlit_system']:,.2f}")
            
            items.append({
                "code": f"PLAF-{i}",
                "description": "".join(desc_parts),
                "quantity": 1,
                "unit_price": ceiling.get('total_price', 0),
                "discount_percent": ceiling.get('discount', 0),
                "vat_rate": 21
            })
        
        return items
    
    def _items_table(self, items: list):
        """Create items table with improved styling - Dutch version"""
        f = self.default_font
        fb = self.bold_font
        
        # Header row - WHITE text on dark background (Dutch)
        headers = ["Artikel", "Omschrijving", "Aantal", "Prijs", "Korting", "Netto", "BTW", "Totaal"]
        header_row = [Paragraph(f"<font face='{fb}' color='white' size='7'>{h}</font>", self.styles["TableHeader"]) for h in headers]
        data = [header_row]
        
        # Data rows
        for item in items:
            unit_price = item.get("unit_price", 0)
            quantity = item.get("quantity", 1)
            discount_percent = item.get("discount_percent", 0)
            vat_rate = item.get("vat_rate", 21)
            
            discount_amount = unit_price * discount_percent / 100
            net_price = unit_price - discount_amount
            total = net_price * quantity
            
            discount_display = f"{discount_percent}%" if discount_percent > 0 else "-"
            
            description = Paragraph(item.get("description", ""), self.styles["TableCellDesc"])
            
            row = [
                Paragraph(f"<font face='{f}' size='8'>{item.get('code', '')}</font>", self.styles["TableCell"]),
                description,
                Paragraph(f"<font face='{f}' size='8'>{quantity}</font>", self.styles["TableCell"]),
                Paragraph(f"<font face='{f}' size='8'>€{unit_price:,.2f}</font>", self.styles["TableCell"]),
                Paragraph(f"<font face='{f}' size='8'>{discount_display}</font>", self.styles["TableCell"]),
                Paragraph(f"<font face='{f}' size='8'>€{net_price:,.2f}</font>", self.styles["TableCell"]),
                Paragraph(f"<font face='{f}' size='8'>{vat_rate}%</font>", self.styles["TableCell"]),
                Paragraph(f"<font face='{fb}' size='8'>€{total:,.2f}</font>", self.styles["TableCell"]),
            ]
            data.append(row)
        
        # Column widths - optimized for 17cm usable width (A4 minus margins)
        # Total: 1.3 + 6.0 + 1.4 + 1.9 + 1.5 + 1.9 + 1.0 + 2.0 = 17.0cm
        tbl = Table(data, colWidths=[1.3*cm, 6.0*cm, 1.4*cm, 1.9*cm, 1.5*cm, 1.9*cm, 1.0*cm, 2.0*cm], repeatRows=1)
        
        num_rows = len(data)
        
        style_list = [
            # Header row - dark background, white text
            ("BACKGROUND", (0, 0), (-1, 0), self.STRETCH_BLACK),
            ("TEXTCOLOR", (0, 0), (-1, 0), self.STRETCH_WHITE),
            ("ALIGN", (0, 0), (-1, 0), "CENTER"),
            ("FONTNAME", (0, 0), (-1, 0), self.bold_font),
            ("FONTSIZE", (0, 0), (-1, 0), 7),
            ("TOPPADDING", (0, 0), (-1, 0), 6),
            ("BOTTOMPADDING", (0, 0), (-1, 0), 6),
            
            # Data rows alignment
            ("ALIGN", (0, 1), (0, -1), "CENTER"),  # Item code
            ("ALIGN", (1, 1), (1, -1), "LEFT"),    # Description
            ("ALIGN", (2, 1), (2, -1), "CENTER"),  # Qty
            ("ALIGN", (3, 1), (-1, -1), "RIGHT"),  # Prices
            ("VALIGN", (0, 1), (-1, -1), "TOP"),
            ("FONTSIZE", (0, 1), (-1, -1), 8),
            ("TOPPADDING", (0, 1), (-1, -1), 6),
            ("BOTTOMPADDING", (0, 1), (-1, -1), 6),
            ("LEFTPADDING", (0, 0), (-1, -1), 4),
            ("RIGHTPADDING", (0, 0), (-1, -1), 4),
            
            # Grid styling
            ("GRID", (0, 0), (-1, -1), 0.5, self.STRETCH_MEDIUM_GRAY),
            ("BOX", (0, 0), (-1, -1), 1, self.STRETCH_BLACK),
        ]
        
        # Alternating row colors
        for i in range(1, num_rows):
            if i % 2 == 0:
                style_list.append(("BACKGROUND", (0, i), (-1, i), self.STRETCH_LIGHT_GRAY))
        
        tbl.setStyle(TableStyle(style_list))
        return tbl
    
    def _totals_table(self, items: list):
        """Create totals table with prominent styling - Dutch version"""
        f = self.default_font
        fb = self.bold_font
        
        # Calculate totals
        vat_groups = {}
        subtotal = 0
        
        for item in items:
            unit_price = item.get("unit_price", 0)
            quantity = item.get("quantity", 1)
            discount_percent = item.get("discount_percent", 0)
            vat_rate = item.get("vat_rate", 21)
            
            discount_amount = unit_price * discount_percent / 100
            net_price = unit_price - discount_amount
            total = net_price * quantity
            
            subtotal += total
            
            if vat_rate not in vat_groups:
                vat_groups[vat_rate] = 0
            vat_groups[vat_rate] += total
        
        # Build totals data (Dutch)
        data = [
            [
                "",
                Paragraph(f"<font face='{f}' size='9'>Subtotaal (excl. BTW)</font>", self.styles["TableCell"]),
                Paragraph(f"<font face='{f}' size='9'>€{subtotal:,.2f}</font>", self.styles["TableCell"])
            ]
        ]
        
        total_vat = 0
        for vat_rate, amount in sorted(vat_groups.items()):
            vat_amount = amount * vat_rate / 100
            total_vat += vat_amount
            data.append([
                "",
                Paragraph(f"<font face='{f}' size='9'>BTW ({vat_rate}%)</font>", self.styles["TableCell"]),
                Paragraph(f"<font face='{f}' size='9'>€{vat_amount:,.2f}</font>", self.styles["TableCell"])
            ])
        
        grand_total = subtotal + total_vat
        data.append([
            "",
            Paragraph(f"<font face='{fb}' size='11' color='white'>TOTAAL (incl. BTW)</font>", self.styles["TableCell"]),
            Paragraph(f"<font face='{fb}' size='11' color='white'>€{grand_total:,.2f}</font>", self.styles["TableCell"])
        ])
        
        # Column widths - aligned with items table (17cm total)
        tbl = Table(data, colWidths=[10 * cm, 4 * cm, 3 * cm])
        
        style_list = [
            ("ALIGN", (1, 0), (1, -1), "RIGHT"),
            ("ALIGN", (2, 0), (2, -1), "RIGHT"),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("TOPPADDING", (0, 0), (-1, -2), 5),
            ("BOTTOMPADDING", (0, 0), (-1, -2), 5),
            ("RIGHTPADDING", (1, 0), (-1, -1), 10),
            ("LEFTPADDING", (1, 0), (-1, -1), 10),
            
            # Total row - RED background, WHITE text
            ("BACKGROUND", (1, -1), (-1, -1), self.STRETCH_RED),
            ("TEXTCOLOR", (1, -1), (-1, -1), self.STRETCH_WHITE),
            ("FONTNAME", (1, -1), (-1, -1), self.bold_font),
            ("FONTSIZE", (1, -1), (-1, -1), 11),
            ("TOPPADDING", (1, -1), (-1, -1), 8),
            ("BOTTOMPADDING", (1, -1), (-1, -1), 8),
            
            # Border for total row
            ("BOX", (1, -1), (-1, -1), 1.5, self.STRETCH_RED),
        ]
        
        tbl.setStyle(TableStyle(style_list))
        return tbl
    
    def _terms_section(self, validity_days: int, user_profile: dict = None):
        """Create terms and conditions section - Dutch version"""
        elements = []
        
        is_business = user_profile and user_profile.get('is_company', False)
        valid_until = datetime.now() + timedelta(days=validity_days)
        
        # Header (Dutch)
        header_text = "Algemene Voorwaarden - Zakelijk:" if is_business else "Algemene Voorwaarden:"
        elements.append(Paragraph(header_text, self.styles["TermsHeader"]))
        
        # Terms content (Dutch)
        f = self.default_font
        
        if is_business:
            terms = [
                f"Deze offerte is geldig tot {valid_until.strftime('%d/%m/%Y')}",
                "Betalingstermijn: 30 dagen netto na factuurdatum",
                "BTW: Alle prijzen zijn exclusief BTW, deze wordt toegevoegd tegen het geldende tarief",
                f"Betaling per bankoverschrijving naar rekening {self.BANK_ACCOUNT} (IBAN: {self.IBAN}, BIC: {self.BIC})",
                f"Rekeninghouder: {self.COMPANY_NAME}",
                "Levertijd: 2-4 weken na orderbevestiging",
                "Garantie: 10 jaar op materialen, 2 jaar op installatiewerk",
                "Installatie- en leveringskosten kunnen van toepassing zijn afhankelijk van locatie en projectvereisten",
            ]
        else:
            terms = [
                f"Deze offerte is geldig tot {valid_until.strftime('%d/%m/%Y')}",
                "BTW: Alle prijzen zijn inclusief 21% BTW",
                "Betalingsvoorwaarden: 50% aanbetaling bij bestelling, restant bij oplevering",
                f"Betaling per bankoverschrijving naar rekening {self.BANK_ACCOUNT} (IBAN: {self.IBAN}, BIC: {self.BIC})",
                f"Rekeninghouder: {self.COMPANY_NAME}",
                "Levertijd: 2-3 weken na orderbevestiging",
                "Garantie: 10 jaar op materialen, 2 jaar op installatiewerk",
                "Installatie inbegrepen in de prijs voor standaard installaties",
            ]
        
        for term in terms:
            elements.append(Paragraph(f"<font face='{f}' size='8'>• {term}</font>", self.styles["TermsText"]))
        
        return elements
    
    def generate_quote(self, quote_data: dict) -> str:
        """
        Generate quote PDF - backwards compatible method for quote_editor.py
        
        Args:
            quote_data: Dictionary containing:
                - quote_number: The quote number
                - ceilings: List of ceiling configurations
                - ceiling_costs: List of cost breakdowns
                - user_profile: Optional user profile dict
                - customer: Optional customer data from Dynamics
                - quote_reference: Optional reference
                - total_price: Total price (used for validation)
        
        Returns:
            str: Path to generated PDF
        """
        # Extract quote number
        quote_number = quote_data.get('quote_number', f"OFF{datetime.now():%Y%m%d%H%M%S}")
        
        # Extract user profile
        user_profile = quote_data.get('user_profile', {})
        
        # Merge ceiling_costs into ceilings if not already done
        if "ceilings" in quote_data and "ceiling_costs" in quote_data:
            for i, ceiling in enumerate(quote_data.get("ceilings", [])):
                if i < len(quote_data.get("ceiling_costs", [])):
                    costs = quote_data["ceiling_costs"][i]
                    if not ceiling.get("total_price"):
                        ceiling["total_price"] = costs.get("total", 0)
                    if not ceiling.get("cost_breakdown"):
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
        
        # Call the main build_pdf method
        return self.build_pdf(quote_number, quote_data, user_profile)


# Backwards compatibility alias
StretchQuotePDFGenerator = ImprovedStretchQuotePDFGenerator