"""
PDF Generator for Stretch Ceiling Bot with User Profile Integration
Version 2.1 - Fixed consolidated ceiling costs display
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
    """Generates professional PDF quotes with STRETCH branding and user profile integration"""
    
    # Brand colors
    STRETCH_RED = HexColor("#E30613")
    STRETCH_BLACK = HexColor("#000000")
    STRETCH_WHITE = HexColor("#FFFFFF")
    STRETCH_GRAY = HexColor("#666666")
    STRETCH_LIGHT_GRAY = HexColor("#f5f5f5")
    
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
        self.styles = getSampleStyleSheet()
        self.setup_styles()
    
    def setup_styles(self):
        """Setup custom PDF styles matching STRETCH branding"""
        self.styles.add(
            ParagraphStyle(
                name="QuoteTitle",
                parent=self.styles["Title"],
                fontSize=22,
                textColor=self.STRETCH_BLACK,
                alignment=TA_LEFT,
                spaceAfter=20,
                spaceBefore=10,
                fontName="Helvetica-Bold",
            )
        )
        self.styles.add(
            ParagraphStyle(
                name="SectionHeader",
                parent=self.styles["Heading2"],
                fontSize=14,
                textColor=self.STRETCH_RED,
                spaceAfter=10,
                spaceBefore=15,
                fontName="Helvetica-Bold",
            )
        )
        self.styles.add(
            ParagraphStyle(
                name="NormalLeft",
                parent=self.styles["Normal"],
                fontSize=10,
                textColor=self.STRETCH_BLACK,
                leading=14,
                alignment=TA_LEFT,
            )
        )
        self.styles.add(
            ParagraphStyle(
                name="CompanyInfo",
                parent=self.styles["Normal"],
                fontSize=9,
                textColor=self.STRETCH_BLACK,
                leading=12,
                alignment=TA_LEFT,
            )
        )
        self.styles.add(
            ParagraphStyle(
                name="TableHeader",
                parent=self.styles["Normal"],
                fontSize=11,
                textColor=self.STRETCH_WHITE,
                alignment=TA_CENTER,
                fontName="Helvetica-Bold",
            )
        )
        self.styles.add(
            ParagraphStyle(
                name="TableCell",
                parent=self.styles["Normal"],
                fontSize=10,
                textColor=self.STRETCH_BLACK,
                alignment=TA_LEFT,
                leading=12,
            )
        )
        self.styles.add(
            ParagraphStyle(
                name="Footer",
                parent=self.styles["Normal"],
                fontSize=9,
                textColor=self.STRETCH_GRAY,
                alignment=TA_CENTER,
                leading=11,
            )
        )
        self.styles.add(
            ParagraphStyle(
                name="ClientNote",
                parent=self.styles["Normal"],
                fontSize=9,
                textColor=self.STRETCH_GRAY,
                alignment=TA_LEFT,
                fontStyle='italic',
            )
        )
    
    def add_page_template(self, canvas, doc):
        """Draws branded header and footer on each page"""
        width, height = A4
        canvas.saveState()
        
        # Header background
        header_height = 75
        canvas.setFillColor(self.STRETCH_RED)
        canvas.rect(0, height - header_height, width, header_height, fill=True, stroke=False)
        
        # Header text
        canvas.setFillColor(self.STRETCH_WHITE)
        canvas.setFont("Helvetica-Bold", 22)
        canvas.drawCentredString(width / 2, height - 35, self.COMPANY_NAME.upper())
        canvas.setFont("Helvetica", 11)
        canvas.drawCentredString(width / 2, height - 55, "Professionele Spanplafond Oplossingen")
        
        # Footer background
        footer_height = 50
        canvas.setFillColor(HexColor("#f5f5f5"))
        canvas.rect(0, 0, width, footer_height, fill=True, stroke=False)
        
        # Footer text
        y = 35
        canvas.setFillColor(HexColor("#333333"))
        canvas.setFont("Helvetica", 8)
        canvas.drawCentredString(width / 2, y, f"{self.COMPANY_NAME} • {self.COMPANY_ADDRESS}")
        canvas.drawCentredString(width / 2, y - 10, f"Tel: {self.COMPANY_PHONE} • Email: {self.COMPANY_EMAIL} • {self.COMPANY_WEBSITE}")
        canvas.drawCentredString(width / 2, y - 20, f"BTW: {self.COMPANY_VAT} • IBAN: {self.IBAN} • BIC: {self.BIC}")
        
        # Page number
        canvas.setFont("Helvetica", 9)
        canvas.drawRightString(width - 20, 10, f"Pagina {doc.page}")
        
        canvas.restoreState()
    
    def build_pdf(self, quote_number: str, quote_data: dict, user_profile: dict = None) -> str:
        """
        Build PDF with user profile integration
        
        Args:
            quote_number: The quote number
            quote_data: Quote data dictionary
            user_profile: User profile dictionary with keys like:
                - first_name, last_name
                - is_company, company_name, vat_number
                - address, email, phone
                - client_group (price_b2c, price_b2b_reseller, price_b2b_hospitality)
        
        Returns:
            str: Path to generated PDF
        """
        # Create filename
        filename = f"offerte_{quote_number}_{datetime.now():%Y%m%d_%H%M%S}.pdf"
        filepath = os.path.join(self.output_dir, filename)
        
        # Create PDF document
        doc = SimpleDocTemplate(
            filepath,
            pagesize=A4,
            leftMargin=25 * mm,
            rightMargin=25 * mm,
            topMargin=85,
            bottomMargin=60
        )
        
        elements = []
        
        # Prepare client data from user profile or quote data
        client_data = self._prepare_client_data(quote_data, user_profile)
        
        # Header with company and client info
        elements.append(self._make_header(client_data))
        elements.append(Spacer(1, 15))
        
        # Quote title
        elements.append(Paragraph(f"OFFERTE #{quote_number}", self.styles["QuoteTitle"]))
        
        # Quote metadata
        quote_date = datetime.now()
        validity_days = self.TERMS_VALIDITY_DAYS
        reference = quote_data.get("quote_reference", "")
        project_name = quote_data.get("project_name", "")
        
        elements.extend(self._quote_info_table(quote_number, quote_date, validity_days, reference, project_name))
        elements.append(Spacer(1, 20))
        
        # Convert ceiling data to items - FIXED VERSION
        items = self._convert_ceilings_to_items(quote_data)
        
        # Items section
        elements.append(Paragraph("Offerte Details", self.styles["SectionHeader"]))
        elements.append(self._items_table(items))
        elements.append(Spacer(1, 20))
        
        # Totals
        elements.append(self._totals_table(items))
        
        # Add client type pricing note (Dutch)
        client_group = user_profile.get('client_group', 'price_b2c') if user_profile else 'price_b2c'
        if client_group == 'price_b2b_reseller':
            elements.append(Spacer(1, 10))
            elements.append(Paragraph(
                "* B2B resellerprijzen van toepassing - Volumekortingen beschikbaar voor grote bestellingen",
                self.styles["ClientNote"]
            ))
        elif client_group == 'price_b2b_hospitality':
            elements.append(Spacer(1, 10))
            elements.append(Paragraph(
                "* B2B horeca-tarieven van toepassing - Commerciele tarieven voor de horecasector",
                self.styles["ClientNote"]
            ))
        
        elements.append(Spacer(1, 30))
        
        # Notes section (if provided)
        notes = quote_data.get("notes", "")
        if notes:
            elements.append(Paragraph("Notes", self.styles["SectionHeader"]))
            elements.append(Paragraph(notes, self.styles["NormalLeft"]))
            elements.append(Spacer(1, 20))
        
        # Terms - different for business vs individual
        elements.append(self._terms_section(validity_days, user_profile))
        
        # Build PDF
        doc.build(elements, onFirstPage=self.add_page_template, onLaterPages=self.add_page_template)
        
        logger.info(f"PDF offerte gegenereerd: {filepath}")
        return filepath
    
    def _prepare_client_data(self, quote_data: dict, user_profile: dict = None):
        """Client data: Dynamics 365 customer > user_profile > quote fallback."""
        client = {
            "name": "", "company": "", "address": "",
            "postal_code": "", "city": "", "country": "",
            "phone": "", "email": "", "vat_number": ""
        }

        # PRIORITY 1: Dynamics 365 customer
        if quote_data.get("customer"):
            customer = quote_data["customer"]
            logger.info(f"PDF gebruikt klantgegevens van Dynamics 365: {customer.get('display_name', '')}")
            company_name  = customer.get("company_name") or customer.get("display_name", "")
            contact_name  = customer.get("contact_name", "")
            if customer.get("dynamics_account_id") or customer.get("is_company"):
                client["company"] = company_name
                client["name"]    = contact_name
            else:
                client["name"] = contact_name or company_name
            client["email"]      = customer.get("email", "")
            client["phone"]      = customer.get("phone", "")
            client["address"]    = customer.get("address", "")
            client["vat_number"] = customer.get("vat_number", customer.get("vat", ""))

        # PRIORITY 2: user_profile
        elif user_profile:
            logger.info(f"PDF gebruikt gebruikersprofiel: {user_profile.get('full_name') or user_profile.get('first_name', '')}")
            client["name"]    = (
                user_profile.get("full_name")
                or f"{user_profile.get('first_name','')} {user_profile.get('last_name','')}".strip()
            )
            client["email"]   = user_profile.get("email", "")
            client["phone"]   = user_profile.get("phone", "")
            client["address"] = user_profile.get("address", "")
            if user_profile.get("is_company"):
                client["company"]    = user_profile.get("company_name", "")
                client["vat_number"] = user_profile.get("vat_number", "")

        # PRIORITY 3: contact_info from email form
        else:
            contact = quote_data.get("contact_info", {}) or {}
            # Use name from contact, not company_type which may just be "BV"
            client["name"]    = contact.get("name") or quote_data.get("quote_reference", "Klant")
            client["email"]   = contact.get("email") or quote_data.get("email", "")
            client["phone"]   = contact.get("phone", "")
            # Only set company if contact explicitly flags is_company AND has a real name
            if contact.get("is_company") and contact.get("name"):
                client["company"] = contact.get("name", "")
                client["name"]    = ""

        return client
    
    def _make_header(self, client: dict):
        """Create header with company and client information"""
        # Logo handling
        logo_element = None
        possible_logo_paths = [
            self.logo_path,
            "/home/STRETCH/STRETCH_NEW/stretch_logo.png",
            "/home/STRETCH/stretch_logo.png",
            "stretch_logo.png",
        ]
        for path in possible_logo_paths:
            if path and os.path.exists(path):
                try:
                    logo = Image(path)
                    aspect = logo.imageWidth / float(logo.imageHeight)
                    logo_width = 3.5 * cm
                    logo_height = logo_width / aspect
                    if logo_height > 2 * cm:
                        logo_height = 2 * cm
                        logo_width = logo_height * aspect
                    logo.drawWidth = logo_width
                    logo.drawHeight = logo_height
                    logo.hAlign = "LEFT"
                    logo_element = logo
                    break
                except Exception as e:
                    logger.warning(f"Kon logo niet laden: {e}")
        
        if not logo_element:
            # Text-based logo fallback
            logo_element = Paragraph('<b>STR<font color="#E30613">â‰¡</font>TCH</b><br/><font size="8">PLAFONDS & WANDEN</font>', self.styles["CompanyInfo"])
        
        # Company info
        comp_text = (
            f"<b>{self.COMPANY_NAME}</b><br/>"
            f"{self.COMPANY_ADDRESS}<br/>"
            f"BTW: {self.COMPANY_VAT}<br/>"
            f"Tel: {self.COMPANY_PHONE}<br/>"
            f"Email: {self.COMPANY_EMAIL}"
        )
        comp_para = Paragraph(comp_text, self.styles["CompanyInfo"])
        
        # Client info - enhanced for company vs individual
        client_parts = []
        if client.get("company"):
            client_parts.append(f"<b>{client['company']}</b>")
            if client.get("vat_number"):
                client_parts.append(f"BTW: {client['vat_number']}")
        
        if client.get("name"):
            if client.get("company"):
                client_parts.append(f"T.a.v.: {client['name']}")
            else:
                client_parts.append(f"<b>{client['name']}</b>")
        
        address_parts = []
        if client.get("address"):
            address_parts.append(client["address"])
        if client.get("postal_code") or client.get("city"):
            city_line = " ".join(filter(None, [client.get("postal_code"), client.get("city")]))
            if city_line:
                address_parts.append(city_line)
        if client.get("country"):
            address_parts.append(client["country"])
        
        client_text = "<b>KLANT:</b><br/>"
        client_text += "<br/>".join(client_parts)
        if address_parts:
            client_text += "<br/>" + "<br/>".join(address_parts)
        
        contact_parts = []
        if client.get("phone"):
            contact_parts.append(f"Tel: {client['phone']}")
        if client.get("email"):
            contact_parts.append(f"Email: {client['email']}")
        
        if contact_parts:
            client_text += "<br/>" + "<br/>".join(contact_parts)
        
        client_para = Paragraph(client_text, self.styles["NormalLeft"])
        
        # Create table
        tbl = Table([[logo_element, comp_para, client_para]], colWidths=[4 * cm, 7 * cm, 7.5 * cm])
        tbl.setStyle(TableStyle([
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("LEFTPADDING", (0, 0), (0, 0), 0),
            ("LEFTPADDING", (1, 0), (1, 0), 10),
            ("LEFTPADDING", (2, 0), (2, 0), 15),
            ("RIGHTPADDING", (0, 0), (-1, -1), 5),
        ]))
        return tbl
    
    def _quote_info_table(self, quote_number: str, quote_date: datetime, validity_days: int, reference: str, project_name: str) -> list:
        """Create quote information table"""
        elements = []
        
        # Quote metadata table
        quote_info_data = []
        
        # First row - Quote number and date
        quote_info_data.append([
            Paragraph(f"<b>Offertenummer:</b> {quote_number}", self.styles['NormalLeft']),
            Paragraph(f"<b>Datum:</b> {quote_date.strftime('%d/%m/%Y')}", self.styles['NormalLeft'])
        ])
        
        # Second row - Validity and reference
        valid_until = quote_date + timedelta(days=validity_days)
        quote_info_data.append([
            Paragraph(f"<b>Geldig tot:</b> {valid_until.strftime('%d/%m/%Y')}", self.styles['NormalLeft']),
            Paragraph(f"<b>Referentie:</b> {reference if reference else '-'}", self.styles['NormalLeft'])
        ])
        
        # Third row - Project name if provided
        if project_name:
            quote_info_data.append([
                Paragraph(f"<b>Projectnaam:</b> {project_name}", self.styles['NormalLeft']),
                ""
            ])
        
        # Create table
        info_table = Table(quote_info_data, colWidths=[9 * cm, 9 * cm])
        info_table.setStyle(TableStyle([
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('LEFTPADDING', (0, 0), (-1, -1), 0),
            ('RIGHTPADDING', (0, 0), (-1, -1), 5),
            ('TOPPADDING', (0, 0), (-1, -1), 3),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 3),
            ('BACKGROUND', (0, 0), (-1, -1), self.STRETCH_LIGHT_GRAY),
            ('BOX', (0, 0), (-1, -1), 1, self.STRETCH_BLACK),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
        ]))
        
        elements.append(info_table)
        return elements
    
    def _convert_ceilings_to_items(self, quote_data: dict) -> list:
        """Convert ceiling data to line items — Dutch version with quote_lights support."""
        items = []

        for i, (ceiling, costs) in enumerate(
            zip(quote_data.get('ceilings', []), quote_data.get('ceiling_costs', []))
        ):
            # Calculate total cost from ceiling_costs array
            total_ceiling_cost = 0
            if isinstance(costs, dict):
                total_ceiling_cost = costs.get('total', 0)
                if total_ceiling_cost == 0:
                    total_ceiling_cost = (
                        costs.get('ceiling_cost', 0) +
                        costs.get('perimeter_structure_cost', 0) +
                        costs.get('perimeter_profile_cost', 0) +
                        costs.get('corners_cost', 0) +
                        costs.get('seam_cost', 0) +
                        costs.get('lights_cost', 0) +
                        costs.get('wood_structures_cost', 0) +
                        costs.get('acoustic_absorber_cost', 0)
                    )

            # Dutch description
            perimeter_note = " (omtrek handmatig aangepast)" if ceiling.get('perimeter_edited') else ""
            description = (
                f"<b>{ceiling.get('name', f'Plafond {i+1}')}</b> - "
                f"{ceiling.get('length', 0)}m x {ceiling.get('width', 0)}m "
                f"({ceiling.get('area', 0):.2f}m2)<br/>"
                f"Type: {ceiling.get('ceiling_type', 'STOF').upper()} - "
                f"{ceiling.get('type_ceiling', 'Standaard')}<br/>"
                f"Kleur: {ceiling.get('color', 'Wit').capitalize()}<br/>"
                f"Omtrek: {ceiling.get('perimeter', 0):.1f}m{perimeter_note}<br/>"
                f"Hoeken: {ceiling.get('corners', 4)}"
            )

            if ceiling.get('acoustic'):
                description += "<br/>Akoestisch: Ja"
                if ceiling.get('acoustic_performance'):
                    description += f" - {ceiling['acoustic_performance']}"

            # Dutch cost breakdown
            if costs:
                description += "<br/><br/><b>Kostenoverzicht:</b>"
                if costs.get('ceiling_cost', 0) > 0:
                    description += f"<br/>* Plafondmateriaal: EUR{float(costs['ceiling_cost']):,.2f}"
                if costs.get('perimeter_structure_cost', 0) > 0:
                    description += f"<br/>* Omtrekstructuur: EUR{float(costs['perimeter_structure_cost']):,.2f}"
                if costs.get('perimeter_profile_cost', 0) > 0:
                    description += f"<br/>* Omtrekprofiel: EUR{float(costs['perimeter_profile_cost']):,.2f}"
                if costs.get('corners_cost', 0) > 0:
                    description += f"<br/>* Hoeken: EUR{float(costs['corners_cost']):,.2f}"
                if costs.get('seam_cost', 0) > 0:
                    description += f"<br/>* Naden: EUR{float(costs['seam_cost']):,.2f}"
                if costs.get('lights_cost', 0) > 0:
                    description += f"<br/>* Verlichting: EUR{float(costs['lights_cost']):,.2f}"
                    for light in ceiling.get('lights', []):
                        lc = light.get('product_code', light.get('code', ''))
                        description += f"<br/>  - {lc}: {light.get('quantity', 0)} st"
                if costs.get('wood_structures_cost', 0) > 0:
                    description += f"<br/>* Houtconstructies: EUR{float(costs['wood_structures_cost']):,.2f}"
                if costs.get('acoustic_absorber_cost', 0) > 0:
                    description += f"<br/>* Akoestische absorber: EUR{float(costs['acoustic_absorber_cost']):,.2f}"

            items.append({
                "code":             f"PLAF-{i+1}",
                "description":      description,
                "quantity":         1,
                "unit_price":       total_ceiling_cost,
                "discount_percent": 0,
                "vat_rate":         21,
            })

        # Quote-level lights (not assigned to a specific room)
        for j, light in enumerate(quote_data.get("quote_lights", []), 1):
            unit_price = float(light.get("price") or light.get("price_b2c") or 0)
            quantity   = int(light.get("quantity", 1))
            desc = (
                f"<b>{light.get('description', light.get('product_code', 'Lichtstructuur'))}</b><br/>"
                f"Productcode: {light.get('product_code', '')}<br/>"
                f"Kamer niet opgegeven - gelieve te bevestigen welke kamer(s)"
            )
            items.append({
                "code":             light.get("product_code", f"LICHT-{j}"),
                "description":      desc,
                "quantity":         quantity,
                "unit_price":       unit_price,
                "discount_percent": 0,
                "vat_rate":         21,
            })

        return items
    
    def _items_table(self, items: list):
        """Create items table with all details"""
        headers = ["Artikel", "Omschrijving", "Aantal", "Prijs", "Korting", "Netto", "BTW", "Totaal"]
        
        data = [headers]
        
        for item in items:
            # Calculate prices
            unit_price = item.get("unit_price", 0)
            quantity = item.get("quantity", 1)
            discount_percent = item.get("discount_percent", 0)
            vat_rate = item.get("vat_rate", 21)  # Default 21% VAT
            
            # Calculate net price after discount
            discount_amount = unit_price * discount_percent / 100
            net_price = unit_price - discount_amount
            
            # Calculate total (excluding VAT)
            total = net_price * quantity
            
            # Format discount display
            discount_display = f"{discount_percent}%" if discount_percent > 0 else "-"
            
            # Parse the HTML description for the table
            description = Paragraph(item.get("description", ""), self.styles["TableCell"])
            
            row = [
                item.get("code", ""),
                description,
                str(quantity),
                f"EUR {unit_price:,.2f}",
                discount_display,
                f"EUR {net_price:,.2f}",
                f"{vat_rate}%",
                f"EUR {total:,.2f}"
            ]
            data.append(row)
        
        tbl = Table(data, colWidths=[2.5 * cm, 6.5 * cm, 1.5 * cm, 2.5 * cm, 1.8 * cm, 2.5 * cm, 1.5 * cm, 2.5 * cm], repeatRows=1)
        
        # Calculate number of data rows for alternating colors
        num_rows = len(data)
        
        style_list = [
            # Header row
            ("BACKGROUND", (0, 0), (-1, 0), self.STRETCH_BLACK),
            ("TEXTCOLOR", (0, 0), (-1, 0), self.STRETCH_WHITE),
            ("ALIGN", (0, 0), (-1, 0), "CENTER"),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, 0), 10),
            ("TOPPADDING", (0, 0), (-1, 0), 10),
            ("BOTTOMPADDING", (0, 0), (-1, 0), 10),
            
            # Data rows
            ("ALIGN", (0, 1), (0, -1), "CENTER"),
            ("ALIGN", (1, 1), (1, -1), "LEFT"),
            ("ALIGN", (2, 1), (2, -1), "CENTER"),
            ("ALIGN", (3, 1), (-1, -1), "RIGHT"),
            ("VALIGN", (0, 1), (-1, -1), "TOP"),
            ("FONTSIZE", (0, 1), (-1, -1), 9),
            ("TOPPADDING", (0, 1), (-1, -1), 8),
            ("BOTTOMPADDING", (0, 1), (-1, -1), 8),
            
            # Grid
            ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
            ("BOX", (0, 0), (-1, -1), 1, self.STRETCH_BLACK),
        ]
        
        # Add alternating row colors
        for i in range(1, num_rows):
            if i % 2 == 0:
                style_list.append(("BACKGROUND", (0, i), (-1, i), self.STRETCH_LIGHT_GRAY))
        
        tbl.setStyle(TableStyle(style_list))
        return tbl
    
    def _totals_table(self, items: list):
        """Create totals table with calculations"""
        # Calculate totals by VAT rate
        vat_groups = {}
        subtotal = 0
        
        for item in items:
            unit_price = item.get("unit_price", 0)
            quantity = item.get("quantity", 1)
            discount_percent = item.get("discount_percent", 0)
            vat_rate = item.get("vat_rate", 21)
            
            # Calculate net price after discount
            discount_amount = unit_price * discount_percent / 100
            net_price = unit_price - discount_amount
            total = net_price * quantity
            
            subtotal += total
            
            # Group by VAT rate
            if vat_rate not in vat_groups:
                vat_groups[vat_rate] = 0
            vat_groups[vat_rate] += total
        
        # Build totals data
        data = [
            ["", "Subtotaal (excl. BTW)", f"EUR{subtotal:,.2f}"]
        ]
        
        total_vat = 0
        for vat_rate, amount in sorted(vat_groups.items()):
            vat_amount = amount * vat_rate / 100
            total_vat += vat_amount
            data.append(["", f"BTW ({vat_rate}%)", f"EUR{vat_amount:,.2f}"])
        
        grand_total = subtotal + total_vat
        data.append(["", "<b>TOTAAL (incl. BTW)</b>", f"<b>EUR{grand_total:,.2f}</b>"])
        
        # Convert cells to Paragraphs for bold formatting
        for i in range(len(data)):
            for j in range(1, len(data[i])):
                data[i][j] = Paragraph(data[i][j], self.styles["TableCell"])
        
        tbl = Table(data, colWidths=[10.5 * cm, 3.5 * cm, 3.5 * cm])
        tbl.setStyle(
            TableStyle([
                ("ALIGN", (1, 0), (1, -1), "RIGHT"),
                ("ALIGN", (2, 0), (2, -1), "RIGHT"),
                ("FONTSIZE", (0, 0), (-1, -1), 11),
                ("TOPPADDING", (0, 0), (-1, -1), 6),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
                
                # Total row
                ("BACKGROUND", (1, -1), (-1, -1), self.STRETCH_RED),
                ("TEXTCOLOR", (1, -1), (-1, -1), self.STRETCH_WHITE),
                ("FONTNAME", (1, -1), (-1, -1), "Helvetica-Bold"),
                ("FONTSIZE", (1, -1), (-1, -1), 12),
                ("LINEABOVE", (1, -1), (-1, -1), 2, self.STRETCH_BLACK),
                ("TOPPADDING", (1, -1), (-1, -1), 8),
                ("BOTTOMPADDING", (1, -1), (-1, -1), 8),
            ])
        )
        return tbl
    
    def _terms_section(self, validity_days: int, user_profile: dict = None):
        """Create terms and conditions section based on client type"""
        
        # Determine if business or individual terms
        is_business = user_profile and user_profile.get('is_company', False)
        
        if is_business:
            # Business terms
            valid_until = (datetime.now() + timedelta(days=validity_days)).strftime('%d/%m/%Y')
            terms = (
                f"<b>Algemene Voorwaarden - Zakelijk:</b><br/>"
                f"• Deze offerte is geldig tot {valid_until}<br/>"
                f"• Betalingstermijn: 30 dagen netto na factuurdatum<br/>"
                f"• BTW: Alle prijzen zijn exclusief BTW<br/>"
                f"• Betaling per bankoverschrijving naar rekening {self.BANK_ACCOUNT} (IBAN: {self.IBAN}, BIC: {self.BIC})<br/>"
                f"• Rekeninghouder: {self.COMPANY_NAME}<br/>"
                f"• Levertijd: 2-4 weken na orderbevestiging<br/>"
                f"• Garantie: 10 jaar op materialen, 2 jaar op installatiewerk<br/>"
                f"• Installatie- en leveringskosten kunnen van toepassing zijn"
            )
        else:
            valid_until = (datetime.now() + timedelta(days=validity_days)).strftime('%d/%m/%Y')
            terms = (
                f"<b>Algemene Voorwaarden:</b><br/>"
                f"• Deze offerte is geldig tot {valid_until}<br/>"
                f"• BTW: Alle prijzen zijn inclusief 21% BTW<br/>"
                f"• Betalingsvoorwaarden: 50% aanbetaling bij bestelling, restant bij oplevering<br/>"
                f"• Betaling per bankoverschrijving naar rekening {self.BANK_ACCOUNT} (IBAN: {self.IBAN}, BIC: {self.BIC})<br/>"
                f"• Rekeninghouder: {self.COMPANY_NAME}<br/>"
                f"• Levertijd: 2-3 weken na orderbevestiging<br/>"
                f"• Garantie: 10 jaar op materialen, 2 jaar op installatiewerk<br/>"
                f"• Installatie inbegrepen in de prijs voor standaard installaties"
            )
        
        return Paragraph(terms, self.styles["Footer"])
    
    def generate_quote(self, quote_data: dict) -> str:
        """
        Generate quote PDF — merges ceiling_costs into ceilings for cost breakdown display.
        Called by email_listener._generate_pdf() for email-flow quotes.
        """
        user_profile = quote_data.get("user_profile", None)
        quote_number = quote_data.get(
            "quote_number", f"OFF{datetime.now():%Y%m%d%H%M%S}"
        )

        # Merge ceiling_costs → ceiling dict so cost breakdown shows in PDF.
        # Always overwrite so dedup changes (lights_cost=0) are respected.
        ceilings      = quote_data.get("ceilings", [])
        ceiling_costs = quote_data.get("ceiling_costs", [])
        for i, ceiling in enumerate(ceilings):
            if i < len(ceiling_costs):
                costs = ceiling_costs[i]
                if isinstance(costs, dict):
                    # Always set total_price from ceiling_costs (respects dedup)
                    ceiling["total_price"] = float(costs.get("total", 0))
                    # Always rebuild cost_breakdown (respects lights_cost=0 after dedup)
                    ceiling["cost_breakdown"] = {
                        "ceiling_material":    costs.get("ceiling_cost", 0),
                        "perimeter_structure": costs.get("perimeter_structure_cost", 0),
                        "perimeter_profile":   costs.get("perimeter_profile_cost", 0),
                        "corners":             costs.get("corners_cost", 0),
                        "acoustic_absorber":   costs.get("acoustic_absorber_cost", 0),
                        "seams":               costs.get("seam_cost", 0),
                        "lights":              costs.get("lights_cost", 0),
                        "wood_structures":     costs.get("wood_structures_cost", 0),
                    }
            if not ceiling.get("material_type"):
                ceiling["material_type"] = ceiling.get("ceiling_type", "fabric")

        return self.build_pdf(quote_number, quote_data, user_profile)