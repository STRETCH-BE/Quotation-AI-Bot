"""
email_quote_processor.py
STRETCH Bot — Email Quote Processor  v1.1

Parses inbound lead emails via Azure OpenAI, extracts ceiling specs,
makes smart assumptions for every missing field, validates all values
against the live product database, and calculates costs via CostCalculator.

Handles real-world STRETCH website contact-form cases:
  - Dutch / French / English
  - "Request quote", "Contact", "Calculator" form types
  - L-shaped rooms        → two ceiling objects
  - Wall requests         → is_wall_request flag
  - RAL / NCS colours     → catalog mapping + custom-order flag
  - Area-only input       → dimension inference
  - Zero-info contacts    → is_qualification_only flag (no PDF)
  - B2B auto-detection    → upgrades client_group

Imports follow the FLAT layout used by the rest of the bot:
    from config import Config
    from models import ...
    from cost_calculator import CostCalculator
    from manager import EnhancedDatabaseManager  ← passed in, never imported directly
"""

import json
import logging
import re
from dataclasses import asdict, dataclass, field
from difflib import get_close_matches
from typing import Dict, List, Optional, Tuple

from openai import AzureOpenAI

from config import Config
from ..cost_calculator import CostCalculator
from models import CeilingCost, CeilingConfig

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
#  Data classes
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class Assumption:
    """One assumption made while parsing — shown to the customer in the reply."""
    field: str
    user_said: str
    assumed: str
    reason: str
    confidence: str   # "high" | "medium" | "low"
    question: str


@dataclass
class EmailQuoteResult:
    """Full result consumed by EmailListener."""
    success: bool
    sender_email: str
    sender_name: str
    language: str                            # "nl" | "fr" | "en"

    session_data: Optional[Dict] = None
    total_price: float = 0.0
    quote_number: Optional[str] = None
    pdf_path: Optional[str] = None

    assumptions: List[Assumption] = field(default_factory=list)
    missing_fields: List[str] = field(default_factory=list)
    confidence_score: float = 0.0

    is_wall_request: bool = False
    is_qualification_only: bool = False      # not enough info → send info-request
    needs_custom_color: bool = False
    custom_color_codes: List[str] = field(default_factory=list)

    error: Optional[str] = None


# ─────────────────────────────────────────────────────────────────────────────
#  Static lookup tables
# ─────────────────────────────────────────────────────────────────────────────

# RAL code (as string, no leading zeros stripped) → English catalog name
RAL_TO_NAME: Dict[str, str] = {
    # Whites / creams
    "9010": "white",       "9016": "white",       "9003": "white",
    "9001": "cream",       "1013": "cream",       "1015": "cream",
    # Greys
    "7035": "light grey",  "7044": "light grey",  "7047": "light grey",
    "7016": "anthracite",  "7021": "anthracite",  "7024": "anthracite",
    "7015": "grey",        "7037": "grey",
    # Blacks
    "9005": "black",       "9011": "black",
    # Beiges / browns
    "1019": "beige",       "1014": "beige",       "1001": "beige",
    "8017": "dark brown",  "8016": "dark brown",  "8019": "dark brown",
    # Blues
    "5015": "blue",        "5014": "blue",        "5024": "blue",
    # Greens
    "6003": "olive green", "6005": "green",       "6018": "green",
    # Reds
    "3020": "red",         "3005": "dark red",
    # Yellows
    "1021": "yellow",      "1023": "yellow",
}

# Free-text colour synonyms (NL / FR / EN) → English catalog name
COLOR_SYNONYMS: Dict[str, str] = {
    # Dutch
    "wit": "white",        "gebroken wit": "cream",  "creme": "cream",
    "crème": "cream",      "grijs": "grey",           "lichtgrijs": "light grey",
    "donkergrijs": "anthracite", "antraciet": "anthracite", "zwart": "black",
    "beige": "beige",      "zilver": "silver",        "blauw": "blue",
    "rood": "red",         "groen": "green",          "bruin": "brown",
    "donkerbruin": "dark brown", "ivoor": "cream",
    # French
    "blanc": "white",      "noir": "black",           "gris": "grey",
    "gris clair": "light grey", "gris fonce": "anthracite",
    "gris foncé": "anthracite", "anthracite": "anthracite",
    "bleu": "blue",        "rouge": "red",            "vert": "green",
    "brun": "brown",       "ivoire": "cream",
    # English
    "ivory": "cream",      "off-white": "cream",      "charcoal": "anthracite",
    "gray": "grey",        "silver": "silver",        "cream": "cream",
}

# Finish normalisation map
FINISH_MAP: Dict[str, str] = {
    "mat": "Mat",       "matt": "Mat",       "matte": "Mat",    "mat pvc": "Mat",
    "satijn": "Satin",  "satin": "Satin",    "satiné": "Satin",
    "glans": "Gloss",   "gloss": "Gloss",    "glossy": "Gloss",
    "brillant": "Gloss", "brillante": "Gloss",
}

# Common area (m²) → best rectangular (length, width) pair
AREA_TABLE: Dict[int, Tuple[float, float]] = {
    6:  (2.0, 3.0),  8:  (2.0, 4.0),  9:  (3.0, 3.0),
    10: (2.0, 5.0),  12: (3.0, 4.0),  15: (3.0, 5.0),
    16: (4.0, 4.0),  18: (3.0, 6.0),  20: (4.0, 5.0),
    24: (4.0, 6.0),  25: (5.0, 5.0),  28: (4.0, 7.0),
    30: (5.0, 6.0),  31: (5.0, 6.2),  32: (4.0, 8.0),
    35: (5.0, 7.0),  36: (6.0, 6.0),  40: (5.0, 8.0),
    45: (5.0, 9.0),  50: (5.0, 10.0),
}


# ─────────────────────────────────────────────────────────────────────────────
#  Processor
# ─────────────────────────────────────────────────────────────────────────────

class EmailQuoteProcessor:
    """
    Full pipeline: email body ──► EmailQuoteResult.

    Usage:
        processor = EmailQuoteProcessor(db_manager)
        result = await processor.process(
            email_body=body,
            sender_email="customer@example.com",
            sender_name="Martijn Dijkman",
            subject="New Entry: Request quote - Belgium",
        )
    """

    # ── AI system prompt ──────────────────────────────────────────────────────
    SYSTEM_PROMPT = """You are a stretch ceiling quote parser for STRETCH BV, a Belgian company.
Parse customer emails / contact-form submissions and return ONLY a valid JSON object.
No markdown, no code fences, no explanation — raw JSON only.

OUTPUT STRUCTURE:
{
  "is_quote_request": true/false,
  "is_wall_request": true/false,
  "has_enough_info": true/false,
  "language": "nl|fr|en",
  "confidence": 0.0-1.0,
  "ceilings": [
    {
      "name": "room name or Ceiling 1",
      "length": <float or null>,
      "width": <float or null>,
      "area_only": <float or null>,
      "ceiling_type": "fabric|pvc|null",
      "type_ceiling": "standard|acoustic|translucent|printed|null",
      "color": "<english name, RAL XXXX, NCS code, or null>",
      "finish": "mat|satin|gloss|null",
      "acoustic": false,
      "corners": 4,
      "has_seams": false,
      "seam_length": 0.0,
      "lights": [],
      "is_wall": false,
      "special_profile": null
    }
  ],
  "assumptions": [
    {
      "field": "field_name",
      "user_said": "exact text from email",
      "assumed": "value we will use",
      "reason": "short explanation",
      "confidence": "high|medium|low",
      "question": "polite clarifying question for the customer"
    }
  ],
  "missing_required": [],
  "custom_colors": [],
  "contact_info": {
    "name": null,
    "email": null,
    "phone": null,
    "address": null,
    "is_company": false,
    "company_type": null
  }
}

PARSING RULES:

DIMENSIONS
- "5x4", "5 bij 4", "5 par 4", "5m x 4m"   → length=5, width=4
- "31 vierkante meter", "31m2", "31 m²"      → area_only=31
- "6,00 de longueur x 2,60 de hauteur"       → WALL: is_wall_request=true, length=6.0, width=2.60
- Comma decimals: "6,00" = 6.0

CEILING TYPE
- "spanplafond", "spanband", "plafond tendu", "stretch ceiling" → fabric, standard
- "pvc", "glans", "brillant"                                    → pvc
- "mat pvc"                                                     → pvc, finish=mat
- "akoestisch", "acoustic", "acoustique"                        → fabric, acoustic=true
- default when unspecified                                      → fabric, standard

COLORS
- Return as English name when obvious (wit→white, zwart→black, grijs→grey)
- RAL codes as "RAL XXXX" e.g. "RAL 1019"
- NCS codes as "NCS <code>"
- Multiple RAL colors: create one ceiling per color OR note all in one ceiling

PROFILES
- "schaduwvoeg", "shadow joint", "faux joint" → special_profile="shadow_joint"

LIGHTS
- When customer mentions lights, spots, inbouwspots, luminaires, opbouwspots,
  downlights, surface mounted lights — capture them in the lights array as objects:
  {"type": "spot", "quantity": <int or 1 if unspecified>}
  {"type": "surface_mounted", "quantity": <int or 1 if unspecified>}
- "spot", "inbouwspot", "spots", "downlight", "inbouw" → type="spot"
- "opbouwspot", "surface mounted", "carré", "square light" → type="surface_mounted"
- Always return lights as a list of objects with "type" and "quantity" keys
- If quantity not mentioned, default to 1
- ROOM ASSIGNMENT: If lights are mentioned for a specific room (e.g. "4 spots in de woonkamer"),
  add them to THAT ceiling's lights array only.
- If lights are mentioned WITHOUT a specific room (e.g. "I want spots" or "met inbouwspots"),
  add them to the FIRST ceiling's lights array AND set "lights_room_unspecified": true on
  that ceiling so the processor knows to ask the customer which room(s).
- If there is only ONE ceiling total, always assign lights to it regardless.
- CORRECTION CONTEXT: When parsing a message that starts with "CUSTOMER CORRECTION",
  any NEW lights added that don't mention a specific room MUST set
  "lights_room_unspecified": true — do NOT copy lights to every ceiling from the
  original request. The correction adds lights as a new product line, not per ceiling.

L-SHAPED ROOMS
- "L-vormige woonkamer" with one area → TWO ceiling objects, split ~60/40
  Name them "Woonkamer deel 1" and "Woonkamer deel 2"
  Add assumption documenting the split

MULTIPLE APARTMENTS
- "3 appartementen" with one spec → THREE identical ceiling objects
  Name them "Appartement 1", "Appartement 2", "Appartement 3"

CLIENT TYPE
- "Personne privée", "particulier", "privé person"  → is_company=false
- "Professionnel", "bedrijf", "aannemer"             → is_company=true
- "Andere" or ambiguous                              → is_company=false

NOT ENOUGH INFO
- has_enough_info=false when ONLY contact details, no project description
- has_enough_info=false for "Calculator" subject with empty body

FORM SUBJECTS
- "New Entry: Request quote"  → is_quote_request=true
- "New Entry: Contact"        → check body; often a quote request
- "New Entry: Calculator"     → check body; often just contact info"""

    # ── Pre-filter patterns ───────────────────────────────────────────────────
    QUOTE_PATTERNS = [
        r'\d+\s*[xX×*]\s*\d+',
        r'\d+[\.,]?\d*\s*m[²2]',
        r'\d+\s*vierkante?\s*meter',
        r'\d+\s*m[eè]tres?\s*carr[eé]s?',
        r'\b(quote|offerte|devis|prijsopgave|plafond|plafon|ceiling|stretch|'
        r'stof|fabric|pvc|spanplafond|renovati|project|woonkamer|slaapkamer|'
        r'keuken|bureau|salon|appartement|chambre|cuisine|wand|mur|wall)\b',
        r'New Entry:',
    ]
    AUTORESPONSE_PATTERNS = [
        r'out of office',          r'automatisch antwoord',
        r'r[eé]ponse automatique', r'auto.?reply',
        r'delivery status',        r'undeliverable',
        r'mailer.?daemon',         r'mail delivery failed',
        r'postmaster',             r'do not reply',
        r'niet beantwoorden',      r'ne pas r[eé]pondre',
    ]

    def __init__(self, db_manager):
        self.db = db_manager
        self.calculator = CostCalculator(db_manager)
        self.ai_client: Optional[AzureOpenAI] = None
        self._init_ai()

    def _init_ai(self):
        try:
            if all([
                Config.AZURE_OPENAI_API_KEY,
                Config.AZURE_OPENAI_ENDPOINT,
                Config.DEPLOYMENT_NAME,
                Config.AZURE_OPENAI_API_VERSION,
            ]):
                self.ai_client = AzureOpenAI(
                    api_key=Config.AZURE_OPENAI_API_KEY,
                    api_version=Config.AZURE_OPENAI_API_VERSION,
                    azure_endpoint=Config.AZURE_OPENAI_ENDPOINT,
                )
                logger.info("✅ EmailQuoteProcessor: AI client ready")
            else:
                logger.warning("⚠️ EmailQuoteProcessor: Azure OpenAI config incomplete")
        except Exception as e:
            logger.error(f"❌ EmailQuoteProcessor._init_ai: {e}")

    # ─────────────────────────────────────────────────────────────────────────
    #  Public entry point
    # ─────────────────────────────────────────────────────────────────────────

    async def process(
        self,
        email_body: str,
        sender_email: str,
        sender_name: str,
        subject: str = "",
        client_group: str = "price_b2c",
    ) -> EmailQuoteResult:
        """
        Run the full pipeline for one inbound email.
        Always returns EmailQuoteResult — check .success and .error.
        """
        result = EmailQuoteResult(
            success=False,
            sender_email=sender_email,
            sender_name=sender_name,
            language="nl",
        )

        try:
            full_text = f"{subject} {email_body}"

            # 1. Pre-filter: auto-responses
            if self._is_autoresponse(full_text):
                result.error = "autoresponse"
                return result

            # 2. Pre-filter: quote-related content?
            if not self._looks_like_quote(full_text):
                result.error = "not_a_quote_request"
                return result

            # 3. AI extraction
            logger.info(f"📧 Processing: '{subject}' from {sender_email}")
            ai = self._call_ai(email_body, subject)
            if not ai:
                result.error = "ai_extraction_failed"
                return result

            if not ai.get("is_quote_request", True):
                result.error = "not_a_quote_request"
                return result

            result.language         = ai.get("language", "nl")
            result.confidence_score = float(ai.get("confidence", 0.5))
            result.is_wall_request  = bool(ai.get("is_wall_request", False))
            result.missing_fields   = list(ai.get("missing_required", []))
            result.custom_color_codes = list(ai.get("custom_colors", []))

            # 4. Not enough info → qualification-only, no PDF
            if not ai.get("has_enough_info", True) or not ai.get("ceilings"):
                result.is_qualification_only = True
                result.success = True
                logger.info(f"📧 Qualification-only (no project info): {sender_email}")
                return result

            # 5. Better sender name from parsed contact block
            contact = ai.get("contact_info", {})
            if contact.get("name") and not sender_name:
                result.sender_name = contact["name"]
                sender_name = contact["name"]

            # 6. Upgrade to B2B if form says company
            if contact.get("is_company") and client_group == "price_b2c":
                client_group = "price_b2b_reseller"
                logger.info(f"📧 B2B detected → price_b2b_reseller for {sender_email}")

            # 7. Parse Assumption objects
            result.assumptions = [
                Assumption(**a)
                for a in ai.get("assumptions", [])
                if all(k in a for k in (
                    "field", "user_said", "assumed", "reason", "confidence", "question"
                ))
            ]

            # 8. Resolve each ceiling against the DB
            resolved_ceilings: List[Dict] = []
            ceiling_costs: List[Dict] = []
            quote_lights: List[Dict] = []   # lights not assigned to a specific room
            total_ceilings = len([c for c in ai.get("ceilings", []) if isinstance(c, dict)])

            for raw_c in ai.get("ceilings", []):
                # Guard: AI occasionally returns a string instead of a dict
                if not isinstance(raw_c, dict):
                    logger.warning(f"⚠️ Skipping non-dict ceiling entry: {raw_c!r}")
                    continue
                resolved, extra_assumptions, missing, ql = self._resolve_ceiling(
                    raw_c, client_group, ceiling_count=total_ceilings
                )
                result.assumptions.extend(extra_assumptions)
                quote_lights.extend(ql)
                for m in missing:
                    if m not in result.missing_fields:
                        result.missing_fields.append(m)

                # Track custom RAL/NCS colors
                raw_color = raw_c.get("color", "")
                if raw_color and re.match(r'(?i)(ral|ncs)\s*\S+', raw_color):
                    result.needs_custom_color = True
                    if raw_color not in result.custom_color_codes:
                        result.custom_color_codes.append(raw_color)

                resolved_ceilings.append(resolved)

                config = self._dict_to_config(resolved)
                cost: CeilingCost = self.calculator.calculate_ceiling_costs(
                    config, client_group
                )
                ceiling_costs.append(cost.to_dict())

            total_price = sum(c.get("total", 0.0) for c in ceiling_costs)

            # ── Deduplicate lights: only when the AI flagged lights_room_unspecified.
            # "4 spots per ruimte" → intentional per-ceiling, don't dedup.
            # "I want spots" (no room mentioned) → AI copies to all → do dedup.
            if total_ceilings > 1 and resolved_ceilings:
                any_unspecified = any(
                    bool(c.get("lights_room_unspecified", False))
                    for c in resolved_ceilings
                )
                all_have_lights = all(
                    len(c.get("lights", [])) > 0 for c in resolved_ceilings
                )
                if all_have_lights and any_unspecified:
                    logger.info(
                        f"💡 Lights dedup: unspecified lights on all {total_ceilings} ceilings "
                        f"→ promoting to quote-level product lines"
                    )
                    seen_codes = set()
                    for i, ceiling in enumerate(resolved_ceilings):
                        for light in ceiling.get("lights", []):
                            code = light.get("product_code", "")
                            qty  = int(light.get("quantity", 1))
                            key  = (code, qty)
                            if key not in seen_codes:
                                seen_codes.add(key)
                                quote_lights.append(light)
                        resolved_ceilings[i]["lights"] = []
                        resolved_ceilings[i]["lights_room_unspecified"] = False
                        if i < len(ceiling_costs) and isinstance(ceiling_costs[i], dict):
                            lights_cost = float(ceiling_costs[i].get("lights_cost", 0))
                            if lights_cost:
                                old_total = float(ceiling_costs[i].get("total", 0))
                                ceiling_costs[i]["total"]       = old_total - lights_cost
                                ceiling_costs[i]["lights_cost"] = 0

                    total_price = (
                        sum(float(c.get("total", 0)) for c in ceiling_costs)
                        + sum(
                            float(l.get("price", 0)) * int(l.get("quantity", 1))
                            for l in quote_lights
                        )
                    )

            # 9. Build session_data in the same shape as quote_flow.py
            ref = (
                ai.get("ceilings", [{}])[0].get("name")
                or re.sub(r'^New Entry:\s*', '', subject).strip()
                or "Email Quote"
            )
            session_data = {
                "user_id":          None,          # email flow — no Telegram user
                "client_group":     client_group,
                "ceilings":         resolved_ceilings,
                "ceiling_costs":    ceiling_costs,
                "quote_lights":     quote_lights,   # lights not tied to a specific room
                "quote_reference":  ref,
                "email":            sender_email,
                "state":            "completed",
                "source":           "email",
                "contact_info":     contact,
                "assumptions_made": [asdict(a) for a in result.assumptions],
                "indicative_note": (
                    "⚠️ INDICATIEVE OFFERTE — gebaseerd op aangenomen waarden. "
                    "Definitieve prijs na bevestiging van afmetingen en specificaties."
                    if result.assumptions else ""
                ),
            }

            result.session_data = session_data
            result.total_price  = total_price
            result.success      = True

            logger.info(
                f"✅ EmailQuoteProcessor done: {len(resolved_ceilings)} ceiling(s), "
                f"€{total_price:.2f}, conf={result.confidence_score:.2f}, "
                f"assumptions={len(result.assumptions)}"
            )

        except Exception as e:
            logger.error(f"❌ EmailQuoteProcessor.process error: {e}", exc_info=True)
            result.error = str(e)

        return result

    # ─────────────────────────────────────────────────────────────────────────
    #  AI call
    # ─────────────────────────────────────────────────────────────────────────

    def _call_ai(self, body: str, subject: str) -> Optional[Dict]:
        """Send body + subject to Azure OpenAI, return parsed dict or None."""
        if not self.ai_client:
            logger.error("❌ AI client not initialised — check Azure OpenAI config")
            return None
        try:
            clean = re.sub(r'<[^>]+>', ' ', body)      # strip HTML tags
            clean = re.sub(r'\s+', ' ', clean).strip()
            user_msg = f"Subject: {subject}\n\n{clean}"

            resp = self.ai_client.chat.completions.create(
                model=Config.DEPLOYMENT_NAME,
                messages=[
                    {"role": "system", "content": self.SYSTEM_PROMPT},
                    {"role": "user",   "content": user_msg},
                ],
                temperature=0.1,
                max_tokens=1600,
            )
            raw = resp.choices[0].message.content.strip()
            # Strip accidental markdown fences
            raw = re.sub(r'^```json\s*', '', raw)
            raw = re.sub(r'^```\s*',     '', raw)
            raw = re.sub(r'\s*```$',     '', raw)
            return json.loads(raw)

        except json.JSONDecodeError as e:
            logger.error(f"❌ AI response not valid JSON: {e}")
        except Exception as e:
            logger.error(f"❌ AI call failed: {e}")
        return None

    # ─────────────────────────────────────────────────────────────────────────
    #  Ceiling resolution against DB
    # ─────────────────────────────────────────────────────────────────────────

    def _resolve_ceiling(
        self, raw: Dict, client_group: str, ceiling_count: int = 1
    ) -> Tuple[Dict, List[Assumption], List[str], List[Dict]]:
        """
        Validate one AI-extracted ceiling dict against the live product database.
        Returns (resolved_dict, new_assumptions, missing_required_fields).
        """
        assumptions: List[Assumption] = []
        missing:     List[str]        = []

        # ── Dimensions ────────────────────────────────────────────────────────
        length    = raw.get("length")
        width     = raw.get("width")
        area_only = raw.get("area_only")

        if length and width:
            length, width = float(length), float(width)
            area      = round(length * width, 2)
            perimeter = round(2 * (length + width), 2)
        elif area_only:
            area_only = float(area_only)
            length, width = self._area_to_dims(area_only)
            area      = round(length * width, 2)
            perimeter = round(2 * (length + width), 2)
            assumptions.append(Assumption(
                field="dimensions",
                user_said=f"{area_only} m²",
                assumed=f"{length}m × {width}m",
                reason="Most common rectangle for this area",
                confidence="medium",
                question=(
                    f"We assumed {length}m × {width}m from the {area_only}m² "
                    f"you mentioned. Can you confirm the exact length and width?"
                ),
            ))
        else:
            missing.append("dimensions")
            length, width, area, perimeter = 4.0, 5.0, 20.0, 18.0
            assumptions.append(Assumption(
                field="dimensions",
                user_said="(not provided)",
                assumed="4m × 5m (placeholder)",
                reason="No dimensions found in the message",
                confidence="low",
                question=(
                    "We could not find dimensions in your message. "
                    "What are the exact length and width (or area in m²)?"
                ),
            ))

        # ── Ceiling type ──────────────────────────────────────────────────────
        ceiling_type = (raw.get("ceiling_type") or "fabric").lower().strip()
        if ceiling_type not in ("fabric", "pvc"):
            orig = ceiling_type
            ceiling_type = "fabric"
            assumptions.append(Assumption(
                field="ceiling_type",
                user_said=orig,
                assumed="fabric",
                reason="Could not match to fabric or pvc; defaulting to fabric",
                confidence="low",
                question="We defaulted to a fabric ceiling. Would you prefer PVC?",
            ))
        elif not raw.get("ceiling_type"):
            assumptions.append(Assumption(
                field="ceiling_type",
                user_said="(not specified)",
                assumed="fabric",
                reason="Fabric is the most common stretch ceiling material",
                confidence="high",
                question="We assumed a fabric ceiling. Would you prefer PVC instead?",
            ))

        # ── Type ceiling (subcategory) ─────────────────────────────────────────
        type_ceiling = (raw.get("type_ceiling") or "standard").lower().strip()
        db_types = self.db.get_type_ceilings_for_product_type(ceiling_type) or ["standard"]
        type_ceiling = self._fuzzy_match(type_ceiling, db_types, "standard")

        acoustic = bool(raw.get("acoustic", False))
        if acoustic and "acoustic" in db_types:
            type_ceiling = "acoustic"

        # ── Color ─────────────────────────────────────────────────────────────
        raw_color = (raw.get("color") or "").strip()
        db_colors = self.db.get_colors_for_type_ceiling(ceiling_type, type_ceiling) or ["white"]
        resolved_color, color_assumption, _ = self._resolve_color(raw_color, db_colors)
        if color_assumption:
            assumptions.append(color_assumption)

        # ── Finish ────────────────────────────────────────────────────────────
        raw_finish = (raw.get("finish") or "mat").lower().strip()
        finish = FINISH_MAP.get(raw_finish, "Mat")
        if not raw.get("finish"):
            assumptions.append(Assumption(
                field="finish",
                user_said="(not specified)",
                assumed="Mat",
                reason="Mat is the most popular finish",
                confidence="high",
                question="We assumed a mat finish. Would you prefer satin or gloss?",
            ))

        # ── Auto-seam when any dimension > 5m ─────────────────────────────────
        has_seams   = bool(raw.get("has_seams", False))
        seam_length = float(raw.get("seam_length", 0.0))
        if not has_seams and (length > 5.0 or width > 5.0):
            has_seams   = True
            seam_length = max(length, width)
            assumptions.append(Assumption(
                field="seams",
                user_said="(not mentioned)",
                assumed=f"1 seam of {seam_length:.1f}m",
                reason="Fabric panels are max 5m wide; a seam join is required",
                confidence="high",
                question=(
                    f"A panel join (seam) of approx. {seam_length:.0f}m is needed "
                    f"for this room size — already included in the price."
                ),
            ))

        # ── Perimeter profile ──────────────────────────────────────────────────
        special_profile   = raw.get("special_profile")
        perimeter_profile = self._get_perimeter_profile(special_profile, client_group)

        # ── Lights — resolve against DB ───────────────────────────────────────
        raw_lights       = raw.get("lights", [])
        room_unspecified = bool(raw.get("lights_room_unspecified", False))
        ceiling_lights, quote_lights_from_ceiling, light_assumptions = self._resolve_lights(
            raw_lights, client_group,
            room_unspecified=room_unspecified,
            ceiling_count=ceiling_count,
        )
        assumptions.extend(light_assumptions)

        resolved = {
            "name":                      raw.get("name") or "Ceiling 1",
            "length":                    length,
            "width":                     width,
            "area":                      area,
            "perimeter":                 perimeter,
            "perimeter_edited":          False,
            "corners":                   int(raw.get("corners", 4)),
            "ceiling_type":              ceiling_type,
            "type_ceiling":              type_ceiling,
            "color":                     resolved_color,
            "finish":                    finish,
            "acoustic":                  acoustic,
            "acoustic_performance":      None,
            "acoustic_product":          None,
            "perimeter_profile":         perimeter_profile,
            "has_seams":                 has_seams,
            "seam_length":               seam_length,
            "lights":                    ceiling_lights,
            "lights_room_unspecified":   room_unspecified,
            "wood_structures":           [],
            "is_wall":                   bool(raw.get("is_wall", False)),
            "special_profile_requested": special_profile,
        }
        return resolved, assumptions, missing, quote_lights_from_ceiling

    # ─────────────────────────────────────────────────────────────────────────
    #  Lights resolver
    # ─────────────────────────────────────────────────────────────────────────

    # Maps light type keywords → search terms for DB description lookup
    LIGHT_TYPE_MAP = {
        "spot":            ["recessed", "inbouw", "spot", "76mm", "downlight"],
        "surface_mounted": ["surface", "opbouw", "200x200", "300x300"],
    }

    def _resolve_lights(
        self, raw_lights: list, client_group: str,
        room_unspecified: bool = False,
        ceiling_count: int = 1,
    ) -> Tuple[list, List[Assumption]]:
        """
        Resolve AI-extracted light entries against the 'light' base_category
        in the products DB.

        raw_lights can be:
          - list of dicts: [{"type": "spot", "quantity": 4}]   ← ideal AI output
          - list of strings: ["spot", "4 spots"]              ← fallback
          - empty list                                         ← no lights mentioned

        room_unspecified: True when AI set lights_room_unspecified=true, meaning
          the customer mentioned lights but didn't say which room. We include them
          in the quote but add a clarifying assumption.

        ceiling_count: total number of ceilings in the quote. Used to phrase
          the clarifying question correctly.

        Returns (resolved_light_list, assumptions).
        Each resolved light dict has the shape expected by CostCalculator:
          {
            "product_id":   int,
            "product_code": str,
            "description":  str,
            "quantity":     int,
            "price":        float,
            "price_b2c":    float,
            "unit":         str,
          }
        """
        if not raw_lights:
            return [], [], []

        # Fetch all active light structure products from DB once
        try:
            price_col = client_group if client_group.startswith("price_") else "price_b2c"
            db_lights = self.db.execute_query(
                "SELECT * FROM products WHERE base_category='light' AND is_active=1",
                fetch=True,
            ) or []
        except Exception as e:
            logger.warning(f"⚠️ Could not fetch light products from DB: {e}")
            return [], [], []

        if not db_lights:
            logger.warning("⚠️ No active light products found in DB")
            return [], [], []

        ceiling_lights  = []   # lights assigned to this specific ceiling
        quote_lights    = []   # lights NOT assigned to a room → extra product line
        assumptions     = []

        for entry in raw_lights:
            # Normalise entry to (light_type, quantity)
            if isinstance(entry, dict):
                light_type = str(entry.get("type", "spot")).lower()
                quantity   = int(entry.get("quantity", 1))
            elif isinstance(entry, str):
                qty_m      = re.search(r'(\d+)', entry)
                quantity   = int(qty_m.group(1)) if qty_m else 1
                entry_l    = entry.lower()
                if any(k in entry_l for k in ["surface", "opbouw", "carré"]):
                    light_type = "surface_mounted"
                else:
                    light_type = "spot"
            else:
                continue

            # Match light_type to a DB product
            product = self._match_light_product(light_type, db_lights)

            if product:
                price = float(product.get(price_col) or product.get("price_b2c") or 0)
                light_dict = {
                    "product_id":   product["id"],
                    "product_code": product["product_code"],
                    "description":  product["description"],
                    "quantity":     quantity,
                    "price":        price,
                    "price_b2c":    float(product.get("price_b2c") or 0),
                    "unit":         product.get("unit", "pcs"),
                }
                logger.info(
                    f"💡 Light resolved: {light_type} × {quantity} "
                    f"→ {product['product_code']} ({product['description']})"
                )

                if room_unspecified and ceiling_count > 1:
                    # No room specified + multiple ceilings →
                    # extra product line at quote level, not inside a ceiling
                    quote_lights.append(light_dict)
                    assumptions.append(Assumption(
                        field="lights_room",
                        user_said=f"{quantity}× {light_type}",
                        assumed="extra product line (room not specified)",
                        reason="Lights mentioned but no specific room specified",
                        confidence="low",
                        question=(
                            f"You mentioned {quantity}× {product['description']} but didn't "
                            f"specify which room(s). We've listed them as a separate product line. "
                            f"Please let us know which room(s) the lights are for so we can "
                            f"assign them correctly."
                        ),
                    ))
                else:
                    # Room specified, or only one ceiling → assign to this ceiling
                    ceiling_lights.append(light_dict)

            else:
                assumptions.append(Assumption(
                    field="lights",
                    user_said=str(entry),
                    assumed="(not matched)",
                    reason=f"Could not find a matching light structure for '{light_type}'",
                    confidence="low",
                    question=(
                        f"You mentioned lights ({entry}). "
                        f"We could not match this to a standard light structure. "
                        f"Please specify: recessed spot (76mm) or "
                        f"surface mounted (200×200 / 300×300)?"
                    ),
                ))

        return ceiling_lights, quote_lights, assumptions

    def _match_light_product(self, light_type: str, db_lights: list) -> Optional[Dict]:
        """
        Match a normalised light_type string to the best DB product.
        Uses keyword scoring against product description.
        """
        keywords = self.LIGHT_TYPE_MAP.get(light_type, [light_type])
        best_product = None
        best_score   = 0

        for p in db_lights:
            desc = (p.get("description") or "").lower()
            code = (p.get("product_code") or "").lower()
            combined = desc + " " + code
            score = sum(1 for kw in keywords if kw.lower() in combined)
            if score > best_score:
                best_score   = score
                best_product = p

        return best_product if best_score > 0 else None

    # ─────────────────────────────────────────────────────────────────────────
    #  Color resolver
    # ─────────────────────────────────────────────────────────────────────────

    def _resolve_color(
        self, input_color: str, available_colors: List[str]
    ) -> Tuple[str, Optional[Assumption], bool]:
        """
        Map input_color to the best available DB color.
        Returns (resolved_color, assumption_or_None, is_custom_ral_ncs).
        """
        avail_lower = [c.lower() for c in available_colors]
        default     = "white" if "white" in avail_lower else available_colors[0]

        # No color given
        if not input_color:
            return default, Assumption(
                field="color",
                user_said="(not specified)",
                assumed=default,
                reason="No color mentioned; used default",
                confidence="high",
                question=f"We assumed {default} as the ceiling color. What color would you prefer?",
            ), False

        inp = input_color.lower().strip()

        # RAL code
        ral_m = re.match(r'(?i)ral\s*(\d{3,4})', input_color)
        if ral_m:
            ral_num = ral_m.group(1).zfill(4)
            name = RAL_TO_NAME.get(ral_num)
            if name:
                resolved, _, _ = self._resolve_color(name, available_colors)
                return resolved, Assumption(
                    field="color",
                    user_said=f"RAL {ral_num}",
                    assumed=resolved,
                    reason=f"RAL {ral_num} corresponds to '{name}' in our catalog",
                    confidence="medium",
                    question=(
                        f"RAL {ral_num} ({name}) — matched to our '{resolved}' option. "
                        f"For an exact RAL match, custom production is possible (±6 weeks)."
                    ),
                ), False
            else:
                return default, Assumption(
                    field="color",
                    user_said=f"RAL {ral_num}",
                    assumed=default,
                    reason=f"RAL {ral_num} is not in our standard color range",
                    confidence="low",
                    question=(
                        f"RAL {ral_num} is not a standard catalog color. "
                        f"Custom production is possible (±6 weeks, price on request). "
                        f"Would you like a custom order, or choose from our standard colors?"
                    ),
                ), True

        # NCS code
        if re.match(r'(?i)ncs', input_color):
            return default, Assumption(
                field="color",
                user_said=input_color,
                assumed=default,
                reason="NCS codes need manual conversion to a catalog color",
                confidence="low",
                question=(
                    f"The NCS code '{input_color}' needs converting. "
                    f"Could you provide the RAL code or describe the color?"
                ),
            ), True

        # Synonym map
        syn = COLOR_SYNONYMS.get(inp)
        if syn:
            resolved, a, c = self._resolve_color(syn, available_colors)
            if a:
                a.user_said = input_color
            return resolved, a, c

        # Exact match (case-insensitive)
        if inp in avail_lower:
            return available_colors[avail_lower.index(inp)], None, False

        # Fuzzy string match
        m = get_close_matches(inp, avail_lower, n=1, cutoff=0.65)
        if m:
            resolved = available_colors[avail_lower.index(m[0])]
            return resolved, Assumption(
                field="color",
                user_said=input_color,
                assumed=resolved,
                reason=f"Closest catalog match to '{input_color}'",
                confidence="medium",
                question=f"We matched '{input_color}' to our '{resolved}' — does that work?",
            ), False

        # No match → use default
        return default, Assumption(
            field="color",
            user_said=input_color,
            assumed=default,
            reason=f"'{input_color}' not found in catalog; using {default}",
            confidence="low",
            question=(
                f"We couldn't match '{input_color}' in our catalog. "
                f"We used '{default}' as a placeholder — please tell us your preferred color."
            ),
        ), False

    # ─────────────────────────────────────────────────────────────────────────
    #  Helpers
    # ─────────────────────────────────────────────────────────────────────────

    def _area_to_dims(self, area: float) -> Tuple[float, float]:
        """Return (length, width) for a given area using lookup table."""
        closest = min(AREA_TABLE.keys(), key=lambda k: abs(k - area))
        if abs(closest - area) <= 3.0:
            return AREA_TABLE[closest]
        # Fallback: nearest rectangle rounded to 0.5m
        side = area ** 0.5
        w = round(side * 2) / 2
        l = round((area / max(w, 0.5)) * 2) / 2
        return (max(l, w), min(l, w))

    def _fuzzy_match(self, value: str, options: List[str], default: str) -> str:
        lo = [o.lower() for o in options]
        if value.lower() in lo:
            return options[lo.index(value.lower())]
        m = get_close_matches(value.lower(), lo, n=1, cutoff=0.4)
        if m:
            return options[lo.index(m[0])]
        dl = default.lower()
        return options[lo.index(dl)] if dl in lo else (options[0] if options else default)

    def _get_perimeter_profile(
        self, special_profile: Optional[str], client_group: str
    ) -> Optional[Dict]:
        """Fetch the appropriate perimeter profile product from DB."""
        try:
            price_col = client_group if client_group.startswith("price_") else "price_b2c"
            if special_profile == "shadow_joint":
                rows = self.db.execute_query(
                    """SELECT * FROM products WHERE base_category='perimeter'
                       AND (LOWER(description) LIKE '%shadow%'
                            OR LOWER(description) LIKE '%schaduw%'
                            OR LOWER(description) LIKE '%faux joint%')
                       AND is_active=1 LIMIT 1""",
                    fetch=True,
                )
                if rows:
                    p = rows[0]
                    return {
                        "product_id":   p["id"],
                        "product_code": p["product_code"],
                        "description":  p["description"],
                        "price":        float(p.get(price_col, p.get("price_b2c", 0))),
                    }
            # Default: cheapest active perimeter profile
            rows = self.db.execute_query(
                "SELECT * FROM products WHERE base_category='perimeter' "
                "AND is_active=1 ORDER BY price_b2c ASC LIMIT 1",
                fetch=True,
            )
            if rows:
                p = rows[0]
                return {
                    "product_id":   p["id"],
                    "product_code": p["product_code"],
                    "description":  p["description"],
                    "price":        float(p.get(price_col, p.get("price_b2c", 0))),
                }
        except Exception as e:
            logger.warning(f"⚠️ perimeter profile lookup failed: {e}")
        return None

    def _dict_to_config(self, d: Dict) -> CeilingConfig:
        """Convert a resolved ceiling dict to a CeilingConfig dataclass."""
        return CeilingConfig(
            name=d["name"],
            length=d["length"],
            width=d["width"],
            area=d["area"],
            perimeter=d["perimeter"],
            perimeter_edited=d.get("perimeter_edited", False),
            corners=d.get("corners", 4),
            ceiling_type=d.get("ceiling_type", "fabric"),
            type_ceiling=d.get("type_ceiling", "standard"),
            color=d.get("color", "white"),
            finish=d.get("finish", "Mat"),
            acoustic=d.get("acoustic", False),
            perimeter_profile=d.get("perimeter_profile"),
            has_seams=d.get("has_seams", False),
            seam_length=d.get("seam_length", 0.0),
            lights=d.get("lights", []),
            wood_structures=d.get("wood_structures", []),
        )

    def _is_autoresponse(self, text: str) -> bool:
        tl = text.lower()
        return any(re.search(p, tl) for p in self.AUTORESPONSE_PATTERNS)

    def _looks_like_quote(self, text: str) -> bool:
        tl = text.lower()
        return any(re.search(p, tl, re.IGNORECASE) for p in self.QUOTE_PATTERNS)

    def get_client_group_for_email(self, email: str) -> str:
        """Look up client_group by email; falls back to price_b2c."""
        try:
            rows = self.db.execute_query(
                "SELECT client_group FROM users WHERE email=%s LIMIT 1",
                (email,), fetch=True,
            )
            if rows:
                return rows[0].get("client_group", "price_b2c")
        except Exception as e:
            logger.warning(f"⚠️ client_group lookup failed for {email}: {e}")
        return "price_b2c"
