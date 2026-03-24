"""
email_listener.py
STRETCH Bot — Email Listener  v1.4

CHANGE v1.4:
  - actionable statuses now includes 'confirmed' so customers can
    continue replying after a site_visit/acceptance was acknowledged
  - _handle_follow_up keeps status as 'awaiting_reply' (not 'confirmed')
    so subsequent messages are still matched to the session
  - _strip_quoted_reply: fixed HTML detection (was incorrectly routing
    plain-text emails with <email@address> to the HTML path); improved
    'On ... wrote:' pattern to reliably catch Gmail plain-text dividers

Background asyncio task that drives the full email-quote pipeline:

  1. Polls leads@stretchgroup.be inbox via Microsoft Graph API
  2. Routes each message: new request OR correction reply
  3. Calls EmailQuoteProcessor for AI parsing + cost calculation
  4. Saves quotation to DB via manager.py methods
  5. Generates PDF via existing pdf_generator.py
  6. Sends branded HTML reply with PDF via Graph API sendMail
  7. Manages state in email_quote_sessions table

Polling schedule:
  Business hours Mon-Sat 07:00-19:00 Brussels → every 2 min
  Off-hours                                    → every 15 min

Wire into bot.py startup_tasks():
    from email_listener import EmailListener
    self.email_listener = EmailListener(
        db_manager=self.db,
        pdf_gen=self.pdf_generator,
        d365_service=getattr(self.dynamics_integration, 'dynamics_service', None),
    )
    # Alternative if bot already instantiates without d365_service:
    # self.email_listener.set_d365_service(self.dynamics_integration.dynamics_service)
    self.email_listener_task = asyncio.create_task(self.email_listener.start())

Imports (services/mail/ package):
    from config import Config                                      ← root level
    from .email_quote_processor import EmailQuoteProcessor, ...   ← same package
    from .email_reply_builder import EmailReplyBuilder             ← same package
    from ..pdf_generator import ImprovedStretchQuotePDFGenerator  ← parent services/
"""

import asyncio
import base64
import json
import logging
import os
import re
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import aiohttp

from config import Config
from .email_quote_processor import EmailQuoteProcessor, EmailQuoteResult, Assumption
from .email_reply_builder import EmailReplyBuilder

logger = logging.getLogger(__name__)

# ── Timezone helper (Python 3.9+ has zoneinfo; fall back to pytz for 3.8) ───
try:
    from zoneinfo import ZoneInfo
    BRUSSELS = ZoneInfo("Europe/Brussels")
except ImportError:
    import pytz
    BRUSSELS = pytz.timezone("Europe/Brussels")


class EmailListener:
    """
    Polls the STRETCH leads mailbox and drives the full email-quote pipeline.

    The listener READS from Config.LEADS_MAILBOX and SENDS replies
    FROM Config.EMAIL_FROM using the same app-only Azure credentials
    as the existing EntraIDEmailSender.
    """

    GRAPH = "https://graph.microsoft.com/v1.0"

    def __init__(self, db_manager, pdf_gen=None, d365_service=None):
        self.db        = db_manager
        self.processor = EmailQuoteProcessor(db_manager)
        self.builder   = EmailReplyBuilder()
        self.d365      = d365_service

        # Bot token + admin IDs for Telegram push notifications
        self._bot_token  = Config.BOT_TOKEN
        self._admin_ids  = Config.ADMIN_USER_IDS  # list of int

        # Accept an existing pdf_gen instance, or create one locally
        if pdf_gen is not None:
            self.pdf_gen = pdf_gen
        else:
            from ..pdf_generator import ImprovedStretchQuotePDFGenerator
            self.pdf_gen = ImprovedStretchQuotePDFGenerator(
                output_dir=Config.PDF_OUTPUT_DIR,
                logo_path=Config.COMPANY_LOGO_PATH,
            )

        self._token: Optional[str] = None
        self._token_expiry: datetime = datetime.utcnow()
        self._running: bool = False

    def set_d365_service(self, d365_service) -> None:
        """Wire in the Dynamics 365 service after instantiation."""
        self.d365 = d365_service
        if d365_service:
            logger.info("✅ EmailListener: Dynamics 365 service connected for CRM sync")

    # ─────────────────────────────────────────────────────────────────────────
    #  Start / stop
    # ─────────────────────────────────────────────────────────────────────────

    async def start(self):
        """
        Start the polling loop.
        Call as: asyncio.create_task(self.email_listener.start())
        """
        self._running = True
        logger.info(f"📬 EmailListener started — polling {Config.LEADS_MAILBOX}")
        await self._verify_session_table()

        while self._running:
            try:
                await self._poll_cycle()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"❌ EmailListener poll error: {e}", exc_info=True)

            interval = self._poll_interval()
            logger.debug(f"📬 Next poll in {interval}s")

            # Run follow-up scheduler roughly once per hour (every 30 polls)
            self._followup_tick = getattr(self, '_followup_tick', 0) + 1
            if self._followup_tick >= 30:
                self._followup_tick = 0
                await self._followup_cycle()

            # Run session expiry once per day (every 1440 polls at 60s interval)
            self._expiry_tick = getattr(self, '_expiry_tick', 0) + 1
            if self._expiry_tick >= 1440:
                self._expiry_tick = 0
                await self._expire_old_sessions()

            await asyncio.sleep(interval)

        logger.info("📬 EmailListener stopped")

    def stop(self):
        """Signal the loop to stop after the current cycle."""
        self._running = False

    # ─────────────────────────────────────────────────────────────────────────
    #  Poll cycle
    # ─────────────────────────────────────────────────────────────────────────

    async def _poll_cycle(self):
        token = await self._get_token()
        if not token:
            logger.warning("⚠️ EmailListener: no token — skipping cycle")
            return

        messages = await self._fetch_unread(token)
        if not messages:
            return

        logger.info(f"📬 {len(messages)} unread message(s) found")
        for msg in messages:
            try:
                await self._handle_message(msg, token)
            except Exception as e:
                logger.error(f"❌ handle_message error: {e}", exc_info=True)
            finally:
                # Always mark as read to prevent re-processing
                await self._mark_read(msg["id"], token)

    # ─────────────────────────────────────────────────────────────────────────
    #  Message routing
    # ─────────────────────────────────────────────────────────────────────────

    async def _handle_message(self, msg: Dict, token: str):
        """
        Route one inbound message: new quote request OR customer follow-up reply.

        Session matching uses two strategies:
          1. Graph conversationId  — works when reply stays in same thread
          2. QT number in subject  — fallback for replies from different clients
             (mobile, forwarded, different email app) that start a new thread
        """
        message_id      = msg["id"]
        conversation_id = msg.get("conversationId", "")
        sender_addr     = msg.get("from", {}).get("emailAddress", {}).get("address", "")
        sender_name     = msg.get("from", {}).get("emailAddress", {}).get("name", sender_addr)
        subject         = msg.get("subject", "")
        body            = msg.get("body", {}).get("content", "")
        received_str    = msg.get("receivedDateTime", "")

        # Loop guard: skip our own addresses
        own_addrs = {Config.EMAIL_FROM.lower(), Config.LEADS_MAILBOX.lower()}
        if sender_addr.lower() in own_addrs:
            logger.debug(f"📬 Skipping own address: {sender_addr}")
            return

        # Skip system / no-reply / NDR senders
        if self._is_system_sender(sender_addr, subject, body):
            logger.info(f"📬 Skipping system email from {sender_addr}")
            return

        try:
            received_at = datetime.fromisoformat(received_str.replace("Z", "+00:00"))
        except Exception:
            received_at = datetime.now(timezone.utc)

        # ── Strategy 1: match by Graph conversationId ─────────────────────────
        existing = self.db.get_email_session_by_conversation(conversation_id)

        # ── Strategy 2: fallback — match by QT number in subject ─────────────
        if not existing:
            qt_match = re.search(r'QT[A-Z0-9]{8,16}', subject)
            if qt_match:
                qt_number = qt_match.group(0)
                existing  = self.db.get_email_session_by_quote_number(qt_number)
                if existing:
                    logger.info(
                        f"📧 Session matched by QT number {qt_number} "
                        f"(new thread from {sender_addr})"
                    )

        # ── Strategy 3: fallback — match by sender email (most recent open session)
        # Catches replies where customer starts a new email without QT number
        if not existing:
            existing = self._get_latest_open_session_by_email(sender_addr)
            if existing:
                logger.info(
                    f"📧 Session matched by sender email {sender_addr} "
                    f"→ session {existing['id']} ({existing.get('quote_number', 'no QT')})"
                )

        actionable = ("quote_sent", "awaiting_reply", "revised", "confirmed", "campaign_sent")
        if existing and existing.get("status") in actionable:
            # Strip quoted history from the body so AI only reads the NEW text
            clean_body = self._strip_quoted_reply(body)
            logger.info(
                f"📧 Follow-up reply for session {existing['id']} "
                f"from {sender_addr} — intent will be classified"
            )
            await self._handle_correction(existing, clean_body, subject, token)
        else:
            logger.info(f"📧 New request from {sender_addr}: '{subject}'")
            await self._handle_new_request(
                message_id=message_id,
                conversation_id=conversation_id,
                sender_email=sender_addr,
                sender_name=sender_name,
                subject=subject,
                body=body,
                received_at=received_at,
                token=token,
            )

    # ─────────────────────────────────────────────────────────────────────────
    #  New request pipeline
    # ─────────────────────────────────────────────────────────────────────────

    async def _handle_new_request(
        self,
        message_id: str,
        conversation_id: str,
        sender_email: str,
        sender_name: str,
        subject: str,
        body: str,
        received_at: datetime,
        token: str,
    ):
        # Look up client_group using the forwarding address (may already be known)
        client_group = self.processor.get_client_group_for_email(sender_email)

        # Insert session row (status = 'processing')
        # Store forwarding address for now; we update it below once we have the real email
        session_id = self.db.create_email_session(
            conversation_id=conversation_id,
            message_id=message_id,
            sender_email=sender_email,
            sender_name=sender_name,
            client_group=client_group,
            original_message=body,
            received_at=received_at,
        )
        if not session_id:
            logger.error(f"❌ Could not create email_quote_session for {sender_email}")
            return

        # Run AI processor
        result: EmailQuoteResult = await self.processor.process(
            email_body=body,
            sender_email=sender_email,
            sender_name=sender_name,
            subject=subject,
            client_group=client_group,
        )

        # Auto-response or irrelevant email
        if result.error in ("autoresponse", "not_a_quote_request"):
            self.db.update_email_session(session_id, {"status": "spam"})
            return

        # Processing failure
        if not result.success:
            self.db.update_email_session(session_id, {
                "status":        "failed",
                "error_message": result.error or "unknown error",
            })
            logger.error(f"❌ Processing failed for {sender_email}: {result.error}")
            return

        # ── Resolve actual customer email / name from parsed contact block ────
        # Website forms forward via marketing@stretchplafonds.be but embed the
        # real customer email (e.g. martijn.dijkman@gmail.com) in the body.
        # The AI extracts it into contact_info.email — use that when available.
        actual_email, actual_name = self._extract_customer_contact(
            result, sender_email, sender_name
        )

        # If we found a better email, look up client_group again & log it
        if actual_email != sender_email:
            better_group = self.processor.get_client_group_for_email(actual_email)
            if better_group != "price_b2c":
                client_group = better_group
            logger.info(
                f"📧 Resolved customer: {sender_email} → {actual_email} ({actual_name})"
            )

        # ── Qualification only: not enough project info, send info-request ────
        if result.is_qualification_only:
            subject_out, html_body = self.builder.build_qualification(
                sender_name=actual_name,
                language=result.language,
            )
            sent = await self._send_reply(
                to_email=actual_email,
                subject=subject_out,
                html_body=html_body,
                pdf_path=None,
                quote_number="",
                token=token,
            )
            self.db.update_email_session(session_id, {
                "status":        "quote_sent" if sent else "failed",
                "sender_email":  actual_email,
                "sender_name":   actual_name,
                "processed_at":  datetime.utcnow(),
                "quote_sent_at": datetime.utcnow() if sent else None,
                "language":      result.language,
                "error_message": None if sent else "qualification send failed",
            })
            logger.info(
                f"{'✅' if sent else '❌'} Qualification email "
                f"{'sent' if sent else 'FAILED'} → {actual_email}"
            )
            return

        # ── Full quote ────────────────────────────────────────────────────────
        quote_id = self.db.save_quotation(
            user_id=0,
            quote_data=result.session_data,
            total_price=result.total_price,
            client_group=client_group,
        )
        quote_number = self._get_quote_number(quote_id)
        result.quote_number = quote_number

        # Generate PDF with actual customer details
        pdf_path = self._generate_pdf(result, actual_name, actual_email)
        result.pdf_path = pdf_path

        # Build reply HTML addressed to actual customer
        subject_out, html_body = self.builder.build_initial_reply(
            sender_name=actual_name,
            quote_number=quote_number,
            total_price=result.total_price,
            session_data=result.session_data,
            assumptions=result.assumptions,
            missing_fields=result.missing_fields,
            language=result.language,
            confidence_score=result.confidence_score,
            is_wall=result.is_wall_request,
            needs_custom_color=result.needs_custom_color,
            custom_color_codes=result.custom_color_codes,
        )

        sent = await self._send_reply(
            to_email=actual_email,
            subject=subject_out,
            html_body=html_body,
            pdf_path=pdf_path,
            quote_number=quote_number,
            token=token,
        )

        self.db.update_email_session(session_id, {
            "status":           "quote_sent" if sent else "failed",
            "sender_email":     actual_email,   # store real customer email
            "sender_name":      actual_name,    # store real customer name
            "quotation_id":     quote_id,
            "quote_number":     quote_number,
            "pdf_path":         pdf_path,
            "total_price":      result.total_price,
            "parsed_data":      json.dumps(result.session_data, default=str),
            "assumed_data":     json.dumps(
                [asdict(a) for a in result.assumptions], default=str
            ),
            "missing_fields":   json.dumps(result.missing_fields),
            "language":         result.language,
            "confidence_score": result.confidence_score,
            "processed_at":     datetime.utcnow(),
            "quote_sent_at":    datetime.utcnow() if sent else None,
            "error_message":    None if sent else "sendMail failed",
        })

        logger.info(
            f"{'✅' if sent else '❌'} Email quote "
            f"{'sent' if sent else 'FAILED'} → {actual_email} ({actual_name}) | "
            f"{quote_number} | €{result.total_price:.2f}"
        )

        # ── Telegram admin notification ───────────────────────────────────────
        if sent:
            ceilings    = result.session_data.get("ceilings", [])
            room_lines  = "\n".join(
                f"  • {c.get('name', '?')} — {c.get('length')}×{c.get('width')}m"
                for c in ceilings[:6]
            )
            if len(ceilings) > 6:
                room_lines += f"\n  • … (+{len(ceilings)-6} more)"
            asyncio.create_task(self._notify_admin(
                f"📬 <b>New lead received</b>\n"
                f"👤 {actual_name} ({actual_email})\n"
                f"🧾 {quote_number} | €{result.total_price:,.2f}\n"
                f"🏠 {len(ceilings)} ceiling(s):\n{room_lines}\n"
                f"🌐 Language: {result.language or 'nl'}"
            ))

        # ── Dynamics 365 sync ─────────────────────────────────────────────────
        if sent and self.d365:
            asyncio.create_task(
                self._sync_to_dynamics365(
                    actual_email=actual_email,
                    actual_name=actual_name,
                    result=result,
                    quote_id=quote_id,
                    quote_number=quote_number,
                    pdf_path=pdf_path,
                    client_group=client_group,
                )
            )

    # ─────────────────────────────────────────────────────────────────────────
    #  Correction / follow-up reply pipeline
    # ─────────────────────────────────────────────────────────────────────────

    async def _handle_correction(
        self, session: Dict, body: str, subject: str, token: str
    ):
        """
        Route a customer reply based on intent:
          - acceptance / site visit / more info request
              → send acknowledgment to customer + forward to info@
          - spec correction (dimensions, color, etc.)
              → generate revised quote as before
        """
        session_id   = session["id"]
        language     = session.get("language", "nl")
        client_group = session.get("client_group", "price_b2c")
        revision     = session.get("revision_count", 0) + 1
        actual_email = session.get("sender_email", "")
        actual_name  = session.get("sender_name", "")
        quote_number = session.get("quote_number", "")
        pdf_path     = session.get("pdf_path")      # latest PDF already on disk

        # ── Strip signature once — used for all downstream processing ─────────
        body_clean = self._strip_signature(body)
        if body_clean != body:
            logger.info(f"✂️ Signature stripped: {len(body)} → {len(body_clean)} chars")

        # ── Detect intent ─────────────────────────────────────────────────────
        intent = self._classify_reply_intent(body_clean)
        logger.info(f"📧 Reply intent from {actual_email}: '{intent}'")

        if intent in ("acceptance", "site_visit", "more_info"):
            await self._handle_follow_up(
                session_id=session_id,
                actual_email=actual_email,
                actual_name=actual_name,
                language=language,
                quote_number=quote_number,
                pdf_path=pdf_path,
                body=body_clean,
                intent=intent,
                token=token,
            )
            return

        if intent == "no_interest":
            await self._handle_no_interest(
                session_id=session_id,
                actual_email=actual_email,
                actual_name=actual_name,
                language=language,
                quote_number=quote_number,
                token=token,
            )
            return

        if intent == "timeframe":
            await self._handle_timeframe(
                session_id=session_id,
                actual_email=actual_email,
                actual_name=actual_name,
                language=language,
                quote_number=quote_number,
                pdf_path=pdf_path,
                body=body_clean,
                token=token,
            )
            return

        # ── Spec correction → revised quote ───────────────────────────────────
        import json as _json
        latest_data = None
        try:
            raw = session.get("parsed_data")
            if raw:
                latest_data = _json.loads(raw) if isinstance(raw, str) else raw
        except Exception:
            latest_data = None

        if latest_data:
            # ── Fast path: carry forward ceilings from DB ─────────────────────
            result = EmailQuoteResult(
                success=True,
                sender_email=actual_email,
                sender_name=actual_name,
                language=language,
            )
            result.session_data    = dict(latest_data)
            result.assumptions     = []
            result.is_wall_request = False

            # ── AI-powered unified correction parser ──────────────────────────
            corrections = await self._parse_corrections_ai(
                body_clean, result.session_data, client_group
            )

            if corrections:
                import copy
                ceilings       = copy.deepcopy(result.session_data.get("ceilings", []))
                ceiling_costs  = list(result.session_data.get("ceiling_costs", []))
                recalc_indices = set()
                remove_indices = set()

                for patch in corrections.get("ceiling_patches", []):
                    idx   = patch.get("ceiling_idx", -1)
                    field = patch.get("field", "")
                    val   = patch.get("value")

                    # Guard against out-of-bounds index from AI
                    if idx < 0 or idx >= len(ceilings):
                        logger.warning(
                            f"⚠️ Ceiling patch idx={idx} out of range "
                            f"(have {len(ceilings)} ceilings) — skipping"
                        )
                        continue

                    if field == "remove":
                        remove_indices.add(idx)
                    elif field == "dimensions":
                        ceilings[idx]["length"]    = float(val[0])
                        ceilings[idx]["width"]     = float(val[1])
                        ceilings[idx]["area"]      = round(float(val[0]) * float(val[1]), 4)
                        ceilings[idx]["perimeter"] = round(2 * (float(val[0]) + float(val[1])), 4)
                        recalc_indices.add(idx)
                    else:
                        ceilings[idx][field] = val
                        recalc_indices.add(idx)
                    logger.info(
                        f"✏️ Ceiling patch: {ceilings[idx].get('name')} "
                        f"{field}={val}"
                    )

                # Apply removals
                if remove_indices:
                    removed_names = [ceilings[i].get("name") for i in remove_indices]
                    ceilings      = [c for i, c in enumerate(ceilings) if i not in remove_indices]
                    ceiling_costs = [c for i, c in enumerate(ceiling_costs) if i not in remove_indices]
                    recalc_indices = {
                        i - sum(1 for r in remove_indices if r < i)
                        for i in recalc_indices if i not in remove_indices
                    }
                    logger.info(f"🗑️ Removed ceiling(s): {removed_names}")

                # Recalculate costs for patched ceilings
                if recalc_indices:
                    from models import CeilingConfig
                    from services.cost_calculator import CostCalculator
                    calc = CostCalculator(self.db)
                    for idx in recalc_indices:
                        c   = ceilings[idx]
                        cfg = CeilingConfig(
                            name         = c.get("name", f"Ceiling {idx+1}"),
                            length       = float(c.get("length") or 0),
                            width        = float(c.get("width") or 0),
                            ceiling_type = c.get("ceiling_type", "fabric"),
                            type_ceiling = c.get("type_ceiling", "standard"),
                            color        = c.get("color", "white"),
                            lights       = c.get("lights", []),
                            seam_length  = float(c.get("seam_length") or 0),
                        )
                        cfg.calculate_dimensions()
                        new_cost  = calc.calculate_ceiling_costs(cfg, client_group)
                        cost_dict = new_cost.to_dict() if hasattr(new_cost, "to_dict") else {
                            "ceiling":   float(getattr(new_cost, "ceiling", 0)),
                            "perimeter": float(getattr(new_cost, "perimeter", 0)),
                            "corners":   float(getattr(new_cost, "corners", 0)),
                            "seams":     float(getattr(new_cost, "seams", 0)),
                            "lights":    float(getattr(new_cost, "lights", 0)),
                            "total":     float(getattr(new_cost, "total", 0)),
                        }
                        while len(ceiling_costs) <= idx:
                            ceiling_costs.append({})
                        ceiling_costs[idx] = cost_dict
                        logger.info(f"💰 Recalculated {c.get('name')}: €{cost_dict.get('total', 0):.2f}")

                result.session_data["ceilings"]      = ceilings
                result.session_data["ceiling_costs"] = ceiling_costs

                # Add new ceilings
                if corrections.get("ceilings_add"):
                    from models import CeilingConfig
                    from services.cost_calculator import CostCalculator
                    calc      = CostCalculator(self.db)
                    price_col = client_group if client_group.startswith("price_") else "price_b2c"
                    db_lights = self.processor.db.execute_query(
                        "SELECT * FROM products WHERE base_category='light' AND is_active=1",
                        params=None, fetch=True,
                    ) or []
                    for new_c in corrections["ceilings_add"]:
                        # Resolve lights inside the new ceiling
                        ceiling_lights = []
                        for light in new_c.get("lights", []):
                            match_type = "surface_mounted" if "surface" in light.get("type", "") else "spot"
                            product    = self.processor._match_light_product(match_type, db_lights)
                            if product:
                                price = float(product.get(price_col) or product.get("price_b2c") or 0)
                                ceiling_lights.append({
                                    "product_id":   product["id"],
                                    "product_code": product["product_code"],
                                    "description":  product["description"],
                                    "quantity":     int(light.get("quantity", 1)),
                                    "price":        price,
                                    "price_b2c":    float(product.get("price_b2c") or 0),
                                    "unit":         product.get("unit", "pcs"),
                                })
                        cfg = CeilingConfig(
                            name         = new_c.get("name", "New Room"),
                            length       = float(new_c.get("length", 3)),
                            width        = float(new_c.get("width", 3)),
                            ceiling_type = new_c.get("ceiling_type", "fabric"),
                            type_ceiling = new_c.get("type_ceiling", "standard"),
                            color        = new_c.get("color", "white"),
                            lights       = ceiling_lights,
                        )
                        cfg.calculate_dimensions()
                        new_cost  = calc.calculate_ceiling_costs(cfg, client_group)
                        cost_dict = new_cost.to_dict() if hasattr(new_cost, "to_dict") else {
                            "ceiling":   float(getattr(new_cost, "ceiling", 0)),
                            "perimeter": float(getattr(new_cost, "perimeter", 0)),
                            "corners":   float(getattr(new_cost, "corners", 0)),
                            "seams":     float(getattr(new_cost, "seams", 0)),
                            "lights":    float(getattr(new_cost, "lights", 0)),
                            "total":     float(getattr(new_cost, "total", 0)),
                        }
                        ceilings.append({
                            "name":         cfg.name,
                            "length":       cfg.length,
                            "width":        cfg.width,
                            "area":         round(cfg.length * cfg.width, 4),
                            "perimeter":    round(2 * (cfg.length + cfg.width), 4),
                            "ceiling_type": cfg.ceiling_type,
                            "type_ceiling": cfg.type_ceiling,
                            "color":        cfg.color,
                            "lights":       ceiling_lights,
                        })
                        ceiling_costs.append(cost_dict)
                        logger.info(
                            f"➕ AI: added new ceiling '{cfg.name}' "
                            f"{cfg.length}×{cfg.width}m → €{cost_dict.get('total', 0):.2f}"
                        )
                    result.session_data["ceilings"]      = ceilings
                    result.session_data["ceiling_costs"] = ceiling_costs

                # Apply quote-level light changes
                existing_ql = [l for l in result.session_data.get("quote_lights", []) if l]
                by_code = {l.get("product_code"): i for i, l in enumerate(existing_ql)}

                for light in corrections.get("lights_add", []):
                    code = light.get("product_code")
                    if code in by_code:
                        existing_ql[by_code[code]]["quantity"] = (
                            int(existing_ql[by_code[code]].get("quantity", 0)) +
                            int(light.get("quantity", 0))
                        )
                    else:
                        existing_ql.append(light)
                    logger.info(f"💡 AI: add {light.get('quantity')} × {code}")

                for removal in corrections.get("lights_remove", []):
                    code = removal.get("product_code")
                    qty_remove = int(removal.get("quantity", 0))
                    for i, ql in enumerate(existing_ql):
                        if ql and ql.get("product_code") == code:
                            current = int(ql.get("quantity", 0))
                            if qty_remove >= current:
                                existing_ql[i] = None
                                logger.info(f"💡 AI: removed {code} row")
                            else:
                                existing_ql[i]["quantity"] = current - qty_remove
                                logger.info(f"💡 AI: reduced {code} → {current - qty_remove}")
                            break

                result.session_data["quote_lights"] = [l for l in existing_ql if l]

            # Recalculate total
            ceiling_costs = result.session_data.get("ceiling_costs", [])
            result.total_price = sum(float(c.get("total", 0)) for c in ceiling_costs)
            for ql in result.session_data.get("quote_lights", []):
                result.total_price += float(ql.get("price", 0)) * int(ql.get("quantity", 1))

            logger.info(
                f"📋 Correction fast-path: "
                f"{len(result.session_data.get('ceilings', []))} ceiling(s), "
                f"total=€{result.total_price:.2f}"
            )

            # ── Escalation: no changes detected → ask customer to clarify ────
            # If AI parsed zero changes, we don't understand the request.
            # Rather than silently resending the unchanged quote, send a
            # clarification request so the customer knows we received their email.
            no_changes = (
                not corrections or (
                    not corrections.get("ceiling_patches") and
                    not corrections.get("ceilings_add") and
                    not corrections.get("lights_add") and
                    not corrections.get("lights_remove")
                )
            )
            if no_changes:
                logger.warning(
                    f"⚠️ No corrections detected for session {session_id} "
                    f"— sending clarification request"
                )
                await self._send_clarification_request(
                    actual_email=actual_email,
                    actual_name=actual_name,
                    language=language,
                    quote_number=quote_number,
                    body_clean=body_clean,
                    token=token,
                )
                return
        else:
            # ── First correction: no previous data, use AI to parse ───────────
            original = session.get("original_message", "")
            combined = (
                f"ORIGINAL REQUEST:\n{original}\n\n"
                f"CUSTOMER CORRECTION (reply #{revision}):\n{body}"
            )
            result: EmailQuoteResult = await self.processor.process(
                email_body=combined,
                sender_email=actual_email,
                sender_name=actual_name,
                subject=subject,
                client_group=client_group,
            )
            if not result.success or result.is_qualification_only:
                logger.error(f"❌ Correction re-parse failed for session {session_id}")
                return

        quote_id     = self.db.save_quotation(
            user_id=0,
            quote_data=result.session_data,
            total_price=result.total_price,
            client_group=client_group,
        )
        quote_number = session.get("quote_number") or self._get_quote_number(quote_id)
        remaining    = [a for a in result.assumptions if a.confidence in ("low", "medium")]
        new_pdf      = self._generate_pdf(result, actual_name, actual_email)

        subject_out, html_body = self.builder.build_revised_reply(
            sender_name=actual_name,
            quote_number=quote_number,
            total_price=result.total_price,
            session_data=result.session_data,
            remaining_assumptions=remaining,
            language=language,
            revision_number=revision,
            is_wall=result.is_wall_request,
        )

        sent = await self._send_reply(
            to_email=actual_email,
            subject=subject_out,
            html_body=html_body,
            pdf_path=new_pdf,
            quote_number=quote_number,
            token=token,
        )

        self.db.update_email_session(session_id, {
            "status":         "revised" if sent else "failed",
            "quotation_id":   quote_id,
            "pdf_path":       new_pdf,
            "total_price":    result.total_price,
            "revision_count": revision,
            "parsed_data":    json.dumps(result.session_data, default=str),
            "quote_sent_at":  datetime.utcnow() if sent else None,  # restart follow-up timer
            "followup_1_sent_at": None,   # reset so follow-ups fire from new quote date
            "followup_2_sent_at": None,
            "error_message":  None if sent else "revised sendMail failed",
        })

        logger.info(
            f"{'✅' if sent else '❌'} Revised quote #{revision} "
            f"{'sent' if sent else 'FAILED'} → {actual_email}"
        )

        # ── D365 sync: update existing quote or create if first revision ──────
        if sent and self.d365:
            asyncio.create_task(
                self._sync_revised_quote_to_dynamics365(
                    session=session,
                    actual_email=actual_email,
                    actual_name=actual_name,
                    result=result,
                    quote_id=quote_id,
                    quote_number=quote_number,
                    pdf_path=new_pdf,
                    client_group=client_group,
                )
            )

    # ─────────────────────────────────────────────────────────────────────────
    #  Follow-up handler (acceptance / site visit / more info)
    # ─────────────────────────────────────────────────────────────────────────

    async def _handle_no_interest(
        self,
        session_id:    int,
        actual_email:  str,
        actual_name:   str,
        language:      str,
        quote_number:  str,
        token:         str,
    ):
        """Customer is not interested — stop all follow-ups, send polite closing."""
        name = actual_name.split()[0] if actual_name else ""
        if language == "fr":
            subject  = f"Merci pour votre réponse – {quote_number}"
            html_msg = (
                f"<p>Bonjour {name},</p>"
                f"<p>Merci de nous avoir informés. Nous prenons bonne note "
                f"et n'enverrons plus de rappels concernant ce devis.</p>"
                f"<p>N'hésitez pas à nous recontacter si vos besoins évoluent.</p>"
            )
        else:
            subject  = f"Bedankt voor uw reactie – {quote_number}"
            html_msg = (
                f"<p>Beste {name},</p>"
                f"<p>Bedankt voor uw reactie. We nemen dit ter kennis en "
                f"zullen u geen verdere herinneringen sturen voor deze offerte.</p>"
                f"<p>Mocht u in de toekomst toch interesse hebben, staan we "
                f"uiteraard voor u klaar.</p>"
            )

        from .email_reply_builder import EmailReplyBuilder as _ERB
        wrap = self.builder._wrap(language, quote_number, html_msg)
        await self._send_reply(
            to_email=actual_email,
            subject=subject,
            html_body=wrap,
            pdf_path=None,
            quote_number=quote_number,
            token=token,
        )

        self.db.update_email_session(session_id, {
            "status":       "awaiting_reply",
            "no_followup":  1,
            "followup_notes": "Customer indicated no interest",
        })
        logger.info(f"🚫 No-interest flagged for session {session_id} — follow-ups stopped")

    async def _handle_timeframe(
        self,
        session_id:    int,
        actual_email:  str,
        actual_name:   str,
        language:      str,
        quote_number:  str,
        pdf_path:      Optional[str],
        body:          str,
        token:         str,
    ):
        """Customer gave a future timeframe — schedule personalised follow-up."""
        name           = actual_name.split()[0] if actual_name else ""
        scheduled_date = self._extract_timeframe_date(body)
        note           = body[:200].strip()

        if language == "fr":
            subject  = f"Bien noté – Nous vous recontacterons – {quote_number}"
            html_msg = (
                f"<p>Bonjour {name},</p>"
                f"<p>Merci pour votre retour. Nous avons bien pris note de votre "
                f"calendrier et nous vous recontacterons à ce moment-là avec "
                f"votre devis mis à jour.</p>"
                f"<p>Notre offre reste valable. N'hésitez pas à nous contacter "
                f"si vos plans changent.</p>"
            )
        else:
            scheduled_str = scheduled_date.strftime("%d/%m/%Y") if scheduled_date else "de opgegeven periode"
            subject  = f"Begrepen – We nemen contact op rond {scheduled_str} – {quote_number}"
            html_msg = (
                f"<p>Beste {name},</p>"
                f"<p>Bedankt voor uw bericht. We hebben uw planning genoteerd en "
                f"nemen rond <strong>{scheduled_str}</strong> opnieuw contact "
                f"met u op met uw offerte.</p>"
                f"<p>Onze offerte blijft geldig. Aarzel niet om eerder contact "
                f"op te nemen als uw plannen wijzigen.</p>"
            )

        wrap = self.builder._wrap(language, quote_number, html_msg)
        await self._send_reply(
            to_email=actual_email,
            subject=subject,
            html_body=wrap,
            pdf_path=pdf_path,
            quote_number=quote_number,
            token=token,
        )

        update = {
            "status":                "awaiting_reply",
            "followup_notes":        f"Timeframe: {note}",
        }
        if scheduled_date:
            update["followup_scheduled_at"] = scheduled_date
            # Reset standard follow-up counters so they don't fire before scheduled date
            update["followup_1_sent_at"] = scheduled_date  # mark as "used"

        self.db.update_email_session(session_id, update)
        logger.info(
            f"📅 Timeframe follow-up scheduled for session {session_id}: "
            f"{scheduled_date.strftime('%Y-%m-%d') if scheduled_date else 'unknown'}"
        )

    async def _handle_follow_up(
        self,
        session_id: int,
        actual_email: str,
        actual_name: str,
        language: str,
        quote_number: str,
        pdf_path: Optional[str],
        body: str,
        intent: str,
        token: str,
    ):
        """
        Send personalised acknowledgment to customer + forward to info@.
        Both emails carry the latest PDF as attachment.
        """
        # 1. AI-generated personalised acknowledgment to customer
        ack_subject, ack_html = self.builder.build_acknowledgment(
            sender_name=actual_name,
            quote_number=quote_number,
            intent=intent,
            customer_message=body,
            language=language,
        )

        sent_customer = await self._send_reply(
            to_email=actual_email,
            subject=ack_subject,
            html_body=ack_html,
            pdf_path=pdf_path,
            quote_number=quote_number,
            token=token,
        )

        # 2. Forward to info@ with full context for the team
        fwd_subject, fwd_html = self.builder.build_team_forward(
            customer_name=actual_name,
            customer_email=actual_email,
            quote_number=quote_number,
            intent=intent,
            customer_message=body,
            language=language,
        )

        sent_team = await self._send_reply(
            to_email=Config.COMPANY_EMAIL,   # info@stretchgroup.be
            subject=fwd_subject,
            html_body=fwd_html,
            pdf_path=pdf_path,
            quote_number=quote_number,
            token=token,
        )

        self.db.update_email_session(session_id, {
            # Keep awaiting_reply so customer can send further messages
            # (e.g. acceptance after site_visit, or question after acceptance)
            "status":        "awaiting_reply" if sent_customer else "failed",
            "error_message": None if sent_customer else "acknowledgment send failed",
        })

        logger.info(
            f"{'✅' if sent_customer else '❌'} Acknowledgment ({intent}) "
            f"→ {actual_email} | "
            f"{'✅' if sent_team else '❌'} Team forward → {Config.COMPANY_EMAIL}"
        )

    # ─────────────────────────────────────────────────────────────────────────
    #  Intent classifier
    # ─────────────────────────────────────────────────────────────────────────

    # ─────────────────────────────────────────────────────────────────────────
    #  Extra lights extractor (correction replies)
    # ─────────────────────────────────────────────────────────────────────────

    # Regex patterns that indicate ADDING lights without a specific room
    EXTRA_LIGHTS_PATTERNS = [
        # Dutch: "nog 4 spots", "4 extra inbouwspots", "voeg 4 spots toe"
        r'(?:nog|extra|meer|toevoegen?|voeg.*?toe)\s*(\d+)?\s*'
        r'(spots?|inbouwspots?|downlights?|verlichting|lichten?)',
        r'(\d+)\s*(?:extra|meer|nog)\s*(spots?|inbouwspots?|downlights?)',
        # French: "ajouter 4 spots", "4 spots supplémentaires"
        r'(?:ajouter?|rajouter?|plus\s+de)\s*(\d+)?\s*(spots?|luminaires?|lumières?)',
        r'(\d+)\s*(?:spots?|luminaires?)\s*(?:supplémentaires?|en\s+plus)',
        # English: "add 4 spots", "4 extra spots"
        r'(?:add|extra|more|include)\s*(\d+)?\s*(spots?|downlights?|lights?)',
        r'(\d+)\s*(?:extra|more|additional)\s*(spots?|downlights?|lights?)',
    ]
    SURFACE_KEYWORDS = [
        "opbouw", "opbouwspot",
        "surface mounted", "surface-mounted",
        "suface mounted",    # common typo
        "surfaced mounted",  # another typo
        "surface mount",     # abbreviated
        "200x200", "300x300", "carré",
    ]

    def _extract_extra_lights(
        self, correction_body: str, client_group: str
    ) -> list:
        """
        Scan the correction body for "add X spots/lights" patterns.
        Detects BOTH spot and surface_mounted independently in the same message.
        Returns a list of resolved light dicts, one per detected type.

        Examples:
          "add 4 spots"                        → [spot×4]
          "add 8 surface mounted lights"       → [surface_mounted×8]
          "add 4 spots and 8 surface mounted"  → [spot×4, surface_mounted×8]
        """
        # Only scan line 1 — signature and quoted history always appear after
        lines = [l.strip() for l in correction_body.splitlines() if l.strip()]
        text = lines[0].lower() if lines else ""
        logger.info(f"🔍 _extract_extra_lights: checking body '{text[:120]}'")

        # Require an add signal somewhere in the line
        add_signal = any(kw in text for kw in [
            "extra", "nog", "meer", "toevoeg", "voeg", "add", "ajouter",
            "supplémentaire", "additional", "more",
        ])
        if not add_signal:
            logger.info("🔍 _extract_extra_lights: no add signal → skip")
            return []

        # Skip if a room keyword is NEAR the add signal (room-specific → AI handles)
        room_keywords = [
            "woonkamer", "slaapkamer", "keuken", "badkamer", "bureau",
            "living", "bedroom", "kitchen", "bathroom", "salon", "chambre",
        ]
        add_pos = next(
            (text.find(kw) for kw in ["extra","nog","meer","toevoeg","add","ajouter"]
             if text.find(kw) >= 0), -1
        )
        if add_pos >= 0:
            window = text[max(0, add_pos - 30): add_pos + 60]
            room_near = [kw for kw in room_keywords if kw in window]
            if room_near:
                logger.info(f"🔍 _extract_extra_lights: room near add {room_near} → skip")
                return []

        # ── Pattern-based detection for each light type ───────────────────────
        # Spot keywords (recessed, inbouw)
        SPOT_KW      = ["spot", "inbouwspot", "downlight", "recessed", "inbouw"]
        SURFACE_KW   = self.SURFACE_KEYWORDS  # opbouw, surface mounted, etc.

        # Regex: optional number BEFORE keyword, or keyword BEFORE number
        # e.g. "4 spots", "spots × 4", "add 4 extra spots"
        NUM_BEFORE = r'(\d+)\s*(?:x|×|stuks?)?\s*(?:{kw})'
        NUM_AFTER  = r'(?:{kw})\s*(?:x|×|stuks?)?\s*(\d+)'

        def find_qty(text, keywords):
            """Find quantity closest to any of the given keywords."""
            for kw in keywords:
                if kw not in text:
                    continue
                # Try number before keyword
                m = re.search(NUM_BEFORE.format(kw=re.escape(kw)), text)
                if m:
                    return int(m.group(1))
                # Try number after keyword
                m = re.search(NUM_AFTER.format(kw=re.escape(kw)), text)
                if m:
                    return int(m.group(1))
                # Keyword found but no adjacent number — default 1
                return 1
            return 0  # keyword not found

        results = []
        price_col = client_group if client_group.startswith("price_") else "price_b2c"

        try:
            db_lights = self.processor.db.execute_query(
                "SELECT * FROM products WHERE base_category='light' AND is_active=1",
                params=None, fetch=True,
            ) or []
            if not db_lights:
                logger.warning("⚠️ _extract_extra_lights: no light products in DB")
                return []

            # ── Check for surface mounted first (more specific keywords) ──────
            # Explicit presence check FIRST — only call find_qty if keyword exists
            if any(kw in text for kw in SURFACE_KW):
                surface_qty = find_qty(text, SURFACE_KW)
                if surface_qty > 0:
                    product = self.processor._match_light_product("surface_mounted", db_lights)
                    if product:
                        price = float(product.get(price_col) or product.get("price_b2c") or 0)
                        results.append({
                            "product_id":   product["id"],
                            "product_code": product["product_code"],
                            "description":  product["description"],
                            "quantity":     surface_qty,
                            "price":        price,
                            "price_b2c":    float(product.get("price_b2c") or 0),
                            "unit":         product.get("unit", "pcs"),
                        })
                        logger.info(
                            f"💡 Extra lights detected: surface_mounted × {surface_qty} "
                            f"→ {product['product_code']}"
                        )

            # ── Check for spots — explicit keyword presence check ─────────────
            if any(kw in text for kw in SPOT_KW):
                spot_qty = find_qty(text, SPOT_KW)
                if spot_qty > 0:
                    product = self.processor._match_light_product("spot", db_lights)
                    if product:
                        price = float(product.get(price_col) or product.get("price_b2c") or 0)
                        results.append({
                            "product_id":   product["id"],
                            "product_code": product["product_code"],
                            "description":  product["description"],
                            "quantity":     spot_qty,
                            "price":        price,
                            "price_b2c":    float(product.get("price_b2c") or 0),
                            "unit":         product.get("unit", "pcs"),
                        })
                        logger.info(
                            f"💡 Extra lights detected: spot × {spot_qty} "
                            f"→ {product['product_code']}"
                        )

            # ── Fallback: generic "X lights" with no type keyword ─────────────
            # "15 lights", "10 lichten" — default to surface_mounted (more common
            # for unspecified ceiling lights in Belgian residential projects)
            if not results:
                generic_kw  = ["light", "lights", "lumière", "lichten", "verlichting"]
                generic_qty = find_qty(text, generic_kw)
                if generic_qty > 0:
                    # Use surface_mounted as default for generic "lights"
                    light_type = "surface_mounted"
                    product = self.processor._match_light_product(light_type, db_lights)
                    if not product:
                        light_type = "spot"
                        product = self.processor._match_light_product("spot", db_lights)
                    if product:
                        price = float(product.get(price_col) or product.get("price_b2c") or 0)
                        results.append({
                            "product_id":   product["id"],
                            "product_code": product["product_code"],
                            "description":  product["description"],
                            "quantity":     generic_qty,
                            "price":        price,
                            "price_b2c":    float(product.get("price_b2c") or 0),
                            "unit":         product.get("unit", "pcs"),
                        })
                        logger.info(
                            f"💡 Extra lights detected: generic({light_type}) × {generic_qty} "
                            f"→ {product['product_code']}"
                        )

        except Exception as e:
            logger.warning(f"⚠️ _extract_extra_lights error: {e}", exc_info=True)

        return results

    # ─────────────────────────────────────────────────────────────────────────
    #  Ceiling correction extractor (fast-path)
    # ─────────────────────────────────────────────────────────────────────────

    # Color name map — Dutch/French/English → normalized English
    COLOR_MAP = {
        # Whites
        "wit": "white", "blanc": "white", "white": "white",
        "warm wit": "warm-white", "warm-wit": "warm-white",
        "warm white": "warm-white", "blanc chaud": "warm-white",
        # Blacks
        "zwart": "black", "noir": "black", "black": "black",
        # RAL/NCS passthrough
    }

    # Type map
    TYPE_MAP = {
        "standaard": "standard", "standard": "standard",
        "akoestisch": "acoustic", "acoustique": "acoustic", "acoustic": "acoustic",
        "akoestisch gekleurd": "acoustic-color", "acoustic color": "acoustic-color",
        "licht": "light", "lumineux": "light", "light": "light",
        "bedrukt": "print", "imprimé": "print", "print": "print",
    }

    def _extract_ceiling_corrections(
        self, body: str, ceilings: list
    ) -> list:
        """
        Parse correction body for dimension, color, or type changes.
        Returns list of patch dicts: {ceiling_idx, field, old_value, new_value}

        Supported patterns:
          Dimensions : "woonkamer 5x4", "slaapkamer 1: 4m x 3m", "maak X naar 5 bij 4"
          Color      : "verander kleur naar zwart", "in wit", "kleur: warm-wit"
          Type       : "akoestisch plafond voor slaapkamer 1"
        """
        text  = body.lower()
        lines = [l.strip() for l in body.splitlines() if l.strip()]
        patches = []

        # Build a room name index for matching
        room_index = {}
        for i, c in enumerate(ceilings):
            name = (c.get("name") or "").lower()
            room_index[name] = i
            # Also index abbreviated names: "slaapkamer 1" → "slaapkamer1"
            room_index[name.replace(" ", "")] = i
            # Index just the base name without number
            base = re.sub(r'\s*\d+$', '', name).strip()
            if base not in room_index:
                room_index[base] = i

        def find_ceiling_idx(text_fragment):
            """Find which ceiling index is referenced in a text fragment."""
            frag = text_fragment.lower()
            # Try longest match first
            for name in sorted(room_index.keys(), key=len, reverse=True):
                if name and name in frag:
                    return room_index[name]
            return None

        # ── Dimension patterns ────────────────────────────────────────────────
        # Patterns: "5x4", "5 x 4", "5 bij 4", "5m x 4m", "5.5 x 3.2"
        DIM_RE = re.compile(
            r'(\d+(?:[.,]\d+)?)\s*(?:m|meter)?\s*(?:x|bij|×|par)\s*'
            r'(\d+(?:[.,]\d+)?)\s*(?:m|meter)?'
        )
        for m in DIM_RE.finditer(text):
            length = float(m.group(1).replace(',', '.'))
            width  = float(m.group(2).replace(',', '.'))
            # Find surrounding context (50 chars before match)
            ctx_start = max(0, m.start() - 60)
            context   = text[ctx_start: m.start() + 20]
            idx = find_ceiling_idx(context)
            if idx is None and len(ceilings) == 1:
                idx = 0  # single ceiling — apply directly
            if idx is not None:
                old_l = ceilings[idx].get("length", 0)
                old_w = ceilings[idx].get("width", 0)
                if abs(length - old_l) > 0.01 or abs(width - old_w) > 0.01:
                    patches.append({
                        "ceiling_idx": idx,
                        "field":       "dimensions",
                        "old_value":   f"{old_l}x{old_w}",
                        "new_value":   (length, width),
                    })
                    logger.info(
                        f"📐 Ceiling correction: {ceilings[idx].get('name')} "
                        f"dimensions {old_l}x{old_w} → {length}x{width}"
                    )

        # ── Color patterns ────────────────────────────────────────────────────
        # "verander kleur naar zwart", "kleur: wit", "in zwart", "zwart plafond"
        COLOR_RE = re.compile(
            r'(?:kleur|couleur|color|colour|naar|to|en|in)\s*:?\s*'
            r'(warm[\s-]?w(?:it|hite)|warm[\s-]?blanc|zwart|blanc|wit|black|white|'
            r'ral\s*\d{4}|ncs\s*\S+)',
            re.IGNORECASE
        )
        for m in COLOR_RE.finditer(text):
            raw_color = m.group(1).strip().lower()
            color = self.COLOR_MAP.get(raw_color, raw_color)
            # RAL/NCS passthrough
            if raw_color.startswith("ral") or raw_color.startswith("ncs"):
                color = raw_color.upper()

            ctx_start = max(0, m.start() - 60)
            context   = text[ctx_start: m.start() + 20]
            idx = find_ceiling_idx(context)

            if idx is not None:
                # Single room color change
                old_color = ceilings[idx].get("color", "")
                if old_color.lower() != color.lower():
                    patches.append({
                        "ceiling_idx": idx,
                        "field":       "color",
                        "old_value":   old_color,
                        "new_value":   color,
                    })
                    logger.info(
                        f"🎨 Ceiling correction: {ceilings[idx].get('name')} "
                        f"color {old_color} → {color}"
                    )
            else:
                # No specific room → apply to ALL ceilings
                for i, c in enumerate(ceilings):
                    old_color = c.get("color", "")
                    if old_color.lower() != color.lower():
                        patches.append({
                            "ceiling_idx": i,
                            "field":       "color",
                            "old_value":   old_color,
                            "new_value":   color,
                        })
                logger.info(f"🎨 Ceiling correction: ALL ceilings color → {color}")

        # ── Type patterns ─────────────────────────────────────────────────────
        # "akoestisch plafond voor slaapkamer 1"
        TYPE_RE = re.compile(
            r'(standaard|standard|akoestisch|acoustique|acoustic|'
            r'akoestisch\s+gekleurd|acoustic\s+color|licht|lumineux|light|'
            r'bedrukt|imprim[eé]|print)',
            re.IGNORECASE
        )
        for m in TYPE_RE.finditer(text):
            raw_type = m.group(1).strip().lower()
            type_ceiling = self.TYPE_MAP.get(raw_type, raw_type)
            ctx_start = max(0, m.start() - 60)
            context   = text[ctx_start: m.start() + 40]
            idx = find_ceiling_idx(context)
            if idx is not None:
                old_type = ceilings[idx].get("type_ceiling", "")
                if old_type.lower() != type_ceiling.lower():
                    patches.append({
                        "ceiling_idx": idx,
                        "field":       "type_ceiling",
                        "old_value":   old_type,
                        "new_value":   type_ceiling,
                    })
                    logger.info(
                        f"🏗️ Ceiling correction: {ceilings[idx].get('name')} "
                        f"type {old_type} → {type_ceiling}"
                    )

        # ── Remove ceiling patterns ───────────────────────────────────────────
        # "verwijder de badkamer", "remove keuken", "supprimer salle de bain"
        REMOVE_RE = re.compile(
            r'(?:verwijder|remove|supprimer|weg|weghalen|laat\s+weg|niet\s+meer|'
            r'scrap|cancel|annuleer)\s+(?:de|het|the|le|la|les)?\s*(.+?)(?:\s*$|,|\.|;)',
            re.IGNORECASE
        )
        for m in REMOVE_RE.finditer(text):
            room_hint = m.group(1).strip()
            idx = find_ceiling_idx(room_hint)
            if idx is not None:
                # Check not already marked for removal
                if not any(
                    p["field"] == "remove" and p["ceiling_idx"] == idx
                    for p in patches
                ):
                    patches.append({
                        "ceiling_idx": idx,
                        "field":       "remove",
                        "old_value":   ceilings[idx].get("name"),
                        "new_value":   None,
                    })
                    logger.info(
                        f"🗑️ Ceiling removal: {ceilings[idx].get('name')} "
                        f"will be removed from quote"
                    )

        return patches

    def _extract_remove_lights(self, body: str, quote_lights: list) -> list:
        """
        Detect requests to REMOVE lights from quote_lights.
        Mirrors _extract_extra_lights logic — uses same keyword/quantity patterns.

        Returns list of {product_code, quantity} to subtract.
        """
        if not quote_lights:
            return []

        lines = [l.strip() for l in body.splitlines() if l.strip()]
        text  = lines[0].lower() if lines else ""

        # Require a remove signal
        remove_signal = any(kw in text for kw in [
            "verwijder", "verwijderen", "delete", "remove", "supprimer",
            "weg", "weghalen", "annuleer", "niet meer", "schrap",
        ])
        if not remove_signal:
            return []

        # Don't fire if it's actually an add request
        if any(kw in text for kw in [
            "toevoeg", "voeg", "add", "ajouter", "extra", "meer", "nog",
        ]):
            return []

        # Same keyword sets as _extract_extra_lights
        SPOT_KW    = ["spot", "inbouwspot", "downlight", "recessed", "inbouw"]
        SURFACE_KW = self.SURFACE_KEYWORDS

        # Same find_qty helper
        NUM_BEFORE = r'(\d+)\s*(?:x|×|stuks?)?\s*(?:{kw})'
        NUM_AFTER  = r'(?:{kw})\s*(?:x|×|stuks?)?\s*(\d+)'

        def find_qty(text, keywords):
            for kw in keywords:
                if kw not in text:
                    continue
                m = re.search(NUM_BEFORE.format(kw=re.escape(kw)), text)
                if m:
                    return int(m.group(1))
                m = re.search(NUM_AFTER.format(kw=re.escape(kw)), text)
                if m:
                    return int(m.group(1))
                # Keyword found but no number → remove 1
                return 1
            return 0  # keyword not found at all

        # Build product code maps from existing quote_lights
        spot_codes    = {
            ql["product_code"] for ql in quote_lights
            if "3250" in ql.get("product_code", "")
            and "32501" not in ql.get("product_code", "")
        }
        surface_codes = {
            ql["product_code"] for ql in quote_lights
            if "32501" in ql.get("product_code", "")
        }

        results = []

        # ── Surface mounted (check first — more specific) ─────────────────────
        if any(kw in text for kw in SURFACE_KW):
            surface_qty = find_qty(text, SURFACE_KW)
            if surface_qty > 0 and surface_codes:
                for code in surface_codes:
                    results.append({"product_code": code, "quantity": surface_qty})
                    logger.info(f"💡 Remove lights detected: surface_mounted × {surface_qty} → {code}")

        # ── Spots — explicit keyword presence check ───────────────────────────
        if any(kw in text for kw in SPOT_KW):
            spot_qty = find_qty(text, SPOT_KW)
            if spot_qty > 0 and spot_codes:
                for code in spot_codes:
                    results.append({"product_code": code, "quantity": spot_qty})
                    logger.info(f"💡 Remove lights detected: spot × {spot_qty} → {code}")

        # ── Generic "X lights/verlichting" with no type keyword ───────────────
        if not results:
            generic_kw  = ["light", "lights", "licht", "lichten", "verlichting", "lumière"]
            generic_qty = find_qty(text, generic_kw)
            if generic_qty > 0:
                for ql in quote_lights:
                    results.append({
                        "product_code": ql["product_code"],
                        "quantity":     generic_qty,
                    })
                    logger.info(
                        f"💡 Remove lights detected: generic × {generic_qty} "
                        f"→ {ql['product_code']}"
                    )

        return results

    # ─────────────────────────────────────────────────────────────────────────
    #  AI-powered light changes extractor
    # ─────────────────────────────────────────────────────────────────────────

    async def _extract_light_changes_ai(
        self,
        body: str,
        client_group: str,
        existing_quote_lights: list,
    ) -> dict:
        """
        Use AI to extract light add/remove requests from any correction email.
        Returns {"add": [...], "remove": [...]} with resolved product dicts.

        Replaces fragile regex approach — handles any language, typos, long emails.
        Falls back to empty result on any error so the quote still processes.
        """
        try:
            # Fetch light products from DB
            price_col = client_group if client_group.startswith("price_") else "price_b2c"
            db_lights = self.processor.db.execute_query(
                "SELECT * FROM products WHERE base_category='light' AND is_active=1",
                params=None, fetch=True,
            ) or []
            if not db_lights:
                return {}

            # Build product summary for AI context
            products_desc = "\n".join(
                f"- {p['product_code']}: {p['description']} "
                f"(type: {'surface_mounted' if '32501' in p['product_code'] else 'recessed_spot'})"
                for p in db_lights
            )

            # Build existing lights context
            existing_desc = ""
            if existing_quote_lights:
                existing_desc = "\nCurrently on quote (quote-level lights):\n" + "\n".join(
                    f"- {l.get('product_code')}: {l.get('quantity')} pcs"
                    for l in existing_quote_lights
                )

            prompt = f"""Analyze this customer email correction and extract ONLY light/spot add or remove requests.
Ignore all other content (ceilings, dimensions, colors, signature, etc.)

Available light products:
{products_desc}
{existing_desc}

Customer email:
{body}

Return ONLY valid JSON, no other text:
{{
  "add": [
    {{"type": "recessed_spot|surface_mounted", "quantity": N}}
  ],
  "remove": [
    {{"type": "recessed_spot|surface_mounted", "quantity": N}}
  ]
}}

Rules:
- "spots", "inbouwspots", "downlights", "recessed" → type: "recessed_spot"
- "opbouw", "surface mounted", "surface-mounted" → type: "surface_mounted"
- "verwijder/delete/remove" → goes in "remove"
- "toevoegen/add/extra/nog" → goes in "add"
- If no light changes mentioned, return {{"add": [], "remove": []}}
- Only include items where the customer EXPLICITLY mentions lights/spots
- Do NOT infer lights from other content"""

            response = self.processor.ai_client.chat.completions.create(
                model=Config.DEPLOYMENT_NAME,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=200,
                temperature=0,
            )

            raw = response.choices[0].message.content.strip()
            # Strip markdown fences if present
            raw = re.sub(r'^```(?:json)?\s*|\s*```$', '', raw, flags=re.MULTILINE).strip()

            import json as _json
            parsed = _json.loads(raw)

            result = {"add": [], "remove": []}

            for action in ["add", "remove"]:
                for item in parsed.get(action, []):
                    light_type = item.get("type", "")
                    qty        = int(item.get("quantity", 1))
                    if qty <= 0:
                        continue

                    # Map type to product
                    match_type = "surface_mounted" if "surface" in light_type else "spot"
                    product = self.processor._match_light_product(match_type, db_lights)
                    if not product:
                        continue

                    price = float(product.get(price_col) or product.get("price_b2c") or 0)
                    result[action].append({
                        "product_id":   product["id"],
                        "product_code": product["product_code"],
                        "description":  product["description"],
                        "quantity":     qty,
                        "price":        price,
                        "price_b2c":    float(product.get("price_b2c") or 0),
                        "unit":         product.get("unit", "pcs"),
                    })
                    logger.info(
                        f"💡 AI light change: {action} {match_type} × {qty} "
                        f"→ {product['product_code']}"
                    )

            return result

        except Exception as e:
            logger.warning(f"⚠️ _extract_light_changes_ai error: {e}", exc_info=True)
            return {}

    # ─────────────────────────────────────────────────────────────────────────
    #  Unified AI correction parser
    # ─────────────────────────────────────────────────────────────────────────

    async def _parse_corrections_ai(
        self,
        body: str,
        session_data: dict,
        client_group: str,
    ) -> dict:
        """
        Use AI to parse ALL correction types from a customer email:
          - Ceiling dimension changes
          - Color changes
          - Ceiling type changes (standard/acoustic/etc)
          - Ceiling removals
          - Quote-level light additions
          - Quote-level light removals

        Returns a structured dict with ceiling_patches, lights_add, lights_remove.
        Falls back to empty dict on any error — quote still processes safely.
        """
        try:
            import json as _json

            # Build ceiling context for AI
            ceilings = session_data.get("ceilings", [])
            ceiling_list = "\n".join(
                f"{i}: {c.get('name')} — {c.get('length')}×{c.get('width')}m, "
                f"{c.get('ceiling_type')}/{c.get('type_ceiling')}, {c.get('color')}"
                for i, c in enumerate(ceilings)
            )

            # Build light products context
            price_col = client_group if client_group.startswith("price_") else "price_b2c"
            db_lights = self.processor.db.execute_query(
                "SELECT * FROM products WHERE base_category='light' AND is_active=1",
                params=None, fetch=True,
            ) or []
            light_products = "\n".join(
                f"- {p['product_code']}: {p['description']} "
                f"({'surface_mounted' if '32501' in p['product_code'] else 'recessed_spot'})"
                for p in db_lights
            )

            # Existing quote lights
            existing_lights = "\n".join(
                f"- {l.get('product_code')}: {l.get('quantity')} pcs"
                for l in session_data.get("quote_lights", [])
            ) or "none"

            prompt = f"""You are parsing a customer correction email for a stretch ceiling quote.
Extract ONLY the changes the customer is requesting. Ignore signature, greetings, and unrelated content.

CURRENT QUOTE CEILINGS (index: name — dimensions, type, color):
{ceiling_list}

AVAILABLE LIGHT PRODUCTS:
{light_products}

CURRENT QUOTE-LEVEL LIGHTS:
{existing_lights}

CUSTOMER EMAIL:
{body}

Return ONLY valid JSON with this exact structure:
{{
  "ceiling_patches": [
    {{
      "ceiling_idx": <int — index from list above>,
      "field": "dimensions|color|type_ceiling|ceiling_type|remove",
      "value": <[length, width] for dimensions, string for others, null for remove>
    }}
  ],
  "ceilings_add": [
    {{
      "name": "<room name>",
      "length": <float, meters>,
      "width": <float, meters>,
      "ceiling_type": "fabric",
      "type_ceiling": "standard|acoustic|acoustic-color|light|print",
      "color": "white|black|warm-white",
      "lights": [
        {{"type": "recessed_spot|surface_mounted", "quantity": <int>}}
      ]
    }}
  ],
  "lights_add": [
    {{"type": "recessed_spot|surface_mounted", "quantity": <int>}}
  ],
  "lights_remove": [
    {{"type": "recessed_spot|surface_mounted", "quantity": <int>}}
  ]
}}

Rules:
- ceiling_patches: only for EXISTING ceilings (from the list above) that need changes
- ceilings_add: only for NEW rooms/ceilings the customer wants to add
- Do NOT use ceiling_patches for new rooms — use ceilings_add instead
- dimensions: value must be [length, width] as floats in meters
- color: normalize to english (wit→white, zwart→black, warm-wit→warm-white)
- type_ceiling: standard|acoustic|acoustic-color|light|print
- remove: set field="remove", value=null
- lights: "spots/inbouwspots/recessed" → recessed_spot; "opbouw/surface mounted" → surface_mounted
- lights_add: quote-level lights (not attached to a specific room)
- lights_remove: only when customer asks to REMOVE lights (verwijder/delete/remove)
- If nothing changed in a category, return empty array []
- Do NOT infer or assume changes not explicitly stated"""

            response = self.processor.ai_client.chat.completions.create(
                model=Config.DEPLOYMENT_NAME,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=600,
                temperature=0,
            )

            raw = response.choices[0].message.content.strip()
            raw = re.sub(r'^```(?:json)?\s*|\s*```$', '', raw, flags=re.MULTILINE).strip()
            parsed = _json.loads(raw)

            # Resolve light types to actual DB products
            result = {
                "ceiling_patches": parsed.get("ceiling_patches", []),
                "ceilings_add":    parsed.get("ceilings_add", []),
                "lights_add":      [],
                "lights_remove":   [],
            }

            for action_key, action_list in [("lights_add", parsed.get("lights_add", [])),
                                             ("lights_remove", parsed.get("lights_remove", []))]:
                for item in action_list:
                    qty        = int(item.get("quantity", 1))
                    light_type = item.get("type", "recessed_spot")
                    match_type = "surface_mounted" if "surface" in light_type else "spot"
                    product    = self.processor._match_light_product(match_type, db_lights)
                    if product and qty > 0:
                        price = float(product.get(price_col) or product.get("price_b2c") or 0)
                        result[action_key].append({
                            "product_id":   product["id"],
                            "product_code": product["product_code"],
                            "description":  product["description"],
                            "quantity":     qty,
                            "price":        price,
                            "price_b2c":    float(product.get("price_b2c") or 0),
                            "unit":         product.get("unit", "pcs"),
                        })

            total_changes = (
                len(result["ceiling_patches"]) +
                len(result["lights_add"]) +
                len(result["lights_remove"])
            )
            if total_changes > 0:
                logger.info(
                    f"🤖 AI correction: {len(result['ceiling_patches'])} ceiling patch(es), "
                    f"{len(result['lights_add'])} light add(s), "
                    f"{len(result['lights_remove'])} light removal(s)"
                )

            return result

        except Exception as e:
            logger.warning(f"⚠️ _parse_corrections_ai error: {e}", exc_info=True)
            return {}

    def _classify_reply_intent(self, body: str) -> str:
        """
        Classify a customer reply into one of six intents using a two-pass approach:
          1. Fast keyword check for clear-cut cases (no_interest, explicit acceptance)
          2. AI classification for ambiguous cases where keywords overlap

        Intents:
          no_interest — customer explicitly not interested
          timeframe   — customer gives a future start date
          acceptance  — customer agrees / wants to proceed
          site_visit  — customer requests a site visit / measurement
          more_info   — customer asks a general question
          correction  — customer wants spec changes (default)
        """
        # Use only first 2 lines to avoid signature interference
        lines = [l.strip() for l in body.splitlines() if l.strip()]
        text  = ' '.join(lines[:3]).lower()

        # ── No interest — unambiguous, checked first ──────────────────────────
        no_interest_kw = [
            "geen interesse", "niet geïnteresseerd", "not interested",
            "bedankt maar", "dank maar", "hoef niet", "zien we niet zitten",
            "gaan we niet doen", "no thanks", "no thank you", "pas intéressé",
            "merci mais", "sans suite", "annuler", "geen nood",
            "niet meer nodig", "niet meer van toepassing",
        ]
        if any(kw in text for kw in no_interest_kw):
            return "no_interest"

        # ── Explicit correction signals — unambiguous change requests ─────────
        # Only very specific technical change keywords, NOT room names
        explicit_correction_kw = [
            # Dimension change verbs
            "aanpassen", "wijzigen", "veranderen", "modifier", "changer",
            "change ", "update ", "ajouter", "toevoegen", "verwijderen",
            "add ", "remove ",
            # Specific technical specs
            "m²", "m2", "meter bij", "bij meter",
            "ral ", "ncs ", "kleur wijzigen", "couleur modifier",
            "akoestisch", "acoustic", "standard plafond", "standaard plafond",
            # Light changes
            "spots", "inbouwspot", "downlight", "surface mounted", "opbouwspot",
            "verlichting toevoegen", "verlichting verwijderen",
        ]
        if any(kw in text for kw in explicit_correction_kw):
            return "correction"

        # ── Explicit acceptance — unambiguous proceed signals ─────────────────
        acceptance_kw = [
            "ik ga akkoord", "we gaan akkoord", "willen bestellen",
            "bestelling plaatsen", "ik bestel", "we bestellen",
            "kunnen we verder gaan", "graag verder gaan",
            "offerte aanvaarden", "aanvaard de offerte",
            "go ahead", "let's go", "proceed",
            "on y va", "je confirme", "nous confirmons",
        ]
        if any(kw in text for kw in acceptance_kw):
            return "acceptance"

        # ── Explicit timeframe ────────────────────────────────────────────────
        timeframe_kw = [
            "volgende maand", "volgend jaar", "over een maand",
            "over 2 maand", "over twee maand", "over 3 maand",
            "na de zomer", "na de feestdagen", "na nieuwjaar",
            "next month", "next year", "in a few months",
            "le mois prochain", "dans un mois", "après les vacances",
        ]
        if any(kw in text for kw in timeframe_kw):
            return "timeframe"

        # ── Explicit site visit ───────────────────────────────────────────────
        site_visit_kw = [
            "plaatsbezoek", "opmeting", "langskomen", "ter plaatse",
            "site visit", "measurement", "rendez-vous", "visite",
        ]
        if any(kw in text for kw in site_visit_kw):
            return "site_visit"

        # ── More info ─────────────────────────────────────────────────────────
        info_kw = [
            "meer info", "meer informatie", "uitleg", "garantie",
            "more info", "how does", "warranty", "plus d'info",
        ]
        if any(kw in text for kw in info_kw):
            return "more_info"

        # ── Ambiguous: use AI to classify ─────────────────────────────────────
        # Handles cases like "Ik ga akkoord voor de woonkamer offerte" which
        # contains room names (correction trigger) but is clearly acceptance.
        try:
            prompt = f"""Classify this customer reply to a stretch ceiling quote into exactly one category.

Reply: "{body[:400]}"

Categories:
- acceptance: customer agrees, wants to proceed, confirms the order
- correction: customer wants to change specs (dimensions, color, lights, ceiling type)
- site_visit: customer wants a site visit or measurement
- timeframe: customer gives a future date when they want to proceed
- more_info: customer asks a question about the quote
- no_interest: customer is not interested
- correction: DEFAULT if unclear

Reply with ONLY the category name, nothing else."""

            response = self.processor.ai_client.chat.completions.create(
                model=Config.DEPLOYMENT_NAME,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=10,
                temperature=0,
            )
            intent = response.choices[0].message.content.strip().lower()
            valid  = {"acceptance", "correction", "site_visit", "timeframe",
                      "more_info", "no_interest"}
            if intent in valid:
                logger.info(f"🤖 AI intent: '{intent}'")
                return intent
        except Exception as e:
            logger.warning(f"⚠️ AI intent classification failed: {e}")

        return "correction"

    def _extract_timeframe_date(self, body: str) -> Optional[datetime]:
        """
        Parse a future date from a timeframe reply.
        Returns a datetime to schedule the follow-up, or None.
        Subtracts 3 days from the mentioned date so we reach out slightly before.
        """
        text  = body.lower()
        now   = datetime.utcnow()
        month_map = {
            "januari": 1,  "january": 1,   "janvier": 1,
            "februari": 2, "february": 2,  "février": 2,
            "maart": 3,    "march": 3,     "mars": 3,
            "april": 4,    "april": 4,     "avril": 4,
            "mei": 5,      "may": 5,       "mai": 5,
            "juni": 6,     "june": 6,      "juin": 6,
            "juli": 7,     "july": 7,      "juillet": 7,
            "augustus": 8, "august": 8,    "août": 8,
            "september": 9,"september": 9, "septembre": 9,
            "oktober": 10, "october": 10,  "octobre": 10,
            "november": 11,"november": 11, "novembre": 11,
            "december": 12,"december": 12, "décembre": 12,
        }

        # "over X maand/weken" patterns
        m = re.search(r'over\s+(\d+|een|twee|drie|vier|vijf|one|two|three)\s*(maand|month|mois|week|weken|weeks)', text)
        if m:
            qty_str = m.group(1)
            unit    = m.group(2)
            qty_map = {"een": 1, "twee": 2, "drie": 3, "vier": 4, "vijf": 5,
                       "one": 1, "two": 2, "three": 3}
            qty = int(qty_str) if qty_str.isdigit() else qty_map.get(qty_str, 1)
            if "week" in unit:
                target = now + timedelta(weeks=qty)
            else:
                target = now + timedelta(days=qty * 30)
            return target - timedelta(days=3)

        # "volgende maand" / "next month"
        if any(kw in text for kw in ["volgende maand", "next month", "le mois prochain"]):
            target = now + timedelta(days=30)
            return target - timedelta(days=3)

        # "volgend jaar" / "next year"
        if any(kw in text for kw in ["volgend jaar", "next year", "l'année prochaine"]):
            target = now + timedelta(days=365)
            return target - timedelta(days=7)

        # "na de zomer" / "after summer"
        if any(kw in text for kw in ["na de zomer", "after summer", "après l'été"]):
            target = datetime(now.year, 9, 1) if now.month < 9 else datetime(now.year + 1, 9, 1)
            return target - timedelta(days=7)

        # "in [month]"
        for name, num in month_map.items():
            if f"in {name}" in text or f"begin {name}" in text or f"eind {name}" in text:
                year = now.year if num >= now.month else now.year + 1
                target = datetime(year, num, 1)
                return target - timedelta(days=7)

        # Fallback: generic "later" → 6 weeks
        return now + timedelta(weeks=6) - timedelta(days=3)

    # ─────────────────────────────────────────────────────────────────────────
    #  Reply body cleaner
    # ─────────────────────────────────────────────────────────────────────────

    def _strip_quoted_reply(self, body: str) -> str:
        """
        Remove the quoted original email from a customer reply so the AI
        only reads the NEW text the customer typed.

        Critical: must correctly distinguish HTML emails from plain-text emails
        that contain email addresses in angle brackets (e.g. <user@domain.com>).

        HTML detection: looks for actual HTML tags (<div, <p, <span, <br, <table)
        NOT just any < > characters — plain text with email addresses would
        otherwise be misclassified as HTML causing the stripper to fail.

        Handles:
          HTML  : strips <blockquote>, gmail_quote div, OutlookMessageHeader
          Plain : strips '>' quoted lines and 'On ... wrote:' dividers
                  including multi-line Gmail dividers where the wrote: line
                  wraps or the email address appears on the same line
        """
        if not body:
            return body

        # ── Detect actual HTML (not just angle brackets around email addresses) ─
        is_html = bool(re.search(
            r'<\s*(div|p|span|br|table|html|body|blockquote|a\s)',
            body, re.IGNORECASE
        ))

        if is_html:
            # Remove all <blockquote> blocks
            cleaned = re.sub(
                r'<blockquote[^>]*>.*?</blockquote>',
                '', body, flags=re.DOTALL | re.IGNORECASE
            )
            # Remove Gmail / Outlook reply dividers and everything after
            cleaned = re.sub(
                r'<div[^>]*(gmail_quote|gmail_attr|divRplyFwdMsg|'
                r'OutlookMessageHeader|yahoo_quoted|reply-separator)[^>]*>.*',
                '', cleaned, flags=re.DOTALL | re.IGNORECASE
            )
            # Strip remaining HTML tags and collapse whitespace
            text = re.sub(r'<[^>]+>', ' ', cleaned)
            text = re.sub(r'&nbsp;', ' ', text)
            text = re.sub(r'\s+', ' ', text).strip()
            # Fallback: if stripping left near-nothing, strip tags from full body
            if len(text) < 20:
                text = re.sub(r'<[^>]+>', ' ', body)
                text = re.sub(r'\s+', ' ', text).strip()
            return text

        # ── Plain text ────────────────────────────────────────────────────────
        lines = body.splitlines()
        clean_lines = []
        skip = False

        for line in lines:
            stripped = line.strip()

            if skip:
                continue

            # Stop at '>' quoted lines (standard email quoting)
            if stripped.startswith('>'):
                skip = True
                continue

            # Stop at Gmail / Outlook / Apple Mail 'On ... wrote:' dividers.
            if re.match(r'^On .{5,}wrote:\s*$', stripped, re.IGNORECASE):
                skip = True
                continue

            # Dutch: "Op ... schreef:"
            if re.match(r'^Op .{5,}schreef:\s*$', stripped, re.IGNORECASE):
                skip = True
                continue

            # French: "Le ... a écrit :"
            if re.match(r'^Le .{5,}crit\s*:\s*$', stripped, re.IGNORECASE):
                skip = True
                continue

            # Also catch lines starting with 'On ' that contain a date pattern
            # and end with 'wrote:' — even if the address wraps to next line
            if (stripped.startswith('On ')
                    and re.search(r'\d{4}', stripped)
                    and stripped.endswith('wrote:')):
                skip = True
                continue

            # Dutch equivalent: 'Op ' + year + 'schreef:'
            if (stripped.startswith('Op ')
                    and re.search(r'\d{4}', stripped)
                    and stripped.lower().endswith('schreef:')):
                skip = True
                continue

            # Stop at horizontal rule dividers (---, ___, ===)
            if re.match(r'^[-_=]{3,}\s*$', stripped):
                skip = True
                continue

            # Stop at Outlook "-----Oorspronkelijk bericht-----" style dividers
            # (Dutch, French, English variations)
            if re.match(r'^-{2,}.{3,}-{2,}\s*$', stripped):
                skip = True
                continue

            # Stop at Outlook-style 'From: ... Sent: ...' blocks
            # Also Dutch: 'Van: ... Verzonden: ...' and French: 'De: ... Envoyé:'
            if re.match(r'^(From|Van|De):\s+.+', stripped, re.IGNORECASE) and len(stripped) > 10:
                skip = True
                continue

            clean_lines.append(line)

        result = '\n'.join(clean_lines).strip()
        # Fallback: if stripping left near-nothing, return full body
        return result if len(result) >= 10 else body.strip()

    # ─────────────────────────────────────────────────────────────────────────
    #  Customer contact resolver
    # ─────────────────────────────────────────────────────────────────────────

    # ─────────────────────────────────────────────────────────────────────────
    #  Signature stripping
    # ─────────────────────────────────────────────────────────────────────────

    # Markers that reliably indicate the start of an email signature.
    # Anything from this line onward is stripped.
    _SIG_MARKERS = [
        # Dutch
        "met vriendelijke groeten", "met vriendel", "mvg,", "mvg ",
        "hoogachtend", "groeten,", "groeten ",
        # German
        "freundliche grüße", "mit freundlichen grüßen", "mfg,",
        # English
        "best regards", "kind regards", "regards,", "regards ",
        "sincerely,", "thanks,", "thank you,", "cheers,",
        # French
        "meilleures salutations", "cordialement", "bien cordialement",
        "avec mes salutations",
        # Generic separator lines
        "-- \n", "--\n", "___", "---",
        # Common signature openers
        "sent from my", "get outlook", "unsubscribe",
    ]

    async def _send_clarification_request(
        self,
        actual_email: str,
        actual_name:  str,
        language:     str,
        quote_number: str,
        body_clean:   str,
        token:        str,
    ):
        """
        Send a polite clarification request when we couldn't parse the customer's
        correction. Acknowledges receipt and asks them to be more specific.
        """
        first_name = actual_name.split()[0] if actual_name else actual_name

        if language == "fr":
            subject = f"Re: Votre offerte {quote_number} — Précision nécessaire"
            html = f"""
<p>Bonjour {first_name},</p>
<p>Merci pour votre message concernant votre offerte <strong>{quote_number}</strong>.</p>
<p>Nous n'avons malheureusement pas pu identifier exactement les modifications souhaitées.
Pourriez-vous préciser ce que vous souhaitez modifier ? Par exemple :</p>
<ul>
  <li>Les dimensions d'une pièce (ex. : salon 5×4m)</li>
  <li>La couleur ou le type de plafond</li>
  <li>L'ajout ou la suppression d'une pièce</li>
  <li>Le nombre de spots ou luminaires</li>
</ul>
<p>Votre message reçu :<br><em>{body_clean[:300]}</em></p>
<p>Cordialement,<br>L'équipe STRETCH</p>"""
        elif language == "nl":
            subject = f"Re: Uw offerte {quote_number} — Verduidelijking gewenst"
            html = f"""
<p>Beste {first_name},</p>
<p>Bedankt voor uw bericht over uw offerte <strong>{quote_number}</strong>.</p>
<p>We konden helaas niet precies begrijpen welke wijzigingen u wenst.
Kunt u verduidelijken wat u wilt aanpassen? Bijvoorbeeld:</p>
<ul>
  <li>De afmetingen van een kamer (bijv. woonkamer 5×4m)</li>
  <li>De kleur of het type plafond</li>
  <li>Het toevoegen of verwijderen van een kamer</li>
  <li>Het aantal spots of verlichtingspunten</li>
</ul>
<p>Uw ontvangen bericht:<br><em>{body_clean[:300]}</em></p>
<p>Met vriendelijke groeten,<br>Het STRETCH team</p>"""
        else:
            subject = f"Re: Your quote {quote_number} — Clarification needed"
            html = f"""
<p>Dear {first_name},</p>
<p>Thank you for your message regarding quote <strong>{quote_number}</strong>.</p>
<p>We weren't able to identify exactly what changes you'd like to make.
Could you clarify what you'd like to adjust? For example:</p>
<ul>
  <li>The dimensions of a room (e.g. living room 5×4m)</li>
  <li>The colour or type of ceiling</li>
  <li>Adding or removing a room</li>
  <li>The number of spots or light fixtures</li>
</ul>
<p>Your message received:<br><em>{body_clean[:300]}</em></p>
<p>Kind regards,<br>The STRETCH team</p>"""

        await self._send_reply(
            to_email=actual_email,
            subject=subject,
            html_body=html,
            pdf_path=None,
            quote_number=quote_number,
            token=token,
        )
        logger.info(f"📧 Clarification request sent → {actual_email}")

    # ─────────────────────────────────────────────────────────────────────────
    #  Telegram admin notifications
    # ─────────────────────────────────────────────────────────────────────────

    async def _notify_admin(self, message: str) -> None:
        """
        Push a message to all admin Telegram IDs via the Bot HTTP API.
        Fire-and-forget — never raises, failures are only logged.
        """
        if not self._bot_token or not self._admin_ids:
            return
        url = f"https://api.telegram.org/bot{self._bot_token}/sendMessage"
        async with aiohttp.ClientSession() as session:
            for admin_id in self._admin_ids:
                try:
                    async with session.post(url, json={
                        "chat_id":    admin_id,
                        "text":       message,
                        "parse_mode": "HTML",
                    }, timeout=aiohttp.ClientTimeout(total=8)) as r:
                        if r.status != 200:
                            logger.warning(
                                f"⚠️ Telegram notify failed for {admin_id}: {r.status}"
                            )
                except Exception as e:
                    logger.warning(f"⚠️ Telegram notify error: {e}")

    # ─────────────────────────────────────────────────────────────────────────
    #  Session expiry
    # ─────────────────────────────────────────────────────────────────────────

    async def _expire_old_sessions(self) -> None:
        """
        Close sessions that have had no activity for 30+ days.
        Sets status='expired' and no_followup=1 so follow-ups stop firing.
        Runs once per day (every 1440 poll cycles ≈ 24h at 60s interval).
        """
        try:
            result = self.db.execute_query(
                """
                UPDATE email_quote_sessions
                SET    status     = 'expired',
                       no_followup = 1
                WHERE  status    IN ('quote_sent', 'awaiting_reply',
                                     'revised', 'campaign_sent')
                  AND  (no_followup IS NULL OR no_followup = 0)
                  AND  COALESCE(quote_sent_at, received_at)
                           < NOW() - INTERVAL 30 DAY
                """,
            )
            if result:
                logger.info(f"🕐 Session expiry: closed old sessions")
        except Exception as e:
            logger.warning(f"⚠️ Session expiry error: {e}")

    def _strip_signature(self, body: str) -> str:
        """
        Remove email signature from body text.
        Returns the message content above the first signature marker.
        Falls back to the original body if no marker found.
        """
        if not body:
            return body

        lower = body.lower()
        earliest = len(body)

        for marker in self._SIG_MARKERS:
            pos = lower.find(marker)
            if 0 < pos < earliest:
                earliest = pos

        stripped = body[:earliest].strip()
        # Keep original if stripping removed too much (< 10 chars left)
        return stripped if len(stripped) >= 10 else body

    def _extract_customer_contact(
        self,
        result: EmailQuoteResult,
        fallback_email: str,
        fallback_name: str,
    ) -> tuple:
        """
        Return (actual_email, actual_name) for the real customer.

        Priority order:
          1. contact_info.email  parsed by AI from the form body
          2. result.sender_email updated by AI processor if it found a better one
          3. fallback_email      the forwarding address (last resort)

        Same logic for name:
          1. contact_info.name
          2. result.sender_name
          3. fallback_name
        """
        contact = {}
        if result.session_data:
            contact = result.session_data.get("contact_info", {}) or {}

        # Email resolution
        email = (
            contact.get("email")
            or (result.sender_email if result.sender_email != fallback_email else None)
            or fallback_email
        )

        # Name resolution
        name = (
            contact.get("name")
            or (result.sender_name if result.sender_name != fallback_name else None)
            or fallback_name
        )

        # Basic email validation — reject obviously invalid values
        if not email or "@" not in email or "." not in email.split("@")[-1]:
            email = fallback_email

        return email.strip(), (name or fallback_name).strip()

    # ─────────────────────────────────────────────────────────────────────────
    #  Graph API helpers
    # ─────────────────────────────────────────────────────────────────────────

    async def _fetch_unread(self, token: str) -> List[Dict]:
        """Fetch unread messages from the leads inbox."""
        url = (
            f"{self.GRAPH}/users/{Config.LEADS_MAILBOX}"
            f"/mailFolders/inbox/messages"
        )
        params = {
            "$filter":  "isRead eq false",
            "$orderby": "receivedDateTime asc",
            "$top":     "25",
            "$select":  "id,conversationId,subject,from,body,receivedDateTime,isRead",
        }
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type":  "application/json",
        }
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(url, headers=headers, params=params) as r:
                    if r.status == 200:
                        return (await r.json()).get("value", [])
                    err = await r.text()
                    logger.error(f"❌ fetch_unread {r.status}: {err[:200]}")
        except Exception as e:
            logger.error(f"❌ fetch_unread exception: {e}")
        return []

    async def _mark_read(self, message_id: str, token: str):
        """Mark message as read so it isn't re-processed next cycle."""
        url     = f"{self.GRAPH}/users/{Config.LEADS_MAILBOX}/messages/{message_id}"
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        try:
            async with aiohttp.ClientSession() as s:
                async with s.patch(url, headers=headers, json={"isRead": True}) as r:
                    if r.status not in (200, 204):
                        logger.warning(f"⚠️ mark_read {r.status} for {message_id[:20]}")
        except Exception as e:
            logger.warning(f"⚠️ mark_read exception: {e}")

    async def _send_reply(
        self,
        to_email: str,
        subject: str,
        html_body: str,
        pdf_path: Optional[str],
        quote_number: str,
        token: str,
        extra_cc: Optional[List[str]] = None,
    ) -> bool:
        """Send reply email from Config.EMAIL_FROM with optional PDF attachment.

        replyTo is always set to Config.LEADS_MAILBOX so that when the customer
        hits Reply their email client addresses it back to the leads inbox,
        where the listener is polling — not to assistant_quotes@.

        extra_cc: additional CC addresses beyond Config.COMPANY_EMAIL.
        """
        url     = f"{self.GRAPH}/users/{Config.EMAIL_FROM}/sendMail"
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

        cc_list = [{"emailAddress": {"address": Config.COMPANY_EMAIL}}]
        for addr in (extra_cc or []):
            if addr and addr not in (Config.COMPANY_EMAIL, to_email):
                cc_list.append({"emailAddress": {"address": addr}})

        message: Dict[str, Any] = {
            "message": {
                "subject": subject,
                "body": {"contentType": "HTML", "content": html_body},
                "toRecipients": [{"emailAddress": {"address": to_email}}],
                "ccRecipients": cc_list,
                "replyTo": [
                    {"emailAddress": {"address": Config.LEADS_MAILBOX}}
                ],
            },
            "saveToSentItems": "true",
        }

        if pdf_path and os.path.isfile(pdf_path):
            try:
                with open(pdf_path, "rb") as f:
                    pdf_b64 = base64.b64encode(f.read()).decode()
                filename = (
                    f"Quote_{quote_number}.pdf" if quote_number else "Quote.pdf"
                )
                message["message"]["attachments"] = [{
                    "@odata.type":  "#microsoft.graph.fileAttachment",
                    "name":         filename,
                    "contentType":  "application/pdf",
                    "contentBytes": pdf_b64,
                }]
            except Exception as e:
                logger.warning(f"⚠️ Could not attach PDF {pdf_path}: {e}")

        try:
            async with aiohttp.ClientSession() as s:
                async with s.post(url, headers=headers, json=message) as r:
                    if r.status == 202:
                        return True
                    err = await r.text()
                    logger.error(f"❌ sendMail {r.status}: {err[:300]}")
        except Exception as e:
            logger.error(f"❌ _send_reply exception: {e}")
        return False

    async def _get_token(self) -> Optional[str]:
        """Obtain (or reuse cached) OAuth2 access token for Graph API."""
        if self._token and datetime.utcnow() < self._token_expiry:
            return self._token
        url  = (
            f"https://login.microsoftonline.com/"
            f"{Config.AZURE_TENANT_ID}/oauth2/v2.0/token"
        )
        data = {
            "client_id":     Config.AZURE_CLIENT_ID,
            "client_secret": Config.AZURE_CLIENT_SECRET,
            "scope":         "https://graph.microsoft.com/.default",
            "grant_type":    "client_credentials",
        }
        try:
            async with aiohttp.ClientSession() as s:
                async with s.post(url, data=data) as r:
                    if r.status == 200:
                        resp = await r.json()
                        self._token = resp["access_token"]
                        self._token_expiry = (
                            datetime.utcnow()
                            + timedelta(seconds=resp.get("expires_in", 3600) - 300)
                        )
                        return self._token
                    err = await r.text()
                    logger.error(f"❌ token fetch {r.status}: {err[:200]}")
        except Exception as e:
            logger.error(f"❌ _get_token exception: {e}")
        return None

    # ─────────────────────────────────────────────────────────────────────────
    #  PDF generation
    # ─────────────────────────────────────────────────────────────────────────

    def _generate_pdf(
        self,
        result: EmailQuoteResult,
        sender_name: str,
        sender_email: str,
    ) -> Optional[str]:
        """Generate PDF via generate_quote() which merges ceiling_costs into
        each ceiling dict so the Dutch pdf_generator shows the cost breakdown."""
        try:
            contact = (
                result.session_data.get("contact_info", {})
                if result.session_data else {}
            )
            user_profile = {
                "first_name":   sender_name.split()[0] if sender_name else "",
                "last_name":    " ".join(sender_name.split()[1:]) if sender_name else "",
                "full_name":    sender_name,
                "email":        sender_email,
                "client_group": (
                    result.session_data.get("client_group", "price_b2c")
                    if result.session_data else "price_b2c"
                ),
                "is_company":   contact.get("is_company", False),
                "company_name": contact.get("company_name", ""),  # not company_type which may just be "BV"
                "phone":        contact.get("phone", ""),
                "address":      contact.get("address", ""),
            }

            quote_data = dict(result.session_data) if result.session_data else {}
            quote_data["user_profile"] = user_profile
            quote_data["quote_number"] = (
                result.quote_number or f"QT{datetime.now():%Y%m%d%H%M%S}"
            )

            # generate_quote() handles ceiling_costs → ceiling merge internally
            path = self.pdf_gen.generate_quote(quote_data)
            logger.info(f"📄 PDF generated: {path}")
            return path
        except Exception as e:
            logger.error(f"❌ PDF generation failed: {e}", exc_info=True)
            return None

    # ─────────────────────────────────────────────────────────────────────────
    #  Misc helpers
    # ─────────────────────────────────────────────────────────────────────────

    def _get_quote_number(self, quote_id: Optional[int]) -> str:
        """Fetch quote_number from DB; fall back to timestamp-based string."""
        if not quote_id:
            return f"QT{datetime.now():%Y%m%d%H%M%S}"
        try:
            rows = self.db.execute_query(
                "SELECT quote_number FROM quotations WHERE id=%s",
                (quote_id,), fetch=True,
            )
            if rows:
                return rows[0]["quote_number"]
        except Exception:
            pass
        return f"QT{datetime.now():%Y%m%d%H%M%S}"

    def _poll_interval(self) -> int:
        """Return 120s during business hours, 900s at night."""
        try:
            now = datetime.now(BRUSSELS)
        except Exception:
            now = datetime.now()
        if 7 <= now.hour < 19:
            return 120
        return 900

    async def _sync_revised_quote_to_dynamics365(
        self,
        session:      Dict,
        actual_email: str,
        actual_name:  str,
        result,
        quote_id:     int,
        quote_number: str,
        pdf_path:     Optional[str],
        client_group: str,
    ):
        """
        Sync a revised quote to Dynamics 365.
        If a D365 quote already exists (dynamics_quote_id stored in DB),
        update it. Otherwise create a new one linked to the contact.
        """
        try:
            logger.info(f"🔄 D365 revised quote sync: {quote_number}")

            # Get existing D365 quote ID — check DB first, then search D365
            existing_d365_quote_id = None

            # 1. Check DB for stored D365 quote ID from initial sync
            existing_quotation = self.db.execute_query(
                "SELECT dynamics_quote_id FROM quotations WHERE id=%s",
                (session.get("quotation_id"),), fetch=True
            )
            if existing_quotation and existing_quotation[0].get("dynamics_quote_id"):
                existing_d365_quote_id = existing_quotation[0]["dynamics_quote_id"]
                logger.info(f"🔍 D365: found stored quote ID: {existing_d365_quote_id}")

            # 2. Fallback: search D365 by quotenumber (original QT number)
            if not existing_d365_quote_id:
                original_qt = session.get("quote_number", "")
                if original_qt:
                    try:
                        import urllib.parse
                        filter_q  = f"quotenumber eq '{original_qt}'"
                        encoded   = urllib.parse.quote(filter_q)
                        response  = await self.d365.make_request(
                            method="GET",
                            endpoint=f"quotes?$filter={encoded}&$select=quoteid,quotenumber"
                        )
                        if response and response.get("value"):
                            existing_d365_quote_id = response["value"][0]["quoteid"]
                            logger.info(
                                f"🔍 D365: found quote by quotenumber "
                                f"{original_qt} → {existing_d365_quote_id}"
                            )
                    except Exception as e:
                        logger.warning(f"⚠️ D365 quote search failed: {e}")

            # Find contact by email
            contacts = await self.d365.search_contacts_by_email(actual_email)
            if not contacts:
                # Contact doesn't exist yet — create it
                name_parts = actual_name.split(" ", 1)
                user_data = {
                    "first_name": name_parts[0],
                    "last_name":  name_parts[1] if len(name_parts) > 1 else "",
                    "email":      actual_email,
                }
                contact_id = await self.d365.create_or_update_contact(user_data)
            else:
                contact_id = contacts[0]["contactid"]

            if not contact_id:
                logger.warning(f"⚠️ D365 revised sync: no contact for {actual_email}")
                return

            # Build quote data
            quote_data = dict(result.session_data)
            quote_data["quote_number"] = quote_number
            quote_data["total_price"]  = result.total_price
            quote_data["client_group"] = client_group

            # Update existing or create new
            d365_quote_id = await self.d365.create_or_update_quote(
                quote_data=quote_data,
                contact_id=contact_id,
                dynamics_quote_id=existing_d365_quote_id,
            )

            # Attach updated PDF
            if d365_quote_id and pdf_path:
                await self.d365.attach_pdf_to_quote(
                    quote_id=d365_quote_id,
                    pdf_path=pdf_path,
                    quote_number=quote_number,
                )

            # Store D365 quote ID on new quotation record
            if d365_quote_id:
                self.db.execute_query(
                    "UPDATE quotations SET dynamics_quote_id=%s WHERE id=%s",
                    (d365_quote_id, quote_id),
                )

            logger.info(
                f"✅ D365 revised quote sync complete: "
                f"contact={contact_id}, quote={d365_quote_id} "
                f"({'updated' if existing_d365_quote_id else 'created'})"
            )

        except Exception as e:
            logger.error(
                f"❌ D365 revised sync failed for {actual_email}: {e}",
                exc_info=True
            )

    async def _sync_to_dynamics365(
        self,
        actual_email: str,
        actual_name:  str,
        result,
        quote_id:     int,
        quote_number: str,
        pdf_path:     Optional[str],
        client_group: str,
    ):
        """
        Sync a new email lead to Dynamics 365:
          1. Find or create Contact by email
          2. Find or create Account if company
          3. Create Quote linked to the contact
          4. Attach PDF to the quote
          5. Store D365 IDs back in DB session
        """
        try:
            logger.info(f"🔄 D365 sync starting for {actual_email} | {quote_number}")

            # ── 1. Build user_data dict from parsed contact info ──────────────
            contact_info = result.session_data.get("contact_info", {})
            name_parts   = actual_name.split(" ", 1)
            user_data = {
                "first_name":   name_parts[0] if name_parts else actual_name,
                "last_name":    name_parts[1] if len(name_parts) > 1 else "",
                "email":        actual_email,
                "phone":        contact_info.get("phone", ""),
                "address":      contact_info.get("address", ""),
                "company_name": contact_info.get("company_name", ""),
                "vat_number":   contact_info.get("vat", ""),
                "is_company":   bool(contact_info.get("company_name")),
                "lead_source":  "Email - Website Form",
            }

            # ── 2. Create / find Contact ──────────────────────────────────────
            contact_id = await self.d365.create_or_update_contact(user_data)
            if not contact_id:
                logger.warning(f"⚠️ D365: could not create contact for {actual_email}")
                return

            # ── 3. Create / find Account (if company) ────────────────────────
            account_id = None
            if user_data["is_company"] and user_data["company_name"]:
                account_id = await self.d365.create_or_update_account(
                    user_data, primary_contact_id=contact_id
                )

            # ── 4. Create Quote in D365 ───────────────────────────────────────
            quote_data = dict(result.session_data)
            quote_data["quote_number"] = quote_number
            quote_data["total_price"]  = result.total_price
            quote_data["client_group"] = client_group

            d365_quote_id = await self.d365.create_or_update_quote(
                quote_data=quote_data,
                contact_id=contact_id,
                account_id=account_id,
            )

            # ── 5. Attach PDF ─────────────────────────────────────────────────
            if d365_quote_id and pdf_path:
                await self.d365.attach_pdf_to_quote(
                    quote_id=d365_quote_id,
                    pdf_path=pdf_path,
                    quote_number=quote_number,
                )

            # ── 6. Store D365 IDs back in DB ──────────────────────────────────
            if d365_quote_id:
                self.db.execute_query(
                    "UPDATE quotations SET dynamics_quote_id=%s WHERE id=%s",
                    (d365_quote_id, quote_id),
                )

            logger.info(
                f"✅ D365 sync complete: contact={contact_id}, "
                f"account={account_id}, quote={d365_quote_id}"
            )

        except Exception as e:
            logger.error(f"❌ D365 sync failed for {actual_email}: {e}", exc_info=True)

    def _get_latest_open_session_by_email(self, email: str) -> Optional[Dict]:
        """
        Strategy 3 email threading: find the most recent open session for a
        sender email when no conversationId or QT number match is found.
        Only matches sessions that are actionable and recent (< 60 days).
        Returns None if multiple recent sessions exist (ambiguous).
        """
        try:
            rows = self.db.execute_query(
                "SELECT * FROM email_quote_sessions "
                "WHERE sender_email = %s "
                "AND status IN ('quote_sent', 'awaiting_reply', 'revised', 'campaign_sent') "
                "AND quote_sent_at > NOW() - INTERVAL 60 DAY "
                "ORDER BY quote_sent_at DESC "
                "LIMIT 2",
                params=(email,),
                fetch=True,
            ) or []
            if len(rows) == 1:
                return rows[0]
            if len(rows) > 1:
                # Multiple open sessions — too ambiguous, treat as new request
                logger.info(
                    f"📧 Strategy 3: {len(rows)} open sessions for {email} "
                    f"— treating as new request"
                )
            return None
        except Exception as e:
            logger.warning(f"⚠️ _get_latest_open_session_by_email: {e}")
            return None

    def _is_system_sender(self, sender: str, subject: str, body: str) -> bool:
        """Return True for auto-replies, NDRs and no-reply addresses."""
        sl = sender.lower()
        system_starts = [
            "mailer-daemon@", "postmaster@", "noreply@",
            "no-reply@", "donotreply@", "do-not-reply@",
        ]
        if any(sl.startswith(s) for s in system_starts):
            return True
        check = (subject + " " + body[:400]).lower()
        patterns = [
            r'out of office',           r'automatisch antwoord',
            r'r[eé]ponse automatique',  r'auto.?reply',
            r'delivery status',         r'undeliverable',
            r'mail delivery failed',    r'mailer.?daemon',
            r'returned mail',
        ]
        return any(re.search(p, check) for p in patterns)

    # ─────────────────────────────────────────────────────────────────────────
    #  Follow-up scheduler
    # ─────────────────────────────────────────────────────────────────────────

    async def _followup_cycle(self):
        """
        Personalised follow-up scheduler:

        SCENARIO A — Standard (no reply received):
          Day 3: "Just checking in on your quote"
          Day 8: "Following up again as we haven't heard from you"

        SCENARIO B — Timeframe given (followup_scheduled_at set):
          On/around that date: "You mentioned [timeframe] — here's your quote"

        Never sends if no_followup=1 (customer said not interested).
        Never sends if session moved to confirmed/failed/spam.
        """
        try:
            token = await self._get_token()
            now   = datetime.utcnow()

            sessions = self.db.execute_query(
                "SELECT * FROM email_quote_sessions "
                "WHERE status IN ('quote_sent', 'awaiting_reply', 'revised', 'campaign_sent') "
                "AND (no_followup IS NULL OR no_followup = 0) "
                "AND quote_sent_at IS NOT NULL "
                "ORDER BY quote_sent_at ASC",
                fetch=True,
            ) or []

            sent_count = 0
            for session in sessions:
                sid            = session["id"]
                customer_email = session.get("sender_email", "")
                customer_name  = session.get("sender_name", "")
                quote_number   = session.get("quote_number", "")
                language       = session.get("language", "nl")
                total_price    = float(session.get("total_price") or 0)
                pdf_path       = session.get("pdf_path")
                followup_notes = session.get("followup_notes") or ""

                quote_sent_at = session.get("quote_sent_at")
                if isinstance(quote_sent_at, str):
                    quote_sent_at = datetime.fromisoformat(quote_sent_at)
                days_since = (now - quote_sent_at).total_seconds() / 86400

                followup_1_sent = session.get("followup_1_sent_at")
                followup_2_sent = session.get("followup_2_sent_at")
                scheduled_at    = session.get("followup_scheduled_at")

                # Parse session_data for ceiling table
                session_data = {}
                try:
                    import json as _json
                    raw = session.get("parsed_data") or session.get("assumed_data") or "{}"
                    if isinstance(raw, str):
                        session_data = _json.loads(raw)
                except Exception:
                    pass

                # ── Scenario B: Scheduled follow-up (timeframe given) ─────────
                if scheduled_at:
                    if isinstance(scheduled_at, str):
                        scheduled_at = datetime.fromisoformat(scheduled_at)
                    # Fire when we've reached the scheduled date and haven't sent followup_2 yet
                    if now >= scheduled_at and not followup_2_sent:
                        subject, html_body = self.builder.build_followup_reminder(
                            sender_name=customer_name,
                            quote_number=quote_number,
                            total_price=total_price,
                            session_data=session_data,
                            followup_number="scheduled",
                            followup_notes=followup_notes,
                            language=language,
                        )
                        sent = await self._send_reply(
                            to_email=customer_email,
                            subject=subject,
                            html_body=html_body,
                            pdf_path=pdf_path,
                            quote_number=quote_number,
                            token=token,
                        )
                        if sent:
                            self.db.execute_query(
                                "UPDATE email_quote_sessions "
                                "SET followup_2_sent_at=%s WHERE id=%s",
                                (now, sid),
                            )
                            logger.info(
                                f"📅 Scheduled follow-up sent → {customer_email} | {quote_number}"
                            )
                            sent_count += 1
                    continue  # don't apply standard schedule for timeframe sessions

                # ── Scenario A: Standard (no reply) ──────────────────────────
                followup_number = None
                if not followup_2_sent and followup_1_sent and days_since >= 8:
                    followup_number = 2   # "following up again"
                elif not followup_1_sent and days_since >= 3:
                    followup_number = 1   # "just checking in"

                if not followup_number:
                    continue

                subject, html_body = self.builder.build_followup_reminder(
                    sender_name=customer_name,
                    quote_number=quote_number,
                    total_price=total_price,
                    session_data=session_data,
                    followup_number=followup_number,
                    followup_notes=followup_notes,
                    language=language,
                )
                sent = await self._send_reply(
                    to_email=customer_email,
                    subject=subject,
                    html_body=html_body,
                    pdf_path=pdf_path,
                    quote_number=quote_number,
                    token=token,
                )
                if sent:
                    col = f"followup_{followup_number}_sent_at"
                    self.db.execute_query(
                        f"UPDATE email_quote_sessions SET {col}=%s WHERE id=%s",
                        (now, sid),
                    )
                    logger.info(
                        f"📧 Follow-up #{followup_number} sent → "
                        f"{customer_email} | {quote_number} | "
                        f"{days_since:.1f} days since quote"
                    )
                    sent_count += 1

            if sent_count:
                logger.info(f"📧 Follow-up cycle complete: {sent_count} reminder(s) sent")

        except Exception as e:
            logger.error(f"❌ _followup_cycle error: {e}", exc_info=True)

    async def _verify_session_table(self):
        """Log a clear error if email_quote_sessions table is missing.
        Also adds followup tracking columns if they don't exist yet."""
        try:
            rows = self.db.execute_query(
                "SELECT COUNT(*) AS cnt FROM INFORMATION_SCHEMA.TABLES "
                "WHERE TABLE_SCHEMA=DATABASE() "
                "AND TABLE_NAME='email_quote_sessions'",
                fetch=True,
            )
            if rows and rows[0]["cnt"] == 0:
                logger.error(
                    "❌ Table 'email_quote_sessions' is MISSING. "
                    "Run email_quote_sessions_migration.sql before starting."
                )
            else:
                # Add followup columns if not present (idempotent)
                for col, definition in [
                    ("followup_1_sent_at",    "DATETIME NULL DEFAULT NULL"),
                    ("followup_2_sent_at",    "DATETIME NULL DEFAULT NULL"),
                    ("no_followup",           "TINYINT(1) NOT NULL DEFAULT 0"),
                    ("followup_scheduled_at", "DATETIME NULL DEFAULT NULL"),
                    ("followup_notes",        "VARCHAR(500) NULL DEFAULT NULL"),
                ]:
                    exists = self.db.execute_query(
                        "SELECT COUNT(*) AS cnt FROM INFORMATION_SCHEMA.COLUMNS "
                        "WHERE TABLE_SCHEMA=DATABASE() "
                        "AND TABLE_NAME='email_quote_sessions' "
                        f"AND COLUMN_NAME='{col}'",
                        fetch=True,
                    )
                    if exists and exists[0]["cnt"] == 0:
                        self.db.execute_query(
                            f"ALTER TABLE email_quote_sessions "
                            f"ADD COLUMN {col} {definition}",
                        )
                        logger.info(f"✅ Added column email_quote_sessions.{col}")
                logger.info("✅ email_quote_sessions table verified")

                # Also ensure quotations.dynamics_quote_id column exists
                dq_exists = self.db.execute_query(
                    "SELECT COUNT(*) AS cnt FROM INFORMATION_SCHEMA.COLUMNS "
                    "WHERE TABLE_SCHEMA=DATABASE() "
                    "AND TABLE_NAME='quotations' "
                    "AND COLUMN_NAME='dynamics_quote_id'",
                    fetch=True,
                )
                if dq_exists and dq_exists[0]["cnt"] == 0:
                    self.db.execute_query(
                        "ALTER TABLE quotations "
                        "ADD COLUMN dynamics_quote_id VARCHAR(100) NULL DEFAULT NULL"
                    )
                    logger.info("✅ Added column quotations.dynamics_quote_id")
        except Exception as e:
            logger.warning(f"⚠️ Could not verify email_quote_sessions: {e}")

    def expire_stale_sessions(self, ttl_days: int = 7) -> int:
        """
        Close sessions stuck in awaiting_reply for > ttl_days.
        Call once daily from a scheduled task or cron.
        Returns number of rows updated.
        """
        try:
            n = self.db.expire_stale_email_sessions(ttl_days)
            if n:
                logger.info(f"🗑️ Expired {n} stale email_quote_session(s)")
            return n or 0
        except Exception as e:
            logger.warning(f"⚠️ expire_stale_sessions: {e}")
            return 0
