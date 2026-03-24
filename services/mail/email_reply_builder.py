"""
email_reply_builder.py
STRETCH Bot — Email Reply Builder  v1.2

Builds five types of branded HTML email replies:

  1. build_initial_reply()    — First reply: PDF + what-we-used table + questions
  2. build_revised_reply()    — Revised quote after customer spec corrections
  3. build_qualification()    — Info-request when zero project data provided
  4. build_acknowledgment()   — Personalised reply when customer accepts/requests visit/asks info
  5. build_team_forward()     — Internal forward to info@ with full customer context

Supports NL / FR / EN.
All output is self-contained HTML safe to send via Graph API sendMail.

Imports follow the flat layout:
    from .email_quote_processor import Assumption
"""

from datetime import datetime
from typing import Dict, List, Optional, Tuple

# Assumption is a plain dataclass defined in email_quote_processor.py
# Use relative import since both files live in services/mail/
from .email_quote_processor import Assumption


# ─────────────────────────────────────────────────────────────────────────────
#  Translations
# ─────────────────────────────────────────────────────────────────────────────

T: Dict[str, Dict] = {
    "nl": {
        "subj_initial":  "Uw STRETCH Plafond Offerte – {qn}",
        "subj_revised":  "Herziene STRETCH Plafond Offerte – {qn}",
        "subj_qualify":  "Bedankt voor uw interesse – STRETCH",
        "subj_wall":     "Uw STRETCH Wand Offerte – {qn}",
        "greeting":      "Beste {name},",
        "intro_initial": (
            "Bedankt voor uw aanvraag! Op basis van de informatie in uw bericht "
            "hebben we een <strong>indicatieve offerte</strong> opgesteld — "
            "zie de bijlage als PDF."
        ),
        "intro_revised": (
            "Bedankt voor uw correcties. We hebben uw offerte bijgewerkt — "
            "de herziene PDF vindt u in bijlage."
        ),
        "intro_wall": (
            "Bedankt voor uw aanvraag! We hebben een indicatieve offerte voor "
            "uw <strong>rekwand</strong> opgesteld — zie bijlage."
        ),
        "qualify_intro": (
            "Bedankt voor uw contact via onze website! "
            "Om u een nauwkeurige offerte te bezorgen, "
            "hebben we nog wat meer informatie nodig."
        ),
        "qualify_questions": [
            "In welke ruimte(s) wenst u een rekplafond of rekwand?",
            "Wat zijn de afmetingen? (lengte × breedte, of oppervlakte in m²)",
            "Welke kleur en afwerking heeft uw voorkeur? (wit/mat is meest gekozen)",
            "Wanneer wenst u te starten met de renovatie?",
        ],
        "qualify_cta": (
            "Antwoord gewoon op deze e-mail en we sturen u "
            "<strong>binnen enkele minuten</strong> een indicatieve offerte."
        ),
        "low_conf_warning": (
            "⚠️ <em>We konden niet alle specificaties met hoge zekerheid uit uw "
            "bericht halen. Controleer de aannames hieronder zorgvuldig.</em>"
        ),
        "section_used":    "📋 Wat we hebben gebruikt voor uw offerte",
        "section_qa":      "❓ Gelieve te bevestigen of te corrigeren",
        "section_missing": "⚠️ Ontbrekende informatie",
        "col_from":        "Uit uw bericht",
        "col_used":        "Gebruikt",
        "qa_intro": (
            "<strong>Antwoord op deze e-mail</strong> met uw correcties en "
            "we sturen u binnen enkele minuten een bijgewerkte offerte."
        ),
        "missing_intro":   "De volgende informatie is nog nodig voor een nauwkeurige offerte:",
        "total_label":     "Indicatief totaal",
        "validity":        "Deze offerte is geldig gedurende {days} dagen.",
        "indicative": (
            "* Indicatieve offerte op basis van aangenomen waarden. "
            "Definitieve prijs na bevestiging van afmetingen en specificaties."
        ),
        "custom_color": (
            "⚠️ <strong>Opmerking RAL/speciale kleur:</strong> "
            "Eén of meer gevraagde kleuren ({colors}) zijn niet standaard leverbaar. "
            "Speciale productie mogelijk (±6 weken, prijs op aanvraag). "
            "We berekenden de offerte met de dichtstbijzijnde standaardkleur."
        ),
        "wall_note": (
            "Uw aanvraag betreft een <strong>rekwand</strong> "
            "({length}m × {height}m). We berekenden dit als wandoppervlak."
        ),
        "contact_line": (
            "Vragen? <a href='mailto:{email}'>{email}</a> &nbsp;|&nbsp; {phone}"
        ),
        # ── Acknowledgment (acceptance / site visit / more info) ──────────────
        "subj_ack_acceptance":  "Bevestiging ontvangen – Offerte {qn}",
        "subj_ack_site_visit":  "Plaatsbezoek aanvraag ontvangen – Offerte {qn}",
        "subj_ack_more_info":   "Uw vraag ontvangen – Offerte {qn}",
        "ack_acceptance_intro": (
            "Bedankt voor uw bevestiging! We hebben uw bericht goed ontvangen "
            "en ons team neemt zo snel mogelijk contact met u op om alles verder af te handelen."
        ),
        "ack_site_visit_intro": (
            "Bedankt voor uw aanvraag voor een plaatsbezoek! We hebben uw bericht goed ontvangen. "
            "Ons team neemt spoedig contact met u op om een afspraak in te plannen."
        ),
        "ack_more_info_intro": (
            "Bedankt voor uw vraag! We hebben uw bericht goed ontvangen "
            "en ons team beantwoordt uw vraag zo snel mogelijk."
        ),
        "ack_pdf_note": (
            "Ter referentie vindt u uw meest recente offerte opnieuw in bijlage."
        ),
        "ack_closing": (
            "Tot binnenkort!"
        ),
        # ── Team forward ──────────────────────────────────────────────────────
        "subj_fwd_acceptance":  "🟢 AKKOORD – {name} – Offerte {qn}",
        "subj_fwd_site_visit":  "📍 PLAATSBEZOEK GEVRAAGD – {name} – Offerte {qn}",
        "subj_fwd_more_info":   "❓ VRAAG VAN KLANT – {name} – Offerte {qn}",
        "fwd_intro":            "Een klant heeft gereageerd op offerte {qn}.",
        "fwd_intent_label":     "Type reactie",
        "fwd_intent_acceptance":"Akkoord / wil verder gaan",
        "fwd_intent_site_visit":"Plaatsbezoek / opmeting gevraagd",
        "fwd_intent_more_info": "Bijkomende vraag",
        "fwd_customer_label":   "Klant",
        "fwd_email_label":      "E-mail",
        "fwd_message_label":    "Bericht van de klant",
        "fwd_action":           "Gelieve deze klant zo snel mogelijk te contacteren.",
        "fwd_pdf_note":         "De meest recente offerte is bijgevoegd als referentie.",
    },
    "fr": {
        "subj_initial":  "Votre Devis Plafond Tendu – {qn}",
        "subj_revised":  "Devis Révisé Plafond Tendu – {qn}",
        "subj_qualify":  "Merci pour votre intérêt – STRETCH",
        "subj_wall":     "Votre Devis Mur Tendu – {qn}",
        "greeting":      "Cher(e) {name},",
        "intro_initial": (
            "Merci pour votre demande ! Sur la base de votre message, "
            "nous avons préparé un <strong>devis indicatif</strong> — "
            "veuillez trouver le PDF en pièce jointe."
        ),
        "intro_revised": (
            "Merci pour vos corrections. Nous avons mis à jour votre devis — "
            "le PDF révisé est en pièce jointe."
        ),
        "intro_wall": (
            "Merci pour votre demande ! Nous avons préparé un devis indicatif "
            "pour votre <strong>mur tendu</strong> — voir pièce jointe."
        ),
        "qualify_intro": (
            "Merci de nous avoir contactés ! "
            "Pour vous fournir un devis précis, "
            "nous avons besoin de quelques informations supplémentaires."
        ),
        "qualify_questions": [
            "Dans quelle(s) pièce(s) souhaitez-vous un plafond ou mur tendu ?",
            "Quelles sont les dimensions ? (longueur × largeur, ou surface en m²)",
            "Quelle couleur et finition préférez-vous ? (blanc/mat est le plus courant)",
            "Quand souhaitez-vous commencer les travaux ?",
        ],
        "qualify_cta": (
            "Répondez simplement à cet e-mail et nous vous enverrons un devis "
            "indicatif <strong>en quelques minutes</strong>."
        ),
        "low_conf_warning": (
            "⚠️ <em>Nous n'avons pas pu extraire toutes les spécifications "
            "avec certitude. Veuillez vérifier les hypothèses ci-dessous.</em>"
        ),
        "section_used":    "📋 Ce que nous avons utilisé pour votre devis",
        "section_qa":      "❓ Veuillez confirmer ou corriger",
        "section_missing": "⚠️ Informations manquantes",
        "col_from":        "De votre message",
        "col_used":        "Utilisé",
        "qa_intro": (
            "<strong>Répondez à cet e-mail</strong> avec vos corrections et "
            "nous vous enverrons un devis mis à jour en quelques minutes."
        ),
        "missing_intro":   "Les informations suivantes sont nécessaires pour un devis précis :",
        "total_label":     "Total indicatif",
        "validity":        "Ce devis est valable {days} jours.",
        "indicative": (
            "* Devis indicatif basé sur des valeurs supposées. "
            "Prix final après confirmation des dimensions et spécifications."
        ),
        "custom_color": (
            "⚠️ <strong>Note couleur RAL/spéciale :</strong> "
            "Une ou plusieurs couleurs demandées ({colors}) ne sont pas disponibles en standard. "
            "Production spéciale possible (±6 semaines, prix sur demande). "
            "Devis calculé avec la couleur standard la plus proche."
        ),
        "wall_note": (
            "Votre demande concerne un <strong>mur tendu</strong> "
            "({length}m × {height}m). Nous avons calculé cela comme surface murale."
        ),
        "contact_line": (
            "Questions ? <a href='mailto:{email}'>{email}</a> &nbsp;|&nbsp; {phone}"
        ),
        # ── Acknowledgment ────────────────────────────────────────────────────
        "subj_ack_acceptance":  "Confirmation reçue – Devis {qn}",
        "subj_ack_site_visit":  "Demande de visite reçue – Devis {qn}",
        "subj_ack_more_info":   "Votre question reçue – Devis {qn}",
        "ack_acceptance_intro": (
            "Merci pour votre confirmation ! Nous avons bien reçu votre message "
            "et notre équipe vous contactera dans les plus brefs délais pour finaliser les détails."
        ),
        "ack_site_visit_intro": (
            "Merci pour votre demande de visite ! Nous avons bien reçu votre message. "
            "Notre équipe vous contactera prochainement pour planifier un rendez-vous."
        ),
        "ack_more_info_intro": (
            "Merci pour votre question ! Nous avons bien reçu votre message "
            "et notre équipe vous répondra dans les meilleurs délais."
        ),
        "ack_pdf_note": (
            "Pour référence, vous trouverez votre devis le plus récent en pièce jointe."
        ),
        "ack_closing": "À bientôt !",
        # ── Team forward ──────────────────────────────────────────────────────
        "subj_fwd_acceptance":  "🟢 ACCORD – {name} – Devis {qn}",
        "subj_fwd_site_visit":  "📍 VISITE DEMANDÉE – {name} – Devis {qn}",
        "subj_fwd_more_info":   "❓ QUESTION CLIENT – {name} – Devis {qn}",
        "fwd_intro":            "Un client a répondu au devis {qn}.",
        "fwd_intent_label":     "Type de réponse",
        "fwd_intent_acceptance":"Accord / souhaite procéder",
        "fwd_intent_site_visit":"Visite / métrage demandé",
        "fwd_intent_more_info": "Question complémentaire",
        "fwd_customer_label":   "Client",
        "fwd_email_label":      "E-mail",
        "fwd_message_label":    "Message du client",
        "fwd_action":           "Veuillez contacter ce client dans les plus brefs délais.",
        "fwd_pdf_note":         "Le devis le plus récent est joint en référence.",
    },
    "en": {
        "subj_initial":  "Your Stretch Ceiling Quote – {qn}",
        "subj_revised":  "Revised Stretch Ceiling Quote – {qn}",
        "subj_qualify":  "Thank you for your interest – STRETCH",
        "subj_wall":     "Your Stretch Wall Quote – {qn}",
        "greeting":      "Dear {name},",
        "intro_initial": (
            "Thank you for your request! Based on your message we have prepared "
            "an <strong>indicative quote</strong> — please find the PDF attached."
        ),
        "intro_revised": (
            "Thank you for your corrections. We have updated your quote — "
            "the revised PDF is attached."
        ),
        "intro_wall": (
            "Thank you for your request! We have prepared an indicative quote "
            "for your <strong>stretch wall</strong> — please find the PDF attached."
        ),
        "qualify_intro": (
            "Thank you for contacting us! "
            "To prepare an accurate quote we need a little more information."
        ),
        "qualify_questions": [
            "Which room(s) would you like a stretch ceiling or wall in?",
            "What are the dimensions? (length × width, or area in m²)",
            "What color and finish do you prefer? (white/mat is most popular)",
            "When are you planning to start the renovation?",
        ],
        "qualify_cta": (
            "Simply reply to this email and we will send you an indicative quote "
            "<strong>within minutes</strong>."
        ),
        "low_conf_warning": (
            "⚠️ <em>We could not extract all specifications with high confidence. "
            "Please review the assumptions below carefully.</em>"
        ),
        "section_used":    "📋 What we used to build this quote",
        "section_qa":      "❓ Please confirm or correct",
        "section_missing": "⚠️ Missing information",
        "col_from":        "From your message",
        "col_used":        "Used",
        "qa_intro": (
            "<strong>Reply to this email</strong> with any corrections and "
            "we will send you an updated quote within minutes."
        ),
        "missing_intro":   "The following information is still needed for an accurate quote:",
        "total_label":     "Indicative total",
        "validity":        "This quote is valid for {days} days.",
        "indicative": (
            "* Indicative quote based on assumed values. "
            "Final price subject to confirmation of dimensions and specifications."
        ),
        "custom_color": (
            "⚠️ <strong>Custom color note:</strong> "
            "One or more requested colors ({colors}) are not in our standard range. "
            "Custom production possible (±6 weeks, price on request). "
            "Quote calculated using the nearest standard color."
        ),
        "wall_note": (
            "Your request is for a <strong>stretch wall</strong> "
            "({length}m × {height}m). We calculated this as a wall surface."
        ),
        "contact_line": (
            "Questions? <a href='mailto:{email}'>{email}</a> &nbsp;|&nbsp; {phone}"
        ),
        # ── Acknowledgment ────────────────────────────────────────────────────
        "subj_ack_acceptance":  "Confirmation received – Quote {qn}",
        "subj_ack_site_visit":  "Site visit request received – Quote {qn}",
        "subj_ack_more_info":   "Your question received – Quote {qn}",
        "ack_acceptance_intro": (
            "Thank you for your confirmation! We have received your message "
            "and our team will contact you as soon as possible to finalise the details."
        ),
        "ack_site_visit_intro": (
            "Thank you for requesting a site visit! We have received your message. "
            "Our team will contact you shortly to schedule an appointment."
        ),
        "ack_more_info_intro": (
            "Thank you for your question! We have received your message "
            "and our team will get back to you as soon as possible."
        ),
        "ack_pdf_note": (
            "For your reference, the latest version of your quote is attached."
        ),
        "ack_closing": "See you soon!",
        # ── Team forward ──────────────────────────────────────────────────────
        "subj_fwd_acceptance":  "🟢 ACCEPTED – {name} – Quote {qn}",
        "subj_fwd_site_visit":  "📍 SITE VISIT REQUESTED – {name} – Quote {qn}",
        "subj_fwd_more_info":   "❓ CUSTOMER QUESTION – {name} – Quote {qn}",
        "fwd_intro":            "A customer has replied to quote {qn}.",
        "fwd_intent_label":     "Reply type",
        "fwd_intent_acceptance":"Accepted / wants to proceed",
        "fwd_intent_site_visit":"Site visit / measurement requested",
        "fwd_intent_more_info": "Additional question",
        "fwd_customer_label":   "Customer",
        "fwd_email_label":      "Email",
        "fwd_message_label":    "Customer message",
        "fwd_action":           "Please contact this customer as soon as possible.",
        "fwd_pdf_note":         "The latest quote is attached for reference.",
    },
}


# ─────────────────────────────────────────────────────────────────────────────
#  Builder
# ─────────────────────────────────────────────────────────────────────────────

class EmailReplyBuilder:
    """
    Builds subject + HTML body for all outbound email types.

    Usage:
        builder = EmailReplyBuilder()

        subject, html = builder.build_initial_reply(
            sender_name="Martijn Dijkman",
            quote_number="QT20260322ABCD",
            total_price=1245.50,
            session_data=session_data,
            assumptions=result.assumptions,
            missing_fields=result.missing_fields,
            language="nl",
            confidence_score=0.55,
            is_wall=False,
            needs_custom_color=False,
            custom_color_codes=[],
        )
    """

    COMPANY_NAME    = "STRETCH BV"
    COMPANY_EMAIL   = "assistant_quotes@stretchgroup.be"
    COMPANY_PHONE   = "+32 3 284 68 18"
    COMPANY_WEBSITE = "www.stretchplafond.be"
    QUOTE_VALIDITY  = 30

    # Brand palette
    CR = "#E30613"    # STRETCH red
    CB = "#1a1a1a"    # near-black
    CG = "#555555"    # dark grey text
    CL = "#f5f5f5"    # light background
    CE = "#e0e0e0"    # border
    CW = "#fff8e1"    # warning background
    CWB = "#ffc107"   # warning border
    CI = "#e8f4fd"    # info background
    CIB = "#2196f3"   # info border

    # ─────────────────────────────────────────────────────────────────────────
    #  Public builders
    # ─────────────────────────────────────────────────────────────────────────

    def build_initial_reply(
        self,
        sender_name: str,
        quote_number: str,
        total_price: float,
        session_data: Dict,
        assumptions: List[Assumption],
        missing_fields: List[str],
        language: str = "nl",
        confidence_score: float = 1.0,
        is_wall: bool = False,
        needs_custom_color: bool = False,
        custom_color_codes: Optional[List[str]] = None,
    ) -> Tuple[str, str]:
        """First reply: PDF attachment + assumption table + clarifying questions."""
        t       = self._t(language)
        subject = t["subj_wall" if is_wall else "subj_initial"].format(qn=quote_number)
        body    = self._build_body(
            t=t, sender_name=sender_name, quote_number=quote_number,
            total_price=total_price, session_data=session_data,
            assumptions=assumptions, missing_fields=missing_fields,
            confidence_score=confidence_score, is_revision=False,
            is_wall=is_wall, needs_custom_color=needs_custom_color,
            custom_color_codes=custom_color_codes or [],
        )
        return subject, self._wrap(language, quote_number, body)

    def build_revised_reply(
        self,
        sender_name: str,
        quote_number: str,
        total_price: float,
        session_data: Dict,
        remaining_assumptions: List[Assumption],
        language: str = "nl",
        revision_number: int = 1,
        is_wall: bool = False,
    ) -> Tuple[str, str]:
        """Revised reply after customer corrections."""
        t       = self._t(language)
        subject = t["subj_revised"].format(qn=quote_number)
        body    = self._build_body(
            t=t, sender_name=sender_name, quote_number=quote_number,
            total_price=total_price, session_data=session_data,
            assumptions=remaining_assumptions, missing_fields=[],
            confidence_score=1.0, is_revision=True,
            is_wall=is_wall, needs_custom_color=False, custom_color_codes=[],
        )
        return subject, self._wrap(language, quote_number, body)

    def build_qualification(
        self,
        sender_name: str,
        language: str = "nl",
    ) -> Tuple[str, str]:
        """Info-request email when not enough data for any quote."""
        t    = self._t(language)
        name = sender_name.split()[0] if sender_name else "there"
        qs   = "".join(f"<li>{q}</li>" for q in t["qualify_questions"])
        body = (
            f"<p>{t['greeting'].format(name=name)}</p>"
            f"<p>{t['qualify_intro']}</p>"
            f'<div class="info-box"><ol>{qs}</ol></div>'
            f"<p>{t['qualify_cta']}</p>"
            f"{self._contact_line(t)}"
        )
        return t["subj_qualify"], self._wrap(language, "", body)

    def build_acknowledgment(
        self,
        sender_name: str,
        quote_number: str,
        intent: str,
        customer_message: str,
        language: str = "nl",
    ) -> Tuple[str, str]:
        """
        Personalised acknowledgment to the customer after they:
          - accept the quote ('acceptance')
          - request a site visit ('site_visit')
          - ask a general question ('more_info')
        PDF is attached by the caller via _send_reply.
        """
        t    = self._t(language)
        name = sender_name.split()[0] if sender_name else "there"

        subj_key  = f"subj_ack_{intent}"
        intro_key = f"ack_{intent}_intro"
        subject   = t.get(subj_key,  t["subj_ack_more_info"]).format(qn=quote_number)
        intro     = t.get(intro_key, t["ack_more_info_intro"])

        body = (
            f"<p>{t['greeting'].format(name=name)}</p>"
            f"<p>{intro}</p>"
            f'<div class="info-box">'
            f'{t["ack_pdf_note"]}'
            f'</div>'
            f"<p>{t['ack_closing']}</p>"
            f"{self._contact_line(t)}"
        )
        return subject, self._wrap(language, quote_number, body)

    def build_followup_reminder(
        self,
        sender_name:     str,
        quote_number:    str,
        total_price:     float,
        session_data:    dict,
        followup_number: int,       # 1, 2, or "scheduled"
        followup_notes:  str = "",
        language:        str = "nl",
    ) -> Tuple[str, str]:
        """
        Personalised follow-up reminder.

        followup_number=1        → Day 3, first contact, no previous messages
        followup_number=2        → Day 8, mention we already sent a reminder
        followup_number="scheduled" → Customer gave a timeframe, now it's time
        """
        t    = self._t(language)
        name = sender_name.split()[0] if sender_name else ""
        greet = f"Beste {name}," if name else "Beste,"
        if language == "fr":
            greet = f"Bonjour {name}," if name else "Bonjour,"

        if language == "fr":
            if followup_number == "scheduled":
                subject = f"Votre devis {quote_number} – Comme convenu"
                intro   = (
                    f"Comme vous nous l'aviez indiqué, voici votre devis "
                    f"{quote_number} pour un montant de EUR {total_price:,.2f}. "
                    f"Nous restons à votre disposition pour avancer sur votre projet."
                )
            elif followup_number == 2:
                subject = f"Deuxième rappel – Votre devis {quote_number}"
                intro   = (
                    f"Nous nous permettons de vous recontacter suite à notre "
                    f"précédent message au sujet de votre devis {quote_number}. "
                    f"Avez-vous des questions ou souhaitez-vous procéder?"
                )
            else:
                subject = f"Rappel – Votre devis {quote_number} est toujours valide"
                intro   = (
                    f"Nous espérons que vous avez bien reçu votre devis {quote_number}. "
                    f"Nous voulions simplement vérifier si vous avez des questions "
                    f"ou souhaitez procéder."
                )
        else:
            if followup_number == "scheduled":
                subject = f"Uw offerte {quote_number} – Zoals besproken"
                intro   = (
                    f"Zoals u had aangegeven is het nu een goed moment om terug "
                    f"te keren naar uw offerte {quote_number} voor EUR {total_price:,.2f}. "
                    f"We helpen u graag verder met uw project."
                )
            elif followup_number == 2:
                subject = f"Tweede herinnering – Offerte {quote_number}"
                intro   = (
                    f"We nemen nogmaals contact op, want we hebben nog geen reactie "
                    f"ontvangen op onze vorige herinnering over offerte {quote_number}. "
                    f"Heeft u nog vragen, of kan u ons laten weten hoe u verder wenst te gaan?"
                )
            else:
                subject = f"Herinnering – Uw offerte {quote_number} is nog geldig"
                intro   = (
                    f"We wilden even opvolgen of u onze offerte {quote_number} "
                    f"heeft ontvangen en of u nog vragen heeft of wenst verder te gaan."
                )

        body = (
            f"<p>{greet}</p>"
            f"<p>{intro}</p>"
            f"{self._ceiling_table(session_data, t)}"
            f"{self._total_block(total_price, t, session_data)}"
            f"<p>Heeft u vragen of wilt u een afspraak plannen? "
            f'Neem contact op via <a href="mailto:{self.COMPANY_EMAIL}">'
            f"{self.COMPANY_EMAIL}</a> of bel {self.COMPANY_PHONE}.</p>"
            f"{self._contact_line(t)}"
        )
        return subject, self._wrap(language, quote_number, body)

    def build_team_forward(
        self,
        customer_name: str,
        customer_email: str,
        quote_number: str,
        intent: str,
        customer_message: str,
        language: str = "nl",
    ) -> Tuple[str, str]:
        """
        Internal forward to info@stretchgroup.be with full context.
        Always in Dutch regardless of customer language (internal comms).
        PDF attached by the caller.
        """
        t = self._t("nl")   # always Dutch for internal team email

        subj_key = f"subj_fwd_{intent}"
        subject  = t.get(subj_key, t["subj_fwd_more_info"]).format(
            name=customer_name, qn=quote_number
        )

        intent_labels = {
            "acceptance": t["fwd_intent_acceptance"],
            "site_visit": t["fwd_intent_site_visit"],
            "more_info":  t["fwd_intent_more_info"],
        }
        intent_label = intent_labels.get(intent, intent)

        # Highlight color per intent
        intent_color = {
            "acceptance": "#28a745",
            "site_visit": "#2196f3",
            "more_info":  "#fd7e14",
        }.get(intent, "#555555")

        # Clean up the customer message for display
        clean_msg = customer_message.strip()[:1500].replace("<", "&lt;").replace(">", "&gt;")

        body = (
            f"<p>Beste team,</p>"
            f"<p>{t['fwd_intro'].format(qn=quote_number)}</p>"
            f'<table class="data-table">'
            f'<tbody>'
            f'<tr><td><strong>{t["fwd_intent_label"]}</strong></td>'
            f'<td><span style="color:{intent_color};font-weight:bold">'
            f'{intent_label}</span></td></tr>'
            f'<tr><td><strong>{t["fwd_customer_label"]}</strong></td>'
            f'<td>{customer_name}</td></tr>'
            f'<tr><td><strong>{t["fwd_email_label"]}</strong></td>'
            f'<td><a href="mailto:{customer_email}">{customer_email}</a></td></tr>'
            f'</tbody></table>'
            f'<div class="section-box">'
            f'<h3>{t["fwd_message_label"]}</h3>'
            f'<p style="white-space:pre-wrap;font-family:monospace;font-size:13px">'
            f'{clean_msg}</p>'
            f'</div>'
            f'<div class="warn-box">'
            f'<strong>{t["fwd_action"]}</strong><br/>'
            f'{t["fwd_pdf_note"]}'
            f'</div>'
        )
        return subject, self._wrap("nl", quote_number, body)

    # ─────────────────────────────────────────────────────────────────────────
    #  Core body builder
    # ─────────────────────────────────────────────────────────────────────────

    def _build_body(
        self,
        t: Dict, sender_name: str, quote_number: str,
        total_price: float, session_data: Dict,
        assumptions: List[Assumption], missing_fields: List[str],
        confidence_score: float, is_revision: bool,
        is_wall: bool, needs_custom_color: bool,
        custom_color_codes: List[str],
    ) -> str:
        name = sender_name.split()[0] if sender_name else "there"
        out  = []

        out.append(f"<p>{t['greeting'].format(name=name)}</p>")

        if confidence_score < 0.65 and not is_revision:
            out.append(f'<div class="warn-box">{t["low_conf_warning"]}</div>')

        if is_revision:
            out.append(f"<p>{t['intro_revised']}</p>")
        elif is_wall:
            ceilings = session_data.get("ceilings", [{}])
            c = ceilings[0] if ceilings else {}
            out.append(f"<p>{t['intro_wall']}</p>")
            out.append(
                f'<div class="info-box">'
                f'{t["wall_note"].format(length=c.get("length","?"), height=c.get("width","?"))}'
                f'</div>'
            )
        else:
            out.append(f"<p>{t['intro_initial']}</p>")

        if needs_custom_color and custom_color_codes:
            out.append(
                f'<div class="warn-box">'
                f'{t["custom_color"].format(colors=", ".join(custom_color_codes))}'
                f'</div>'
            )

        out.append(self._ceiling_table(session_data, t))
        out.append(self._total_block(total_price, t, session_data))

        if assumptions:
            out.append(self._assumptions_table(assumptions, t))

        if missing_fields:
            items = "".join(f"<li>{f}</li>" for f in missing_fields)
            out.append(
                f'<div class="warn-box">'
                f'<strong>{t["section_missing"]}</strong><br/>'
                f'{t["missing_intro"]}<ul>{items}</ul>'
                f'</div>'
            )

        if assumptions or missing_fields:
            out.append(self._questions_block(assumptions, missing_fields, t))

        out.append(
            f'<p class="small">'
            f'{t["validity"].format(days=self.QUOTE_VALIDITY)}<br/>'
            f'{t["indicative"]}'
            f'</p>'
        )
        out.append(self._contact_line(t))
        return "\n".join(out)

    # ─────────────────────────────────────────────────────────────────────────
    #  Section builders
    # ─────────────────────────────────────────────────────────────────────────

    def _ceiling_table(self, session_data: Dict, t: Dict) -> str:
        ceilings     = session_data.get("ceilings", [])
        costs        = session_data.get("ceiling_costs", [])
        quote_lights = session_data.get("quote_lights", [])
        rows = ""
        for i, c in enumerate(ceilings):
            cost_total = 0.0
            if i < len(costs):
                cv = costs[i]
                cost_total = cv.get("total", 0.0) if isinstance(cv, dict) else 0.0
            rows += (
                f"<tr>"
                f"<td><strong>{c.get('name', f'Ceiling {i+1}')}</strong></td>"
                f"<td>{c.get('length',0)}m × {c.get('width',0)}m "
                f"<span style='color:{self.CG};font-size:12px'>({c.get('area',0):.1f} m²)</span></td>"
                f"<td>{c.get('ceiling_type','').upper()} — "
                f"{c.get('type_ceiling','')} — "
                f"{c.get('color','').capitalize()}</td>"
                f"<td style='text-align:right'><strong>€ {cost_total:,.2f}</strong></td>"
                f"</tr>"
            )
        # Quote-level lights (not assigned to a specific room)
        for light in quote_lights:
            qty   = int(light.get("quantity", 1))
            price = float(light.get("price") or light.get("price_b2c") or 0)
            rows += (
                f"<tr style='background:{self.CW}'>"
                f"<td><strong>{light.get('description', light.get('product_code',''))}</strong>"
                f"<br/><span style='font-size:11px;color:{self.CG}'>"
                f"⚠️ Room not specified — please confirm</span></td>"
                f"<td>{light.get('product_code','')}</td>"
                f"<td>{qty} pcs</td>"
                f"<td style='text-align:right'><strong>€ {price * qty:,.2f}</strong></td>"
                f"</tr>"
            )
        return (
            f'<table class="data-table">'
            f'<thead><tr>'
            f'<th>Room / Product</th><th>Dimensions</th><th>Specification</th>'
            f'<th style="text-align:right">Price</th>'
            f'</tr></thead>'
            f'<tbody>{rows}</tbody>'
            f'</table>'
        )

    def _total_block(self, total: float, t: Dict, session_data: Optional[Dict] = None) -> str:
        # Add quote-level lights cost to total
        extra = 0.0
        if session_data:
            for light in session_data.get("quote_lights", []):
                price = float(light.get("price") or light.get("price_b2c") or 0)
                extra += price * int(light.get("quantity", 1))
        grand_total = total + extra
        return (
            f'<div class="total-block">'
            f'{t["total_label"]}: '
            f'<span class="total-amount">€ {grand_total:,.2f}</span>'
            f'</div>'
        )

    def _assumptions_table(self, assumptions: List[Assumption], t: Dict) -> str:
        rows = ""
        for a in assumptions:
            cc = {"high": "#28a745", "medium": "#fd7e14", "low": "#dc3545"}.get(
                a.confidence, "#fd7e14"
            )
            rows += (
                f"<tr>"
                f"<td><em>{t['col_from']}:</em> {a.user_said}</td>"
                f"<td>→</td>"
                f"<td><strong>{a.assumed}</strong></td>"
                f"<td><span style='color:{cc};font-size:11px;font-weight:bold'>"
                f"{a.confidence}</span></td>"
                f"</tr>"
            )
        return (
            f'<div class="section-box">'
            f'<h3>{t["section_used"]}</h3>'
            f'<table class="assump-table"><tbody>{rows}</tbody></table>'
            f'</div>'
        )

    def _questions_block(
        self, assumptions: List[Assumption], missing: List[str], t: Dict
    ) -> str:
        items = ""
        idx   = 1
        # Low/medium first (most uncertain)
        for a in sorted(
            assumptions,
            key=lambda x: {"low": 0, "medium": 1, "high": 2}[x.confidence]
        ):
            if a.confidence in ("low", "medium"):
                items += f"<li><strong>V{idx}:</strong> {a.question}</li>"
                idx   += 1
        # High confidence (just for verification)
        for a in assumptions:
            if a.confidence == "high":
                items += f"<li><strong>V{idx}:</strong> {a.question}</li>"
                idx   += 1
        for f in missing:
            items += (
                f"<li><strong>V{idx}:</strong> "
                f"Kunt u de <em>{f}</em> opgeven?</li>"
            )
            idx += 1
        if not items:
            return ""
        return (
            f'<div class="qa-box">'
            f'<h3>{t["section_qa"]}</h3>'
            f'<p>{t["qa_intro"]}</p>'
            f'<ol>{items}</ol>'
            f'</div>'
        )

    def _contact_line(self, t: Dict) -> str:
        return (
            f'<p class="contact">'
            f'{t["contact_line"].format(email=self.COMPANY_EMAIL, phone=self.COMPANY_PHONE)}'
            f'</p>'
        )

    # ─────────────────────────────────────────────────────────────────────────
    #  Full HTML wrapper
    # ─────────────────────────────────────────────────────────────────────────

    def _wrap(self, language: str, quote_number: str, body: str) -> str:
        ref_bar = (
            f'<div class="ref-bar">'
            f'Referentie: {quote_number} &nbsp;|&nbsp; '
            f'{datetime.now().strftime("%d %B %Y")}'
            f'</div>'
        ) if quote_number else ""

        css = f"""
  *{{box-sizing:border-box;margin:0;padding:0}}
  body{{font-family:Arial,Helvetica,sans-serif;font-size:14px;
        color:{self.CB};background:#fff}}
  .wrap{{max-width:660px;margin:0 auto;background:#fff}}
  .hdr{{background:{self.CB};padding:22px 28px}}
  .logo{{color:#fff;font-size:26px;font-weight:900;letter-spacing:4px}}
  .logo-accent{{color:{self.CR}}}
  .tagline{{color:#aaa;font-size:10px;letter-spacing:2px;
            text-transform:uppercase;margin-top:4px}}
  .ref-bar{{background:{self.CR};color:#fff;padding:9px 28px;
            font-size:12px;font-weight:bold;letter-spacing:1px}}
  .body{{padding:26px 28px;line-height:1.65}}
  p{{margin:0 0 14px}}
  h3{{font-size:14px;color:{self.CB};margin:22px 0 10px;
      border-bottom:2px solid {self.CR};padding-bottom:5px}}
  .data-table,.assump-table{{width:100%;border-collapse:collapse;
                              font-size:13px;margin:10px 0}}
  .data-table th{{background:{self.CL};padding:7px 9px;text-align:left;
                  border-bottom:2px solid {self.CE};font-size:11px;
                  text-transform:uppercase;letter-spacing:.4px}}
  .data-table td,.assump-table td{{padding:7px 9px;
                                   border-bottom:1px solid {self.CE};
                                   vertical-align:top}}
  .total-block{{background:{self.CB};color:#fff;padding:14px 18px;
                margin:18px 0;font-size:15px;font-weight:bold;
                border-radius:3px}}
  .total-amount{{color:{self.CR};font-size:20px;margin-left:8px}}
  .section-box{{background:{self.CL};border-left:4px solid {self.CR};
                padding:14px 18px;margin:18px 0;border-radius:0 3px 3px 0}}
  .warn-box{{background:{self.CW};border-left:4px solid {self.CWB};
             padding:12px 16px;margin:14px 0;font-size:13px;
             border-radius:0 3px 3px 0}}
  .qa-box{{background:{self.CI};border-left:4px solid {self.CIB};
           padding:14px 18px;margin:18px 0;border-radius:0 3px 3px 0}}
  .qa-box ol{{margin:10px 0 0 0;padding-left:18px;line-height:1.9}}
  .info-box{{background:{self.CL};border-left:4px solid {self.CIB};
             padding:14px 18px;margin:14px 0}}
  .info-box ol{{margin:8px 0 0 0;padding-left:18px;line-height:1.9}}
  .small{{font-size:11px;color:#888;margin-top:20px;
          border-top:1px solid {self.CE};padding-top:8px}}
  .contact{{font-size:12px;color:#888;margin-top:6px}}
  .contact a{{color:{self.CR};text-decoration:none}}
  .ftr{{background:{self.CL};border-top:3px solid {self.CR};
        padding:14px 28px;font-size:11px;color:#888;text-align:center}}
  .ftr a{{color:{self.CR};text-decoration:none}}"""

        return f"""<!DOCTYPE html>
<html lang="{language}">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1.0"/>
<style>{css}</style>
</head>
<body>
<div class="wrap">
  <div class="hdr">
    <div class="logo">STR<span class="logo-accent">═</span>TCH</div>
    <div class="tagline">Ceilings &amp; Walls</div>
  </div>
  {ref_bar}
  <div class="body">{body}</div>
  <div class="ftr">
    <strong>{self.COMPANY_NAME}</strong> &nbsp;|&nbsp;
    Gentseweg 309 A3, 9120 Beveren-Waas &nbsp;|&nbsp;
    {self.COMPANY_PHONE}<br/>
    <a href="https://{self.COMPANY_WEBSITE}">{self.COMPANY_WEBSITE}</a>
    &nbsp;|&nbsp; BE0675875709 &nbsp;|&nbsp; IBAN BE63001882761108
  </div>
</div>
</body>
</html>"""

    def _t(self, language: str) -> Dict:
        return T.get(language, T["nl"])