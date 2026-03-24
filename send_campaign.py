#!/usr/bin/env python3
"""
send_campaign.py — STRETCH re-engagement email campaign

Usage:
    python3 send_campaign.py --csv contacts.csv [--dry-run] [--template spring2026]

CSV format (header required):
    email, name, language
    jan.peeters@gmail.com, Jan Peeters, nl
    marie.dupont@gmail.com, Marie Dupont, fr
    john.smith@gmail.com, John Smith, en

The script:
  1. Reads the CSV
  2. Sends each contact a personalised re-engagement email
  3. Creates an email_quote_session in the DB (status='campaign_sent')
     so any reply is automatically picked up and processed by the bot

Requirements:
  - .env file in the same directory as the bot (~/STRETCH_NEW/.env)
  - MySQL accessible from this machine
  - Microsoft Graph API credentials in .env
"""

import argparse
import csv
import json
import os
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path

import mysql.connector
import requests

# ── Load .env ─────────────────────────────────────────────────────────────────
ENV_PATH = Path(__file__).parent / ".env"
if ENV_PATH.exists():
    for line in ENV_PATH.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, _, v = line.partition("=")
            os.environ.setdefault(k.strip(), v.strip())

# ── Config ────────────────────────────────────────────────────────────────────
TENANT_ID     = os.environ.get("AZURE_TENANT_ID",     "53a74225-a02a-4073-958e-57c31880e64b")
CLIENT_ID     = os.environ.get("AZURE_CLIENT_ID",     "623d7bd2-b256-4ecd-8234-d316afbc9357")
CLIENT_SECRET = os.environ.get("AZURE_CLIENT_SECRET", "")
FROM_EMAIL    = os.environ.get("EMAIL_FROM",           "assistant_quotes@stretchgroup.be")

DB_HOST   = os.environ.get("MYSQL_HOST",     "aibot.mysql.database.azure.com")
DB_PORT   = int(os.environ.get("MYSQL_PORT", "3306"))
DB_NAME   = os.environ.get("MYSQL_DB",       "chatbot_db")
DB_USER   = os.environ.get("MYSQL_USER",     "STRETCH")
DB_PASS   = os.environ.get("DB_PASSWORD") or os.environ.get("MYSQL_PASSWORD", "")


# ─────────────────────────────────────────────────────────────────────────────
#  Email templates
# ─────────────────────────────────────────────────────────────────────────────

def build_email(name: str, language: str, template: str) -> tuple[str, str]:
    """Return (subject, html_body) for the given contact and template."""
    first = name.split()[0] if name else name

    # ── Dutch ──────────────────────────────────────────────────────────────
    if language in ("nl", "be"):
        subject = "Bent u klaar voor een nieuw spanplafond?"
        html = f"""
<div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
  <div style="background: #cc0000; padding: 20px; text-align: center;">
    <h1 style="color: white; margin: 0;">STRETCH BV</h1>
    <p style="color: #ffcccc; margin: 5px 0;">Professionele Spanplafond Oplossingen</p>
  </div>

  <div style="padding: 30px; background: #ffffff;">
    <p>Beste {first},</p>

    <p>We nemen even contact met u op om te vragen of u nog interesse heeft in een
    spanplafond voor uw woning of project.</p>

    <p>Bij STRETCH BV bieden we:</p>
    <ul>
      <li>Professionele installatie door gecertificeerde monteurs</li>
      <li>Meer dan 200 kleuren en afwerkingen</li>
      <li>10 jaar garantie op materialen</li>
      <li>Snelle levering: 2–3 weken na bestelling</li>
      <li>Gratis offerte op maat</li>
    </ul>

    <p><strong>Vraag vandaag nog uw gratis offerte aan!</strong><br>
    Stuur ons gewoon een bericht terug met de afmetingen van de ruimte(s) die u wilt
    voorzien van een spanplafond, en wij maken een gedetailleerde offerte voor u op.</p>

    <p>Bijvoorbeeld:<br>
    <em>"Woonkamer 6×4m, slaapkamer 4×3m, keuken 3×3m"</em></p>

    <div style="background: #f5f5f5; padding: 15px; border-radius: 5px; margin: 20px 0;">
      <p style="margin: 0;"><strong>Of bel ons:</strong> +32 3 284 68 18<br>
      <strong>Of bezoek:</strong> <a href="https://www.stretchplafond.be">www.stretchplafond.be</a></p>
    </div>

    <p>Met vriendelijke groeten,<br>
    <strong>Het STRETCH BV team</strong></p>
  </div>

  <div style="background: #333; padding: 15px; text-align: center; font-size: 12px; color: #aaa;">
    STRETCH BV • Gentseweg 309 A3, 9120 Beveren-Waas, België<br>
    BTW: BE0675875709 • Tel: +32 3 284 68 18
  </div>
</div>"""

    # ── French ──────────────────────────────────────────────────────────────
    elif language == "fr":
        subject = "Êtes-vous prêt(e) pour un nouveau plafond tendu?"
        html = f"""
<div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
  <div style="background: #cc0000; padding: 20px; text-align: center;">
    <h1 style="color: white; margin: 0;">STRETCH BV</h1>
    <p style="color: #ffcccc; margin: 5px 0;">Solutions professionnelles de plafonds tendus</p>
  </div>

  <div style="padding: 30px; background: #ffffff;">
    <p>Cher(e) {first},</p>

    <p>Nous prenons contact avec vous pour savoir si vous êtes toujours intéressé(e)
    par un plafond tendu pour votre habitation ou votre projet.</p>

    <p>Chez STRETCH BV, nous proposons :</p>
    <ul>
      <li>Installation professionnelle par des monteurs certifiés</li>
      <li>Plus de 200 couleurs et finitions</li>
      <li>10 ans de garantie sur les matériaux</li>
      <li>Livraison rapide : 2–3 semaines après commande</li>
      <li>Devis gratuit et personnalisé</li>
    </ul>

    <p><strong>Demandez votre devis gratuit aujourd'hui !</strong><br>
    Répondez simplement à cet e-mail avec les dimensions de la/des pièce(s) que vous
    souhaitez équiper, et nous vous préparerons un devis détaillé.</p>

    <p>Par exemple :<br>
    <em>« Salon 6×4m, chambre 4×3m, cuisine 3×3m »</em></p>

    <div style="background: #f5f5f5; padding: 15px; border-radius: 5px; margin: 20px 0;">
      <p style="margin: 0;"><strong>Ou appelez-nous :</strong> +32 3 284 68 18<br>
      <strong>Ou visitez :</strong> <a href="https://www.stretchplafond.be">www.stretchplafond.be</a></p>
    </div>

    <p>Cordialement,<br>
    <strong>L'équipe STRETCH BV</strong></p>
  </div>

  <div style="background: #333; padding: 15px; text-align: center; font-size: 12px; color: #aaa;">
    STRETCH BV • Gentseweg 309 A3, 9120 Beveren-Waas, Belgique<br>
    TVA: BE0675875709 • Tél: +32 3 284 68 18
  </div>
</div>"""

    # ── English (default) ──────────────────────────────────────────────────
    else:
        subject = "Ready for a new stretch ceiling?"
        html = f"""
<div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
  <div style="background: #cc0000; padding: 20px; text-align: center;">
    <h1 style="color: white; margin: 0;">STRETCH BV</h1>
    <p style="color: #ffcccc; margin: 5px 0;">Professional Stretch Ceiling Solutions</p>
  </div>

  <div style="padding: 30px; background: #ffffff;">
    <p>Dear {first},</p>

    <p>We're reaching out to see if you're still interested in a stretch ceiling
    for your home or project.</p>

    <p>At STRETCH BV we offer:</p>
    <ul>
      <li>Professional installation by certified fitters</li>
      <li>Over 200 colours and finishes</li>
      <li>10-year material guarantee</li>
      <li>Fast delivery: 2–3 weeks after order</li>
      <li>Free personalised quote</li>
    </ul>

    <p><strong>Request your free quote today!</strong><br>
    Simply reply to this email with the dimensions of the room(s) you'd like a
    stretch ceiling for, and we'll prepare a detailed quote for you.</p>

    <p>For example:<br>
    <em>"Living room 6×4m, bedroom 4×3m, kitchen 3×3m"</em></p>

    <div style="background: #f5f5f5; padding: 15px; border-radius: 5px; margin: 20px 0;">
      <p style="margin: 0;"><strong>Or call us:</strong> +32 3 284 68 18<br>
      <strong>Or visit:</strong> <a href="https://www.stretchplafond.be">www.stretchplafond.be</a></p>
    </div>

    <p>Kind regards,<br>
    <strong>The STRETCH BV team</strong></p>
  </div>

  <div style="background: #333; padding: 15px; text-align: center; font-size: 12px; color: #aaa;">
    STRETCH BV • Gentseweg 309 A3, 9120 Beveren-Waas, Belgium<br>
    VAT: BE0675875709 • Tel: +32 3 284 68 18
  </div>
</div>"""

    return subject, html


# ─────────────────────────────────────────────────────────────────────────────
#  Graph API helpers
# ─────────────────────────────────────────────────────────────────────────────

def get_graph_token() -> str:
    url  = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
    data = {
        "grant_type":    "client_credentials",
        "client_id":     CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope":         "https://graph.microsoft.com/.default",
    }
    r = requests.post(url, data=data, timeout=15)
    r.raise_for_status()
    return r.json()["access_token"]


def send_email(token: str, to_email: str, subject: str, html_body: str) -> bool:
    url     = f"https://graph.microsoft.com/v1.0/users/{FROM_EMAIL}/sendMail"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {
        "message": {
            "subject": subject,
            "body":    {"contentType": "HTML", "content": html_body},
            "toRecipients": [{"emailAddress": {"address": to_email}}],
        },
        "saveToSentItems": True,
    }
    r = requests.post(url, headers=headers, json=payload, timeout=15)
    return r.status_code == 202


# ─────────────────────────────────────────────────────────────────────────────
#  Database helpers
# ─────────────────────────────────────────────────────────────────────────────

def get_db():
    return mysql.connector.connect(
        host=DB_HOST, port=DB_PORT, database=DB_NAME,
        user=DB_USER, password=DB_PASS,
        ssl_ca=str(Path(__file__).parent / "BaltimoreCyberTrustRoot_crt.pem"),
    )


def session_exists(db, email: str) -> bool:
    """Return True if an open/campaign session already exists for this email."""
    cursor = db.cursor(dictionary=True)
    cursor.execute(
        """
        SELECT id FROM email_quote_sessions
        WHERE sender_email=%s
          AND status IN ('campaign_sent', 'quote_sent', 'processing')
          AND received_at > NOW() - INTERVAL 60 DAY
        LIMIT 1
        """,
        (email,),
    )
    row = cursor.fetchone()
    cursor.close()
    return row is not None


def create_campaign_session(db, email: str, name: str, language: str) -> int:
    """Create a session row so the bot can match replies to this campaign."""
    conv_id    = f"campaign-{uuid.uuid4().hex}"
    message_id = f"campaign-{uuid.uuid4().hex}"
    cursor     = db.cursor()
    cursor.execute(
        """
        INSERT INTO email_quote_sessions (
            conversation_id, message_id,
            sender_email, sender_name,
            client_group, original_message,
            status, received_at,
            parsed_data, assumed_data, language
        ) VALUES (
            %s, %s, %s, %s,
            'price_b2c', 'Campaign outreach email',
            'campaign_sent', NOW(),
            '{}', '[]', %s
        )
        """,
        (conv_id, message_id, email, name, language),
    )
    session_id = cursor.lastrowid
    db.commit()
    cursor.close()
    return session_id


# ─────────────────────────────────────────────────────────────────────────────
#  Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="STRETCH re-engagement campaign")
    parser.add_argument("--csv",      required=True, help="Path to contacts CSV")
    parser.add_argument("--template", default="reengagement", help="Template name (unused, reserved)")
    parser.add_argument("--dry-run",  action="store_true", help="Parse CSV but don't send")
    parser.add_argument("--delay",    type=float, default=1.0, help="Seconds between emails (default 1)")
    parser.add_argument("--skip-existing", action="store_true",
                        help="Skip contacts that already have an open session")
    args = parser.parse_args()

    # Read CSV
    csv_path = Path(args.csv)
    if not csv_path.exists():
        print(f"❌ CSV file not found: {csv_path}")
        sys.exit(1)

    contacts = []
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        # Auto-detect delimiter: semicolon or comma
        sample = f.read(1024)
        f.seek(0)
        delimiter = ";" if sample.count(";") > sample.count(",") else ","
        reader = csv.DictReader(f, delimiter=delimiter)
        for row in reader:
            email = row.get("email", "").strip().lower()
            name  = row.get("name", "").strip()
            lang  = row.get("language", "nl").strip().lower()
            if email and "@" in email:
                contacts.append({"email": email, "name": name, "language": lang})

    print(f"📋 Loaded {len(contacts)} contact(s) from {csv_path.name}")

    if args.dry_run:
        print("🔍 DRY RUN — no emails will be sent\n")
        for c in contacts:
            subj, _ = build_email(c["name"], c["language"], args.template)
            print(f"  → {c['email']} ({c['language']}) | {subj}")
        return

    # Get Graph token
    print("🔑 Acquiring Graph API token...")
    try:
        token = get_graph_token()
        print("✅ Token acquired")
    except Exception as e:
        print(f"❌ Failed to get Graph token: {e}")
        sys.exit(1)

    # Connect to DB
    print("🗄️  Connecting to database...")
    try:
        db = get_db()
        print("✅ Database connected")
    except Exception as e:
        print(f"❌ DB connection failed: {e}")
        sys.exit(1)

    # Send
    sent = skipped = failed = 0
    print(f"\n📧 Sending campaign to {len(contacts)} contact(s)...\n")

    for i, contact in enumerate(contacts, 1):
        email = contact["email"]
        name  = contact["name"]
        lang  = contact["language"]

        # Skip if already has open session
        if args.skip_existing and session_exists(db, email):
            print(f"  [{i:3}] ⏭️  SKIP (existing session) — {email}")
            skipped += 1
            continue

        try:
            subject, html = build_email(name, lang, args.template)
            ok = send_email(token, email, subject, html)

            if ok:
                session_id = create_campaign_session(db, email, name, lang)
                print(f"  [{i:3}] ✅ SENT — {email} ({lang}) | session={session_id}")
                sent += 1
            else:
                print(f"  [{i:3}] ❌ FAILED — {email}")
                failed += 1

        except Exception as e:
            print(f"  [{i:3}] ❌ ERROR — {email}: {e}")
            failed += 1

        # Rate limiting
        if i < len(contacts):
            time.sleep(args.delay)

    db.close()

    print(f"""
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📊 Campaign complete
   ✅ Sent:    {sent}
   ⏭️  Skipped: {skipped}
   ❌ Failed:  {failed}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Replies will be automatically picked up by the bot and processed:
  • If they include dimensions → full quote generated and sent
  • If not → qualification email asking for dimensions
""")


if __name__ == "__main__":
    main()