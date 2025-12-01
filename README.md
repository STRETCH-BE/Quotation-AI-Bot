# 🏢 STRETCH Ceiling Bot

<p align="center">
  <img src="stretch_logo.png" alt="STRETCH Logo" width="200"/>
</p>

<p align="center">
  <strong>Professional Telegram Bot for Stretch Ceiling Quotations</strong><br>
  Complete solution for automated quote generation, customer management, and CRM integration
</p>

<p align="center">
  <img src="https://img.shields.io/badge/version-8.8-blue.svg" alt="Version"/>
  <img src="https://img.shields.io/badge/python-3.12+-green.svg" alt="Python"/>
  <img src="https://img.shields.io/badge/telegram--bot-20.7-blue.svg" alt="Telegram Bot"/>
  <img src="https://img.shields.io/badge/license-Proprietary-red.svg" alt="License"/>
</p>

---

## 📋 Table of Contents

- [Overview](#-overview)
- [Features](#-features)
- [Architecture](#-architecture)
- [Installation](#-installation)
- [Configuration](#-configuration)
- [Usage](#-usage)
- [Admin Commands](#-admin-commands)
- [API Reference](#-api-reference)
- [Database Schema](#-database-schema)
- [Integrations](#-integrations)
- [Deployment](#-deployment)
- [Troubleshooting](#-troubleshooting)
- [Contributing](#-contributing)

---

## 🎯 Overview

STRETCH Ceiling Bot is an enterprise-grade Telegram bot designed for **STRETCH BV** to automate the quotation process for stretch ceiling installations. The bot provides a complete solution for:

- **Instant Quote Generation** - Calculate prices for multi-room ceiling installations
- **Customer Relationship Management** - Full integration with Microsoft Dynamics 365
- **Professional PDF Quotes** - Branded, multilingual quote documents
- **AI-Powered Assistance** - Intelligent chat support using Azure OpenAI
- **Admin Dashboard** - Complete user and message management

### 🏢 Company Information

| | |
|---|---|
| **Company** | STRETCH BV |
| **Address** | Gentseweg 309 A3, 9120 Beveren-Waas, België |
| **Phone** | +32 3 284 68 18 |
| **Email** | info@stretchgroup.be |
| **Website** | [www.stretchplafond.be](https://www.stretchplafond.be) |
| **BTW** | BE0675875709 |

---

## ✨ Features

### 📊 Quote Generation
- **Multi-Ceiling Support** - Add unlimited rooms/ceilings per quote
- **Dynamic Pricing** - B2C, B2B Reseller, and B2B Hospitality price tiers
- **Cost Breakdown** - Detailed itemization (materials, profiles, corners, lights, etc.)
- **Manual Perimeter Override** - For complex room shapes
- **Acoustic & Backlit Options** - Special ceiling types supported

### 👤 User Management
- **User Onboarding** - Guided registration flow for new users
- **Profile Management** - Complete user profiles with company info
- **Client Groups** - Automatic pricing based on user type
- **Activity Tracking** - Full audit trail of user actions

### 🔗 CRM Integration (Dynamics 365)
- **Bidirectional Sync** - Real-time data synchronization
- **Customer Search** - Search existing CRM contacts/accounts
- **Quote Sync** - Automatic quote creation in CRM
- **Activity Logging** - All interactions logged to CRM

### 📧 Communication
- **Professional Emails** - HTML-formatted quote emails via Microsoft Graph
- **PDF Attachments** - Branded PDF quotes with full details
- **Admin Messaging** - Broadcast messages to user groups

### 🤖 AI Features
- **Intelligent Chat** - Azure OpenAI-powered conversations
- **Website Knowledge** - Scrapes company website for context
- **User Memory** - Remembers conversation history per user
- **Personalization** - Adapts responses based on user profile

### 🛡️ Security
- **Token Redaction** - Sensitive data filtered from logs
- **Admin Authorization** - Role-based access control
- **SSL/TLS** - Encrypted database connections
- **Secure Credentials** - Environment variable configuration

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        TELEGRAM USERS                           │
└─────────────────────────────────┬───────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────┐
│                      TELEGRAM BOT API                           │
└─────────────────────────────────┬───────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────┐
│                     STRETCH CEILING BOT                         │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌───────────┐ │
│  │   bot.py    │ │ quote_flow  │ │ user_onboard│ │  admin    │ │
│  │  (Main)     │ │   .py       │ │   ing.py    │ │ _mgmt.py  │ │
│  └─────────────┘ └─────────────┘ └─────────────┘ └───────────┘ │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌───────────┐ │
│  │  ai_chat    │ │ pdf_gen     │ │ email_send  │ │ dynamics  │ │
│  │   .py       │ │   .py       │ │   er.py     │ │ 365.py    │ │
│  └─────────────┘ └─────────────┘ └─────────────┘ └───────────┘ │
└─────────────────────────────────┬───────────────────────────────┘
                                  │
         ┌────────────────────────┼────────────────────────┐
         ▼                        ▼                        ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   MySQL/Azure   │    │   Azure OpenAI  │    │  Dynamics 365   │
│    Database     │    │      API        │    │      CRM        │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

### File Structure

```
STRETCH_NEW/
├── bot.py                      # Main bot application (1,200+ lines)
├── main.py                     # Entry point
├── config.py                   # Configuration management
│
├── handlers/
│   ├── quote_flow.py           # Multi-ceiling quote wizard (2,100+ lines)
│   ├── quote_editor.py         # Quote editing functionality
│   ├── user_onboarding.py      # User registration flow
│   ├── admin_user_management.py # Admin user management
│   ├── admin_messaging.py      # Admin broadcast system
│   ├── dynamics365_integration.py # CRM handlers
│   └── conversational.py       # Conversational handlers
│
├── services/
│   ├── ai_chat.py              # Azure OpenAI integration
│   ├── pdf_generator.py        # PDF quote generation
│   ├── email_sender.py         # Microsoft Graph email
│   ├── cost_calculator.py      # Pricing calculations
│   └── dynamics365_service.py  # Dynamics 365 API client
│
├── database/
│   ├── manager.py              # Database operations (2,000+ lines)
│   └── models.py               # Data models
│
├── docker/
│   ├── Dockerfile              # Container definition
│   ├── docker-compose.yml      # Full stack deployment
│   └── docker-entrypoint.sh    # Container startup
│
├── assets/
│   └── stretch_logo.png        # Company logo
│
├── requirements.txt            # Python dependencies
└── .env                        # Environment variables (not in repo)
```

---

## 🚀 Installation

### Prerequisites

- Python 3.12+
- MySQL 8.0+ (or Azure Database for MySQL)
- Telegram Bot Token
- (Optional) Azure OpenAI API access
- (Optional) Microsoft 365 tenant for email
- (Optional) Dynamics 365 CRM instance

### Quick Start

```bash
# Clone the repository
git clone https://github.com/yourusername/stretch-ceiling-bot.git
cd stretch-ceiling-bot

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Linux/Mac
# or
.\venv\Scripts\activate   # Windows

# Install dependencies
pip install -r requirements.txt

# Configure environment
cp .env.example .env
# Edit .env with your credentials

# Run the bot
python main.py
```

### Docker Deployment

```bash
# Build and start all services
docker-compose up -d

# View logs
docker-compose logs -f stretch_bot

# Stop services
docker-compose down
```

---

## ⚙️ Configuration

### Environment Variables

Create a `.env` file with the following variables:

```env
# =============================================================================
# TELEGRAM BOT
# =============================================================================
TELEGRAM_BOT_TOKEN=your_bot_token_here

# =============================================================================
# DATABASE (MySQL/Azure)
# =============================================================================
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_DB=stretch_ceiling_db
MYSQL_USER=stretch_bot
MYSQL_PASSWORD=your_password
MYSQL_SSL_CA=BaltimoreCyberTrustRoot_crt.pem  # For Azure

# =============================================================================
# COMPANY INFO
# =============================================================================
COMPANY_NAME=STRETCH BV
COMPANY_EMAIL=info@stretchgroup.be
COMPANY_PHONE=+32 3 284 68 18
COMPANY_WEBSITE=https://stretchplafond.be

# =============================================================================
# ADMIN USERS (Telegram User IDs)
# =============================================================================
ADMIN_USER_IDS=123456789,987654321

# =============================================================================
# AZURE OPENAI (Optional)
# =============================================================================
AZURE_OPENAI_API_KEY=your_api_key
AZURE_OPENAI_ENDPOINT=https://your-resource.openai.azure.com/
AZURE_OPENAI_API_VERSION=2024-02-15-preview
DEPLOYMENT_NAME=gpt-4.1-nano

# =============================================================================
# MICROSOFT ENTRA ID (Email)
# =============================================================================
ENTRA_CLIENT_ID=your_client_id
ENTRA_CLIENT_SECRET=your_client_secret
ENTRA_TENANT_ID=your_tenant_id
EMAIL_FROM=assistant_quotes@stretchgroup.be

# =============================================================================
# DYNAMICS 365 CRM
# =============================================================================
DYNAMICS_ENABLED=true
DYNAMICS_CLIENT_ID=your_client_id
DYNAMICS_CLIENT_SECRET=your_client_secret
DYNAMICS_TENANT_ID=your_tenant_id
DYNAMICS_RESOURCE_URL=https://yourorg.crm4.dynamics.com
DYNAMICS_SYNC_QUOTES=true
DYNAMICS_SYNC_USERS=true
DYNAMICS_CREATE_ACTIVITIES=true

# =============================================================================
# FEATURE FLAGS
# =============================================================================
ENABLE_PDF_GENERATION=true
ENABLE_EMAIL_SENDING=true
ENABLE_AI_CHAT=true

# =============================================================================
# LOGGING
# =============================================================================
LOG_LEVEL=INFO
LOG_TO_FILE=true
LOG_FILE=bot.log
```

---

## 📱 Usage

### Starting a Quote

1. **Start the bot**: Send `/start` to the bot
2. **New users**: Complete onboarding (name, email, phone, company details)
3. **Create quote**: Tap "📊 New Quote" button
4. **Select customer**: Choose new or existing customer
5. **Add ceilings**: Enter dimensions for each room
6. **Review & send**: Get PDF quote via email

### User Commands

| Command | Description |
|---------|-------------|
| `/start` | Start the bot / Main menu |
| `/help` | Show help information |
| `/profile` | View/edit your profile |
| `/quotes` | View your quote history |
| `/chat` | Start AI chat mode |

### Quote Flow

```
📊 New Quote
     │
     ▼
┌─────────────────┐
│ Select Customer │ ──► New or Existing (Dynamics 365)
└────────┬────────┘
         ▼
┌─────────────────┐
│  Add Ceiling    │ ──► Dimensions, type, options
└────────┬────────┘
         │
    ┌────┴────┐
    ▼         ▼
 Add More   Finish
    │         │
    └────┬────┘
         ▼
┌─────────────────┐
│  Review Quote   │ ──► Full breakdown, edit options
└────────┬────────┘
         ▼
┌─────────────────┐
│  Email Quote    │ ──► PDF + Email via Microsoft Graph
└─────────────────┘
```

---

## 👑 Admin Commands

### Accessing Admin Mode

Admins (configured via `ADMIN_USER_IDS`) can access additional features:

```
/admin - Open admin dashboard
```

### Admin Menu Options

| Option | Description |
|--------|-------------|
| 📨 Send Messages | Broadcast to users/groups |
| 📊 View Statistics | Bot usage analytics |
| 👥 User Management | Manage all users |
| 📊 Dynamics 365 Sync | Manual CRM sync |
| 🔧 System Diagnostics | Health checks |

### User Management Features

- **📋 List All Users** - Paginated user list with search
- **🔍 Search Users** - Find by name, email, company
- **📊 User Statistics** - Revenue, quotes, activity
- **👥 User Groups** - Organize users by tags
- **📤 Export User Data** - JSON export
- **🏷️ Manage Tags** - VIP, Reseller, etc.

---

## 🔌 API Reference

### REST API Endpoints

The bot includes an optional REST API for external integrations:

```
GET  /health              - Health check
GET  /api/users           - List users
GET  /api/users/:id       - Get user details
GET  /api/quotes          - List quotes
POST /api/quotes          - Create quote
GET  /api/statistics      - Bot statistics
```

### Webhook Support

```python
# Configure webhook mode
WEBHOOK_URL=https://yourdomain.com/webhook
WEBHOOK_PORT=8443
```

---

## 💾 Database Schema

### Core Tables

```sql
-- Users table
CREATE TABLE users (
    user_id BIGINT PRIMARY KEY,
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    telegram_username VARCHAR(255),
    email VARCHAR(255),
    phone VARCHAR(50),
    company_name VARCHAR(255),
    vat_number VARCHAR(50),
    address TEXT,
    client_group VARCHAR(50) DEFAULT 'price_b2c',
    is_company BOOLEAN DEFAULT FALSE,
    onboarding_completed BOOLEAN DEFAULT FALSE,
    tags JSON,
    notes TEXT,
    dynamics_contact_id VARCHAR(100),
    dynamics_account_id VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_activity TIMESTAMP
);

-- Quotations table
CREATE TABLE quotations (
    id INT AUTO_INCREMENT PRIMARY KEY,
    quotation_id VARCHAR(50) UNIQUE,
    user_id BIGINT,
    quote_data JSON,
    customer_data JSON,
    total_price DECIMAL(10,2),
    status VARCHAR(50) DEFAULT 'draft',
    dynamics_quote_id VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);

-- Conversation history
CREATE TABLE conversation_history (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id BIGINT,
    role VARCHAR(20),
    content TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- User activity log
CREATE TABLE user_activity_log (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id BIGINT,
    activity_type VARCHAR(100),
    activity_data JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

---

## 🔗 Integrations

### Microsoft Dynamics 365

The bot integrates with Dynamics 365 CRM for:

- **Contacts/Accounts**: Customer data synchronization
- **Quotes**: Automatic quote creation
- **Activities**: Interaction logging
- **Search**: Real-time customer lookup

### Microsoft Graph (Email)

Professional emails sent via Microsoft Graph API:
- HTML-formatted quote summaries
- PDF attachments
- Company branding

### Azure OpenAI

AI-powered features:
- Natural language chat
- Quote assistance
- Product information
- Multilingual support (NL, FR, EN)

---

## 🐳 Deployment

### Production Deployment (systemd)

```bash
# Create service file
sudo nano /etc/systemd/system/stretch-bot.service
```

```ini
[Unit]
Description=STRETCH Ceiling Bot
After=network.target mysql.service

[Service]
Type=simple
User=stretch
WorkingDirectory=/home/stretch/STRETCH_NEW
Environment=PATH=/home/stretch/STRETCH_NEW/venv/bin
ExecStart=/home/stretch/STRETCH_NEW/venv/bin/python main.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

```bash
# Enable and start
sudo systemctl enable stretch-bot
sudo systemctl start stretch-bot

# View logs
sudo journalctl -u stretch-bot -f
```

### Docker Production

```bash
# Production deployment
docker-compose -f docker-compose.yml -f docker-compose.prod.yml up -d

# With monitoring stack
docker-compose --profile monitoring up -d
```

---

## 🔧 Troubleshooting

### Common Issues

#### Bot not responding
```bash
# Check bot status
sudo systemctl status stretch-bot

# View recent logs
sudo journalctl -u stretch-bot -n 100

# Restart bot
sudo systemctl restart stretch-bot
```

#### Database connection errors
```bash
# Test MySQL connection
mysql -h $MYSQL_HOST -u $MYSQL_USER -p$MYSQL_PASSWORD $MYSQL_DB

# Check SSL certificate
openssl s_client -connect $MYSQL_HOST:3306 -starttls mysql
```

#### Telegram API errors
```bash
# Verify bot token
curl "https://api.telegram.org/bot$TELEGRAM_BOT_TOKEN/getMe"
```

#### Email sending failures
```bash
# Test Microsoft Graph authentication
# Check Entra ID app permissions: Mail.Send
```

### Logs

| Log File | Contents |
|----------|----------|
| `bot.log` | General bot activity |
| `bot_debug.log` | Detailed debug information |
| `enhanced_multi_ceiling_bot.log` | Legacy log |

---

## 📊 Monitoring

### Health Check Endpoint

```bash
curl http://localhost:8080/health
```

Response:
```json
{
  "status": "healthy",
  "version": "8.8",
  "uptime": "2d 5h 30m",
  "database": "connected",
  "dynamics365": "connected"
}
```

### Prometheus Metrics

```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'stretch-bot'
    static_configs:
      - targets: ['stretch_bot:8080']
```

---

## 🛠️ Development

### Running Tests

```bash
# Run all tests
pytest

# With coverage
pytest --cov=. --cov-report=html
```

### Code Style

```bash
# Format code
black .

# Lint
flake8 .

# Type checking
mypy .
```

### Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

---

## 📝 Changelog

### Version 8.8 (2024-11-30)
- ✅ Fixed admin user management Markdown parsing errors
- ✅ Fixed emoji encoding issues (mojibake)
- ✅ Added escape_markdown for user data
- ✅ Fixed JSON handling for activity/quote data
- ✅ Added try/except fallback to plain text

### Version 8.7
- ✅ Dynamics 365 bidirectional sync
- ✅ Customer selection before quotes
- ✅ PDF pricing display fixes
- ✅ Email VAT calculation fixes

### Version 8.6
- ✅ Complete user management system
- ✅ Admin messaging functionality
- ✅ User onboarding flow

### Version 8.5
- ✅ Azure OpenAI integration
- ✅ Conversation memory
- ✅ Website knowledge scraping

---

## 📄 License

This software is proprietary and confidential. Unauthorized copying, distribution, or use is strictly prohibited.

**© 2024 STRETCH BV. All rights reserved.**

---

## 📞 Support

For technical support or questions:

| | |
|---|---|
| **Email** | support@stretchgroup.be |
| **Phone** | +32 3 284 68 18 |
| **Hours** | Mon-Fri 9:00-17:00 CET |

---

<p align="center">
  Made with ❤️ for STRETCH BV
</p>
