#!/bin/bash
# ============================================================================
# STRETCH Bot - GitHub Push Script (SSH)
# Safely pushes code to GitHub, excluding sensitive files
# ============================================================================

REPO_URL="git@github.com:STRETCH-BE/Quotation-AI-Bot.git"
CODE_DIR="/home/STRETCH/STRETCH_NEW"

echo "=============================================="
echo "🚀 STRETCH Bot - GitHub Push Script"
echo "=============================================="
echo ""
echo "📁 Code directory: $CODE_DIR"
echo "🔗 Repository: $REPO_URL"
echo ""

cd "$CODE_DIR" || exit 1

# ============================================================================
# STEP 1: Create/Update .gitignore
# ============================================================================
echo "📝 Creating .gitignore file..."

cat > .gitignore << 'EOF'
# ============================================================================
# Environment & Secrets (NEVER COMMIT THESE!)
# ============================================================================
.env
.env.*
*.env
!.env.example

# ============================================================================
# Python
# ============================================================================
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
build/
develop-eggs/
dist/
downloads/
eggs/
.eggs/
lib/
lib64/
parts/
sdist/
var/
wheels/
*.egg-info/
.installed.cfg
*.egg

# ============================================================================
# Virtual Environment
# ============================================================================
bin/
include/
lib64
pyvenv.cfg
venv/
ENV/
env/
.venv/

# ============================================================================
# IDE & Editors
# ============================================================================
.vscode/
.idea/
*.swp
*.swo
*~
.project
.pydevproject
.settings/

# ============================================================================
# Logs
# ============================================================================
*.log
logs/
bot.log
bot_debug.log
enhanced_multi_ceiling_bot.log

# ============================================================================
# Generated Files
# ============================================================================
quotes/
*.pdf
/tmp/

# ============================================================================
# SSL Certificates (keep templates, not actual certs)
# ============================================================================
*.pem
*.crt
*.key

# ============================================================================
# OS Files
# ============================================================================
.DS_Store
Thumbs.db
*.bak
*.backup

# ============================================================================
# NLTK Data (large, downloadable)
# ============================================================================
nltk_data/

# ============================================================================
# Backup files
# ============================================================================
*.bak
*_backup*
cleanup_backup*/
EOF

echo "   ✅ .gitignore created"
echo ""

# ============================================================================
# STEP 2: Create .env.example template
# ============================================================================
echo "📝 Creating .env.example template..."

cat > .env.example << 'EOF'
# ============================================================================
# STRETCH Ceiling Bot - Environment Configuration Template
# Copy this file to .env and fill in your values
# ============================================================================

# Telegram Bot Configuration
TELEGRAM_BOT_TOKEN=your_telegram_bot_token_here

# Database Configuration
MYSQL_HOST=your_mysql_host
MYSQL_USER=your_mysql_user
MYSQL_PASSWORD=your_mysql_password
MYSQL_DB=your_database_name
MYSQL_PORT=3306

# Azure OpenAI Configuration
AZURE_OPENAI_API_KEY=your_azure_openai_key
AZURE_OPENAI_ENDPOINT=https://your-resource.openai.azure.com/
AZURE_OPENAI_API_VERSION=2024-06-01
DEPLOYMENT_NAME=gpt-4.1-nano

# Company Information
COMPANY_NAME="Your Company Name"
COMPANY_EMAIL=info@yourcompany.com
COMPANY_PHONE="+32 xxx xx xx xx"
COMPANY_ADDRESS="Your Address"
COMPANY_WEBSITE=https://yourwebsite.com

# Entra ID Configuration (for Email)
AZURE_TENANT_ID=your_tenant_id
AZURE_CLIENT_ID=your_client_id
AZURE_CLIENT_SECRET=your_client_secret
EMAIL_FROM=quotes@yourcompany.com

# Bot Settings
ADMIN_USER_IDS=your_telegram_user_id
QUOTE_VALIDITY_DAYS=30
MAX_CEILINGS_PER_QUOTE=10
PDF_OUTPUT_DIR=/tmp/quotes

# Feature Flags
ENABLE_PDF_GENERATION=true
ENABLE_EMAIL_SENDING=true
ENABLE_AI_CHAT=true

# Logging
LOG_LEVEL=INFO
DEBUG_MODE=false

# Dynamics 365 Configuration (Optional)
DYNAMICS_URL=https://yourorg.crm4.dynamics.com
DYNAMICS_CLIENT_ID=your_dynamics_client_id
DYNAMICS_CLIENT_SECRET=your_dynamics_client_secret
DYNAMICS_TENANT_ID=your_dynamics_tenant_id
ENABLE_DYNAMICS_SYNC=false
DYNAMICS_SYNC_QUOTES=false
DYNAMICS_SYNC_USERS=false
DYNAMICS_CREATE_ACTIVITIES=false
EOF

echo "   ✅ .env.example created"
echo ""

# ============================================================================
# STEP 3: Initialize Git (if not already)
# ============================================================================
echo "🔧 Initializing Git repository..."

if [ -d ".git" ]; then
    echo "   ℹ️  Git already initialized"
else
    git init
    echo "   ✅ Git initialized"
fi
echo ""

# ============================================================================
# STEP 4: Configure Git (if needed)
# ============================================================================
echo "👤 Checking Git configuration..."

if [ -z "$(git config user.name)" ]; then
    git config user.name "STRETCH-BE"
    echo "   ✅ Set user.name to STRETCH-BE"
fi

if [ -z "$(git config user.email)" ]; then
    git config user.email "michael@stretchgroup.be"
    echo "   ✅ Set user.email to michael@stretchgroup.be"
fi
echo ""

# ============================================================================
# STEP 5: Add remote repository
# ============================================================================
echo "🔗 Setting up remote repository..."

# Remove existing origin if exists
git remote remove origin 2>/dev/null

# Add new origin (SSH)
git remote add origin "$REPO_URL"
echo "   ✅ Remote 'origin' set to: $REPO_URL"
echo ""

# ============================================================================
# STEP 6: Stage all files
# ============================================================================
echo "📋 Staging files..."
git add -A
echo ""
echo "Files to be committed:"
git status --short
echo ""

# ============================================================================
# STEP 7: Commit
# ============================================================================
echo "📦 Creating commit..."
git commit -m "Initial commit: STRETCH Ceiling Bot v8.8

Features:
- Telegram bot for stretch ceiling quotations
- Multi-ceiling quote creation with customer selection
- PDF generation with STRETCH branding
- Email sending via Microsoft Graph API (Entra ID)
- Azure OpenAI integration for AI chat
- Dynamics 365 CRM bidirectional sync
- Admin user management system
- User onboarding flow
- Quote editing and recalculation

Tech stack:
- Python 3.12+
- python-telegram-bot 20.7
- Azure OpenAI
- MySQL / Azure Database for MySQL
- Microsoft Graph API
- Dynamics 365 Web API
- ReportLab for PDF generation"

echo ""
echo "   ✅ Commit created"
echo ""

# ============================================================================
# STEP 8: Push to GitHub
# ============================================================================
echo "🚀 Pushing to GitHub via SSH..."
echo ""

git branch -M main
git push -u origin main

if [ $? -eq 0 ]; then
    echo ""
    echo "=============================================="
    echo "✅ PUSH SUCCESSFUL!"
    echo "=============================================="
    echo ""
    echo "🔗 View your repository at:"
    echo "   https://github.com/STRETCH-BE/Quotation-AI-Bot"
    echo ""
else
    echo ""
    echo "=============================================="
    echo "❌ PUSH FAILED"
    echo "=============================================="
    echo ""
    echo "Common issues:"
    echo ""
    echo "1. SSH key not set up:"
    echo "   ssh-keygen -t ed25519 -C \"michael@stretchgroup.be\""
    echo "   cat ~/.ssh/id_ed25519.pub"
    echo "   Then add to: https://github.com/settings/keys"
    echo ""
    echo "2. Test SSH connection:"
    echo "   ssh -T git@github.com"
    echo ""
    echo "3. If repo already has commits, try:"
    echo "   git pull origin main --allow-unrelated-histories"
    echo "   git push -u origin main"
    echo ""
fi