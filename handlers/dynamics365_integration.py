"""
Dynamics 365 Integration Handler for Stretch Ceiling Bot
Manages bidirectional synchronization of users and quotes
ENHANCED: Added idempotency, sync queue, duplicate prevention, and customer selection support
VERSION: 2.1 - Fixed customer linking for quote sync
"""
import logging
import asyncio
from typing import Tuple, Dict, Optional, List, Set
from datetime import datetime, timedelta
import json
import os
from collections import defaultdict

from dynamics365_service import Dynamics365Service
from config import Config

logger = logging.getLogger(__name__)

class SyncQueue:
    """Manages sync queue to prevent duplicate operations"""
    def __init__(self):
        self.processing = defaultdict(set)  # entity_type -> set of IDs being processed
        self.completed = defaultdict(dict)  # entity_type -> {id: timestamp}
        self.lock = asyncio.Lock()
    
    async def can_process(self, entity_type: str, entity_id: str) -> bool:
        """Check if entity can be processed"""
        async with self.lock:
            # Check if already processing
            if entity_id in self.processing[entity_type]:
                logger.info(f"{entity_type} {entity_id} is already being processed")
                return False
            
            # Check if recently completed (within 5 minutes)
            if entity_id in self.completed[entity_type]:
                last_sync = self.completed[entity_type][entity_id]
                if datetime.now() - last_sync < timedelta(minutes=5):
                    logger.info(f"{entity_type} {entity_id} was recently synced")
                    return False
            
            # Mark as processing
            self.processing[entity_type].add(entity_id)
            return True
    
    async def mark_completed(self, entity_type: str, entity_id: str):
        """Mark entity as completed"""
        async with self.lock:
            self.processing[entity_type].discard(entity_id)
            self.completed[entity_type][entity_id] = datetime.now()
            
            # Clean up old completed entries (older than 1 hour)
            cutoff_time = datetime.now() - timedelta(hours=1)
            self.completed[entity_type] = {
                k: v for k, v in self.completed[entity_type].items()
                if v > cutoff_time
            }
    
    async def mark_failed(self, entity_type: str, entity_id: str):
        """Mark entity as failed (remove from processing)"""
        async with self.lock:
            self.processing[entity_type].discard(entity_id)

class Dynamics365IntegrationHandler:
    """Handles bidirectional integration between bot and Dynamics 365 with enhanced reliability"""
    
    def __init__(self, db_manager):
        self.db = db_manager
        self.sync_queue = SyncQueue()
        
        # Initialize Dynamics 365 service if enabled
        self.dynamics_service = None
        if Config.ENABLE_DYNAMICS_SYNC:
            self.dynamics_service = Dynamics365Service(
                tenant_id=Config.DYNAMICS_TENANT_ID,
                client_id=Config.DYNAMICS_CLIENT_ID,
                client_secret=Config.DYNAMICS_CLIENT_SECRET,
                dynamics_url=Config.DYNAMICS_URL
            )
            logger.info("✅ Dynamics 365 integration initialized with enhanced reliability")
            
            # Track last sync time for bidirectional sync
            self.last_sync_time = datetime.now() - timedelta(hours=1)
        else:
            logger.info("⚠️ Dynamics 365 integration disabled")
    
    async def sync_user_to_dynamics(self, user_id: int) -> bool:
        """
        Sync a user to Dynamics 365 as contact/account with idempotency
        Called after user completes onboarding or updates profile
        """
        if not self.dynamics_service or not Config.DYNAMICS_SYNC_USERS:
            return True  # Return True to not block the flow
        
        # Check if we can process this user
        if not await self.sync_queue.can_process('user', str(user_id)):
            logger.info(f"Skipping user {user_id} - already in sync queue")
            return True
        
        try:
            # Get user data
            user_data = self.db.get_user_profile(user_id)
            if not user_data:
                logger.error(f"User {user_id} not found")
                await self.sync_queue.mark_failed('user', str(user_id))
                return False
            
            # Check if already synced successfully
            dynamics_ids = self.db.get_user_dynamics_ids(user_id)
            
            # Only sync if not already synced or if there was an error
            if dynamics_ids.get('sync_status') == 'synced' and dynamics_ids.get('contact_id'):
                logger.info(f"User {user_id} already synced to Dynamics")
                await self.sync_queue.mark_completed('user', str(user_id))
                return True
            
            # Create or update contact
            contact_id = await self.dynamics_service.create_or_update_contact(user_data)
            
            if contact_id:
                # Update database with contact ID
                self.db.update_user_dynamics_id(
                    user_id=user_id,
                    contact_id=contact_id,
                    status='synced'
                )
                
                logger.info(f"✅ User {user_id} synced to Dynamics 365 as contact {contact_id}")
                
                # Log activity
                self.db.log_user_activity(user_id, 'dynamics_sync', {
                    'type': 'contact',
                    'dynamics_id': contact_id,
                    'action': 'created' if not dynamics_ids['contact_id'] else 'updated'
                })
                
                await self.sync_queue.mark_completed('user', str(user_id))
                return True
            else:
                # Log error
                self.db.update_user_dynamics_id(
                    user_id=user_id,
                    status='error',
                    error='Failed to create/update contact in Dynamics 365'
                )
                await self.sync_queue.mark_failed('user', str(user_id))
                return False
                
        except Exception as e:
            logger.error(f"❌ Error syncing user {user_id} to Dynamics: {e}")
            self.db.update_user_dynamics_id(
                user_id=user_id,
                status='error',
                error=str(e)
            )
            await self.sync_queue.mark_failed('user', str(user_id))
            return False
    
    async def sync_quote_to_dynamics(self, quote_id: int) -> bool:
        """
        Sync a quote to Dynamics 365 with enhanced fields and PDF attachment.
        
        FIXED: Now uses customer IDs from customer selection flow when available,
        properly linking quotes to the selected customer (account or contact).
        """
        if not self.dynamics_service or not Config.DYNAMICS_SYNC_QUOTES:
            return True
        
        # Check if we can process this quote
        if not await self.sync_queue.can_process('quote', str(quote_id)):
            logger.info(f"Skipping quote {quote_id} - already in sync queue")
            return True
        
        try:
            # Get quote data
            quote = self.db.get_quote_by_id(quote_id)
            if not quote:
                logger.error(f"Quote {quote_id} not found")
                await self.sync_queue.mark_failed('quote', str(quote_id))
                return False
            
            # Check if already synced
            if quote.get('dynamics_quote_id') and quote.get('dynamics_sync_status') == 'synced':
                logger.info(f"Quote {quote_id} already synced to Dynamics")
                await self.sync_queue.mark_completed('quote', str(quote_id))
                return True
            
            # Get user's Dynamics IDs and profile as fallback
            user_profile = self.db.get_user_profile(quote['user_id'])
            dynamics_ids = self.db.get_user_dynamics_ids(quote['user_id'])
            
            # Ensure user is synced first (as fallback)
            if not dynamics_ids['contact_id']:
                logger.info(f"Syncing user {quote['user_id']} before quote sync")
                await self.sync_user_to_dynamics(quote['user_id'])
                dynamics_ids = self.db.get_user_dynamics_ids(quote['user_id'])
            
            # Parse quote data
            quote_data_parsed = quote['quote_data']
            if isinstance(quote_data_parsed, str):
                quote_data_parsed = json.loads(quote_data_parsed)
            
            # ============================================================
            # FIXED: Check if customer was selected during quote flow
            # Use customer's Dynamics IDs if available, otherwise fall back to user's IDs
            # ============================================================
            customer_data = quote_data_parsed.get('customer', {})
            if customer_data:
                # Use selected customer's Dynamics IDs
                customer_contact_id = customer_data.get('dynamics_contact_id')
                customer_account_id = customer_data.get('dynamics_account_id')
                
                # If customer has IDs, use them instead of user's IDs
                if customer_contact_id or customer_account_id:
                    dynamics_ids = {
                        'contact_id': customer_contact_id,
                        'account_id': customer_account_id
                    }
                    logger.info(f"Using selected customer's Dynamics IDs: contact={customer_contact_id}, account={customer_account_id}")
                else:
                    # Customer was created but not yet synced to Dynamics
                    logger.info(f"Selected customer has no Dynamics IDs yet, using user's IDs as fallback")
            
            # Validate we have at least one ID to link to
            if not dynamics_ids.get('contact_id') and not dynamics_ids.get('account_id'):
                logger.error("Cannot sync quote without contact or account ID")
                await self.sync_queue.mark_failed('quote', str(quote_id))
                return False
            
            # Extract postal and city from address if not in quote data
            postal_code = quote_data_parsed.get('postal_code', '')
            city = quote_data_parsed.get('city', '')
            
            if not postal_code or not city:
                # Try to extract from address
                import re
                address = user_profile.get('address', '') if user_profile else ''
                lines = address.strip().split('\n') if address else []
                for line in lines:
                    match = re.match(r'(\d{4})\s+(.+?)(?:\s+(?:East|West|Flemish|Walloon).*)?$', line.strip())
                    if match:
                        postal_code = postal_code or match.group(1)
                        city = city or match.group(2).strip()
                        break
            
            # Get existing Dynamics quote ID if any (for updates)
            existing_dynamics_id = quote.get('dynamics_quote_id')
            
            # Prepare quote data for Dynamics - ALWAYS include fields with defaults
            dynamics_quote_data = {
                'quote_id': quote_id,
                'quote_number': quote['quote_number'],
                'total_price': float(quote['total_price']),
                'ceilings': quote_data_parsed.get('ceilings', []),
                'ceiling_costs': quote_data_parsed.get('ceiling_costs', []),
                'quote_reference': quote_data_parsed.get('quote_reference', f"REF-{quote['quote_number']}"),
                'created_at': quote.get('created_at'),
                'installation_needed': quote_data_parsed.get('installation_needed', True),
                'postal_code': postal_code or '1000',
                'city': city or 'Brussels',
                'company': user_profile.get('company_name') or user_profile.get('company') or 'Particulier' if user_profile else 'Particulier',
                'vat_number': user_profile.get('vat_number') or 'N/A' if user_profile else 'N/A'
            }
            
            # Generate PDF if enabled
            pdf_path = None
            if Config.ENABLE_PDF_GENERATION:
                try:
                    from services import ImprovedStretchQuotePDFGenerator
                    output_dir = "/tmp/quotes"
                    os.makedirs(output_dir, exist_ok=True)
                    
                    pdf_generator = ImprovedStretchQuotePDFGenerator(output_dir)
                    pdf_path = pdf_generator.build_pdf(
                        quote['quote_number'],
                        quote_data_parsed,
                        user_profile
                    )
                except Exception as e:
                    logger.error(f"Error generating PDF for Dynamics sync: {e}")
            
            # Create or update quote in Dynamics with customer linking
            dynamics_quote_id = await self.dynamics_service.create_or_update_quote(
                quote_data=dynamics_quote_data,
                contact_id=dynamics_ids.get('contact_id'),
                dynamics_quote_id=existing_dynamics_id,
                account_id=dynamics_ids.get('account_id'),
                user_profile=user_profile,
                pdf_path=pdf_path
            )
            
            if dynamics_quote_id:
                # Update database
                self.db.update_quote_dynamics_id(
                    quote_id=quote_id,
                    dynamics_quote_id=dynamics_quote_id,
                    status='synced'
                )
                
                action = 'updated' if existing_dynamics_id else 'created'
                logger.info(f"✅ Quote {quote_id} {action} in Dynamics 365 as {dynamics_quote_id}")
                logger.info(f"   Total amount: €{quote['total_price']:,.2f}")
                
                # Log activity
                self.db.log_user_activity(quote['user_id'], 'dynamics_quote_sync', {
                    'quote_id': quote_id,
                    'dynamics_quote_id': dynamics_quote_id,
                    'total_price': quote['total_price'],
                    'action': action,
                    'customer_contact_id': dynamics_ids.get('contact_id'),
                    'customer_account_id': dynamics_ids.get('account_id')
                })
                
                # Sync conversations to contact if we have a contact ID
                if dynamics_ids.get('contact_id'):
                    await self.dynamics_service.sync_user_conversations(
                        quote['user_id'],
                        dynamics_ids['contact_id']
                    )
                
                await self.sync_queue.mark_completed('quote', str(quote_id))
                return True
            else:
                error_message = f'Failed to {"update" if existing_dynamics_id else "create"} quote in Dynamics 365'
                self.db.update_quote_dynamics_id(
                    quote_id=quote_id,
                    dynamics_quote_id=existing_dynamics_id,
                    status='error',
                    error=error_message
                )
                await self.sync_queue.mark_failed('quote', str(quote_id))
                return False
                
        except Exception as e:
            logger.error(f"❌ Error syncing quote {quote_id} to Dynamics: {e}")
            import traceback
            logger.error(traceback.format_exc())
            self.db.update_quote_dynamics_id(
                quote_id=quote_id,
                dynamics_quote_id=None,
                status='error',
                error=str(e)
            )
            await self.sync_queue.mark_failed('quote', str(quote_id))
            return False
    
    async def sync_from_dynamics(self):
        """
        Sync changes from Dynamics 365 back to the bot
        This should be called periodically to pull updates
        """
        if not self.dynamics_service:
            return
        
        try:
            logger.info("🔄 Starting sync from Dynamics 365...")
            
            # Get recent contact changes
            if Config.DYNAMICS_SYNC_USERS:
                contacts = await self.dynamics_service.get_recent_changes(
                    'contact', 
                    self.last_sync_time,
                    limit=100
                )
                
                for contact in contacts:
                    await self._sync_contact_from_dynamics(contact)
            
            # Get recent account changes
            if Config.DYNAMICS_SYNC_USERS:
                accounts = await self.dynamics_service.get_recent_changes(
                    'account',
                    self.last_sync_time,
                    limit=100
                )
                
                for account in accounts:
                    await self._sync_account_from_dynamics(account)
            
            # Get recent quote changes
            if Config.DYNAMICS_SYNC_QUOTES:
                quotes = await self.dynamics_service.get_recent_changes(
                    'quote',
                    self.last_sync_time,
                    limit=100
                )
                
                for quote in quotes:
                    await self._sync_quote_from_dynamics(quote)
            
            # Update last sync time
            self.last_sync_time = datetime.now()
            logger.info("✅ Sync from Dynamics 365 completed")
            
        except Exception as e:
            logger.error(f"❌ Error syncing from Dynamics: {e}")
    
    async def _sync_contact_from_dynamics(self, contact: Dict):
        """Sync a contact from Dynamics to the bot database"""
        try:
            telegram_id = contact.get('cr229_new_telegramuserid')
            if not telegram_id:
                return  # Skip contacts without Telegram ID
            
            # Check if user exists in bot database
            user_data = self.db.get_user_profile(int(telegram_id))
            
            if user_data:
                # Update user data with Dynamics changes
                updates = {}
                
                if contact.get('firstname') and contact['firstname'] != user_data.get('first_name'):
                    updates['first_name'] = contact['firstname']
                
                if contact.get('lastname') and contact['lastname'] != user_data.get('last_name'):
                    updates['last_name'] = contact['lastname']
                
                if contact.get('emailaddress1') and contact['emailaddress1'] != user_data.get('email'):
                    updates['email'] = contact['emailaddress1']
                
                if contact.get('telephone1') and contact['telephone1'] != user_data.get('phone'):
                    updates['phone'] = contact['telephone1']
                
                if contact.get('address1_composite') and contact['address1_composite'] != user_data.get('address'):
                    updates['address'] = contact['address1_composite']
                
                if updates:
                    # Update user profile
                    updates['user_id'] = int(telegram_id)
                    self.db.save_user_profile({**user_data, **updates})
                    
                    logger.info(f"✅ Updated user {telegram_id} from Dynamics contact {contact['contactid']}")
                    
                    # Log sync
                    self.db.log_dynamics_sync(
                        'contact',
                        telegram_id,
                        contact['contactid'],
                        'sync_from_dynamics',
                        'success',
                        sync_data=updates
                    )
            
        except Exception as e:
            logger.error(f"Error syncing contact from Dynamics: {e}")

    async def _sync_account_from_dynamics(self, account: Dict):
        """Sync an account from Dynamics to the bot database"""
        try:
            # Look for linked contacts with this account
            contacts_response = await self.dynamics_service.make_request(
                method="GET",
                endpoint=f"contacts?$filter=_parentcustomerid_value eq {account['accountid']}&$select=cr229_new_telegramuserid"
            )
            
            if not contacts_response or not contacts_response.get('value'):
                return  # No linked contacts
            
            # Process each linked contact
            for contact in contacts_response['value']:
                telegram_id = contact.get('cr229_new_telegramuserid')
                if not telegram_id:
                    continue
                
                user_data = self.db.get_user_profile(int(telegram_id))
                if not user_data:
                    continue
                
                # Update user with account data
                updates = {}
                
                # Update company information
                if account.get('name') and account['name'] != user_data.get('company_name'):
                    updates['company_name'] = account['name']
                
                # Update VAT number from custom field
                vat_number = account.get('cr229_VATNumber', '')
                if vat_number and vat_number != user_data.get('vat_number'):
                    updates['vat_number'] = vat_number
                
                # Update company address if different
                if account.get('address1_composite') and account['address1_composite'] != user_data.get('company_address'):
                    updates['company_address'] = account['address1_composite']
                
                # Update company phone
                if account.get('telephone1') and account['telephone1'] != user_data.get('company_phone'):
                    updates['company_phone'] = account['telephone1']
                
                # Update company email
                if account.get('emailaddress1') and account['emailaddress1'] != user_data.get('company_email'):
                    updates['company_email'] = account['emailaddress1']
                
                if updates:
                    # Update user profile
                    updates['user_id'] = int(telegram_id)
                    updates['is_company'] = True  # Ensure company flag is set
                    self.db.save_user_profile({**user_data, **updates})
                    
                    logger.info(f"✅ Updated user {telegram_id} from Dynamics account {account['accountid']}")
                    
                    # Log sync
                    self.db.log_dynamics_sync(
                        'account',
                        telegram_id,
                        account['accountid'],
                        'sync_from_dynamics',
                        'success',
                        sync_data=updates
                    )
                    
        except Exception as e:
            logger.error(f"Error syncing account from Dynamics: {e}")

    async def _sync_quote_from_dynamics(self, quote: Dict):
        """Sync a quote from Dynamics to the bot database"""
        try:
            telegram_quote_id = quote.get('cr229_new_telegramquoteid')
            if not telegram_quote_id:
                return  # Skip quotes without Telegram ID
            
            # Check if quote exists in bot database
            bot_quote = self.db.get_quote_by_id(int(telegram_quote_id))
            
            if bot_quote:
                # Map Dynamics status to bot status
                status_mapping = {
                    (0, 1): 'draft',      # Active - In Progress
                    (0, 2): 'sent',       # Active - In Progress
                    (1, 3): 'accepted',   # Won
                    (2, 4): 'rejected',   # Closed - Canceled
                    (2, 5): 'expired'     # Closed - Revised
                }
                
                dynamics_status = (quote.get('statecode', 0), quote.get('statuscode', 1))
                new_status = status_mapping.get(dynamics_status, 'draft')
                
                updates_needed = False
                
                # Check status change
                if new_status != bot_quote.get('status'):
                    # Update quote status
                    self.db.update_quote_status(
                        int(telegram_quote_id),
                        new_status,
                        0,  # System user ID
                        f"Updated from Dynamics 365"
                    )
                    updates_needed = True
                    logger.info(f"✅ Updated quote {telegram_quote_id} status to {new_status}")
                
                # Check total amount change
                dynamics_total = float(quote.get('totalamount', 0))
                bot_total = float(bot_quote.get('total_price', 0))
                
                if abs(dynamics_total - bot_total) > 0.01:
                    # Update total price
                    self.db.execute_query(
                        """
                        UPDATE quotations 
                        SET total_price = %s, 
                            updated_at = NOW(),
                            dynamics_last_modified = %s
                        WHERE id = %s
                        """,
                        (dynamics_total, quote.get('modifiedon'), int(telegram_quote_id))
                    )
                    updates_needed = True
                    logger.info(f"✅ Updated quote {telegram_quote_id} total from €{bot_total:.2f} to €{dynamics_total:.2f}")
                
                # Check effective dates
                if quote.get('effectiveto'):
                    try:
                        new_expiry = datetime.fromisoformat(quote['effectiveto'].replace('Z', '+00:00'))
                        existing_expiry = bot_quote.get('expires_at')
                        if isinstance(existing_expiry, str):
                            existing_expiry = datetime.fromisoformat(existing_expiry)
                        
                        if new_expiry != existing_expiry:
                            self.db.execute_query(
                                "UPDATE quotations SET expires_at = %s WHERE id = %s",
                                (new_expiry, int(telegram_quote_id))
                            )
                            updates_needed = True
                            logger.info(f"✅ Updated quote {telegram_quote_id} expiry date")
                    except Exception as e:
                        logger.warning(f"Could not parse expiry date: {e}")
                
                if updates_needed:
                    # Log sync
                    self.db.log_dynamics_sync(
                        'quote',
                        telegram_quote_id,
                        quote['quoteid'],
                        'sync_from_dynamics',
                        'success',
                        sync_data={
                            'status': new_status,
                            'total': dynamics_total
                        }
                    )
            
        except Exception as e:
            logger.error(f"Error syncing quote from Dynamics: {e}")
            import traceback
            logger.error(traceback.format_exc())
    
    async def _create_quote_activity(self, quote_id: str, dynamics_ids: Dict, quote_data: Dict):
        """Create an activity record for the quote in Dynamics"""
        try:
            activity_data = {
                "subject": f"Quote {quote_data['quote_number']} created via Telegram Bot",
                "description": f"Quote created with total value: €{quote_data['total_price']:.2f}",
                "scheduledend": datetime.now().isoformat(),
                "actualend": datetime.now().isoformat(),
                "statecode": 1,  # Completed
                "regardingobjectid_quote@odata.bind": f"/quotes({quote_id})"
            }
            
            await self.dynamics_service.make_request(
                method="POST",
                endpoint="tasks",
                data=activity_data
            )
            
            logger.info(f"✅ Created activity for quote {quote_id}")
            
        except Exception as e:
            logger.error(f"Error creating quote activity: {e}")
    
    async def sync_pending_entities(self):
        """
        Sync all pending entities to Dynamics 365
        This can be called periodically by a background task
        """
        if not self.dynamics_service:
            return
        
        try:
            # Sync pending users
            if Config.DYNAMICS_SYNC_USERS:
                pending_users = self.db.get_pending_dynamics_syncs('user', limit=50)
                logger.info(f"Found {len(pending_users)} pending users to sync")
                
                for user in pending_users:
                    await self.sync_user_to_dynamics(user['user_id'])
                    await asyncio.sleep(0.5)  # Rate limiting
            
            # Sync pending quotes
            if Config.DYNAMICS_SYNC_QUOTES:
                pending_quotes = self.db.get_pending_dynamics_syncs('quote', limit=50)
                logger.info(f"Found {len(pending_quotes)} pending quotes to sync")
                
                for quote in pending_quotes:
                    await self.sync_quote_to_dynamics(quote['id'])
                    await asyncio.sleep(0.5)  # Rate limiting
                    
        except Exception as e:
            logger.error(f"Error in sync_pending_entities: {e}")
    
    async def periodic_sync_task(self):
        """
        Background task that runs periodically to sync both ways
        """
        while True:
            try:
                # Sync pending items to Dynamics
                await self.sync_pending_entities()
                
                # Sync changes from Dynamics
                await self.sync_from_dynamics()
                
                # Wait before next sync cycle
                await asyncio.sleep(Config.DYNAMICS_SYNC_INTERVAL)
                
            except Exception as e:
                logger.error(f"Error in periodic sync task: {e}")
                await asyncio.sleep(Config.DYNAMICS_RETRY_INTERVAL)
    
    async def test_connection(self) -> bool:
        """Test connection to Dynamics 365"""
        if not self.dynamics_service:
            logger.warning("Dynamics 365 integration not configured")
            return False
        
        return await self.dynamics_service.test_connection()
    
    def get_sync_statistics(self) -> Dict:
        """Get synchronization statistics"""
        stats = self.db.execute_query(
            """
            SELECT
                (SELECT COUNT(*) FROM users WHERE dynamics_sync_status = 'synced') as synced_users,
                (SELECT COUNT(*) FROM users WHERE dynamics_sync_status = 'pending') as pending_users,
                (SELECT COUNT(*) FROM users WHERE dynamics_sync_status = 'error') as error_users,
                (SELECT COUNT(*) FROM quotations WHERE dynamics_sync_status = 'synced') as synced_quotes,
                (SELECT COUNT(*) FROM quotations WHERE dynamics_sync_status = 'pending') as pending_quotes,
                (SELECT COUNT(*) FROM quotations WHERE dynamics_sync_status = 'error') as error_quotes,
                (SELECT COUNT(*) FROM dynamics_sync_log WHERE created_at > DATE_SUB(NOW(), INTERVAL 24 HOUR)) as syncs_24h,
                (SELECT COUNT(*) FROM dynamics_sync_log WHERE status = 'error' AND created_at > DATE_SUB(NOW(), INTERVAL 24 HOUR)) as errors_24h
            """,
            fetch=True
        )
        
        return stats[0] if stats else {}
    
    async def manual_sync_user(self, user_id: int) -> Tuple[bool, str]:
        """Manually sync a specific user"""
        try:
            # Clear from sync queue if present
            async with self.sync_queue.lock:
                self.sync_queue.processing['user'].discard(str(user_id))
                if str(user_id) in self.sync_queue.completed['user']:
                    del self.sync_queue.completed['user'][str(user_id)]
            
            success = await self.sync_user_to_dynamics(user_id)
            if success:
                return True, "User synced successfully"
            else:
                return False, "Failed to sync user"
        except Exception as e:
            return False, str(e)
    
    async def manual_sync_quote(self, quote_id: int) -> Tuple[bool, str]:
        """Manually sync a specific quote"""
        try:
            # Clear from sync queue if present
            async with self.sync_queue.lock:
                self.sync_queue.processing['quote'].discard(str(quote_id))
                if str(quote_id) in self.sync_queue.completed['quote']:
                    del self.sync_queue.completed['quote'][str(quote_id)]
            
            success = await self.sync_quote_to_dynamics(quote_id)
            if success:
                return True, "Quote synced successfully"
            else:
                return False, "Failed to sync quote"
        except Exception as e:
            return False, str(e)
    
    def get_sync_errors(self, limit: int = 20) -> List[Dict]:
        """Get recent sync errors"""
        return self.db.execute_query(
            """
            SELECT * FROM dynamics_sync_log
            WHERE status = 'error'
            ORDER BY created_at DESC
            LIMIT %s
            """,
            (limit,),
            fetch=True
        ) or []

    async def check_dynamics_field_mappings(self) -> Dict:
        """
        Check which fields are available in Dynamics 365 for quotes
        """
        if not self.dynamics_service:
            return {"error": "Dynamics service not initialized"}
        
        try:
            # Get quote metadata from Dynamics
            response = await self.dynamics_service.make_request(
                method="GET",
                endpoint="EntityDefinitions(LogicalName='quote')/Attributes?$select=LogicalName,DisplayName,AttributeType"
            )
            
            if response and 'value' in response:
                fields = {}
                for attr in response['value']:
                    fields[attr['LogicalName']] = {
                        'display_name': attr['DisplayName']['UserLocalizedLabel']['Label'] if 'DisplayName' in attr and 'UserLocalizedLabel' in attr['DisplayName'] else 'N/A',
                        'type': attr['AttributeType']
                    }
                
                # Filter for relevant fields
                relevant_prefixes = ['cr229_', 'quote', 'total', 'customer', 'description', 'name']
                filtered_fields = {k: v for k, v in fields.items() 
                                 if any(k.startswith(prefix) for prefix in relevant_prefixes)}
                
                return {
                    "total_fields": len(fields),
                    "relevant_fields": filtered_fields,
                    "custom_fields": {k: v for k, v in fields.items() if k.startswith('cr229_')}
                }
            
        except Exception as e:
            return {"error": str(e)}