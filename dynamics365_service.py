"""
Dynamics 365 Integration Service for Stretch Ceiling Bot
Handles bidirectional synchronization of users and quotes with Dynamics 365 Sales Professional
ENHANCED: Complete quote field population, PDF attachments, and conversation sync
VERSION 3.0 - FIXED: Customer linking in quotes (account vs contact)
"""
import aiohttp
import asyncio
import logging
import json
import base64
import os
from typing import Dict, Optional, List, Any, Tuple
from datetime import datetime, timedelta
from msal import ConfidentialClientApplication
import urllib.parse
from functools import wraps
import time

logger = logging.getLogger(__name__)

def retry_with_backoff(max_retries: int = 3, backoff_factor: float = 2.0):
    """Decorator for retry logic with exponential backoff"""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt < max_retries - 1:
                        wait_time = backoff_factor ** attempt
                        logger.warning(f"Attempt {attempt + 1} failed: {e}. Retrying in {wait_time}s...")
                        await asyncio.sleep(wait_time)
                    else:
                        logger.error(f"All {max_retries} attempts failed: {e}")
            raise last_exception
        return wrapper
    return decorator

class RateLimiter:
    """Simple rate limiter for API calls"""
    def __init__(self, calls_per_second: float = 2.0):
        self.calls_per_second = calls_per_second
        self.min_interval = 1.0 / calls_per_second
        self.last_call = 0
    
    async def acquire(self):
        """Wait if necessary to respect rate limit"""
        current_time = time.time()
        time_since_last_call = current_time - self.last_call
        if time_since_last_call < self.min_interval:
            await asyncio.sleep(self.min_interval - time_since_last_call)
        self.last_call = time.time()

class Dynamics365Service:
    """Service for integrating with Dynamics 365 Sales Professional with enhanced reliability"""
    
    def __init__(self, tenant_id: str, client_id: str, client_secret: str, dynamics_url: str, db_manager=None):
        """
        Initialize Dynamics 365 service
        
        Args:
            tenant_id: Azure AD tenant ID
            client_id: Application (client) ID
            client_secret: Client secret
            dynamics_url: Your Dynamics 365 instance URL (e.g., https://yourorg.crm4.dynamics.com)
            db_manager: Database manager instance for conversation data
        """
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.dynamics_url = dynamics_url.rstrip('/')
        self.api_version = "v9.2"
        self.base_url = f"{self.dynamics_url}/api/data/{self.api_version}"
        self.db = db_manager
        
        # Initialize MSAL application
        self.msal_app = ConfidentialClientApplication(
            client_id=self.client_id,
            authority=f"https://login.microsoftonline.com/{self.tenant_id}",
            client_credential=self.client_secret
        )
        
        self._access_token = None
        self._token_expiry = None
        
        # Initialize rate limiter (2 calls per second)
        self.rate_limiter = RateLimiter(calls_per_second=2.0)
        
        # Cache for deduplication
        self._contact_cache = {}  # telegram_id -> contact_id
        self._quote_cache = {}    # telegram_quote_id -> dynamics_quote_id
        self._cache_expiry = datetime.now() + timedelta(minutes=30)
    
    def _clear_cache_if_expired(self):
        """Clear cache if expired"""
        if datetime.now() > self._cache_expiry:
            self._contact_cache.clear()
            self._quote_cache.clear()
            self._cache_expiry = datetime.now() + timedelta(minutes=30)
    
    async def get_access_token(self) -> Optional[str]:
        """Get access token for Dynamics 365"""
        try:
            # Check if we have a valid token
            if self._access_token and self._token_expiry and datetime.now() < self._token_expiry:
                return self._access_token
            
            # Get new token
            result = self.msal_app.acquire_token_for_client(
                scopes=[f"{self.dynamics_url}/.default"]
            )
            
            if "access_token" in result:
                self._access_token = result["access_token"]
                # Token typically expires in 1 hour, refresh 5 minutes early
                self._token_expiry = datetime.now() + timedelta(minutes=55)
                logger.info("âœ… Successfully acquired Dynamics 365 access token")
                return self._access_token
            else:
                logger.error(f"âŒ Failed to acquire token: {result.get('error')}")
                return None
                
        except Exception as e:
            logger.error(f"âŒ Error getting access token: {e}")
            return None
    
    @retry_with_backoff(max_retries=3)
    async def make_request(self, method: str, endpoint: str, data: Dict = None, headers: Dict = None) -> Optional[Dict]:
        """Make authenticated request to Dynamics 365 with retry logic"""
        # Apply rate limiting
        await self.rate_limiter.acquire()
        
        token = await self.get_access_token()
        if not token:
            return None
        
        url = f"{self.base_url}/{endpoint}"
        
        default_headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
            "OData-MaxVersion": "4.0",
            "OData-Version": "4.0",
            "Prefer": "return=representation"
        }
        
        if headers:
            default_headers.update(headers)
        
        try:
            async with aiohttp.ClientSession() as session:
                logger.info(f"📡 Making {method} request to: {url[:100]}...")
                async with session.request(
                    method=method,
                    url=url,
                    headers=default_headers,
                    json=data,
                    ssl=True
                ) as response:
                    response_text = await response.text()
                    logger.info(f"📡 Response status: {response.status}, length: {len(response_text)}")
                    
                    if response.status in [200, 201, 204]:
                        if response.status == 204:  # No content
                            return {"success": True}
                        # Parse JSON from the text we already read (can't call response.json() after response.text())
                        try:
                            result = json.loads(response_text)
                            logger.info(f"📡 Parsed JSON successfully, keys: {list(result.keys()) if isinstance(result, dict) else 'not a dict'}")
                            return result
                        except json.JSONDecodeError as e:
                            logger.error(f"JSON decode error: {e}, response: {response_text[:500]}")
                            return None
                    elif response.status == 412:  # Precondition failed - duplicate key
                        error_data = json.loads(response_text) if response_text else {}
                        if "0x80040237" in error_data.get("error", {}).get("code", ""):
                            logger.warning(f"Duplicate key error: {error_data}")
                            # Return None to indicate duplicate, let caller handle
                            return None
                        else:
                            logger.error(f"412 error (not duplicate): {response_text[:500]}")
                            return None
                    else:
                        logger.error(f"API Error {response.status}: {response_text[:500] if response_text else 'empty'}")
                        return None
                        
        except aiohttp.ClientError as e:
            logger.error(f"Network error: {e}")
            raise  # Let retry decorator handle it
        except Exception as e:
            logger.error(f"Request error: {e}")
            raise
    
    async def create_or_update_contact(self, user_data: dict) -> Optional[str]:
        """Create or update a contact in Dynamics 365 using standard fields only"""
        try:
            # Prepare contact data using STANDARD fields only
            contact_data = {
                "firstname": user_data.get('first_name', ''),
                "lastname": user_data.get('last_name', ''),
                "emailaddress1": user_data.get('email', ''),
                "telephone1": user_data.get('phone', ''),
                "address1_line1": user_data.get('address', ''),
                "jobtitle": "Telegram User",  # Standard field
                "description": f"Telegram ID: {user_data.get('user_id', '')}"  # Store custom data in description
            }
            
            # Add company info if it's a business contact
            if user_data.get('is_company'):
                contact_data["parentcustomerid_account@odata.bind"] = None  # Would need account ID
                contact_data["jobtitle"] = "Business Contact"
                if user_data.get('company_name'):
                    contact_data["company"] = user_data['company_name']  # Standard field
                if user_data.get('vat_number'):
                    # Add VAT to description since there's no standard field
                    contact_data["description"] += f"\nVAT: {user_data['vat_number']}"
            
            # Remove empty fields
            contact_data = {k: v for k, v in contact_data.items() if v}
            
            # Check if contact exists by email
            if user_data.get('email'):
                existing = await self.search_contacts_by_email(user_data['email'])
                if existing:
                    # Update existing contact
                    contact_id = existing[0]['contactid']
                    await self.make_request(
                        method="PATCH",
                        endpoint=f"contacts({contact_id})",
                        data=contact_data
                    )
                    logger.info(f"âœ… Updated existing contact {contact_id}")
                    return contact_id
            
            # Create new contact
            response = await self.make_request(
                method="POST",
                endpoint="contacts",
                data=contact_data
            )
            
            if response and 'contactid' in response:
                logger.info(f"âœ… Created new contact {response['contactid']}")
                return response['contactid']
            
            return None
            
        except Exception as e:
            logger.error(f"âŒ Error creating/updating contact: {e}")
            return None
    
    async def create_or_update_account(self, user_data: Dict, primary_contact_id: str = None) -> Optional[str]:
        """
        Create or update an account (company) in Dynamics 365 with deduplication
        
        Args:
            user_data: User data including company information
            primary_contact_id: Primary contact ID to link
            
        Returns:
            Account ID (GUID) if successful
        """
        try:
            vat_number = user_data.get('vat_number', '')
            company_name = user_data.get('company_name', '')
            
            logger.info(f"🏢 create_or_update_account: company={company_name}, vat={vat_number}")
            
            # Check if account exists by VAT number
            existing_account = None
            if vat_number:
                logger.info(f"🔍 Searching for existing account by VAT: {vat_number}")
                existing_account = await self.find_account_by_vat(vat_number)
                if existing_account:
                    logger.info(f"✅ Found existing account by VAT: {existing_account.get('accountid')}")
            
            # If not found by VAT, try by company name
            if not existing_account and user_data.get('company_name'):
                logger.info(f"🔍 Searching for existing account by name: {company_name}")
                existing_account = await self.find_account_by_name(user_data.get('company_name'))
                if existing_account:
                    logger.info(f"✅ Found existing account by name: {existing_account.get('accountid')}")
            
            # Prepare account data - only use standard fields
            vat_info = f" | VAT: {vat_number}" if vat_number else ""
            account_data = {
                "name": user_data.get('company_name', ''),
                "telephone1": user_data.get('phone', ''),
                "emailaddress1": user_data.get('email', ''),
                "address1_composite": user_data.get('address', ''),
                "description": f"Created from Telegram Bot{vat_info}",
            }
            
            # Set primary contact if provided
            if primary_contact_id:
                account_data["primarycontactid@odata.bind"] = f"/contacts({primary_contact_id})"
            
            if existing_account:
                # Update existing account
                account_id = existing_account['accountid']
                
                response = await self.make_request(
                    method="PATCH",
                    endpoint=f"accounts({account_id})",
                    data=account_data
                )
                
                if response is not None:
                    logger.info(f"âœ… Updated account {account_id}")
                    return account_id
                else:
                    return account_id  # Return existing ID even if update failed
            else:
                # Create new account
                logger.info(f"📝 Creating new account: {company_name}")
                response = await self.make_request(
                    method="POST",
                    endpoint="accounts",
                    data=account_data
                )
                
                logger.info(f"📝 Create account response: {response}")
                
                if response and 'accountid' in response:
                    account_id = response['accountid']
                    logger.info(f"âœ… Created account {account_id}")
                    return account_id
                elif response is None:
                    # Duplicate error - try to find it
                    await asyncio.sleep(1)
                    if vat_number:
                        existing = await self.find_account_by_vat(vat_number)
                    else:
                        existing = await self.find_account_by_name(user_data.get('company_name'))
                    
                    if existing:
                        return existing['accountid']
                    
            return None
            
        except Exception as e:
            logger.error(f"âŒ Error creating/updating account: {e}")
            return None
    
    async def create_quote(self, quote_data: dict, contact_id: str, account_id: str = None, 
                      user_profile: dict = None, pdf_path: str = None) -> Optional[str]:
        """Create a quote in Dynamics 365 using only standard fields"""
        try:
            # Build description with all the custom data
            description_parts = [
                f"Stretch ceiling quote for {len(quote_data.get('ceilings', []))} ceiling(s)",
                f"Reference: {quote_data.get('quote_reference', 'N/A')}",
            ]
            
            if user_profile:
                if user_profile.get('company_name'):
                    description_parts.append(f"Company: {user_profile['company_name']}")
                if user_profile.get('vat_number'):
                    description_parts.append(f"VAT: {user_profile['vat_number']}")
                if user_profile.get('address'):
                    description_parts.append(f"Address: {user_profile['address']}")
            
            # Quote header using ONLY standard fields
            quote_payload = {
                "name": f"Quote {quote_data.get('quote_number')}",
                "quotenumber": quote_data.get('quote_number'),
                "totalamount": float(quote_data.get('total_price', 0)),
                "description": "\n".join(description_parts),
                
                # Status fields (standard)
                "statecode": 0,  # Active
                "statuscode": 1,  # Draft/In Progress
                
                # Standard date fields
                "effectivefrom": datetime.now().isoformat(),
                "effectiveto": (datetime.now() + timedelta(days=30)).isoformat(),
                
                # Standard fields
                "pricelevelid": None,  # Can be set if you have price lists
                "paymenttermscode": 1,  # Net 30 days (adjust based on your setup)
                "freighttermscode": 1,  # FOB (adjust as needed)
                "discountpercentage": 0,
                "discountamount": 0,
                
                # Address fields (standard) - if available
                "shipto_line1": user_profile.get('address', '') if user_profile else '',
                "shipto_city": quote_data.get('city', 'Brussels'),
                "shipto_postalcode": quote_data.get('postal_code', '1000'),
                "shipto_country": "Belgium",
                
                # Billing address (same as shipping for now)
                "billto_line1": user_profile.get('address', '') if user_profile else '',
                "billto_city": quote_data.get('city', 'Brussels'),
                "billto_postalcode": quote_data.get('postal_code', '1000'),
                "billto_country": "Belgium"
            }
            
            # Customer reference - prefer account if available, otherwise use contact
            # This sets the "Potential Customer" field in Dynamics 365
            if account_id:
                quote_payload["customerid_account@odata.bind"] = f"/accounts({account_id})"
                logger.info(f"Linking quote to account: {account_id}")
            elif contact_id:
                quote_payload["customerid_contact@odata.bind"] = f"/contacts({contact_id})"
                logger.info(f"Linking quote to contact: {contact_id}")
            else:
                logger.warning("No customer ID provided for quote - Potential Customer will be empty")
            
            # Remove None values to avoid API errors
            quote_payload = {k: v for k, v in quote_payload.items() if v is not None}
            
            logger.info(f"Creating quote with payload: {json.dumps(quote_payload, indent=2)}")
            
            # Create the quote
            response = await self.make_request(
                method="POST",
                endpoint="quotes",
                data=quote_payload
            )
            
            if response and 'quoteid' in response:
                quote_id = response['quoteid']
                logger.info(f"âœ… Created quote {quote_id} in Dynamics 365")
                
                # Create quote line items (products) using standard fields only
                ceilings = quote_data.get('ceilings', [])
                ceiling_costs = quote_data.get('ceiling_costs', [])
                
                for i, (ceiling, costs) in enumerate(zip(ceilings, ceiling_costs)):
                    # Calculate total for this ceiling
                    ceiling_total = 0
                    if isinstance(costs, dict):
                        ceiling_total = costs.get('total', 0)
                        if ceiling_total == 0:
                            # Calculate from components
                            ceiling_total = sum([
                                costs.get('ceiling_cost', 0),
                                costs.get('perimeter_structure_cost', 0),
                                costs.get('perimeter_profile_cost', 0),
                                costs.get('corners_cost', 0),
                                costs.get('seam_cost', 0),
                                costs.get('lights_cost', 0),
                                costs.get('wood_structures_cost', 0),
                                costs.get('acoustic_absorber_cost', 0)
                            ])
                    
                    # Build detailed description for the line item
                    line_description = (
                        f"{ceiling.get('name', f'Ceiling {i+1}')} - "
                        f"{ceiling.get('ceiling_type', 'N/A')}/{ceiling.get('type_ceiling', 'N/A')}/{ceiling.get('color', 'N/A')}\n"
                        f"Dimensions: {ceiling.get('length', 0)}m x {ceiling.get('width', 0)}m = {ceiling.get('area', 0):.2f}mÂ²\n"
                        f"Perimeter: {ceiling.get('perimeter', 0):.1f}m, Corners: {ceiling.get('corners', 4)}"
                    )
                    
                    # Add cost breakdown to description
                    if costs:
                        line_description += "\n\nCost breakdown:"
                        if costs.get('ceiling_cost', 0) > 0:
                            line_description += f"\n- Ceiling: â‚¬{costs['ceiling_cost']:.2f}"
                        if costs.get('perimeter_structure_cost', 0) > 0:
                            line_description += f"\n- Perimeter structure: â‚¬{costs['perimeter_structure_cost']:.2f}"
                        if costs.get('perimeter_profile_cost', 0) > 0:
                            line_description += f"\n- Perimeter profile: â‚¬{costs['perimeter_profile_cost']:.2f}"
                        if costs.get('corners_cost', 0) > 0:
                            line_description += f"\n- Corners: â‚¬{costs['corners_cost']:.2f}"
                        if costs.get('lights_cost', 0) > 0:
                            line_description += f"\n- Lights: â‚¬{costs['lights_cost']:.2f}"
                    
                    # Create quote detail/product line with STANDARD fields only
                    line_payload = {
                        "quoteid@odata.bind": f"/quotes({quote_id})",
                        "productdescription": line_description[:2000],  # Limit to 2000 chars
                        "quantity": 1,
                        "priceperunit": float(ceiling_total),
                        "extendedamount": float(ceiling_total),
                        "manualdiscountamount": 0,
                        "tax": float(ceiling_total * 0.21),  # 21% VAT
                        "ispriceoverridden": True,  # We're setting manual prices
                        "isproductoverridden": True,  # We're using write-in products
                        "lineitemnumber": i + 1,
                        "requestdeliveryby": (datetime.now() + timedelta(days=14)).isoformat(),
                    }
                    
                    # Remove None values
                    line_payload = {k: v for k, v in line_payload.items() if v is not None}
                    
                    logger.info(f"Creating quote line {i+1} with payload: {json.dumps(line_payload, indent=2)}")
                    
                    # Create quote product
                    line_response = await self.make_request(
                        method="POST",
                        endpoint="quotedetails",
                        data=line_payload
                    )
                    
                    if line_response:
                        logger.info(f"âœ… Created quote line for ceiling {i+1}: {ceiling.get('name')}")
                    else:
                        logger.error(f"âŒ Failed to create quote line for ceiling {i+1}")
                
                # Attach PDF if provided
                if pdf_path and os.path.exists(pdf_path):
                    await self.attach_file_to_quote(quote_id, pdf_path, f"Quote_{quote_data.get('quote_number')}.pdf")
                
                return quote_id
            
            return None
            
        except Exception as e:
            logger.error(f"âŒ Error creating quote in Dynamics 365: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None

        # Add these methods to your existing Dynamics365Service class:

    async def create_or_update_quote(self, quote_data: dict, contact_id: str, dynamics_quote_id: str = None,
                                    account_id: str = None, user_profile: dict = None, pdf_path: str = None) -> Optional[str]:
        """Create or update a quote in Dynamics 365 with proper total amount"""
        try:
            # Calculate the correct total from all ceilings
            total_amount = float(quote_data.get('total_price', 0))
            
            # Also recalculate from ceiling costs to ensure accuracy
            calculated_total = 0
            ceiling_costs = quote_data.get('ceiling_costs', [])
            for costs in ceiling_costs:
                if isinstance(costs, dict):
                    ceiling_total = costs.get('total', 0)
                    if ceiling_total == 0:
                        # Calculate from components
                        ceiling_total = sum([
                            costs.get('ceiling_cost', 0),
                            costs.get('perimeter_structure_cost', 0),
                            costs.get('perimeter_profile_cost', 0),
                            costs.get('corners_cost', 0),
                            costs.get('seam_cost', 0),
                            costs.get('lights_cost', 0),
                            costs.get('wood_structures_cost', 0),
                            costs.get('acoustic_absorber_cost', 0)
                        ])
                    calculated_total += ceiling_total
            
            # Use the calculated total if it differs
            if calculated_total > 0:
                total_amount = calculated_total
            
            # Build description
            description_parts = [
                f"Stretch ceiling quote for {len(quote_data.get('ceilings', []))} ceiling(s)",
                f"Reference: {quote_data.get('quote_reference', 'N/A')}",
                f"Total Amount: â‚¬{total_amount:,.2f}",  # Add total to description
            ]
            
            if user_profile:
                if user_profile.get('company_name'):
                    description_parts.append(f"Company: {user_profile['company_name']}")
                if user_profile.get('vat_number'):
                    description_parts.append(f"VAT: {user_profile['vat_number']}")
            
            # Quote payload - base fields
            quote_payload = {
                "name": f"Quote {quote_data.get('quote_number')}",
                "quotenumber": quote_data.get('quote_number'),
                "totalamount": total_amount,  # IMPORTANT: Set the total amount
                "totallineitemamount": total_amount,  # Also set line item total
                "totaldiscountamount": 0,
                "description": "\n".join(description_parts),
                "statecode": 0,
                "statuscode": 1,
                "effectivefrom": datetime.now().isoformat(),
                "effectiveto": (datetime.now() + timedelta(days=30)).isoformat(),
                "paymenttermscode": 1,
                "freighttermscode": 1,
                "discountpercentage": 0,
                "discountamount": 0,
                "shipto_line1": user_profile.get('address', '') if user_profile else '',
                "shipto_city": quote_data.get('city', 'Brussels'),
                "shipto_postalcode": quote_data.get('postal_code', '1000'),
                "shipto_country": "Belgium",
                "billto_line1": user_profile.get('address', '') if user_profile else '',
                "billto_city": quote_data.get('city', 'Brussels'),
                "billto_postalcode": quote_data.get('postal_code', '1000'),
                "billto_country": "Belgium"
            }
            
            # FIXED: Customer reference - prefer account if available, otherwise use contact
            # This properly sets the "Potential Customer" field in Dynamics 365
            if account_id:
                quote_payload["customerid_account@odata.bind"] = f"/accounts({account_id})"
                logger.info(f"Linking quote to account: {account_id}")
            elif contact_id:
                quote_payload["customerid_contact@odata.bind"] = f"/contacts({contact_id})"
                logger.info(f"Linking quote to contact: {contact_id}")
            else:
                logger.warning("No customer ID provided for quote - Potential Customer will be empty")
            
            # Remove None values
            quote_payload = {k: v for k, v in quote_payload.items() if v is not None}
            
            if dynamics_quote_id:
                # UPDATE existing quote
                logger.info(f"Updating quote {dynamics_quote_id} with total: â‚¬{total_amount:,.2f}")
                
                # Update the quote
                await self.make_request(
                    method="PATCH",
                    endpoint=f"quotes({dynamics_quote_id})",
                    data=quote_payload
                )
                
                # Delete existing quote lines
                await self.delete_quote_lines(dynamics_quote_id)
                
                quote_id = dynamics_quote_id
                logger.info(f"âœ… Updated quote {quote_id} in Dynamics 365")
            else:
                # CREATE new quote
                logger.info(f"Creating quote with total: â‚¬{total_amount:,.2f}")
                
                response = await self.make_request(
                    method="POST",
                    endpoint="quotes",
                    data=quote_payload
                )
                
                if not response or 'quoteid' not in response:
                    logger.error("Failed to create quote")
                    return None
                    
                quote_id = response['quoteid']
                logger.info(f"âœ… Created quote {quote_id} in Dynamics 365")
            
            # Create quote line items
            await self.create_quote_lines(quote_id, quote_data, total_amount)
            
            # After creating lines, update the quote total again to ensure it matches
            await self.update_quote_total(quote_id, total_amount)
            
            # Attach PDF
            if pdf_path and os.path.exists(pdf_path):
                await self.attach_file_to_quote(quote_id, pdf_path, f"Quote_{quote_data.get('quote_number')}.pdf")
            
            return quote_id
            
        except Exception as e:
            logger.error(f"âŒ Error creating/updating quote in Dynamics 365: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None

    async def create_quote_lines(self, quote_id: str, quote_data: dict, expected_total: float):
        """Create quote line items and verify total"""
        try:
            ceilings = quote_data.get('ceilings', [])
            ceiling_costs = quote_data.get('ceiling_costs', [])
            line_total = 0
            
            for i, (ceiling, costs) in enumerate(zip(ceilings, ceiling_costs)):
                # Calculate total for this ceiling
                ceiling_total = 0
                if isinstance(costs, dict):
                    ceiling_total = costs.get('total', 0)
                    if ceiling_total == 0:
                        ceiling_total = sum([
                            costs.get('ceiling_cost', 0),
                            costs.get('perimeter_structure_cost', 0),
                            costs.get('perimeter_profile_cost', 0),
                            costs.get('corners_cost', 0),
                            costs.get('seam_cost', 0),
                            costs.get('lights_cost', 0),
                            costs.get('wood_structures_cost', 0),
                            costs.get('acoustic_absorber_cost', 0)
                        ])
                
                line_total += ceiling_total
                
                # Build description
                line_description = (
                    f"{ceiling.get('name', f'Ceiling {i+1}')} - "
                    f"{ceiling.get('ceiling_type', 'N/A')}/{ceiling.get('type_ceiling', 'N/A')}/{ceiling.get('color', 'N/A')}\n"
                    f"Dimensions: {ceiling.get('length', 0)}m x {ceiling.get('width', 0)}m = {ceiling.get('area', 0):.2f}mÂ²\n"
                    f"Perimeter: {ceiling.get('perimeter', 0):.1f}m, Corners: {ceiling.get('corners', 4)}"
                )
                
                # Add cost breakdown
                if costs:
                    line_description += "\n\nCost breakdown:"
                    if costs.get('ceiling_cost', 0) > 0:
                        line_description += f"\n- Ceiling: â‚¬{costs['ceiling_cost']:.2f}"
                    if costs.get('perimeter_structure_cost', 0) > 0:
                        line_description += f"\n- Perimeter structure: â‚¬{costs['perimeter_structure_cost']:.2f}"
                    if costs.get('perimeter_profile_cost', 0) > 0:
                        line_description += f"\n- Perimeter profile: â‚¬{costs['perimeter_profile_cost']:.2f}"
                    if costs.get('corners_cost', 0) > 0:
                        line_description += f"\n- Corners: â‚¬{costs['corners_cost']:.2f}"
                    if costs.get('lights_cost', 0) > 0:
                        line_description += f"\n- Lights: â‚¬{costs['lights_cost']:.2f}"
                    if costs.get('wood_structures_cost', 0) > 0:
                        line_description += f"\n- Wood structures: â‚¬{costs['wood_structures_cost']:.2f}"
                
                # Create line item
                line_payload = {
                    "quoteid@odata.bind": f"/quotes({quote_id})",
                    "productdescription": line_description[:2000],
                    "quantity": 1,
                    "priceperunit": float(ceiling_total),
                    "extendedamount": float(ceiling_total),
                    "baseamount": float(ceiling_total),  # Add base amount
                    "manualdiscountamount": 0,
                    "tax": float(ceiling_total * 0.21),
                    "ispriceoverridden": True,
                    "isproductoverridden": True,
                    "lineitemnumber": i + 1,
                    "requestdeliveryby": (datetime.now() + timedelta(days=14)).isoformat(),
                }
                
                line_payload = {k: v for k, v in line_payload.items() if v is not None}
                
                await self.make_request(
                    method="POST",
                    endpoint="quotedetails",
                    data=line_payload
                )
                
                logger.info(f"âœ… Created quote line {i+1} with amount: â‚¬{ceiling_total:,.2f}")
            
            logger.info(f"Total from lines: â‚¬{line_total:,.2f}, Expected: â‚¬{expected_total:,.2f}")
            
        except Exception as e:
            logger.error(f"âŒ Error creating quote lines: {e}")

    async def update_quote_total(self, quote_id: str, total_amount: float):
        """Update quote total amount to ensure it matches line items"""
        try:
            update_payload = {
                "totalamount": float(total_amount),
                "totallineitemamount": float(total_amount),
                "totaltax": float(total_amount * 0.21),  # 21% VAT
                "totalamountlessfreight": float(total_amount),
                "totaldiscountamount": 0
            }
            
            await self.make_request(
                method="PATCH",
                endpoint=f"quotes({quote_id})",
                data=update_payload
            )
            
            logger.info(f"âœ… Updated quote {quote_id} total to â‚¬{total_amount:,.2f}")
            
        except Exception as e:
            logger.error(f"âŒ Error updating quote total: {e}")

    async def delete_quote_lines(self, quote_id: str):
        """Delete all existing quote lines before updating"""
        try:
            # Get existing quote lines
            response = await self.make_request(
                method="GET",
                endpoint=f"quotes({quote_id})/quote_details"
            )
            
            if response and 'value' in response:
                for line in response['value']:
                    line_id = line.get('quotedetailid')
                    if line_id:
                        await self.make_request(
                            method="DELETE",
                            endpoint=f"quotedetails({line_id})"
                        )
                        logger.info(f"Deleted quote line {line_id}")
                        
        except Exception as e:
            logger.error(f"âŒ Error deleting quote lines: {e}")

    # Update the existing create_quote method to use create_or_update_quote
    async def create_quote(self, quote_data: dict, contact_id: str, account_id: str = None, 
                        user_profile: dict = None, pdf_path: str = None) -> Optional[str]:
        """Create a quote - redirects to create_or_update_quote"""
        return await self.create_or_update_quote(
            quote_data=quote_data,
            contact_id=contact_id,
            dynamics_quote_id=None,
            account_id=account_id,
            user_profile=user_profile,
            pdf_path=pdf_path
        )

    async def attach_file_to_quote(self, quote_id: str, file_path: str, file_name: str):
        """Attach a file to a quote in Dynamics 365 using standard fields"""
        try:
            import base64
            
            with open(file_path, 'rb') as f:
                file_content = f.read()
            
            # Create annotation (note with attachment) using standard fields
            annotation_data = {
                "subject": f"Quote PDF - {file_name}",
                "filename": file_name,
                "documentbody": base64.b64encode(file_content).decode('utf-8'),
                "mimetype": "application/pdf",
                "isdocument": True,
                "objectid_quote@odata.bind": f"/quotes({quote_id})",
                "objecttypecode": "quote"
            }
            
            response = await self.make_request(
                method="POST",
                endpoint="annotations",
                data=annotation_data
            )
            
            if response:
                logger.info(f"âœ… Attached PDF to quote {quote_id}")
            else:
                logger.error(f"âŒ Failed to attach PDF to quote {quote_id}")
            
        except Exception as e:
            logger.error(f"âŒ Error attaching file to quote: {e}")
            import traceback
            logger.error(traceback.format_exc())
    
    async def _prepare_quote_data(self, quote_data: Dict, contact_id: str, account_id: str = None, user_profile: Dict = None) -> Dict:
        """Prepare comprehensive quote data with all required fields"""
        
        # Calculate due date (30 days from creation)
        created_date = quote_data.get('created_at', datetime.now())
        if isinstance(created_date, str):
            created_date = datetime.fromisoformat(created_date)
        due_date = created_date + timedelta(days=30)
        
        # Prepare billing information from user profile
        bill_to_name = ""
        bill_to_address = ""
        bill_to_city = ""
        bill_to_country = "Belgium"  # Default
        email = ""
        
        if user_profile:
            # Get billing name
            if user_profile.get('is_company'):
                bill_to_name = user_profile.get('company_name', '')
            else:
                bill_to_name = f"{user_profile.get('first_name', '')} {user_profile.get('last_name', '')}".strip()
            
            # Parse address
            address = user_profile.get('address', '')
            email = user_profile.get('email', '')
            
            # Try to parse city from address (assuming format: "Street, Number, Postal Code, City")
            if address:
                address_parts = [part.strip() for part in address.split(',')]
                if len(address_parts) >= 4:
                    bill_to_address = f"{address_parts[0]}, {address_parts[1]}"
                    bill_to_city = address_parts[3]
                else:
                    bill_to_address = address
        
        # Prepare quote data with all fields
        dynamics_quote = {
            # Standard fields
            "name": f"{quote_data.get('quote_reference', '')} - {quote_data.get('quote_number', '')}",
            "quotenumber": quote_data.get('quote_number', ''),
            "description": quote_data.get('quote_reference', ''),
            "totalamount": float(quote_data.get('total_price', 0)),
            
            # Customer relationship
            "customerid_contact@odata.bind": f"/contacts({contact_id})" if not account_id else None,
            "customerid_account@odata.bind": f"/accounts({account_id})" if account_id else None,
            
            # Custom fields
            "cr229_new_telegramquoteid": str(quote_data.get('quote_id', '')),
            "cr229_new_numberofceilings": len(quote_data.get('ceilings', [])),
            
            # Billing information
            "billto_name": bill_to_name,
            "billto_line1": bill_to_address,
            "billto_city": bill_to_city,
            "billto_country": bill_to_country,
            "billto_contactname": bill_to_name,
            
            # Dates
            "effectivefrom": created_date.isoformat(),
            "effectiveto": due_date.isoformat(),
            "requestdeliveryby": due_date.isoformat(),
            
            # Additional fields
            "emailaddress": email,
            "shipto_line1": bill_to_address,
            "shipto_city": bill_to_city,
            "shipto_country": bill_to_country,
            
            # Payment terms (customize based on your business rules)
            "paymenttermscode": 2 if user_profile and user_profile.get('is_company') else 1,  # 1=Net30, 2=Net60
            
            # Status - Active by default
            "statecode": 0,
            "statuscode": 1
        }
        
        # Remove None values
        dynamics_quote = {k: v for k, v in dynamics_quote.items() if v is not None}
        
        return dynamics_quote
    
    async def _create_consolidated_quote_products(self, quote_id: str, quote_data: Dict) -> None:
        """Create consolidated quote products with unique ceiling combinations"""
        try:
            # Group ceilings by unique combination
            ceiling_groups = {}
            
            for i, ceiling in enumerate(quote_data.get('ceilings', [])):
                # Create unique key for ceiling type
                ceiling_key = f"{ceiling.get('ceiling_type', '')}-{ceiling.get('type_ceiling', '')}-{ceiling.get('color', '')}"
                
                if ceiling_key not in ceiling_groups:
                    ceiling_groups[ceiling_key] = {
                        'ceilings': [],
                        'total_area': 0,
                        'total_cost': 0,
                        'count': 0
                    }
                
                ceiling_groups[ceiling_key]['ceilings'].append(ceiling)
                ceiling_groups[ceiling_key]['total_area'] += ceiling.get('area', 0)
                ceiling_groups[ceiling_key]['count'] += 1
                
                # Get cost for this ceiling
                ceiling_costs = quote_data.get('ceiling_costs', [])
                if i < len(ceiling_costs):
                    ceiling_groups[ceiling_key]['total_cost'] += ceiling_costs[i].get('total', 0)
            
            # Create a product line for each unique combination
            line_number = 1
            for ceiling_key, group_data in ceiling_groups.items():
                sample_ceiling = group_data['ceilings'][0]
                
                # Create detailed description
                description = f"Stretch Ceiling - {sample_ceiling.get('ceiling_type', '').upper()} "
                description += f"{sample_ceiling.get('type_ceiling', '')} - "
                description += f"{sample_ceiling.get('color', '').capitalize()}\n"
                description += f"Quantity: {group_data['count']} ceiling(s)\n"
                description += f"Total Area: {group_data['total_area']:.2f} mÂ²\n"
                
                # Add room names
                room_names = [c.get('name', f'Room {i+1}') for i, c in enumerate(group_data['ceilings'])]
                description += f"Rooms: {', '.join(room_names)}"
                
                # Calculate unit price
                unit_price = group_data['total_cost'] / group_data['total_area'] if group_data['total_area'] > 0 else 0
                
                quote_product = {
                    "quoteid@odata.bind": f"/quotes({quote_id})",
                    "productdescription": description,
                    "quantity": group_data['total_area'],
                    "priceperunit": unit_price,
                    "extendedamount": group_data['total_cost'],
                    "manualdiscountamount": 0,
                    "tax": group_data['total_cost'] * 0.21,  # 21% VAT
                    "sequencenumber": line_number,
                    "isproductoverridden": True  # Using write-in product
                }
                
                response = await self.make_request(
                    method="POST",
                    endpoint="quotedetails",
                    data=quote_product
                )
                
                if response:
                    logger.info(f"âœ… Created consolidated quote line for {ceiling_key}")
                
                line_number += 1
                
        except Exception as e:
            logger.error(f"âŒ Error creating consolidated quote products: {e}")
    
    async def attach_pdf_to_quote(self, quote_id: str, pdf_path: str, quote_number: str) -> bool:
        """Attach PDF quote to the quote record in Dynamics 365"""
        try:
            # Read PDF file
            with open(pdf_path, 'rb') as pdf_file:
                pdf_content = pdf_file.read()
                pdf_base64 = base64.b64encode(pdf_content).decode('utf-8')
            
            # Create annotation (attachment)
            annotation_data = {
                "subject": f"Quote PDF - {quote_number}",
                "filename": f"Quote_{quote_number}.pdf",
                "documentbody": pdf_base64,
                "mimetype": "application/pdf",
                "isdocument": True,
                "objectid_quote@odata.bind": f"/quotes({quote_id})"
            }
            
            response = await self.make_request(
                method="POST",
                endpoint="annotations",
                data=annotation_data
            )
            
            if response:
                logger.info(f"âœ… Attached PDF to quote {quote_id}")
                return True
            else:
                logger.error(f"Failed to attach PDF to quote {quote_id}")
                return False
                
        except Exception as e:
            logger.error(f"âŒ Error attaching PDF to quote: {e}")
            return False
    
    async def create_conversation_activity(self, contact_id: str, conversation_data: Dict) -> Optional[str]:
        """Create a conversation activity in Dynamics 365"""
        try:
            # Prepare conversation summary
            messages = conversation_data.get('messages', [])
            summary = f"Telegram Bot Conversation - {len(messages)} messages\n"
            summary += f"Topics: {', '.join(conversation_data.get('topics', []))}\n"
            summary += f"Total interactions: {conversation_data.get('interaction_count', 0)}\n\n"
            
            # Add conversation summary if available
            if conversation_data.get('summary'):
                summary += f"AI Summary: {conversation_data['summary']}\n\n"
            
            # Add recent messages
            summary += "Recent Messages:\n"
            recent_messages = messages[-10:]  # Last 10 messages
            for msg in recent_messages:
                timestamp = msg.get('created_at', '')
                if isinstance(timestamp, datetime):
                    timestamp = timestamp.strftime('%Y-%m-%d %H:%M')
                user_type = msg.get('message_type', 'user')
                message = msg.get('message', '')[:100]  # Limit length
                summary += f"[{timestamp}] {user_type}: {message}\n"
            
            activity_data = {
                "subject": f"Bot Conversation - {datetime.now().strftime('%Y-%m-%d')}",
                "description": summary,
                "activitytypecode": "task",
                "scheduledend": datetime.now().isoformat(),
                "actualend": datetime.now().isoformat(),
                "statecode": 1,  # Completed
                "statuscode": 5,  # Completed
                "regardingobjectid_contact@odata.bind": f"/contacts({contact_id})",
                "category": "Bot Conversation"
            }
            
            response = await self.make_request(
                method="POST",
                endpoint="tasks",
                data=activity_data
            )
            
            if response:
                logger.info(f"âœ… Created conversation activity for contact {contact_id}")
                return response.get('activityid')
                
            return None
            
        except Exception as e:
            logger.error(f"âŒ Error creating conversation activity: {e}")
            return None
    
    async def sync_user_conversations(self, user_id: int, contact_id: str) -> bool:
        """Sync user's conversation history to Dynamics 365"""
        try:
            if not self.db:
                logger.warning("Database manager not available for conversation sync")
                return True
            
            # Get conversation history from database
            conversation_logs = self.db.get_conversation_history(user_id, limit=50)
            
            if not conversation_logs:
                return True
            
            # Get conversation memory
            memory = self.db.get_user_conversation_memory(user_id)
            
            # Prepare conversation data
            conversation_data = {
                'messages': conversation_logs,
                'topics': memory.get('last_topics', []),
                'summary': memory.get('conversation_summary', ''),
                'interaction_count': memory.get('interaction_count', 0)
            }
            
            # Create conversation activity
            activity_id = await self.create_conversation_activity(contact_id, conversation_data)
            
            return activity_id is not None
            
        except Exception as e:
            logger.error(f"âŒ Error syncing conversations: {e}")
            return False
    
    async def find_contact_by_email(self, email: str) -> Optional[Dict]:
        """Find a contact by email address"""
        if not email:
            return None
            
        try:
            filter_query = f"emailaddress1 eq '{email}'"
            encoded_filter = urllib.parse.quote(filter_query)
            
            response = await self.make_request(
                method="GET",
                endpoint=f"contacts?$filter={encoded_filter}&$select=contactid,firstname,lastname,emailaddress1,cr229_new_telegramuserid"
            )
            
            if response and response.get('value'):
                return response['value'][0] if response['value'] else None
                
            return None
            
        except Exception as e:
            logger.error(f"âŒ Error finding contact: {e}")
            return None
    
    async def find_contact_by_telegram_id(self, telegram_id: str) -> Optional[Dict]:
        """Find a contact by Telegram user ID using custom field"""
        try:
            filter_query = f"cr229_new_telegramuserid eq '{telegram_id}'"
            encoded_filter = urllib.parse.quote(filter_query)
            
            response = await self.make_request(
                method="GET",
                endpoint=f"contacts?$filter={encoded_filter}&$select=contactid,firstname,lastname,emailaddress1,telephone1,address1_composite,cr229_new_telegramuserid"
            )
            
            if response and response.get('value'):
                return response['value'][0] if response['value'] else None
                
            return None
            
        except Exception as e:
            logger.error(f"âŒ Error finding contact by Telegram ID: {e}")
            return None
    
    async def find_quote_by_telegram_id(self, telegram_quote_id: str) -> Optional[Dict]:
        """Find a quote by Telegram quote ID using custom field"""
        try:
            filter_query = f"cr229_new_telegramquoteid eq '{telegram_quote_id}'"
            encoded_filter = urllib.parse.quote(filter_query)
            
            response = await self.make_request(
                method="GET",
                endpoint=f"quotes?$filter={encoded_filter}&$select=quoteid,quotenumber,name,totalamount,statecode,statuscode"
            )
            
            if response and response.get('value'):
                return response['value'][0] if response['value'] else None
                
            return None
            
        except Exception as e:
            logger.error(f"âŒ Error finding quote by Telegram ID: {e}")
            return None
    
    async def find_account_by_vat(self, vat_number: str) -> Optional[Dict]:
        """Find an account by VAT number - currently disabled as field may not exist"""
        if not vat_number:
            return None
        
        # VAT field may not exist in this Dynamics instance
        # Skip VAT lookup and rely on name matching
        logger.info(f"VAT lookup skipped (field may not exist): {vat_number}")
        return None
    
    async def _disabled_find_account_by_vat(self, vat_number: str) -> Optional[Dict]:
        """DISABLED: Original VAT lookup"""
        try:
            filter_query = f"cr229_VATNumber eq '{vat_number}'"
            encoded_filter = urllib.parse.quote(filter_query)
            
            response = await self.make_request(
                method="GET",
                endpoint=f"accounts?$filter={encoded_filter}&$select=accountid,name"
            )
            
            if response and response.get('value'):
                return response['value'][0] if response['value'] else None
                
            return None
            
        except Exception as e:
            logger.error(f"âŒ Error finding account: {e}")
            return None
    
    async def find_account_by_name(self, company_name: str) -> Optional[Dict]:
        """Find an account by company name"""
        if not company_name:
            return None
            
        try:
            filter_query = f"name eq '{company_name}'"
            encoded_filter = urllib.parse.quote(filter_query)
            
            response = await self.make_request(
                method="GET",
                endpoint=f"accounts?$filter={encoded_filter}&$select=accountid,name"
            )
            
            if response and response.get('value'):
                return response['value'][0] if response['value'] else None
                
            return None
            
        except Exception as e:
            logger.error(f"âŒ Error finding account by name: {e}")
            return None
    
    async def get_quotes_by_contact(self, contact_id: str, limit: int = 50) -> List[Dict]:
        """Get quotes for a specific contact"""
        try:
            filter_query = f"_customerid_value eq {contact_id}"
            encoded_filter = urllib.parse.quote(filter_query)
            
            response = await self.make_request(
                method="GET",
                endpoint=f"quotes?$filter={encoded_filter}&$select=quoteid,quotenumber,name,totalamount,statecode,statuscode,cr229_new_telegramquoteid,cr229_new_numberofceilings&$top={limit}&$orderby=createdon desc"
            )
            
            if response and response.get('value'):
                return response['value']
                
            return []
            
        except Exception as e:
            logger.error(f"âŒ Error getting quotes: {e}")
            return []
    
    async def update_quote_status(self, quote_id: str, state_code: int, status_code: int) -> bool:
        """
        Update quote status in Dynamics
        
        Args:
            quote_id: Quote ID
            state_code: 0=Active, 1=Won, 2=Closed
            status_code: Depends on state_code
        """
        try:
            data = {
                "statecode": state_code,
                "statuscode": status_code
            }
            
            response = await self.make_request(
                method="PATCH",
                endpoint=f"quotes({quote_id})",
                data=data
            )
            
            return response is not None
            
        except Exception as e:
            logger.error(f"âŒ Error updating quote status: {e}")
            return False
    
    async def link_contact_to_account(self, contact_id: str, account_id: str) -> bool:
        """Link a contact to an account"""
        try:
            # Update contact with parent account
            contact_update = {
                "parentcustomerid_account@odata.bind": f"/accounts({account_id})"
            }
            
            response = await self.make_request(
                method="PATCH",
                endpoint=f"contacts({contact_id})",
                data=contact_update
            )
            
            return response is not None
            
        except Exception as e:
            logger.error(f"âŒ Error linking contact to account: {e}")
            return False
    
    @retry_with_backoff(max_retries=3)
    async def test_connection(self) -> bool:
        """Test connection to Dynamics 365"""
        try:
            token = await self.get_access_token()
            if not token:
                logger.error("âŒ Failed to get access token")
                return False
            
            # Try to get WhoAmI
            response = await self.make_request(
                method="GET",
                endpoint="WhoAmI"
            )
            
            if response:
                logger.info(f"âœ… Connected to Dynamics 365 as: {response.get('UserId')}")
                return True
                
            return False
            
        except Exception as e:
            logger.error(f"âŒ Connection test failed: {e}")
            return False
    
    async def get_recent_changes(self, entity_type: str, since: datetime, limit: int = 100) -> List[Dict]:
        """Get recent changes from Dynamics for sync"""
        try:
            # Format datetime for OData
            since_str = since.strftime("%Y-%m-%dT%H:%M:%SZ")
            
            filter_query = f"modifiedon gt {since_str}"
            encoded_filter = urllib.parse.quote(filter_query)
            
            if entity_type == "contact":
                endpoint = f"contacts?$filter={encoded_filter}&$select=contactid,firstname,lastname,emailaddress1,telephone1,address1_composite,cr229_new_telegramuserid,modifiedon&$top={limit}&$orderby=modifiedon desc"
            elif entity_type == "account":
                endpoint = f"accounts?$filter={encoded_filter}&$select=accountid,name,emailaddress1,telephone1,address1_composite,modifiedon&$top={limit}&$orderby=modifiedon desc"
            elif entity_type == "quote":
                endpoint = f"quotes?$filter={encoded_filter}&$select=quoteid,quotenumber,name,totalamount,statecode,statuscode,cr229_new_telegramquoteid,modifiedon&$top={limit}&$orderby=modifiedon desc"
            else:
                return []
            
            response = await self.make_request(
                method="GET",
                endpoint=endpoint
            )
            
            if response and response.get('value'):
                return response['value']
                
            return []
            
        except Exception as e:
            logger.error(f"âŒ Error getting recent changes for {entity_type}: {e}")
            return []
    async def search_customers(self, search_term: str, limit: int = 15) -> dict:
        """
        Combined search for accounts and contacts using the individual search methods.
        
        Args:
            search_term: The search term to look for
            limit: Maximum number of results to return (default 15)
            
        Returns:
            dict with 'accounts' and 'contacts' lists
        """
        import asyncio
        
        results = {
            'accounts': [],
            'contacts': [],
            'total': 0
        }
        
        if not search_term or len(search_term) < 2:
            return results
        
        try:
            # Use the individual search methods which now use FetchXML
            accounts, contacts = await asyncio.gather(
                self.search_accounts(search_term, limit),
                self.search_contacts(search_term, limit),
                return_exceptions=True
            )
            
            # Process results
            if isinstance(accounts, list):
                for acc in accounts:
                    acc['type'] = 'account'
                    results['accounts'].append(acc)
            elif isinstance(accounts, Exception):
                logger.error(f"Account search error: {accounts}")
            
            if isinstance(contacts, list):
                for con in contacts:
                    con['type'] = 'contact'
                    results['contacts'].append(con)
            elif isinstance(contacts, Exception):
                logger.error(f"Contact search error: {contacts}")
            
            results['total'] = len(results['accounts']) + len(results['contacts'])
            return results
            
        except Exception as e:
            logger.error(f"Error in customer search: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return results

    # ============================================================
    # METHODS REQUIRED BY CUSTOMER_SELECTION.PY
    # ============================================================
    
    async def search_accounts(self, search_term: str, limit: int = 20) -> List[Dict]:
        """
        Search for accounts by name using FetchXML (OData filters not supported in this instance).
        Returns list of account dictionaries.
        """
        if not search_term or len(search_term) < 2:
            return []
        
        try:
            # Escape special XML characters
            search_escaped = search_term.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("'", "''")
            logger.info(f"🔍 Searching accounts for: '{search_term}' using FetchXML")
            
            # Use FetchXML with 'like' operator for partial matching
            fetch_xml = f"""<fetch top="{limit}">
              <entity name="account">
                <attribute name="accountid"/>
                <attribute name="name"/>
                <attribute name="emailaddress1"/>
                <attribute name="telephone1"/>
                <attribute name="address1_composite"/>
                <attribute name="address1_city"/>
                <order attribute="name" descending="false"/>
                <filter type="or">
                  <condition attribute="name" operator="like" value="%{search_escaped}%"/>
                  <condition attribute="emailaddress1" operator="like" value="%{search_escaped}%"/>
                </filter>
              </entity>
            </fetch>"""
            
            encoded_fetch = urllib.parse.quote(fetch_xml)
            endpoint = f"accounts?fetchXml={encoded_fetch}"
            
            response = await self.make_request(
                method="GET",
                endpoint=endpoint
            )
            
            logger.info(f"🔍 Account search response: {response is not None}, count: {len(response.get('value', [])) if response else 0}")
            
            if response and response.get('value'):
                accounts = []
                for acc in response['value']:
                    account_data = {
                        'accountid': acc.get('accountid'),
                        'id': acc.get('accountid'),
                        'name': acc.get('name', ''),
                        'email': acc.get('emailaddress1', ''),
                        'phone': acc.get('telephone1', ''),
                        'address': acc.get('address1_composite', ''),
                        'city': acc.get('address1_city', ''),
                        'vat_number': '',
                    }
                    # Log each account's data for debugging
                    logger.info(f"📋 Account data: name={account_data['name']}, email={account_data['email']}, phone={account_data['phone']}, address={account_data['address'][:50] if account_data['address'] else 'NONE'}")
                    accounts.append(account_data)
                logger.info(f"✅ Found {len(accounts)} accounts")
                return accounts
            
            logger.info(f"🔍 No accounts found for '{search_term}'")
            return []
            
        except Exception as e:
            logger.error(f"Error searching accounts: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return []
    
    async def search_contacts(self, search_term: str, limit: int = 20) -> List[Dict]:
        """
        Search for contacts by name using FetchXML.
        Returns list of contact dictionaries.
        """
        if not search_term or len(search_term) < 2:
            return []
        
        try:
            # Escape special XML characters
            search_escaped = search_term.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("'", "''")
            logger.info(f"🔍 Searching contacts for: '{search_term}' using FetchXML")
            
            # Use FetchXML with 'like' operator for partial matching
            fetch_xml = f"""<fetch top="{limit}">
              <entity name="contact">
                <attribute name="contactid"/>
                <attribute name="firstname"/>
                <attribute name="lastname"/>
                <attribute name="fullname"/>
                <attribute name="emailaddress1"/>
                <attribute name="telephone1"/>
                <attribute name="jobtitle"/>
                <attribute name="parentcustomerid"/>
                <attribute name="address1_composite"/>
                <order attribute="fullname" descending="false"/>
                <filter type="or">
                  <condition attribute="firstname" operator="like" value="%{search_escaped}%"/>
                  <condition attribute="lastname" operator="like" value="%{search_escaped}%"/>
                  <condition attribute="emailaddress1" operator="like" value="%{search_escaped}%"/>
                </filter>
              </entity>
            </fetch>"""
            
            encoded_fetch = urllib.parse.quote(fetch_xml)
            endpoint = f"contacts?fetchXml={encoded_fetch}"
            
            response = await self.make_request(
                method="GET",
                endpoint=endpoint
            )
            
            logger.info(f"🔍 Contact search response: {response is not None}, count: {len(response.get('value', [])) if response else 0}")
            
            if response and response.get('value'):
                contacts = []
                for con in response['value']:
                    contacts.append({
                        'contactid': con.get('contactid'),
                        'id': con.get('contactid'),
                        'firstname': con.get('firstname', ''),
                        'lastname': con.get('lastname', ''),
                        'fullname': con.get('fullname', f"{con.get('firstname', '')} {con.get('lastname', '')}".strip()),
                        'name': con.get('fullname', f"{con.get('firstname', '')} {con.get('lastname', '')}".strip()),
                        'email': con.get('emailaddress1', ''),
                        'phone': con.get('telephone1', ''),
                        'jobtitle': con.get('jobtitle', ''),
                        'parent_account_id': con.get('_parentcustomerid_value'),
                        'address': con.get('address1_composite', ''),
                    })
                logger.info(f"✅ Found {len(contacts)} contacts")
                return contacts
            
            logger.info(f"🔍 No contacts found for '{search_term}'")
            return []
            
        except Exception as e:
            logger.error(f"Error searching contacts: {e}")
            return []
    
    async def create_account(self, account_data: Dict) -> Optional[str]:
        """
        Create a new account in Dynamics 365.
        Wrapper for create_or_update_account for new accounts only.
        
        Args:
            account_data: Dictionary with account information
                - name: Company name
                - email: Email address
                - phone: Phone number
                - address: Address
                - vat_number: VAT number
                
        Returns:
            Account ID (GUID) if successful, None otherwise
        """
        try:
            logger.info(f"📊 create_account called with: {account_data}")
            
            # Map incoming data to expected format
            user_data = {
                'company_name': account_data.get('name', ''),
                'email': account_data.get('email', ''),
                'phone': account_data.get('phone', ''),
                'address': account_data.get('address', ''),
                'vat_number': account_data.get('vat_number', ''),
                'is_company': True
            }
            
            logger.info(f"📊 Calling create_or_update_account with: {user_data}")
            result = await self.create_or_update_account(user_data)
            logger.info(f"📊 create_or_update_account returned: {result}")
            return result
            
        except Exception as e:
            logger.error(f"Error creating account: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None
    
    async def create_contact(self, contact_data: Dict) -> Optional[str]:
        """
        Create a new contact in Dynamics 365.
        
        Args:
            contact_data: Dictionary with contact information
                - firstname: First name
                - lastname: Last name
                - email: Email address
                - phone: Phone number
                - address: Address
                - parentcustomerid_account@odata.bind: Link to account (optional)
                
        Returns:
            Contact ID (GUID) if successful, None otherwise
        """
        try:
            # Prepare contact payload
            payload = {
                "firstname": contact_data.get('firstname', contact_data.get('first_name', '')),
                "lastname": contact_data.get('lastname', contact_data.get('last_name', '')),
                "emailaddress1": contact_data.get('email', contact_data.get('emailaddress1', '')),
                "telephone1": contact_data.get('phone', contact_data.get('telephone1', '')),
                "address1_line1": contact_data.get('address', contact_data.get('address1_line1', '')),
            }
            
            # Link to account if provided
            if contact_data.get('parentcustomerid_account@odata.bind'):
                payload['parentcustomerid_account@odata.bind'] = contact_data['parentcustomerid_account@odata.bind']
            elif contact_data.get('account_id'):
                payload['parentcustomerid_account@odata.bind'] = f"/accounts({contact_data['account_id']})"
            
            # Remove empty values
            payload = {k: v for k, v in payload.items() if v}
            
            response = await self.make_request(
                method="POST",
                endpoint="contacts",
                data=payload
            )
            
            if response and 'contactid' in response:
                logger.info(f"✅ Created contact {response['contactid']}")
                return response['contactid']
            
            return None
            
        except Exception as e:
            logger.error(f"Error creating contact: {e}")
            return None
    
    async def get_account(self, account_id: str) -> Optional[Dict]:
        """
        Get a specific account by ID.
        
        Args:
            account_id: The Dynamics 365 account GUID
            
        Returns:
            Account dictionary or None
        """
        try:
            select_fields = "accountid,name,emailaddress1,telephone1,address1_composite,address1_city"
            
            response = await self.make_request(
                method="GET",
                endpoint=f"accounts({account_id})?$select={select_fields}"
            )
            
            if response:
                return {
                    'accountid': response.get('accountid'),
                    'id': response.get('accountid'),
                    'name': response.get('name', ''),
                    'email': response.get('emailaddress1', ''),
                    'phone': response.get('telephone1', ''),
                    'address': response.get('address1_composite', ''),
                    'city': response.get('address1_city', ''),
                    'vat_number': '',  # Not available in this instance
                }
            return None
            
        except Exception as e:
            logger.error(f"Error getting account {account_id}: {e}")
            return None
    
    async def get_contact(self, contact_id: str) -> Optional[Dict]:
        """
        Get a specific contact by ID.
        
        Args:
            contact_id: The Dynamics 365 contact GUID
            
        Returns:
            Contact dictionary or None
        """
        try:
            select_fields = "contactid,firstname,lastname,fullname,emailaddress1,telephone1,jobtitle,_parentcustomerid_value,address1_composite"
            
            response = await self.make_request(
                method="GET",
                endpoint=f"contacts({contact_id})?$select={select_fields}"
            )
            
            if response:
                return {
                    'contactid': response.get('contactid'),
                    'id': response.get('contactid'),
                    'firstname': response.get('firstname', ''),
                    'lastname': response.get('lastname', ''),
                    'fullname': response.get('fullname', ''),
                    'name': response.get('fullname', f"{response.get('firstname', '')} {response.get('lastname', '')}".strip()),
                    'email': response.get('emailaddress1', ''),
                    'phone': response.get('telephone1', ''),
                    'jobtitle': response.get('jobtitle', ''),
                    'parent_account_id': response.get('_parentcustomerid_value'),
                    'address': response.get('address1_composite', ''),
                }
            return None
            
        except Exception as e:
            logger.error(f"Error getting contact {contact_id}: {e}")
            return None
    
    async def get_account_contacts(self, account_id: str, limit: int = 50) -> List[Dict]:
        """
        Get all contacts linked to a specific account.
        
        Args:
            account_id: The Dynamics 365 account GUID
            limit: Maximum number of contacts to return
            
        Returns:
            List of contact dictionaries
        """
        try:
            select_fields = "contactid,firstname,lastname,fullname,emailaddress1,telephone1,jobtitle,address1_composite"
            
            response = await self.make_request(
                method="GET",
                endpoint=f"contacts?$filter=_parentcustomerid_value eq {account_id}&$select={select_fields}&$top={limit}&$orderby=fullname asc"
            )
            
            if response and response.get('value'):
                contacts = []
                for con in response['value']:
                    contact_data = {
                        'contactid': con.get('contactid'),
                        'id': con.get('contactid'),
                        'firstname': con.get('firstname', ''),
                        'lastname': con.get('lastname', ''),
                        'fullname': con.get('fullname', ''),
                        'name': con.get('fullname', f"{con.get('firstname', '')} {con.get('lastname', '')}".strip()),
                        'email': con.get('emailaddress1', ''),
                        'phone': con.get('telephone1', ''),
                        'jobtitle': con.get('jobtitle', ''),
                        'address': con.get('address1_composite', ''),
                    }
                    logger.info(f"📋 Contact data: name={contact_data['fullname']}, email={contact_data['email']}, phone={contact_data['phone']}, address={contact_data['address'][:50] if contact_data['address'] else 'NONE'}")
                    contacts.append(contact_data)
                return contacts
            return []
            
        except Exception as e:
            logger.error(f"Error getting contacts for account {account_id}: {e}")
            return []
    
    async def search_contacts_by_email(self, email: str) -> List[Dict]:
        """
        Search for contacts by exact email match.
        
        Args:
            email: Email address to search for
            
        Returns:
            List of matching contacts
        """
        try:
            if not email:
                return []
            
            encoded_filter = urllib.parse.quote(f"emailaddress1 eq '{email}'")
            select_fields = "contactid,firstname,lastname,fullname,emailaddress1,telephone1"
            
            response = await self.make_request(
                method="GET",
                endpoint=f"contacts?$filter={encoded_filter}&$select={select_fields}"
            )
            
            if response and response.get('value'):
                return response['value']
            return []
            
        except Exception as e:
            logger.error(f"Error searching contacts by email: {e}")
            return []
    
    async def periodic_sync_task(self):
        """
        Background task that runs periodically to sync both ways.
        This method is called by the bot to start periodic synchronization.
        """
        import asyncio
        
        while True:
            try:
                logger.info("🔄 Running periodic Dynamics 365 sync...")
                
                # Run pending entity sync
                await self.sync_pending_entities()
                
                await asyncio.sleep(300)  # Wait 5 minutes between syncs
                
            except asyncio.CancelledError:
                logger.info("Periodic sync task cancelled")
                break
            except Exception as e:
                logger.error(f"Error in periodic sync task: {e}")
                await asyncio.sleep(60)  # Wait 1 minute on error
    
    async def sync_pending_entities(self):
        """
        Sync any pending entities to Dynamics 365.
        This is called by the periodic sync task and on startup.
        """
        try:
            logger.info("🔄 Checking for pending entities to sync...")
            # This is a placeholder - actual implementation would:
            # 1. Query local DB for entities marked as pending sync
            # 2. Push them to Dynamics 365
            # 3. Update local records with Dynamics IDs
            # For now, just log that we checked
            logger.info("✅ Pending entity sync check complete")
        except Exception as e:
            logger.error(f"Error syncing pending entities: {e}")