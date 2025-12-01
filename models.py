"""
Models for Enhanced Stretch Ceiling Bot
Version 8.8 - Complete implementation with Customer Selection states
"""
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional, Dict, Any
from enum import Enum
import json


# ==================== ENUMS ====================

class ConversationState(Enum):
    """Conversation states for quote flow"""
    # Initial states
    CLIENT_GROUP = "client_group"
    CEILING_COUNT = "ceiling_count"
    
    # ==================== CUSTOMER SELECTION STATES (NEW) ====================
    CUSTOMER_TYPE = "customer_type"                    # New or existing customer
    # New customer flow
    NEW_CUSTOMER_COMPANY = "new_customer_company"      # Company name (optional)
    NEW_CUSTOMER_VAT = "new_customer_vat"              # VAT number
    NEW_CUSTOMER_CONTACT = "new_customer_contact"      # Contact name
    NEW_CUSTOMER_ADDRESS = "new_customer_address"      # Address
    NEW_CUSTOMER_PHONE = "new_customer_phone"          # Phone
    NEW_CUSTOMER_EMAIL = "new_customer_email"          # Email
    NEW_CUSTOMER_LEAD_SOURCE = "new_customer_lead_source"  # Lead source
    NEW_CUSTOMER_CONFIRM = "new_customer_confirm"      # Confirm new customer
    # Existing customer flow
    EXISTING_CUSTOMER_SEARCH = "existing_customer_search"  # Search by name
    EXISTING_CUSTOMER_SELECT = "existing_customer_select"  # Select from results
    EXISTING_CONTACT_SELECT = "existing_contact_select"    # Select contact
    NEW_CONTACT_NAME = "new_contact_name"              # New contact under account
    NEW_CONTACT_EMAIL = "new_contact_email"            # New contact email
    NEW_CONTACT_PHONE = "new_contact_phone"            # New contact phone
    # Email selection for quote delivery
    EMAIL_SELECTION = "email_selection"                # Choose email for quote
    CUSTOM_EMAIL_INPUT = "custom_email_input"          # Enter custom email
    # ===========================================================================
    
    # Ceiling configuration states
    CEILING_NAME = "ceiling_name"
    CEILING_SIZE = "ceiling_size"
    SIZE_CONFIRMATION = "size_confirmation"
    PERIMETER_EDIT = "perimeter_edit"  # For manual perimeter editing
    CORNERS_COUNT = "corners_count"
    
    # Ceiling type states (3-step configuration)
    CEILING_TYPE = "ceiling_type"      # Step 1: fabric/pvc
    TYPE_CEILING = "type_ceiling"      # Step 2: standard/acoustic/etc
    CEILING_COLOR = "ceiling_color"    # Step 3: color selection
    CEILING_FINISH = "ceiling_finish"  # Optional finish
    
    # Acoustic states
    CEILING_ACOUSTIC = "ceiling_acoustic"
    ACOUSTIC_PERFORMANCE = "acoustic_performance"
    
    # Installation components
    PERIMETER_PROFILE = "perimeter_profile"
    SEAM_QUESTION = "seam_question"
    SEAM_LENGTH = "seam_length"
    
    # Lighting states
    LIGHTS_QUESTION = "lights_question"
    LIGHT_SELECTION = "light_selection"
    LIGHT_QUANTITY = "light_quantity"
    MORE_LIGHTS = "more_lights"
    
    # Wood structure states
    WOOD_QUESTION = "wood_question"
    WOOD_SELECTION = "wood_selection"
    WOOD_QUANTITY = "wood_quantity"
    MORE_WOOD = "more_wood"
    
    # Completion states
    NEXT_CEILING = "next_ceiling"
    QUOTE_REFERENCE = "quote_reference"
    EMAIL_REQUEST = "email_request"
    EMAIL_INPUT = "email_input"
    
    # Quote editing states
    EDIT_SELECTION = "edit_selection"
    EDIT_VALUE = "edit_value"
    
    # Admin states
    ADMIN_MESSAGE_TYPE = "admin_message_type"
    ADMIN_USER_SELECTION = "admin_user_selection"
    ADMIN_MESSAGE_INPUT = "admin_message_input"
    ADMIN_CONFIRMATION = "admin_confirmation"


class QuoteStatus(Enum):
    """Quote status options"""
    DRAFT = "draft"
    SENT = "sent"
    VIEWED = "viewed"
    ACCEPTED = "accepted"
    REJECTED = "rejected"
    EXPIRED = "expired"
    REVISED = "revised"


class UserRole(Enum):
    """User roles"""
    CUSTOMER = "customer"
    INSTALLER = "installer"
    DEALER = "dealer"
    ADMIN = "admin"


class ProductCategory(Enum):
    """Product categories"""
    CEILING = "ceiling"
    PERIMETER = "perimeter"
    PERIMETER_STRUCTURE = "perimeter_structure"
    CORNER = "corner"
    SEAM = "seam"
    LIGHT = "light"
    WOOD_STRUCTURE = "wood_structure"
    ACOUSTIC_ABSORBER = "acoustic_absorber"
    ACCESSORY = "accessory"


class CeilingType(Enum):
    """Main ceiling types"""
    FABRIC = "fabric"
    PVC = "pvc"
    
    
class ClientGroup(Enum):
    """Client pricing groups"""
    B2C = "price_b2c"
    B2B_RESELLER = "price_b2b_reseller"
    B2B_HOSPITALITY = "price_b2b_hospitality"


class CustomerType(Enum):
    """Customer types for quote creation"""
    NEW = "new"
    EXISTING = "existing"


class LeadSource(Enum):
    """Lead source options"""
    WEBSITE = "Website"
    REFERRAL = "Referral"
    TRADE_SHOW = "Trade Show"
    SOCIAL_MEDIA = "Social Media"
    COLD_CALL = "Cold Call"
    PARTNER = "Partner"
    ADVERTISEMENT = "Advertisement"
    OTHER = "Other"


# ==================== DATA CLASSES ====================

@dataclass
class Product:
    """Product data model"""
    id: int
    product_code: str
    description: str
    base_category: str
    product_type: Optional[str] = None
    type_ceiling: Optional[str] = None
    color: Optional[str] = None
    finish: Optional[str] = None
    acoustic_performance: Optional[str] = None
    price_b2c: float = 0.0
    price_b2b_reseller: float = 0.0
    price_b2b_hospitality: float = 0.0
    unit: str = "pcs"
    is_active: bool = True
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    
    def get_price(self, client_group: str) -> float:
        """Get price for specific client group"""
        price_map = {
            "price_b2c": self.price_b2c,
            "price_b2b_reseller": self.price_b2b_reseller,
            "price_b2b_hospitality": self.price_b2b_hospitality
        }
        return price_map.get(client_group, self.price_b2c)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "id": self.id,
            "product_code": self.product_code,
            "description": self.description,
            "base_category": self.base_category,
            "product_type": self.product_type,
            "type_ceiling": self.type_ceiling,
            "color": self.color,
            "finish": self.finish,
            "acoustic_performance": self.acoustic_performance,
            "price_b2c": self.price_b2c,
            "price_b2b_reseller": self.price_b2b_reseller,
            "price_b2b_hospitality": self.price_b2b_hospitality,
            "unit": self.unit,
            "is_active": self.is_active
        }


@dataclass
class LightItem:
    """Light item in a ceiling configuration"""
    product_id: int
    code: str
    description: str
    quantity: int
    price: float = 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "product_id": self.product_id,
            "code": self.code,
            "description": self.description,
            "quantity": self.quantity,
            "price": self.price
        }


@dataclass
class WoodItem:
    """Wood structure item in a ceiling configuration"""
    product_id: int
    code: str
    description: str
    quantity: float  # in meters
    price: float = 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "product_id": self.product_id,
            "code": self.code,
            "description": self.description,
            "quantity": self.quantity,
            "price": self.price
        }


@dataclass
class CustomerData:
    """Customer data for quote creation"""
    type: str = "new"  # "new" or "existing"
    dynamics_account_id: Optional[str] = None
    dynamics_contact_id: Optional[str] = None
    display_name: str = ""
    contact_name: str = ""
    email: str = ""
    
    # Detailed data
    is_company: bool = False
    company_name: Optional[str] = None
    vat_number: Optional[str] = None
    first_name: str = ""
    last_name: str = ""
    address: str = ""
    phone: str = ""
    lead_source: str = ""
    
    # Full Dynamics objects
    account_data: Optional[Dict] = None
    contact_data: Optional[Dict] = None
    
    # Sync status
    sync_pending: bool = False
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return {
            "type": self.type,
            "dynamics_account_id": self.dynamics_account_id,
            "dynamics_contact_id": self.dynamics_contact_id,
            "display_name": self.display_name,
            "contact_name": self.contact_name,
            "email": self.email,
            "data": {
                "is_company": self.is_company,
                "company_name": self.company_name,
                "vat_number": self.vat_number,
                "first_name": self.first_name,
                "last_name": self.last_name,
                "address": self.address,
                "phone": self.phone,
                "lead_source": self.lead_source
            },
            "account_data": self.account_data,
            "contact_data": self.contact_data,
            "sync_pending": self.sync_pending
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'CustomerData':
        """Create CustomerData from dictionary"""
        inner_data = data.get("data", {})
        return cls(
            type=data.get("type", "new"),
            dynamics_account_id=data.get("dynamics_account_id"),
            dynamics_contact_id=data.get("dynamics_contact_id"),
            display_name=data.get("display_name", ""),
            contact_name=data.get("contact_name", ""),
            email=data.get("email", ""),
            is_company=inner_data.get("is_company", False),
            company_name=inner_data.get("company_name"),
            vat_number=inner_data.get("vat_number"),
            first_name=inner_data.get("first_name", ""),
            last_name=inner_data.get("last_name", ""),
            address=inner_data.get("address", ""),
            phone=inner_data.get("phone", ""),
            lead_source=inner_data.get("lead_source", ""),
            account_data=data.get("account_data"),
            contact_data=data.get("contact_data"),
            sync_pending=data.get("sync_pending", False)
        )


@dataclass
class CeilingConfig:
    """Complete ceiling configuration"""
    name: str
    length: float
    width: float
    area: float = 0.0
    perimeter: float = 0.0
    perimeter_edited: bool = False  # Track if perimeter was manually edited
    corners: int = 4
    
    # Ceiling specifications
    ceiling_type: str = ""  # fabric/pvc
    type_ceiling: str = ""  # standard/acoustic/etc
    color: str = "white"
    finish: str = "Mat"
    acoustic: bool = False
    acoustic_performance: Optional[str] = None
    
    # Components
    perimeter_profile: Optional[Dict] = None
    acoustic_product: Optional[Dict] = None
    
    # Installation details
    has_seams: bool = False
    seam_length: float = 0.0
    
    # Accessories
    lights: List[Dict] = field(default_factory=list)
    wood_structures: List[Dict] = field(default_factory=list)
    
    def __post_init__(self):
        """Calculate dimensions after initialization"""
        if self.area == 0 and self.length > 0 and self.width > 0:
            self.calculate_dimensions()
    
    def calculate_dimensions(self):
        """Calculate area and perimeter"""
        self.area = self.length * self.width
        if not self.perimeter_edited:  # Only calculate if not manually edited
            self.perimeter = 2 * (self.length + self.width)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return {
            "name": self.name,
            "length": self.length,
            "width": self.width,
            "area": self.area,
            "perimeter": self.perimeter,
            "perimeter_edited": self.perimeter_edited,
            "corners": self.corners,
            "ceiling_type": self.ceiling_type,
            "type_ceiling": self.type_ceiling,
            "color": self.color,
            "finish": self.finish,
            "acoustic": self.acoustic,
            "acoustic_performance": self.acoustic_performance,
            "perimeter_profile": self.perimeter_profile,
            "acoustic_product": self.acoustic_product,
            "has_seams": self.has_seams,
            "seam_length": self.seam_length,
            "lights": self.lights,
            "wood_structures": self.wood_structures
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'CeilingConfig':
        """Create CeilingConfig from dictionary"""
        return cls(
            name=data.get("name", "Unnamed"),
            length=data.get("length", 0.0),
            width=data.get("width", 0.0),
            area=data.get("area", 0.0),
            perimeter=data.get("perimeter", 0.0),
            perimeter_edited=data.get("perimeter_edited", False),
            corners=data.get("corners", 4),
            ceiling_type=data.get("ceiling_type", ""),
            type_ceiling=data.get("type_ceiling", ""),
            color=data.get("color", "white"),
            finish=data.get("finish", "Mat"),
            acoustic=data.get("acoustic", False),
            acoustic_performance=data.get("acoustic_performance"),
            perimeter_profile=data.get("perimeter_profile"),
            acoustic_product=data.get("acoustic_product"),
            has_seams=data.get("has_seams", False),
            seam_length=data.get("seam_length", 0.0),
            lights=data.get("lights", []),
            wood_structures=data.get("wood_structures", [])
        )


@dataclass
class CeilingCost:
    """Cost breakdown for a ceiling"""
    ceiling_cost: float = 0.0
    perimeter_structure_cost: float = 0.0
    perimeter_profile_cost: float = 0.0
    corners_cost: float = 0.0
    seam_cost: float = 0.0
    lights_cost: float = 0.0
    wood_structures_cost: float = 0.0
    acoustic_absorber_cost: float = 0.0
    
    # Detailed breakdown
    ceiling_product: Optional[Dict] = None
    perimeter_structure: Optional[Dict] = None
    perimeter_profile: Optional[Dict] = None
    corner_product: Optional[Dict] = None
    seam_product: Optional[Dict] = None
    acoustic_absorber: Optional[Dict] = None
    
    @property
    def total(self) -> float:
        """Calculate total cost"""
        return (
            self.ceiling_cost +
            self.perimeter_structure_cost +
            self.perimeter_profile_cost +
            self.corners_cost +
            self.seam_cost +
            self.lights_cost +
            self.wood_structures_cost +
            self.acoustic_absorber_cost
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "ceiling_cost": self.ceiling_cost,
            "perimeter_structure_cost": self.perimeter_structure_cost,
            "perimeter_profile_cost": self.perimeter_profile_cost,
            "corners_cost": self.corners_cost,
            "seam_cost": self.seam_cost,
            "lights_cost": self.lights_cost,
            "wood_structures_cost": self.wood_structures_cost,
            "acoustic_absorber_cost": self.acoustic_absorber_cost,
            "total": self.total,
            "ceiling_product": self.ceiling_product,
            "perimeter_structure": self.perimeter_structure,
            "perimeter_profile": self.perimeter_profile,
            "corner_product": self.corner_product,
            "seam_product": self.seam_product,
            "acoustic_absorber": self.acoustic_absorber
        }


@dataclass
class User:
    """User data model"""
    user_id: int
    username: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[str] = None
    company_name: Optional[str] = None
    is_company: bool = False
    client_group: str = "price_b2c"
    is_active: bool = True
    is_blocked: bool = False
    onboarding_completed: bool = False
    created_at: Optional[datetime] = None
    last_activity: Optional[datetime] = None
    
    # Dynamics 365 integration
    dynamics_contact_id: Optional[str] = None
    dynamics_account_id: Optional[str] = None
    dynamics_sync_status: str = "pending"
    
    @property
    def full_name(self) -> str:
        """Get user's full name"""
        parts = []
        if self.first_name:
            parts.append(self.first_name)
        if self.last_name:
            parts.append(self.last_name)
        return " ".join(parts) if parts else f"User {self.user_id}"
    
    @property
    def display_name(self) -> str:
        """Get display name (company or personal name)"""
        if self.is_company and self.company_name:
            return self.company_name
        return self.full_name
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "user_id": self.user_id,
            "username": self.username,
            "first_name": self.first_name,
            "last_name": self.last_name,
            "full_name": self.full_name,
            "phone": self.phone,
            "email": self.email,
            "company_name": self.company_name,
            "is_company": self.is_company,
            "client_group": self.client_group,
            "is_active": self.is_active,
            "is_blocked": self.is_blocked,
            "onboarding_completed": self.onboarding_completed,
            "dynamics_contact_id": self.dynamics_contact_id,
            "dynamics_account_id": self.dynamics_account_id,
            "dynamics_sync_status": self.dynamics_sync_status,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "last_activity": self.last_activity.isoformat() if self.last_activity else None
        }


@dataclass
class Quote:
    """Quote data model"""
    quote_id: int
    user_id: int
    quote_number: str
    quote_data: Dict[str, Any]
    total_price: float
    client_group: str
    status: str = "draft"
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    expires_at: Optional[datetime] = None
    
    # Customer data (NEW)
    customer: Optional[CustomerData] = None
    
    # Dynamics 365 integration
    dynamics_quote_id: Optional[str] = None
    dynamics_sync_status: str = "pending"
    
    @property
    def is_expired(self) -> bool:
        """Check if quote is expired"""
        if self.expires_at:
            return datetime.now() > self.expires_at
        return False
    
    @property
    def ceiling_count(self) -> int:
        """Get number of ceilings in quote"""
        return len(self.quote_data.get("ceilings", []))
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "quote_id": self.quote_id,
            "user_id": self.user_id,
            "quote_number": self.quote_number,
            "quote_data": self.quote_data,
            "total_price": self.total_price,
            "client_group": self.client_group,
            "status": self.status,
            "ceiling_count": self.ceiling_count,
            "is_expired": self.is_expired,
            "customer": self.customer.to_dict() if self.customer else None,
            "dynamics_quote_id": self.dynamics_quote_id,
            "dynamics_sync_status": self.dynamics_sync_status,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "expires_at": self.expires_at.isoformat() if self.expires_at else None
        }


@dataclass
class QuoteSession:
    """Active quote creation session"""
    user_id: int
    session_data: Dict[str, Any]
    current_step: str
    edit_history: List[Dict] = field(default_factory=list)
    previous_steps: List[Dict] = field(default_factory=list)
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    expires_at: Optional[datetime] = None
    
    # Customer data (NEW)
    customer: Optional[CustomerData] = None
    
    @property
    def is_expired(self) -> bool:
        """Check if session is expired"""
        if self.expires_at:
            return datetime.now() > self.expires_at
        return False
    
    @property
    def current_ceiling_index(self) -> int:
        """Get current ceiling index"""
        return self.session_data.get("current_ceiling_index", 0)
    
    @property
    def ceiling_count(self) -> int:
        """Get total ceiling count"""
        return self.session_data.get("ceiling_count", 0)
    
    def add_edit_history(self, field: str, old_value: Any, new_value: Any):
        """Add an edit to history"""
        self.edit_history.append({
            "timestamp": datetime.now().isoformat(),
            "field": field,
            "old_value": old_value,
            "new_value": new_value
        })
    
    def add_previous_step(self, state: str, data: Dict = None):
        """Add a step to previous steps for back navigation"""
        step = {
            "state": state,
            "timestamp": datetime.now().isoformat()
        }
        if data:
            step["data"] = data
        
        self.previous_steps.append(step)
        
        # Limit to last 10 steps
        if len(self.previous_steps) > 10:
            self.previous_steps = self.previous_steps[-10:]


@dataclass
class ConversationLog:
    """Conversation log entry"""
    log_id: int
    user_id: int
    message_type: str  # 'user' or 'bot'
    message: str
    context: Optional[Dict] = None
    created_at: Optional[datetime] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "log_id": self.log_id,
            "user_id": self.user_id,
            "message_type": self.message_type,
            "message": self.message,
            "context": self.context,
            "created_at": self.created_at.isoformat() if self.created_at else None
        }


@dataclass
class AdminMessage:
    """Admin message data model"""
    id: int
    message_id: str
    admin_id: int
    recipient_id: Optional[int] = None
    message_type: str = "individual"  # individual, broadcast, group
    message_text: str = ""
    status: str = "sent"
    created_at: Optional[datetime] = None
    delivered_at: Optional[datetime] = None
    read_at: Optional[datetime] = None
    
    @property
    def is_delivered(self) -> bool:
        return self.delivered_at is not None
    
    @property
    def is_read(self) -> bool:
        return self.read_at is not None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "id": self.id,
            "message_id": self.message_id,
            "admin_id": self.admin_id,
            "recipient_id": self.recipient_id,
            "message_type": self.message_type,
            "message_text": self.message_text,
            "status": self.status,
            "is_delivered": self.is_delivered,
            "is_read": self.is_read,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "delivered_at": self.delivered_at.isoformat() if self.delivered_at else None,
            "read_at": self.read_at.isoformat() if self.read_at else None
        }


@dataclass
class SystemStatistics:
    """System statistics model"""
    total_users: int = 0
    active_users_week: int = 0
    active_users_month: int = 0
    total_quotes: int = 0
    quotes_week: int = 0
    accepted_quotes: int = 0
    messages_24h: int = 0
    total_revenue: float = 0.0
    active_sessions: int = 0
    product_categories: int = 0
    total_products: int = 0
    
    @property
    def acceptance_rate(self) -> float:
        """Calculate quote acceptance rate"""
        if self.total_quotes > 0:
            return (self.accepted_quotes / self.total_quotes) * 100
        return 0.0
    
    @property
    def average_quote_value(self) -> float:
        """Calculate average quote value"""
        if self.accepted_quotes > 0:
            return self.total_revenue / self.accepted_quotes
        return 0.0
    
    @property
    def daily_active_users(self) -> float:
        """Calculate average daily active users"""
        if self.active_users_week > 0:
            return self.active_users_week / 7
        return 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "total_users": self.total_users,
            "active_users_week": self.active_users_week,
            "active_users_month": self.active_users_month,
            "total_quotes": self.total_quotes,
            "quotes_week": self.quotes_week,
            "accepted_quotes": self.accepted_quotes,
            "acceptance_rate": round(self.acceptance_rate, 1),
            "messages_24h": self.messages_24h,
            "total_revenue": self.total_revenue,
            "average_quote_value": round(self.average_quote_value, 2),
            "active_sessions": self.active_sessions,
            "product_categories": self.product_categories,
            "total_products": self.total_products,
            "daily_active_users": round(self.daily_active_users, 1)
        }


# ==================== HELPER FUNCTIONS ====================

def create_empty_ceiling_config(name: str = "New Ceiling") -> CeilingConfig:
    """Create an empty ceiling configuration"""
    return CeilingConfig(
        name=name,
        length=0.0,
        width=0.0,
        area=0.0,
        perimeter=0.0,
        perimeter_edited=False,
        corners=4,
        ceiling_type="",
        type_ceiling="",
        color="white",
        finish="Mat",
        acoustic=False,
        lights=[],
        wood_structures=[]
    )


def create_empty_customer_data() -> CustomerData:
    """Create an empty customer data object"""
    return CustomerData(
        type="new",
        display_name="",
        contact_name="",
        email="",
        is_company=False,
        first_name="",
        last_name="",
        address="",
        phone="",
        lead_source=""
    )


def validate_quote_data(quote_data: Dict[str, Any]) -> bool:
    """Validate quote data structure"""
    required_fields = ["user_id", "ceilings", "ceiling_costs", "quote_reference"]
    
    # Check required fields
    for field in required_fields:
        if field not in quote_data:
            return False
    
    # Check ceilings structure
    if not isinstance(quote_data.get("ceilings"), list):
        return False
    
    # Check each ceiling has required fields
    for ceiling in quote_data.get("ceilings", []):
        ceiling_required = ["name", "length", "width", "area", "perimeter", "corners"]
        for field in ceiling_required:
            if field not in ceiling:
                return False
    
    return True


def calculate_quote_totals(quote_data: Dict[str, Any]) -> Dict[str, float]:
    """Calculate quote totals from quote data"""
    totals = {
        "ceiling_total": 0.0,
        "accessories_total": 0.0,
        "grand_total": 0.0
    }
    
    # Sum up ceiling costs
    for costs in quote_data.get("ceiling_costs", []):
        if isinstance(costs, dict):
            totals["ceiling_total"] += costs.get("total", 0.0)
            totals["accessories_total"] += (
                costs.get("lights_cost", 0.0) + 
                costs.get("wood_structures_cost", 0.0)
            )
    
    totals["grand_total"] = totals["ceiling_total"]
    
    return totals


def format_quote_summary(quote_data: Dict[str, Any]) -> str:
    """Format a text summary of the quote"""
    summary_lines = []
    
    # Header
    summary_lines.append(f"Quote Reference: {quote_data.get('quote_reference', 'N/A')}")
    summary_lines.append(f"Date: {quote_data.get('created_at', datetime.now().strftime('%Y-%m-%d'))}")
    summary_lines.append("")
    
    # Customer info (NEW)
    customer = quote_data.get("customer")
    if customer:
        summary_lines.append("Customer Information:")
        if isinstance(customer, dict):
            summary_lines.append(f"  - Name: {customer.get('display_name', 'N/A')}")
            summary_lines.append(f"  - Contact: {customer.get('contact_name', 'N/A')}")
            summary_lines.append(f"  - Email: {customer.get('email', 'N/A')}")
        summary_lines.append("")
    
    # Ceilings
    for i, ceiling in enumerate(quote_data.get("ceilings", [])):
        summary_lines.append(f"Ceiling {i+1}: {ceiling.get('name', 'Unnamed')}")
        summary_lines.append(f"  - Dimensions: {ceiling.get('length', 0)}m x {ceiling.get('width', 0)}m")
        summary_lines.append(f"  - Area: {ceiling.get('area', 0):.2f} m²")
        summary_lines.append(f"  - Type: {ceiling.get('ceiling_type', 'N/A').upper()}")
        summary_lines.append("")
    
    # Totals
    totals = calculate_quote_totals(quote_data)
    summary_lines.append(f"Total: €{totals['grand_total']:.2f}")
    
    return "\n".join(summary_lines)


def is_customer_state(state: ConversationState) -> bool:
    """Check if a state is a customer selection state"""
    customer_states = [
        ConversationState.CUSTOMER_TYPE,
        ConversationState.NEW_CUSTOMER_COMPANY,
        ConversationState.NEW_CUSTOMER_VAT,
        ConversationState.NEW_CUSTOMER_CONTACT,
        ConversationState.NEW_CUSTOMER_ADDRESS,
        ConversationState.NEW_CUSTOMER_PHONE,
        ConversationState.NEW_CUSTOMER_EMAIL,
        ConversationState.NEW_CUSTOMER_LEAD_SOURCE,
        ConversationState.NEW_CUSTOMER_CONFIRM,
        ConversationState.EXISTING_CUSTOMER_SEARCH,
        ConversationState.EXISTING_CUSTOMER_SELECT,
        ConversationState.EXISTING_CONTACT_SELECT,
        ConversationState.NEW_CONTACT_NAME,
        ConversationState.NEW_CONTACT_EMAIL,
        ConversationState.NEW_CONTACT_PHONE,
        ConversationState.EMAIL_SELECTION,
        ConversationState.CUSTOM_EMAIL_INPUT,
    ]
    return state in customer_states