"""
AcousticPro API Server
Bridges Flutter app with existing Telegram bot, MySQL, and Dynamics 365
"""
import os
import logging
from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
import pymysql
from pymysql.cursors import DictCursor
import json
import uuid
import asyncio
from enum import Enum

# Import your existing modules
from database import EnhancedDatabaseManager
from dynamics365_service import Dynamics365Service
from config import Config
import firebase_admin
from firebase_admin import credentials, messaging
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter, A4
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
import base64
from io import BytesIO

# Initialize FastAPI
app = FastAPI(title="AcousticPro API", version="1.0.0")

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure this for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Firebase for push notifications
cred = credentials.Certificate("path/to/serviceAccountKey.json")
firebase_admin.initialize_app(cred)

# Initialize existing services
db = EnhancedDatabaseManager()
dynamics_service = None
if Config.ENABLE_DYNAMICS_SYNC:
    dynamics_service = Dynamics365Service(
        tenant_id=Config.DYNAMICS_TENANT_ID,
        client_id=Config.DYNAMICS_CLIENT_ID,
        client_secret=Config.DYNAMICS_CLIENT_SECRET,
        dynamics_url=Config.DYNAMICS_URL,
        db_manager=db
    )

# Pydantic Models
class UserLogin(BaseModel):
    email: str
    telegram_id: Optional[int] = None
    device_token: Optional[str] = None

class UserProfile(BaseModel):
    telegram_id: Optional[int] = None
    email: str
    first_name: str
    last_name: str
    phone: Optional[str] = None
    company_name: Optional[str] = None
    is_company: bool = False
    vat_number: Optional[str] = None
    address: Optional[str] = None
    device_tokens: List[str] = []

class MeasurementType(str, Enum):
    RT60 = "rt60"
    RT30 = "rt30"
    FREQUENCY_RESPONSE = "frequency_response"
    CLARITY = "clarity"

class AcousticMeasurement(BaseModel):
    room_name: str
    room_type: str
    measurement_type: MeasurementType
    rt60_value: Optional[float] = None
    rt30_value: Optional[float] = None
    clarity_value: Optional[float] = None
    frequency_data: Optional[Dict[str, float]] = None
    temperature: Optional[float] = None
    humidity: Optional[float] = None
    notes: Optional[str] = None
    audio_file_base64: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)

class QuoteRequest(BaseModel):
    user_id: int
    room_measurements: List[int]  # List of measurement IDs
    additional_notes: Optional[str] = None

class NotificationRequest(BaseModel):
    user_id: int
    title: str
    body: str
    data: Optional[Dict[str, str]] = None

# Database schema updates
def create_measurements_tables():
    """Create tables for acoustic measurements"""
    try:
        connection = pymysql.connect(**db.config, cursorclass=DictCursor)
        cursor = connection.cursor()
        
        # Acoustic measurements table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS acoustic_measurements (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id INT NOT NULL,
                room_name VARCHAR(255),
                room_type VARCHAR(100),
                measurement_type VARCHAR(50),
                rt60_value DECIMAL(10, 3),
                rt30_value DECIMAL(10, 3),
                clarity_value DECIMAL(10, 3),
                frequency_data JSON,
                temperature DECIMAL(5, 2),
                humidity DECIMAL(5, 2),
                notes TEXT,
                audio_file_path VARCHAR(500),
                metadata JSON,
                dynamics_measurement_id VARCHAR(36),
                dynamics_sync_status ENUM('pending', 'synced', 'error') DEFAULT 'pending',
                dynamics_sync_error TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
                INDEX idx_user_measurements (user_id, created_at),
                INDEX idx_dynamics_sync (dynamics_sync_status)
            )
        """)
        
        # Measurement-Quote relationship table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS measurement_quotes (
                id INT AUTO_INCREMENT PRIMARY KEY,
                measurement_id INT NOT NULL,
                quote_id INT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (measurement_id) REFERENCES acoustic_measurements(id) ON DELETE CASCADE,
                FOREIGN KEY (quote_id) REFERENCES quotations(id) ON DELETE CASCADE,
                UNIQUE KEY unique_measurement_quote (measurement_id, quote_id)
            )
        """)
        
        # Push notification tokens
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS user_device_tokens (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id INT NOT NULL,
                device_token VARCHAR(500) NOT NULL,
                platform ENUM('ios', 'android', 'web') DEFAULT 'android',
                is_active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_used TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
                UNIQUE KEY unique_user_token (user_id, device_token)
            )
        """)
        
        # Offline queue table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS offline_queue (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id INT NOT NULL,
                action_type VARCHAR(50) NOT NULL,
                payload JSON NOT NULL,
                status ENUM('pending', 'processing', 'completed', 'failed') DEFAULT 'pending',
                retry_count INT DEFAULT 0,
                error_message TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                processed_at TIMESTAMP NULL,
                INDEX idx_status (status, created_at)
            )
        """)
        
        # Analytics table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS measurement_analytics (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id INT NOT NULL,
                date DATE NOT NULL,
                measurement_count INT DEFAULT 0,
                avg_rt60 DECIMAL(10, 3),
                total_area DECIMAL(10, 2),
                room_types JSON,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY unique_user_date (user_id, date),
                INDEX idx_date (date)
            )
        """)
        
        connection.commit()
        cursor.close()
        connection.close()
        logger.info("✅ Measurement tables created successfully")
        
    except Exception as e:
        logger.error(f"❌ Error creating measurement tables: {e}")
        raise

# Initialize tables on startup
@app.on_event("startup")
async def startup_event():
    create_measurements_tables()
    logger.info("🚀 AcousticPro API Server started")

# Authentication dependency
async def get_current_user(email: str) -> Dict:
    """Get user from database by email"""
    user = db.execute_query(
        """
        SELECT u.*, up.* 
        FROM users u
        LEFT JOIN user_profiles up ON u.user_id = up.user_id
        WHERE up.email = %s
        """,
        (email,),
        fetch=True
    )
    
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    return user[0]

# API Endpoints

@app.post("/api/auth/login")
async def login(login_data: UserLogin):
    """Login or register user"""
    try:
        # Check if user exists
        existing_user = db.execute_query(
            """
            SELECT u.*, up.* 
            FROM users u
            LEFT JOIN user_profiles up ON u.user_id = up.user_id
            WHERE up.email = %s OR u.user_id = %s
            """,
            (login_data.email, login_data.telegram_id),
            fetch=True
        )
        
        if existing_user:
            user = existing_user[0]
            user_id = user['user_id']
            
            # Update device token if provided
            if login_data.device_token:
                db.execute_query(
                    """
                    INSERT INTO user_device_tokens (user_id, device_token)
                    VALUES (%s, %s)
                    ON DUPLICATE KEY UPDATE last_used = NOW(), is_active = TRUE
                    """,
                    (user_id, login_data.device_token)
                )
            
            return {
                "user_id": user_id,
                "profile": {
                    "email": user.get('email'),
                    "first_name": user.get('first_name'),
                    "last_name": user.get('last_name'),
                    "company_name": user.get('company_name'),
                    "is_company": bool(user.get('is_company')),
                    "telegram_id": user.get('user_id')
                },
                "token": f"Bearer {user_id}-{uuid.uuid4()}"  # Simple token for demo
            }
        else:
            # Create new user
            user_id = db.execute_query(
                """
                INSERT INTO users (username, telegram_id, first_activity, last_activity)
                VALUES (%s, %s, NOW(), NOW())
                """,
                (login_data.email, login_data.telegram_id or 0),
                fetch=False
            )
            
            # Create user profile
            db.execute_query(
                """
                INSERT INTO user_profiles (user_id, email, onboarding_completed)
                VALUES (%s, %s, FALSE)
                """,
                (user_id, login_data.email)
            )
            
            # Add device token
            if login_data.device_token:
                db.execute_query(
                    """
                    INSERT INTO user_device_tokens (user_id, device_token)
                    VALUES (%s, %s)
                    """,
                    (user_id, login_data.device_token)
                )
            
            return {
                "user_id": user_id,
                "profile": {
                    "email": login_data.email,
                    "telegram_id": login_data.telegram_id
                },
                "token": f"Bearer {user_id}-{uuid.uuid4()}",
                "is_new_user": True
            }
            
    except Exception as e:
        logger.error(f"Login error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/users/{user_id}/profile")
async def get_user_profile(user_id: int):
    """Get user profile"""
    profile = db.get_user_profile(user_id)
    if not profile:
        raise HTTPException(status_code=404, detail="User not found")
    
    # Get device tokens
    tokens = db.execute_query(
        "SELECT device_token FROM user_device_tokens WHERE user_id = %s AND is_active = TRUE",
        (user_id,),
        fetch=True
    )
    
    profile['device_tokens'] = [t['device_token'] for t in tokens] if tokens else []
    
    return profile

@app.put("/api/users/{user_id}/profile")
async def update_user_profile(user_id: int, profile: UserProfile):
    """Update user profile"""
    try:
        profile_data = profile.dict()
        profile_data['user_id'] = user_id
        profile_data['onboarding_completed'] = True
        
        # Remove device_tokens from profile data
        device_tokens = profile_data.pop('device_tokens', [])
        
        # Save profile
        db.save_user_profile(profile_data)
        
        # Update device tokens
        for token in device_tokens:
            db.execute_query(
                """
                INSERT INTO user_device_tokens (user_id, device_token)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE last_used = NOW(), is_active = TRUE
                """,
                (user_id, token)
            )
        
        # Sync to Dynamics 365
        if dynamics_service:
            asyncio.create_task(sync_user_to_dynamics(user_id))
        
        return {"success": True, "message": "Profile updated successfully"}
        
    except Exception as e:
        logger.error(f"Profile update error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/measurements")
async def create_measurement(
    measurement: AcousticMeasurement,
    user_id: int,
    background_tasks: BackgroundTasks
):
    """Create new acoustic measurement"""
    try:
        # Save audio file if provided
        audio_file_path = None
        if measurement.audio_file_base64:
            audio_data = base64.b64decode(measurement.audio_file_base64)
            audio_file_path = f"/tmp/measurements/{user_id}_{datetime.now().timestamp()}.wav"
            os.makedirs(os.path.dirname(audio_file_path), exist_ok=True)
            with open(audio_file_path, 'wb') as f:
                f.write(audio_data)
        
        # Insert measurement
        measurement_id = db.execute_query(
            """
            INSERT INTO acoustic_measurements 
            (user_id, room_name, room_type, measurement_type, rt60_value, rt30_value, 
             clarity_value, frequency_data, temperature, humidity, notes, audio_file_path, metadata)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                user_id, measurement.room_name, measurement.room_type, 
                measurement.measurement_type.value, measurement.rt60_value,
                measurement.rt30_value, measurement.clarity_value,
                json.dumps(measurement.frequency_data) if measurement.frequency_data else None,
                measurement.temperature, measurement.humidity, measurement.notes,
                audio_file_path, json.dumps(measurement.metadata)
            ),
            fetch=False
        )
        
        # Queue for Dynamics 365 sync
        background_tasks.add_task(sync_measurement_to_dynamics, measurement_id, user_id)
        
        # Update analytics
        background_tasks.add_task(update_measurement_analytics, user_id)
        
        # Send notification to Telegram bot
        background_tasks.add_task(notify_telegram_bot, user_id, measurement_id)
        
        return {
            "measurement_id": measurement_id,
            "message": "Measurement created successfully"
        }
        
    except Exception as e:
        logger.error(f"Measurement creation error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/measurements")
async def get_measurements(user_id: int, limit: int = 50, offset: int = 0):
    """Get user's measurements"""
    measurements = db.execute_query(
        """
        SELECT m.*, 
               COUNT(mq.quote_id) as quote_count,
               GROUP_CONCAT(mq.quote_id) as linked_quotes
        FROM acoustic_measurements m
        LEFT JOIN measurement_quotes mq ON m.id = mq.measurement_id
        WHERE m.user_id = %s
        GROUP BY m.id
        ORDER BY m.created_at DESC
        LIMIT %s OFFSET %s
        """,
        (user_id, limit, offset),
        fetch=True
    )
    
    # Parse JSON fields
    for m in measurements:
        if m.get('frequency_data'):
            m['frequency_data'] = json.loads(m['frequency_data'])
        if m.get('metadata'):
            m['metadata'] = json.loads(m['metadata'])
        if m.get('linked_quotes'):
            m['linked_quotes'] = [int(q) for q in m['linked_quotes'].split(',')]
        else:
            m['linked_quotes'] = []
    
    return measurements

@app.get("/api/measurements/{measurement_id}")
async def get_measurement(measurement_id: int, user_id: int):
    """Get specific measurement"""
    measurement = db.execute_query(
        """
        SELECT * FROM acoustic_measurements 
        WHERE id = %s AND user_id = %s
        """,
        (measurement_id, user_id),
        fetch=True
    )
    
    if not measurement:
        raise HTTPException(status_code=404, detail="Measurement not found")
    
    measurement = measurement[0]
    
    # Parse JSON fields
    if measurement.get('frequency_data'):
        measurement['frequency_data'] = json.loads(measurement['frequency_data'])
    if measurement.get('metadata'):
        measurement['metadata'] = json.loads(measurement['metadata'])
    
    return measurement

@app.post("/api/quotes/from-measurements")
async def create_quote_from_measurements(
    quote_request: QuoteRequest,
    background_tasks: BackgroundTasks
):
    """Create quote based on measurements"""
    try:
        # Get measurements
        measurements = db.execute_query(
            """
            SELECT * FROM acoustic_measurements 
            WHERE id IN (%s) AND user_id = %s
            """ % (','.join(['%s'] * len(quote_request.room_measurements)), '%s'),
            (*quote_request.room_measurements, quote_request.user_id),
            fetch=True
        )
        
        if not measurements:
            raise HTTPException(status_code=404, detail="No measurements found")
        
        # Prepare quote data
        quote_data = {
            "user_id": quote_request.user_id,
            "measurements": measurements,
            "additional_notes": quote_request.additional_notes,
            "source": "acoustic_app"
        }
        
        # Send to Telegram bot for quote generation
        # This would integrate with your existing quote flow
        response = await send_to_telegram_bot(
            user_id=quote_request.user_id,
            action="create_quote",
            data=quote_data
        )
        
        if response.get('quote_id'):
            # Link measurements to quote
            for m_id in quote_request.room_measurements:
                db.execute_query(
                    """
                    INSERT INTO measurement_quotes (measurement_id, quote_id)
                    VALUES (%s, %s)
                    """,
                    (m_id, response['quote_id'])
                )
        
        return response
        
    except Exception as e:
        logger.error(f"Quote creation error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/quotes")
async def get_quotes(user_id: int):
    """Get user's quotes with linked measurements"""
    quotes = db.execute_query(
        """
        SELECT q.*, 
               GROUP_CONCAT(mq.measurement_id) as linked_measurements
        FROM quotations q
        LEFT JOIN measurement_quotes mq ON q.id = mq.quote_id
        WHERE q.user_id = %s
        GROUP BY q.id
        ORDER BY q.created_at DESC
        """,
        (user_id,),
        fetch=True
    )
    
    for q in quotes:
        if q.get('quote_data'):
            q['quote_data'] = json.loads(q['quote_data'])
        if q.get('linked_measurements'):
            q['linked_measurements'] = [int(m) for m in q['linked_measurements'].split(',')]
        else:
            q['linked_measurements'] = []
    
    return quotes

@app.post("/api/notifications/send")
async def send_notification(notification: NotificationRequest):
    """Send push notification to user"""
    try:
        # Get user's device tokens
        tokens = db.execute_query(
            """
            SELECT device_token FROM user_device_tokens 
            WHERE user_id = %s AND is_active = TRUE
            """,
            (notification.user_id,),
            fetch=True
        )
        
        if not tokens:
            return {"success": False, "message": "No active device tokens"}
        
        # Send via Firebase
        messages = []
        for token in tokens:
            message = messaging.Message(
                notification=messaging.Notification(
                    title=notification.title,
                    body=notification.body,
                ),
                data=notification.data or {},
                token=token['device_token'],
            )
            messages.append(message)
        
        response = messaging.send_all(messages)
        
        return {
            "success": True,
            "success_count": response.success_count,
            "failure_count": response.failure_count
        }
        
    except Exception as e:
        logger.error(f"Notification error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/offline/sync")
async def sync_offline_data(user_id: int, actions: List[Dict]):
    """Sync offline actions"""
    try:
        for action in actions:
            db.execute_query(
                """
                INSERT INTO offline_queue (user_id, action_type, payload)
                VALUES (%s, %s, %s)
                """,
                (user_id, action['type'], json.dumps(action['data']))
            )
        
        # Process queue
        await process_offline_queue(user_id)
        
        return {"success": True, "synced_count": len(actions)}
        
    except Exception as e:
        logger.error(f"Offline sync error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/reports/measurement-summary")
async def get_measurement_report(user_id: int, start_date: str, end_date: str):
    """Get measurement summary report"""
    summary = db.execute_query(
        """
        SELECT 
            COUNT(*) as total_measurements,
            AVG(rt60_value) as avg_rt60,
            MIN(rt60_value) as min_rt60,
            MAX(rt60_value) as max_rt60,
            COUNT(DISTINCT room_type) as room_types_count,
            COUNT(DISTINCT DATE(created_at)) as measurement_days
        FROM acoustic_measurements
        WHERE user_id = %s 
        AND created_at BETWEEN %s AND %s
        """,
        (user_id, start_date, end_date),
        fetch=True
    )[0]
    
    # Get room type breakdown
    room_breakdown = db.execute_query(
        """
        SELECT room_type, COUNT(*) as count, AVG(rt60_value) as avg_rt60
        FROM acoustic_measurements
        WHERE user_id = %s 
        AND created_at BETWEEN %s AND %s
        GROUP BY room_type
        """,
        (user_id, start_date, end_date),
        fetch=True
    )
    
    return {
        "summary": summary,
        "room_breakdown": room_breakdown
    }

@app.get("/api/reports/generate-pdf/{user_id}")
async def generate_pdf_report(
    user_id: int,
    start_date: str,
    end_date: str,
    include_measurements: bool = True,
    include_quotes: bool = True
):
    """Generate comprehensive PDF report"""
    try:
        # Get user profile
        user_profile = db.get_user_profile(user_id)
        
        # Get measurements
        measurements = []
        if include_measurements:
            measurements = db.execute_query(
                """
                SELECT * FROM acoustic_measurements
                WHERE user_id = %s AND created_at BETWEEN %s AND %s
                ORDER BY created_at DESC
                """,
                (user_id, start_date, end_date),
                fetch=True
            )
        
        # Get quotes
        quotes = []
        if include_quotes:
            quotes = db.execute_query(
                """
                SELECT * FROM quotations
                WHERE user_id = %s AND created_at BETWEEN %s AND %s
                ORDER BY created_at DESC
                """,
                (user_id, start_date, end_date),
                fetch=True
            )
        
        # Generate PDF
        pdf_buffer = generate_acoustic_report_pdf(
            user_profile, measurements, quotes, start_date, end_date
        )
        
        # Return as base64
        pdf_base64 = base64.b64encode(pdf_buffer.getvalue()).decode('utf-8')
        
        return {
            "pdf_base64": pdf_base64,
            "filename": f"acoustic_report_{user_id}_{datetime.now().strftime('%Y%m%d')}.pdf"
        }
        
    except Exception as e:
        logger.error(f"PDF generation error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/dashboard/analytics")
async def get_dashboard_analytics(user_id: Optional[int] = None):
    """Get analytics dashboard data"""
    
    # Base query conditions
    where_clause = "WHERE 1=1"
    params = []
    
    if user_id:
        where_clause += " AND user_id = %s"
        params.append(user_id)
    
    # Get overview stats
    overview = db.execute_query(
        f"""
        SELECT 
            COUNT(DISTINCT user_id) as total_users,
            COUNT(*) as total_measurements,
            AVG(rt60_value) as avg_rt60,
            COUNT(DISTINCT room_type) as unique_room_types
        FROM acoustic_measurements
        {where_clause}
        """,
        params if params else None,
        fetch=True
    )[0]
    
    # Get time series data
    time_series = db.execute_query(
        f"""
        SELECT 
            DATE(created_at) as date,
            COUNT(*) as measurement_count,
            AVG(rt60_value) as avg_rt60
        FROM acoustic_measurements
        {where_clause}
        GROUP BY DATE(created_at)
        ORDER BY date DESC
        LIMIT 30
        """,
        params if params else None,
        fetch=True
    )
    
    # Get room type distribution
    room_distribution = db.execute_query(
        f"""
        SELECT 
            room_type,
            COUNT(*) as count,
            AVG(rt60_value) as avg_rt60,
            MIN(rt60_value) as min_rt60,
            MAX(rt60_value) as max_rt60
        FROM acoustic_measurements
        {where_clause}
        GROUP BY room_type
        ORDER BY count DESC
        """,
        params if params else None,
        fetch=True
    )
    
    return {
        "overview": overview,
        "time_series": time_series,
        "room_distribution": room_distribution
    }

# Background Tasks

async def sync_user_to_dynamics(user_id: int):
    """Sync user to Dynamics 365"""
    if not dynamics_service:
        return
    
    try:
        from dynamics365_integration import Dynamics365IntegrationHandler
        integration = Dynamics365IntegrationHandler(db)
        await integration.sync_user_to_dynamics(user_id)
    except Exception as e:
        logger.error(f"Dynamics user sync error: {e}")

async def sync_measurement_to_dynamics(measurement_id: int, user_id: int):
    """Sync measurement to Dynamics 365"""
    if not dynamics_service:
        return
    
    try:
        # Get measurement data
        measurement = db.execute_query(
            "SELECT * FROM acoustic_measurements WHERE id = %s",
            (measurement_id,),
            fetch=True
        )[0]
        
        # Get user's Dynamics IDs
        dynamics_ids = db.get_user_dynamics_ids(user_id)
        
        if dynamics_ids.get('contact_id'):
            # Create activity in Dynamics
            activity_data = {
                "subject": f"Acoustic Measurement - {measurement['room_name']}",
                "description": f"RT60: {measurement['rt60_value']}s\n"
                              f"Room Type: {measurement['room_type']}\n"
                              f"Notes: {measurement['notes'] or 'N/A'}",
                "scheduledend": datetime.now().isoformat(),
                "actualend": datetime.now().isoformat(),
                "statecode": 1,  # Completed
                "regardingobjectid_contact@odata.bind": f"/contacts({dynamics_ids['contact_id']})",
                "category": "Acoustic Measurement",
                # Custom fields
                "new_measurementid": str(measurement_id),
                "new_rt60value": measurement['rt60_value']
            }
            
            response = await dynamics_service.make_request(
                method="POST",
                endpoint="tasks",
                data=activity_data
            )
            
            if response and 'activityid' in response:
                # Update measurement with Dynamics ID
                db.execute_query(
                    """
                    UPDATE acoustic_measurements 
                    SET dynamics_measurement_id = %s, 
                        dynamics_sync_status = 'synced'
                    WHERE id = %s
                    """,
                    (response['activityid'], measurement_id)
                )
                logger.info(f"✅ Synced measurement {measurement_id} to Dynamics")
            
    except Exception as e:
        logger.error(f"Dynamics measurement sync error: {e}")
        db.execute_query(
            """
            UPDATE acoustic_measurements 
            SET dynamics_sync_status = 'error',
                dynamics_sync_error = %s
            WHERE id = %s
            """,
            (str(e), measurement_id)
        )

async def notify_telegram_bot(user_id: int, measurement_id: int):
    """Notify Telegram bot about new measurement"""
    try:
        # This would send a message through your bot
        # Implementation depends on your bot's internal API
        pass
    except Exception as e:
        logger.error(f"Telegram notification error: {e}")

async def send_to_telegram_bot(user_id: int, action: str, data: Dict) -> Dict:
    """Send action to Telegram bot"""
    # This would integrate with your existing bot
    # For now, returning mock response
    return {"quote_id": 12345, "success": True}

async def update_measurement_analytics(user_id: int):
    """Update measurement analytics"""
    try:
        today = datetime.now().date()
        
        # Calculate daily analytics
        stats = db.execute_query(
            """
            SELECT 
                COUNT(*) as measurement_count,
                AVG(rt60_value) as avg_rt60,
                JSON_OBJECT(
                    'types', JSON_ARRAYAGG(DISTINCT room_type),
                    'count_by_type', JSON_OBJECTAGG(room_type, type_count)
                ) as room_types
            FROM (
                SELECT room_type, rt60_value,
                       COUNT(*) OVER (PARTITION BY room_type) as type_count
                FROM acoustic_measurements
                WHERE user_id = %s AND DATE(created_at) = %s
            ) t
            """,
            (user_id, today),
            fetch=True
        )[0]
        
        # Update or insert analytics
        db.execute_query(
            """
            INSERT INTO measurement_analytics 
            (user_id, date, measurement_count, avg_rt60, room_types)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            measurement_count = VALUES(measurement_count),
            avg_rt60 = VALUES(avg_rt60),
            room_types = VALUES(room_types),
            updated_at = NOW()
            """,
            (user_id, today, stats['measurement_count'], 
             stats['avg_rt60'], json.dumps(stats['room_types']))
        )
        
    except Exception as e:
        logger.error(f"Analytics update error: {e}")

async def process_offline_queue(user_id: int):
    """Process offline queue for user"""
    try:
        # Get pending actions
        actions = db.execute_query(
            """
            SELECT * FROM offline_queue 
            WHERE user_id = %s AND status = 'pending'
            ORDER BY created_at
            """,
            (user_id,),
            fetch=True
        )
        
        for action in actions:
            try:
                # Update status
                db.execute_query(
                    "UPDATE offline_queue SET status = 'processing' WHERE id = %s",
                    (action['id'],)
                )
                
                # Process based on action type
                payload = json.loads(action['payload'])
                
                if action['action_type'] == 'measurement':
                    # Create measurement
                    measurement = AcousticMeasurement(**payload)
                    await create_measurement(measurement, user_id, BackgroundTasks())
                
                elif action['action_type'] == 'quote_request':
                    # Create quote
                    quote_req = QuoteRequest(**payload)
                    await create_quote_from_measurements(quote_req, BackgroundTasks())
                
                # Mark as completed
                db.execute_query(
                    """
                    UPDATE offline_queue 
                    SET status = 'completed', processed_at = NOW() 
                    WHERE id = %s
                    """,
                    (action['id'],)
                )
                
            except Exception as e:
                # Mark as failed
                db.execute_query(
                    """
                    UPDATE offline_queue 
                    SET status = 'failed', 
                        retry_count = retry_count + 1,
                        error_message = %s 
                    WHERE id = %s
                    """,
                    (str(e), action['id'])
                )
                
    except Exception as e:
        logger.error(f"Offline queue processing error: {e}")

def generate_acoustic_report_pdf(user_profile, measurements, quotes, start_date, end_date):
    """Generate PDF report"""
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4)
    story = []
    styles = getSampleStyleSheet()
    
    # Title
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Heading1'],
        fontSize=24,
        textColor=colors.HexColor('#667EEA'),
        spaceAfter=30,
        alignment=1  # Center
    )
    
    story.append(Paragraph("Acoustic Analysis Report", title_style))
    story.append(Spacer(1, 20))
    
    # User info
    user_name = f"{user_profile.get('first_name', '')} {user_profile.get('last_name', '')}".strip()
    if user_profile.get('company_name'):
        user_name = f"{user_name} - {user_profile['company_name']}"
    
    story.append(Paragraph(f"<b>Report for:</b> {user_name}", styles['Normal']))
    story.append(Paragraph(f"<b>Period:</b> {start_date} to {end_date}", styles['Normal']))
    story.append(Spacer(1, 20))
    
    # Summary statistics
    if measurements:
        story.append(Paragraph("Measurement Summary", styles['Heading2']))
        
        summary_data = [
            ['Metric', 'Value'],
            ['Total Measurements', str(len(measurements))],
            ['Average RT60', f"{sum(m['rt60_value'] for m in measurements if m['rt60_value']) / len(measurements):.2f}s"],
            ['Room Types', str(len(set(m['room_type'] for m in measurements)))],
        ]
        
        summary_table = Table(summary_data)
        summary_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#667EEA')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 14),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))
        
        story.append(summary_table)
        story.append(Spacer(1, 20))
    
    # Detailed measurements
    if measurements:
        story.append(Paragraph("Detailed Measurements", styles['Heading2']))
        
        for m in measurements[:10]:  # Limit to 10 for space
            story.append(Paragraph(f"<b>{m['room_name']}</b> ({m['room_type']})", styles['Normal']))
            story.append(Paragraph(f"RT60: {m['rt60_value']}s | Date: {m['created_at']}", styles['Normal']))
            if m['notes']:
                story.append(Paragraph(f"Notes: {m['notes']}", styles['Normal']))
            story.append(Spacer(1, 10))
    
    # Quotes section
    if quotes:
        story.append(Paragraph("Related Quotes", styles['Heading2']))
        
        quote_data = [['Quote #', 'Date', 'Status', 'Total']]
        for q in quotes[:10]:
            quote_data.append([
                q['quote_number'],
                q['created_at'].strftime('%Y-%m-%d'),
                q['status'],
                f"€{q['total_price']:.2f}"
            ])
        
        quote_table = Table(quote_data)
        quote_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#667EEA')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))
        
        story.append(quote_table)
    
    # Build PDF
    doc.build(story)
    buffer.seek(0)
    return buffer

# Health check
@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "version": "1.0.0",
        "database": db.test_connection(),
        "dynamics": await dynamics_service.test_connection() if dynamics_service else False
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)