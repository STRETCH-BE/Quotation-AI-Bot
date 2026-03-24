"""
Enhanced Database Manager for Stretch Ceiling Bot
Version 9.0 - Complete implementation with enhanced user management
"""
import pymysql
import pymysql.cursors
from pymysql import Error
import logging
import json
from decimal import Decimal
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
import time
import traceback

from config import Config
from utils import serialize_for_json
from services.mail.email_session_mixin import EmailSessionMixin

logger = logging.getLogger(__name__)

class EnhancedDatabaseManager(EmailSessionMixin):
    """Enhanced database manager with quote management, conversation logging, admin features, and user management"""
    
    def __init__(self):
        self.config = Config.get_database_config()
        logger.info(
            f"🔌 Initializing connection to {self.config['host']}:{self.config['port']}/{self.config['database']}"
        )
        self._run_schema_updates()
        self._add_enhanced_user_tables()
    
    def _run_schema_updates(self):
        """Run database schema updates for new features"""
        try:
            connection = pymysql.connect(**self.config)
            cursor = connection.cursor()
            
            # Check if quotations table uses 'id' or 'quotation_id'
            cursor.execute(
                """
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE()
                AND TABLE_NAME = 'quotations'
                AND COLUMN_NAME IN ('id', 'quotation_id')
                """
            )
            columns = [row[0] for row in cursor.fetchall()]
            
            if "id" in columns and "quotation_id" not in columns:
                logger.info("📊 Quotations table uses 'id' as primary key")
            
            # Add status field to quotations if not exists
            cursor.execute(
                """
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE()
                AND TABLE_NAME = 'quotations'
                AND COLUMN_NAME = 'status'
                """
            )
            if not cursor.fetchone():
                cursor.execute(
                    """
                    ALTER TABLE quotations ADD COLUMN status VARCHAR(50) DEFAULT 'draft'
                    """
                )
                logger.info("✅ Added 'status' column to quotations table")
            
            # Add last_modified field
            cursor.execute(
                """
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE()
                AND TABLE_NAME = 'quotations'
                AND COLUMN_NAME = 'last_modified'
                """
            )
            if not cursor.fetchone():
                cursor.execute(
                    """
                    ALTER TABLE quotations ADD COLUMN last_modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                    """
                )
                logger.info("✅ Added 'last_modified' column to quotations table")
            
            # Add notes field
            cursor.execute(
                """
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE()
                AND TABLE_NAME = 'quotations'
                AND COLUMN_NAME = 'notes'
                """
            )
            if not cursor.fetchone():
                cursor.execute(
                    """
                    ALTER TABLE quotations ADD COLUMN notes TEXT
                    """
                )
                logger.info("✅ Added 'notes' column to quotations table")
            
            # Create conversation logs table
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS conversation_logs (
                    log_id INT AUTO_INCREMENT PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    message_type VARCHAR(20) NOT NULL,
                    message TEXT NOT NULL,
                    context TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_user_id (user_id),
                    INDEX idx_created_at (created_at)
                )
                """
            )
            
            # Create AI chat contexts table
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS ai_chat_contexts (
                    context_id INT AUTO_INCREMENT PRIMARY KEY,
                    user_id BIGINT NOT NULL UNIQUE,
                    context_data JSON,
                    website_data JSON,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    INDEX idx_user_id (user_id)
                )
                """
            )
            
            # Create quote status history table
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS quote_status_history (
                    history_id INT AUTO_INCREMENT PRIMARY KEY,
                    quotation_id INT NOT NULL,
                    old_status VARCHAR(50),
                    new_status VARCHAR(50),
                    changed_by BIGINT,
                    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    notes TEXT,
                    INDEX idx_quotation_id (quotation_id)
                )
                """
            )
            
            # Create quote sessions table with enhanced fields
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS quote_sessions (
                    session_id INT AUTO_INCREMENT PRIMARY KEY,
                    user_id BIGINT NOT NULL UNIQUE,
                    session_data JSON,
                    current_step VARCHAR(50),
                    edit_history JSON,
                    previous_steps JSON,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    expires_at TIMESTAMP,
                    INDEX idx_user_id (user_id),
                    INDEX idx_expires_at (expires_at)
                )
                """
            )
            
            # Add edit history to quote sessions if not exists
            cursor.execute(
                """
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE()
                AND TABLE_NAME = 'quote_sessions'
                AND COLUMN_NAME = 'edit_history'
                """
            )
            if not cursor.fetchone():
                cursor.execute(
                    """
                    ALTER TABLE quote_sessions ADD COLUMN edit_history JSON
                    """
                )
                logger.info("✅ Added 'edit_history' column to quote_sessions table")
            
            # Add previous_steps column
            cursor.execute(
                """
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE()
                AND TABLE_NAME = 'quote_sessions'
                AND COLUMN_NAME = 'previous_steps'
                """
            )
            if not cursor.fetchone():
                cursor.execute(
                    """
                    ALTER TABLE quote_sessions ADD COLUMN previous_steps JSON
                    """
                )
                logger.info("✅ Added 'previous_steps' column to quote_sessions table")
            
            connection.commit()
            cursor.close()
            connection.close()
            logger.info("✅ Database schema updated for enhanced features")
        
        except Exception as e:
            logger.error(f"❌ Error updating database schema: {e}")
    
    def _add_enhanced_user_tables(self):
        """Create enhanced user tables"""
        try:
            connection = pymysql.connect(**self.config)
            cursor = connection.cursor()
            
            # First, add new columns to existing users table
            new_columns = [
                ("full_name", "VARCHAR(255)"),
                ("is_company", "BOOLEAN DEFAULT FALSE"),
                ("company_name", "VARCHAR(255)"),
                ("vat_number", "VARCHAR(50)"),
                ("address", "TEXT"),
                ("phone", "VARCHAR(50)"),
                ("telegram_username", "VARCHAR(100)"),
                ("whatsapp_number", "VARCHAR(50)"),
                ("onboarding_completed", "BOOLEAN DEFAULT FALSE"),
                ("onboarding_date", "TIMESTAMP NULL"),
                ("profile_updated_at", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"),
                ("notes", "TEXT"),
                ("tags", "JSON"),
                ("preferences", "JSON"),
                ("source", "ENUM('telegram', 'whatsapp', 'both') DEFAULT 'telegram'"),
                ("language", "VARCHAR(10) DEFAULT 'en'"),
                ("timezone", "VARCHAR(50) DEFAULT 'Europe/Brussels'")
            ]
            
            for column_name, column_def in new_columns:
                cursor.execute(f"""
                    SELECT COLUMN_NAME 
                    FROM INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_SCHEMA = DATABASE() 
                    AND TABLE_NAME = 'users' 
                    AND COLUMN_NAME = '{column_name}'
                """)
                
                if not cursor.fetchone():
                    cursor.execute(f"ALTER TABLE users ADD COLUMN {column_name} {column_def}")
                    logger.info(f"✅ Added column {column_name} to users table")
            
            # Create user conversation memory table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS user_conversation_memory (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    conversation_summary TEXT,
                    key_points JSON,
                    preferences_learned JSON,
                    last_topics JSON,
                    interaction_count INT DEFAULT 0,
                    last_interaction TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE KEY unique_user (user_id),
                    INDEX idx_user_id (user_id),
                    INDEX idx_last_interaction (last_interaction)
                )
            """)
            logger.info("✅ Created/verified user_conversation_memory table")
            
            # Create user activity log
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS user_activity_log (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    activity_type VARCHAR(50),
                    activity_data JSON,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_user_id (user_id),
                    INDEX idx_activity_type (activity_type),
                    INDEX idx_created_at (created_at)
                )
            """)
            logger.info("✅ Created/verified user_activity_log table")
            
            # Create user groups table (for custom grouping beyond client types)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS user_custom_groups (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    group_name VARCHAR(100) UNIQUE,
                    description TEXT,
                    color VARCHAR(7),
                    created_by BIGINT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            logger.info("✅ Created/verified user_custom_groups table")
            
            # Create user group members table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS user_custom_group_members (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    group_id INT NOT NULL,
                    user_id BIGINT NOT NULL,
                    added_by BIGINT,
                    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE KEY unique_group_user (group_id, user_id),
                    FOREIGN KEY (group_id) REFERENCES user_custom_groups(id) ON DELETE CASCADE,
                    INDEX idx_user_id (user_id)
                )
            """)
            logger.info("✅ Created/verified user_custom_group_members table")
            
            connection.commit()
            cursor.close()
            connection.close()
            
            logger.info("✅ Enhanced user tables created/updated successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error creating enhanced user tables: {e}")
            return False
    
    def test_connection(self) -> bool:
        """Test database connection"""
        try:
            connection = pymysql.connect(**self.config)
            logger.info(f"✅ Connected to MySQL Server")
            connection.close()
            return True
        except Error as e:
            logger.error(f"❌ Error connecting to MySQL: {e}")
            return False
    
    def get_connection(self):
        """Get database connection with retry logic"""
        max_retries = 3
        retry_delay = 2
        
        for attempt in range(max_retries):
            try:
                connection = pymysql.connect(**self.config)
                return connection
            except Error as e:
                logger.warning(f"⚠️ Connection attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
        
        return None
    
    def execute_query(self, query: str, params: Tuple = None, fetch: bool = False) -> Optional[Any]:
        """Execute a database query with enhanced error handling"""
        connection = self.get_connection()
        if not connection:
            logger.error("❌ Could not establish database connection")
            return None
        
        try:
            # Use DictCursor for dictionary results when fetching
            if fetch:
                cursor = connection.cursor(pymysql.cursors.DictCursor)
            else:
                cursor = connection.cursor()
            
            if Config.DEBUG_MODE:
                logger.debug(f"Executing query: {query}")
                logger.debug(f"With params: {params}")
            
            cursor.execute(query, params or ())
            
            if fetch:
                result = cursor.fetchall()
                if Config.DEBUG_MODE and result:
                    logger.debug(f"Query returned {len(result)} rows")
            else:
                result = cursor.rowcount
            
            connection.commit()
            return result
        
        except Error as e:
            logger.error(f"❌ Database error: {e}")
            connection.rollback()
            return None
        finally:
            cursor.close()
            connection.close()
    
    # ==================== USER MANAGEMENT ====================
    
    def ensure_user_exists(
        self, user_id: int, username: str = None, first_name: str = None, 
        last_name: str = None, email: str = None
    ) -> bool:
        """Ensure user exists in database"""
        try:
            existing_user = self.execute_query(
                "SELECT user_id FROM users WHERE user_id = %s", 
                (user_id,), 
                fetch=True
            )
            
            if not existing_user:
                result = self.execute_query(
                    """
                    INSERT INTO users (user_id, username, first_name, last_name, email, client_group, last_activity)
                    VALUES (%s, %s, %s, %s, %s, %s, NOW())
                    """,
                    (user_id, username or "", first_name or "", last_name or "", email or "", "price_b2c")
                )
                
                logger.info(f"✅ Created new user: {user_id} ({first_name} {last_name})")
                return bool(result)
            else:
                self.execute_query(
                    "UPDATE users SET last_activity = NOW() WHERE user_id = %s", 
                    (user_id,)
                )
                return True
        
        except Exception as e:
            logger.error(f"❌ Error ensuring user exists: {e}")
            return False
    
    def get_user_client_group(self, user_id: int) -> str:
        """Get user's client group"""
        result = self.execute_query(
            "SELECT client_group FROM users WHERE user_id = %s", 
            (user_id,), 
            fetch=True
        )
        
        if result and result[0].get("client_group"):
            return result[0]["client_group"]
        return "price_b2c"  # Default to B2C
    
    def set_user_client_group(self, user_id: int, client_group: str) -> bool:
        """Set user's client group"""
        result = self.execute_query(
            "UPDATE users SET client_group = %s WHERE user_id = %s", 
            (client_group, user_id)
        )
        return bool(result)
    
    def get_all_users(self, limit: int = 100, offset: int = 0) -> List[Dict]:
        """Get all users with pagination"""
        return self.execute_query(
            """
            SELECT user_id, username, first_name, last_name, email, client_group, 
                   created_at, last_activity
            FROM users
            ORDER BY last_activity DESC
            LIMIT %s OFFSET %s
            """,
            (limit, offset),
            fetch=True
        ) or []
    
    def search_users(self, search_term: str) -> List[Dict]:
        """Search users by name or username"""
        search_pattern = f"%{search_term}%"
        return self.execute_query(
            """
            SELECT user_id, username, first_name, last_name, email, client_group
            FROM users
            WHERE username LIKE %s 
               OR first_name LIKE %s 
               OR last_name LIKE %s
               OR email LIKE %s
            ORDER BY last_activity DESC
            LIMIT 20
            """,
            (search_pattern, search_pattern, search_pattern, search_pattern),
            fetch=True
        ) or []
    
    # ==================== ENHANCED USER MANAGEMENT METHODS ====================
    
    def save_user_profile(self, user_data: dict) -> bool:
        """Save or update complete user profile"""
        try:
            # Convert datetime objects to strings for MySQL
            if isinstance(user_data.get('onboarding_date'), datetime):
                user_data['onboarding_date'] = user_data['onboarding_date'].strftime('%Y-%m-%d %H:%M:%S')
            
            # Check if user exists
            existing = self.execute_query(
                "SELECT user_id FROM users WHERE user_id = %s",
                (user_data['user_id'],),
                fetch=True
            )
            
            if existing:
                # Update existing user
                result = self.execute_query(
                    """
                    UPDATE users SET
                        telegram_username = %s,
                        first_name = %s,
                        last_name = %s,
                        full_name = %s,
                        is_company = %s,
                        company_name = %s,
                        vat_number = %s,
                        address = %s,
                        email = %s,
                        phone = %s,
                        onboarding_completed = %s,
                        onboarding_date = %s,
                        source = %s,
                        profile_updated_at = NOW(),
                        last_activity = NOW()
                    WHERE user_id = %s
                    """,
                    (
                        user_data.get('telegram_username'),
                        user_data.get('first_name'),
                        user_data.get('last_name'),
                        user_data.get('full_name'),
                        user_data.get('is_company', False),
                        user_data.get('company_name'),
                        user_data.get('vat_number'),
                        user_data.get('address'),
                        user_data.get('email'),
                        user_data.get('phone'),
                        user_data.get('onboarding_completed', True),
                        user_data.get('onboarding_date'),
                        user_data.get('source', 'telegram'),
                        user_data['user_id']
                    )
                )
            else:
                # Insert new user
                result = self.execute_query(
                    """
                    INSERT INTO users (
                        user_id, telegram_username, first_name, last_name, full_name,
                        is_company, company_name, vat_number, address, email, phone,
                        client_group, onboarding_completed, onboarding_date, source, created_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    """,
                    (
                        user_data['user_id'],
                        user_data.get('telegram_username'),
                        user_data.get('first_name'),
                        user_data.get('last_name'),
                        user_data.get('full_name'),
                        user_data.get('is_company', False),
                        user_data.get('company_name'),
                        user_data.get('vat_number'),
                        user_data.get('address'),
                        user_data.get('email'),
                        user_data.get('phone'),
                        user_data.get('client_group', 'price_b2c'),
                        user_data.get('onboarding_completed', True),
                        user_data.get('onboarding_date'),
                        user_data.get('source', 'telegram')
                    )
                )
            
            # Log activity
            self.log_user_activity(
                user_data['user_id'], 
                'profile_updated', 
                {'action': 'onboarding_completed' if user_data.get('onboarding_completed') else 'profile_edited'}
            )
            
            return bool(result)
            
        except Exception as e:
            logger.error(f"Error saving user profile: {e}")
            return False
    
    def get_user_profile(self, user_id: int) -> Optional[Dict]:
        """Get complete user profile"""
        result = self.execute_query(
            """
            SELECT 
                user_id, telegram_username, first_name, last_name, full_name,
                is_company, company_name, vat_number, address, email, phone,
                client_group, onboarding_completed, onboarding_date,
                created_at, last_activity, profile_updated_at,
                notes, tags, preferences, source, language, timezone
            FROM users
            WHERE user_id = %s
            """,
            (user_id,),
            fetch=True
        )
        
        if result and result[0]:
            user_data = result[0]
            
            # Parse JSON fields
            if user_data.get('tags'):
                try:
                    user_data['tags'] = json.loads(user_data['tags'])
                except:
                    user_data['tags'] = []
            
            if user_data.get('preferences'):
                try:
                    user_data['preferences'] = json.loads(user_data['preferences'])
                except:
                    user_data['preferences'] = {}
            
            return user_data
        
        return None
    
    def get_user_conversation_memory(self, user_id: int) -> Dict:
        """Get user's conversation memory and learned preferences"""
        result = self.execute_query(
            """
            SELECT 
                conversation_summary, key_points, preferences_learned,
                last_topics, interaction_count, last_interaction
            FROM user_conversation_memory
            WHERE user_id = %s
            """,
            (user_id,),
            fetch=True
        )
        
        if result and result[0]:
            memory = result[0]
            
            # Parse JSON fields
            for field in ['key_points', 'preferences_learned', 'last_topics']:
                if memory.get(field):
                    try:
                        memory[field] = json.loads(memory[field])
                    except:
                        memory[field] = [] if field != 'preferences_learned' else {}
            
            return memory
        
        # Return default structure if no memory exists
        return {
            'conversation_summary': '',
            'key_points': [],
            'preferences_learned': {},
            'last_topics': [],
            'interaction_count': 0,
            'last_interaction': None
        }
    
    def update_user_conversation_memory(self, user_id: int, memory_data: Dict) -> bool:
        """Update user's conversation memory"""
        try:
            # Convert lists/dicts to JSON
            json_fields = ['key_points', 'preferences_learned', 'last_topics']
            for field in json_fields:
                if field in memory_data and memory_data[field] is not None:
                    memory_data[field] = json.dumps(memory_data[field], ensure_ascii=False)
            
            result = self.execute_query(
                """
                INSERT INTO user_conversation_memory (
                    user_id, conversation_summary, key_points, 
                    preferences_learned, last_topics, interaction_count
                ) VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    conversation_summary = VALUES(conversation_summary),
                    key_points = VALUES(key_points),
                    preferences_learned = VALUES(preferences_learned),
                    last_topics = VALUES(last_topics),
                    interaction_count = VALUES(interaction_count),
                    last_interaction = NOW()
                """,
                (
                    user_id,
                    memory_data.get('conversation_summary', ''),
                    memory_data.get('key_points', '[]'),
                    memory_data.get('preferences_learned', '{}'),
                    memory_data.get('last_topics', '[]'),
                    memory_data.get('interaction_count', 0)
                )
            )
            
            return bool(result)
            
        except Exception as e:
            logger.error(f"Error updating conversation memory: {e}")
            return False
    
    def log_user_activity(self, user_id: int, activity_type: str, activity_data: Dict = None) -> bool:
        """Log user activity with proper decimal handling"""
        try:
            # Convert activity_data to handle Decimal values
            if activity_data:
                activity_data = self._convert_decimals_in_dict(activity_data)
            
            result = self.execute_query(
                """
                INSERT INTO user_activity_log (user_id, activity_type, activity_data)
                VALUES (%s, %s, %s)
                """,
                (user_id, activity_type, json.dumps(activity_data, ensure_ascii=False) if activity_data else None)
            )
            
            return bool(result)
            
        except Exception as e:
            logger.error(f"Error logging user activity: {e}")
            return False
    
    def _convert_decimals_in_dict(self, obj):
        """Recursively convert Decimal values to float in dictionaries and lists"""
        if isinstance(obj, dict):
            return {k: self._convert_decimals_in_dict(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._convert_decimals_in_dict(item) for item in obj]
        elif isinstance(obj, Decimal):
            return float(obj)
        elif hasattr(obj, '__dict__'):
            return self._convert_decimals_in_dict(obj.__dict__)
        else:
            return obj
    
    def save_quotation(
        self, user_id: int, quote_data: dict, total_price: float, client_group: str
    ) -> Optional[int]:
        """Save quotation to database with proper decimal handling"""
        try:
            if not self.ensure_user_exists(user_id):
                return None
            
            import uuid
            quote_number = f"QT{datetime.now().strftime('%Y%m%d')}{str(uuid.uuid4())[:8].upper()}"
            expires_at = datetime.now() + timedelta(days=Config.QUOTE_VALIDITY_DAYS)
            
            # Convert all Decimal values in quote_data
            quote_data_clean = self._convert_decimals_in_dict(quote_data)
            
            connection = self.get_connection()
            if not connection:
                return None
            
            cursor = connection.cursor()
            cursor.execute(
                """
                INSERT INTO quotations (
                    user_id, quote_number, quote_data, total_price,
                    client_group, expires_at, status, dynamics_sync_status
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (user_id, quote_number, json.dumps(quote_data_clean), total_price, 
                 client_group, expires_at, "draft", "pending")
            )
            
            quote_id = cursor.lastrowid
            connection.commit()
            
            logger.info(f"✅ Quote {quote_number} (ID: {quote_id}) saved for user {user_id}")
            return quote_id
        
        except Exception as e:
            logger.error(f"❌ Error saving quotation: {e}")
            return None
        finally:
            if connection:
                cursor.close()
                connection.close()
    
    def get_user_activity_log(self, user_id: int, limit: int = 50) -> List[Dict]:
        """Get user's activity log"""
        results = self.execute_query(
            """
            SELECT activity_type, activity_data, created_at
            FROM user_activity_log
            WHERE user_id = %s
            ORDER BY created_at DESC
            LIMIT %s
            """,
            (user_id, limit),
            fetch=True
        ) or []
        
        # Parse JSON fields
        for result in results:
            if result.get('activity_data'):
                try:
                    result['activity_data'] = json.loads(result['activity_data'])
                except:
                    result['activity_data'] = {}
        
        return results
    
    def search_users_advanced(self, filters: Dict) -> List[Dict]:
        """Advanced user search with multiple filters"""
        query = """
            SELECT 
                u.user_id, u.telegram_username, u.first_name, u.last_name,
                u.full_name, u.is_company, u.company_name, u.email, u.phone,
                u.client_group, u.onboarding_completed, u.last_activity,
                COUNT(DISTINCT q.id) as quote_count,
                COUNT(DISTINCT cl.log_id) as message_count
            FROM users u
            LEFT JOIN quotations q ON u.user_id = q.user_id
            LEFT JOIN conversation_logs cl ON u.user_id = cl.user_id
            WHERE 1=1
        """
        
        params = []
        
        # Add filters
        if filters.get('search_term'):
            search_pattern = f"%{filters['search_term']}%"
            query += """ AND (
                u.first_name LIKE %s OR u.last_name LIKE %s OR 
                u.full_name LIKE %s OR u.company_name LIKE %s OR
                u.email LIKE %s OR u.phone LIKE %s
            )"""
            params.extend([search_pattern] * 6)
        
        if filters.get('is_company') is not None:
            query += " AND u.is_company = %s"
            params.append(filters['is_company'])
        
        if filters.get('client_group'):
            if '%' in filters['client_group']:
                query += " AND u.client_group LIKE %s"
            else:
                query += " AND u.client_group = %s"
            params.append(filters['client_group'])
        
        if filters.get('onboarding_completed') is not None:
            query += " AND u.onboarding_completed = %s"
            params.append(filters['onboarding_completed'])
        
        if filters.get('has_quotes'):
            query += " AND EXISTS (SELECT 1 FROM quotations WHERE user_id = u.user_id)"
        
        if filters.get('active_days'):
            query += " AND u.last_activity > DATE_SUB(NOW(), INTERVAL %s DAY)"
            params.append(filters['active_days'])
        
        query += """
            GROUP BY u.user_id
            ORDER BY u.last_activity DESC
            LIMIT 100
        """
        
        return self.execute_query(query, params, fetch=True) or []
    
    def get_users_for_admin(self, page: int = 1, per_page: int = 20, filters: Dict = None) -> Dict:
        """Get paginated users list for admin panel"""
        offset = (page - 1) * per_page
        
        # Base query for counting
        count_query = "SELECT COUNT(DISTINCT u.user_id) FROM users u WHERE 1=1"
        
        # Base query for fetching
        fetch_query = """
            SELECT 
                u.user_id, u.telegram_username, u.first_name, u.last_name,
                u.full_name, u.is_company, u.company_name, u.vat_number,
                u.email, u.phone, u.address, u.client_group,
                u.onboarding_completed, u.created_at, u.last_activity,
                COUNT(DISTINCT q.id) as quote_count,
                SUM(q.total_price) as total_revenue
            FROM users u
            LEFT JOIN quotations q ON u.user_id = q.user_id
            WHERE 1=1
        """
        
        params = []
        filter_conditions = ""
        
        # Apply filters if provided
        if filters:
            if filters.get('search'):
                filter_conditions += """ AND (
                    u.first_name LIKE %s OR u.last_name LIKE %s OR
                    u.company_name LIKE %s OR u.email LIKE %s
                )"""
                search_pattern = f"%{filters['search']}%"
                params.extend([search_pattern] * 4)
            
            if filters.get('client_group'):
                if '%' in filters['client_group']:
                    filter_conditions += " AND u.client_group LIKE %s"
                else:
                    filter_conditions += " AND u.client_group = %s"
                params.append(filters['client_group'])
            
            if filters.get('is_company') is not None:
                filter_conditions += " AND u.is_company = %s"
                params.append(filters['is_company'])
        
        count_query += filter_conditions
        fetch_query += filter_conditions
        
        # Get total count
        count_result = self.execute_query(count_query, params, fetch=True)
        total_count = count_result[0]['COUNT(DISTINCT u.user_id)'] if count_result else 0
        
        # Get users
        fetch_query += """
            GROUP BY u.user_id
            ORDER BY u.last_activity DESC
            LIMIT %s OFFSET %s
        """
        
        users = self.execute_query(
            fetch_query, 
            params + [per_page, offset], 
            fetch=True
        ) or []
        
        # Convert Decimal values to float for JSON serialization
        for user in users:
            if user.get('total_revenue') and isinstance(user['total_revenue'], Decimal):
                user['total_revenue'] = float(user['total_revenue'])
        
        return {
            'users': users,
            'total': total_count,
            'page': page,
            'per_page': per_page,
            'total_pages': (total_count + per_page - 1) // per_page if per_page > 0 else 0
        }
    
    def update_user_client_group(self, user_id: int, new_group: str, admin_id: int) -> bool:
        """Update user's client group with admin logging"""
        try:
            # Get old group
            old_group = self.get_user_client_group(user_id)
            
            # Update group
            result = self.execute_query(
                "UPDATE users SET client_group = %s WHERE user_id = %s",
                (new_group, user_id)
            )
            
            if result:
                # Log the change
                self.log_user_activity(
                    user_id,
                    'client_group_changed',
                    {
                        'old_group': old_group,
                        'new_group': new_group,
                        'changed_by': admin_id
                    }
                )
            
            return bool(result)
            
        except Exception as e:
            logger.error(f"Error updating user client group: {e}")
            return False
    
    def add_user_note(self, user_id: int, note: str, admin_id: int) -> bool:
        """Add a note to user profile"""
        try:
            # Get existing notes
            existing = self.execute_query(
                "SELECT notes FROM users WHERE user_id = %s",
                (user_id,),
                fetch=True
            )
            
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M')
            new_note = f"[{timestamp}] (Admin {admin_id}): {note}"
            
            if existing and existing[0].get('notes'):
                # Append to existing notes
                notes = existing[0]['notes'] + f"\n\n{new_note}"
            else:
                notes = new_note
            
            result = self.execute_query(
                "UPDATE users SET notes = %s WHERE user_id = %s",
                (notes, user_id)
            )
            
            return bool(result)
            
        except Exception as e:
            logger.error(f"Error adding user note: {e}")
            return False
    
    def add_user_tag(self, user_id: int, tag: str) -> bool:
        """Add a tag to user"""
        try:
            # Get existing tags
            user = self.get_user_profile(user_id)
            tags = user.get('tags', []) if user else []
            
            if tag not in tags:
                tags.append(tag)
                
                result = self.execute_query(
                    "UPDATE users SET tags = %s WHERE user_id = %s",
                    (json.dumps(tags, ensure_ascii=False), user_id)
                )
                
                return bool(result)
            
            return True
            
        except Exception as e:
            logger.error(f"Error adding user tag: {e}")
            return False
    
    def remove_user_tag(self, user_id: int, tag: str) -> bool:
        """Remove a tag from user"""
        try:
            # Get existing tags
            user = self.get_user_profile(user_id)
            tags = user.get('tags', []) if user else []
            
            if tag in tags:
                tags.remove(tag)
                
                result = self.execute_query(
                    "UPDATE users SET tags = %s WHERE user_id = %s",
                    (json.dumps(tags, ensure_ascii=False), user_id)
                )
                
                return bool(result)
            
            return True
            
        except Exception as e:
            logger.error(f"Error removing user tag: {e}")
            return False
    
    def get_user_statistics(self, user_id: int) -> Dict:
        """Get comprehensive user statistics"""
        stats = self.execute_query(
            """
            SELECT
                (SELECT COUNT(*) FROM quotations WHERE user_id = %s) as total_quotes,
                (SELECT COUNT(*) FROM quotations WHERE user_id = %s AND status = 'accepted') as accepted_quotes,
                (SELECT SUM(total_price) FROM quotations WHERE user_id = %s) as total_value,
                (SELECT COUNT(*) FROM conversation_logs WHERE user_id = %s) as total_messages,
                (SELECT MIN(created_at) FROM conversation_logs WHERE user_id = %s) as first_interaction,
                (SELECT MAX(created_at) FROM conversation_logs WHERE user_id = %s) as last_interaction,
                (SELECT COUNT(*) FROM user_activity_log WHERE user_id = %s) as total_activities
            """,
            (user_id, user_id, user_id, user_id, user_id, user_id, user_id),
            fetch=True
        )
        
        if stats and stats[0]:
            # Convert Decimal to float
            result = stats[0]
            if result.get('total_value') and isinstance(result['total_value'], Decimal):
                result['total_value'] = float(result['total_value'])
            return result
        
        return {}
    
    def create_user_group(self, group_name: str, description: str, created_by: int) -> Optional[int]:
        """Create a custom user group"""
        try:
            result = self.execute_query(
                """
                INSERT INTO user_custom_groups (group_name, description, created_by)
                VALUES (%s, %s, %s)
                """,
                (group_name, description, created_by)
            )
            
            if result:
                # Get the created group ID
                group_id = self.execute_query(
                    "SELECT LAST_INSERT_ID() as id",
                    fetch=True
                )
                return group_id[0]['id'] if group_id else None
            
            return None
            
        except Exception as e:
            logger.error(f"Error creating user group: {e}")
            return None
    
    def add_user_to_group(self, group_id: int, user_id: int, added_by: int) -> bool:
        """Add user to a custom group"""
        try:
            result = self.execute_query(
                """
                INSERT INTO user_custom_group_members (group_id, user_id, added_by)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE added_at = NOW()
                """,
                (group_id, user_id, added_by)
            )
            
            return bool(result)
            
        except Exception as e:
            logger.error(f"Error adding user to group: {e}")
            return False
    
    def get_user_groups(self, user_id: int) -> List[Dict]:
        """Get all groups a user belongs to"""
        return self.execute_query(
            """
            SELECT g.*, gm.added_at
            FROM user_custom_groups g
            JOIN user_custom_group_members gm ON g.id = gm.group_id
            WHERE gm.user_id = %s
            ORDER BY g.group_name
            """,
            (user_id,),
            fetch=True
        ) or []
    
    def get_group_members(self, group_id: int) -> List[Dict]:
        """Get all members of a group"""
        return self.execute_query(
            """
            SELECT u.user_id, u.first_name, u.last_name, u.company_name, 
                   u.email, gm.added_at
            FROM users u
            JOIN user_custom_group_members gm ON u.user_id = gm.user_id
            WHERE gm.group_id = %s
            ORDER BY u.first_name, u.last_name
            """,
            (group_id,),
            fetch=True
        ) or []
    
    # ==================== PRODUCT MANAGEMENT ====================
    
    def get_products_by_category(self, base_category: str, filters: Dict = None) -> List[Dict]:
        """Get products by category with optional filters"""
        query = "SELECT * FROM products WHERE base_category = %s"
        params = [base_category]
        
        if filters:
            for key, value in filters.items():
                if value is not None:
                    query += f" AND {key} = %s"
                    params.append(value)
        
        query += " ORDER BY description"
        
        results = self.execute_query(query, params, fetch=True)
        
        # Convert Decimal values to float
        if results:
            for result in results:
                for key, value in result.items():
                    if isinstance(value, Decimal):
                        result[key] = float(value)
        
        return results or []
    
    def get_product_by_code(self, code: str) -> Optional[Dict]:
        """Get product by code"""
        result = self.execute_query(
            "SELECT * FROM products WHERE product_code = %s LIMIT 1", 
            (code,), 
            fetch=True
        )
        if result and result[0]:
            product = result[0]
            # Convert all Decimal values to float
            for key, value in product.items():
                if isinstance(value, Decimal):
                    product[key] = float(value)
            return product
        return None
    
    def get_unique_values(self, base_category: str, column: str, filters: Dict = None) -> List[str]:
        """Get unique values for a column in a category"""
        query = f"SELECT DISTINCT {column} FROM products WHERE base_category = %s AND {column} IS NOT NULL"
        params = [base_category]
        
        if filters:
            for key, value in filters.items():
                if value is not None:
                    query += f" AND {key} = %s"
                    params.append(value)
        
        query += f" ORDER BY {column}"
        
        results = self.execute_query(query, params, fetch=True)
        return [r[column] for r in results] if results else []
    
    def get_type_ceilings_for_product_type(self, product_type: str) -> List[str]:
        """Get unique type_ceiling values for a specific product_type"""
        query = """
            SELECT DISTINCT type_ceiling
            FROM products
            WHERE base_category = 'ceiling'
            AND LOWER(product_type) = LOWER(%s)
            AND type_ceiling IS NOT NULL
            AND type_ceiling != ''
            ORDER BY type_ceiling
        """
        
        results = self.execute_query(query, (product_type,), fetch=True)
        type_ceilings = [r["type_ceiling"] for r in results] if results else []
        
        logger.info(f"🔍 Found {len(type_ceilings)} type_ceiling values for '{product_type}': {type_ceilings}")
        
        # If no type_ceiling values found, create default options
        if not type_ceilings:
            logger.warning(f"⚠️ No type_ceiling values found in database for product_type '{product_type}'")
            logger.info("🔧 Using default type_ceiling options")
            # Provide sensible defaults based on product_type
            if product_type.lower() == "fabric":
                type_ceilings = ["standard", "acoustic", "translucent", "printed"]
            elif product_type.lower() == "pvc":
                type_ceilings = ["glossy", "matte", "satin"]
            else:
                type_ceilings = ["standard", "premium", "special"]
        
        return type_ceilings
    
    def get_colors_for_type_ceiling(self, product_type: str, type_ceiling: str) -> List[str]:
        """Get unique colors for a specific product_type and type_ceiling combination"""
        query = """
            SELECT DISTINCT color
            FROM products
            WHERE base_category = 'ceiling'
            AND LOWER(product_type) = LOWER(%s)
            AND LOWER(type_ceiling) = LOWER(%s)
            AND color IS NOT NULL
            AND color != ''
            ORDER BY color
        """
        
        results = self.execute_query(query, (product_type, type_ceiling), fetch=True)
        colors = [r["color"] for r in results] if results else []
        
        logger.info(f"🎨 Found {len(colors)} colors for '{product_type}/{type_ceiling}': {colors}")
        
        # If no colors found, provide defaults
        if not colors:
            logger.warning(f"⚠️ No colors found for {product_type}/{type_ceiling}")
            colors = ["white", "black", "grey", "beige", "cream"]
        
        return colors
    
    def get_ceiling_product(self, product_type: str, type_ceiling: str, color: str) -> Optional[Dict]:
        """Get specific ceiling product based on all filters"""
        # Try exact match first
        query = """
            SELECT * FROM products
            WHERE base_category = 'ceiling'
            AND product_type = %s
            AND type_ceiling = %s
            AND color = %s
            LIMIT 1
        """
        
        result = self.execute_query(query, (product_type, type_ceiling, color), fetch=True)
        
        # If no exact match, try case-insensitive
        if not result:
            query = """
                SELECT * FROM products
                WHERE base_category = 'ceiling'
                AND LOWER(product_type) = LOWER(%s)
                AND LOWER(type_ceiling) = LOWER(%s)
                AND LOWER(color) = LOWER(%s)
                LIMIT 1
            """
            result = self.execute_query(query, (product_type, type_ceiling, color), fetch=True)
        
        if result and result[0]:
            product = result[0]
            # Convert all Decimal values to float
            for key, value in product.items():
                if isinstance(value, Decimal):
                    product[key] = float(value)
            
            logger.info(f"✅ Found ceiling product: {product.get('product_code')} - {product.get('description')}")
            return product
        else:
            logger.warning(f"⚠️ No ceiling product found for: {product_type}, {type_ceiling}, {color}")
            return None
    
    def get_acoustic_performance_products(self) -> List[Dict]:
        """Get products that have acoustic_performance values"""
        query = """
            SELECT * FROM products
            WHERE acoustic_performance IS NOT NULL
            AND acoustic_performance != ''
            AND acoustic_performance != 'NULL'
            ORDER BY acoustic_performance
        """
        
        results = self.execute_query(query, fetch=True)
        if results:
            # Convert Decimal values to float
            for result in results:
                for key, value in result.items():
                    if isinstance(value, Decimal):
                        result[key] = float(value)
        return results or []
    
    # ==================== QUOTE MANAGEMENT ====================
    
    def save_quotation(
        self, user_id: int, quote_data: dict, total_price: float, client_group: str
    ) -> Optional[int]:
        """Save quotation to database"""
        try:
            if not self.ensure_user_exists(user_id):
                return None
            
            import uuid
            quote_number = f"QT{datetime.now().strftime('%Y%m%d')}{str(uuid.uuid4())[:8].upper()}"
            expires_at = datetime.now() + timedelta(days=Config.QUOTE_VALIDITY_DAYS)
            
            quote_data_clean = serialize_for_json(quote_data)
            
            connection = self.get_connection()
            if not connection:
                return None
            
            cursor = connection.cursor()
            cursor.execute(
                """
                INSERT INTO quotations (
                    user_id, quote_number, quote_data, total_price,
                    client_group, expires_at, status
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (user_id, quote_number, json.dumps(quote_data_clean), total_price, 
                 client_group, expires_at, "draft")
            )
            
            quote_id = cursor.lastrowid
            connection.commit()
            
            logger.info(f"✅ Quote {quote_number} (ID: {quote_id}) saved for user {user_id}")
            return quote_id
        
        except Exception as e:
            logger.error(f"❌ Error saving quotation: {e}")
            return None
        finally:
            if connection:
                cursor.close()
                connection.close()
    
    def get_user_quotes(self, user_id: int, status: Optional[str] = None) -> List[Dict]:
        """Get user's quotes with optional status filter"""
        query = """
            SELECT id as quotation_id, quote_number, quote_data, total_price,
                   status, created_at, expires_at, notes, last_modified
            FROM quotations
            WHERE user_id = %s
        """
        params = [user_id]
        
        if status:
            query += " AND status = %s"
            params.append(status)
        
        query += " ORDER BY created_at DESC"
        
        return self.execute_query(query, params, fetch=True) or []
    
    def get_quote_by_id(self, quote_id: int) -> Optional[Dict]:
        """Get quote by ID"""
        result = self.execute_query(
            """
            SELECT *, id as quotation_id FROM quotations
            WHERE id = %s
            """,
            (quote_id,),
            fetch=True
        )
        
        return result[0] if result else None
    
    def update_quote_status(
        self, quote_id: int, new_status: str, user_id: int, notes: str = None
    ) -> bool:
        """Update quote status with history tracking"""
        connection = self.get_connection()
        if not connection:
            return False
        
        try:
            cursor = connection.cursor()
            
            # Get current status
            cursor.execute("SELECT status FROM quotations WHERE id = %s", (quote_id,))
            result = cursor.fetchone()
            old_status = result[0] if result else None
            
            # Update quote status
            cursor.execute(
                """
                UPDATE quotations
                SET status = %s, notes = %s, last_modified = NOW()
                WHERE id = %s
                """,
                (new_status, notes, quote_id)
            )
            
            # Add to history
            cursor.execute(
                """
                INSERT INTO quote_status_history
                (quotation_id, old_status, new_status, changed_by, notes)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (quote_id, old_status, new_status, user_id, notes)
            )
            
            connection.commit()
            return True
        
        except Exception as e:
            logger.error(f"❌ Error updating quote status: {e}")
            connection.rollback()
            return False
        finally:
            cursor.close()
            connection.close()
    
    def update_quote_data(self, quote_id: int, quote_data: Dict, total_price: float) -> bool:
        """Update quote data with better error handling - IMPROVED VERSION"""
        try:
            # Serialize the quote data
            quote_data_json = json.dumps(serialize_for_json(quote_data))
            
            # Log the update attempt
            logger.info(f"Attempting to update quote {quote_id} with new total €{total_price:.2f}")
            
            # First check if the quote exists
            existing = self.execute_query(
                "SELECT id FROM quotations WHERE id = %s",
                (quote_id,),
                fetch=True
            )
            
            if not existing:
                logger.error(f"Quote {quote_id} not found in database")
                return False
            
            # Perform the update
            result = self.execute_query(
                """
                UPDATE quotations
                SET quote_data = %s, total_price = %s, last_modified = NOW()
                WHERE id = %s
                """,
                (quote_data_json, total_price, quote_id)
            )
            
            if result and result > 0:
                logger.info(f"✅ Successfully updated quote {quote_id} - {result} row(s) affected")
                return True
            else:
                logger.error(f"❌ Failed to update quote {quote_id} - no rows affected")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error updating quote {quote_id}: {e}")
            logger.error(f"Quote data sample: {str(quote_data)[:200]}...")
            logger.error(traceback.format_exc())
            return False
    
    def get_quote_status_history(self, quote_id: int) -> List[Dict]:
        """Get status change history for a quote"""
        return self.execute_query(
            """
            SELECT qsh.*, u.first_name, u.last_name
            FROM quote_status_history qsh
            LEFT JOIN users u ON qsh.changed_by = u.user_id
            WHERE qsh.quotation_id = %s
            ORDER BY qsh.changed_at DESC
            """,
            (quote_id,),
            fetch=True
        ) or []
    
    # ==================== SESSION MANAGEMENT ====================
    
    def save_quote_session(self, user_id: int, session_data: dict, current_step: str) -> bool:
        """Save quote session with enhanced edit history and state tracking"""
        expires_at = datetime.now() + timedelta(hours=Config.QUOTE_SESSION_TIMEOUT_HOURS)
        
        # Ensure edit_history and previous_steps exist
        if "edit_history" not in session_data:
            session_data["edit_history"] = []
        
        if "previous_steps" not in session_data:
            session_data["previous_steps"] = []
        
        # Serialize the session data
        session_data_json = json.dumps(serialize_for_json(session_data))
        
        result = self.execute_query(
            """
            INSERT INTO quote_sessions (user_id, session_data, current_step, expires_at)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            session_data = VALUES(session_data),
            current_step = VALUES(current_step),
            updated_at = NOW(),
            expires_at = VALUES(expires_at)
            """,
            (user_id, session_data_json, current_step, expires_at)
        )
        
        return bool(result)
    
    def get_quote_session(self, user_id: int) -> Optional[Dict]:
        """Get quote session"""
        # Clean up expired sessions
        self.execute_query("DELETE FROM quote_sessions WHERE expires_at < NOW()")
        
        result = self.execute_query(
            "SELECT * FROM quote_sessions WHERE user_id = %s AND expires_at > NOW()", 
            (user_id,), 
            fetch=True
        )
        return result[0] if result else None
    
    def delete_quote_session(self, user_id: int) -> bool:
        """Delete quote session"""
        result = self.execute_query(
            "DELETE FROM quote_sessions WHERE user_id = %s", 
            (user_id,)
        )
        return bool(result)
    
    def get_active_sessions_count(self) -> int:
        """Get count of active quote sessions"""
        result = self.execute_query(
            "SELECT COUNT(*) as count FROM quote_sessions WHERE expires_at > NOW()",
            fetch=True
        )
        return result[0]["count"] if result else 0
    
    # ==================== CONVERSATION LOGGING ====================
    
    def log_conversation(
        self, user_id: int, message_type: str, message: str, context: Dict = None
    ) -> bool:
        """Log conversation to database"""
        try:
            result = self.execute_query(
                """
                INSERT INTO conversation_logs (user_id, message_type, message, context)
                VALUES (%s, %s, %s, %s)
                """,
                (user_id, message_type, message, json.dumps(context) if context else None)
            )
            return bool(result)
        except Exception as e:
            logger.error(f"❌ Error logging conversation: {e}")
            return False
    
    def get_conversation_history(
        self, user_id: int, limit: int = 20, offset: int = 0
    ) -> List[Dict]:
        """Get user's conversation history"""
        return self.execute_query(
            """
            SELECT log_id, message_type, message, context, created_at
            FROM conversation_logs
            WHERE user_id = %s
            ORDER BY created_at DESC
            LIMIT %s OFFSET %s
            """,
            (user_id, limit, offset),
            fetch=True
        ) or []
    
    def get_user_chat_context(self, user_id: int) -> Dict:
        """Get user's chat context including conversation history"""
        # Get recent conversation logs
        recent_logs = self.execute_query(
            """
            SELECT message_type, message, created_at
            FROM conversation_logs
            WHERE user_id = %s
            ORDER BY created_at DESC
            LIMIT 20
            """,
            (user_id,),
            fetch=True
        )
        
        # Get user's quotes
        user_quotes = self.execute_query(
            """
            SELECT quote_number, total_price, status, created_at
            FROM quotations
            WHERE user_id = %s
            ORDER BY created_at DESC
            LIMIT 5
            """,
            (user_id,),
            fetch=True
        )
        
        # Get stored context
        stored_context = self.execute_query(
            """
            SELECT context_data, website_data
            FROM ai_chat_contexts
            WHERE user_id = %s OR user_id = 0
            ORDER BY user_id DESC
            """,
            (user_id,),
            fetch=True
        )
        
        context = {
            "conversation_history": recent_logs or [],
            "user_quotes": user_quotes or [],
            "website_data": {}
        }
        
        if stored_context:
            for ctx in stored_context:
                if ctx.get("website_data"):
                    context["website_data"] = json.loads(ctx["website_data"])
                if ctx.get("context_data"):
                    context.update(json.loads(ctx["context_data"]))
        
        return context
    
    def save_website_data(self, website_data: Dict) -> bool:
        """Save scraped website data"""
        try:
            result = self.execute_query(
                """
                INSERT INTO ai_chat_contexts (user_id, website_data)
                VALUES (0, %s)
                ON DUPLICATE KEY UPDATE
                website_data = VALUES(website_data),
                last_updated = NOW()
                """,
                (json.dumps(website_data),)
            )
            return bool(result)
        except Exception as e:
            logger.error(f"❌ Error saving website data: {e}")
            return False
    
    # ==================== ADMIN FEATURES ====================
    
    def get_system_statistics(self) -> Dict:
        """Get comprehensive system statistics"""
        stats = self.execute_query(
            """
            SELECT
                (SELECT COUNT(*) FROM users) as total_users,
                (SELECT COUNT(*) FROM users WHERE last_activity > DATE_SUB(NOW(), INTERVAL 7 DAY)) as active_users_week,
                (SELECT COUNT(*) FROM users WHERE last_activity > DATE_SUB(NOW(), INTERVAL 30 DAY)) as active_users_month,
                (SELECT COUNT(*) FROM quotations) as total_quotes,
                (SELECT COUNT(*) FROM quotations WHERE created_at > DATE_SUB(NOW(), INTERVAL 7 DAY)) as quotes_week,
                (SELECT COUNT(*) FROM quotations WHERE status = 'accepted') as accepted_quotes,
                (SELECT COUNT(*) FROM conversation_logs WHERE created_at > DATE_SUB(NOW(), INTERVAL 24 HOUR)) as messages_24h,
                (SELECT SUM(total_price) FROM quotations WHERE status = 'accepted') as total_revenue,
                (SELECT COUNT(*) FROM quote_sessions WHERE expires_at > NOW()) as active_sessions,
                (SELECT COUNT(DISTINCT base_category) FROM products) as product_categories,
                (SELECT COUNT(*) FROM products) as total_products
            """,
            fetch=True
        )
        
        return stats[0] if stats else {}
    
    def get_revenue_by_period(self, days: int = 30) -> List[Dict]:
        """Get revenue statistics by period"""
        return self.execute_query(
            """
            SELECT 
                DATE(created_at) as date,
                COUNT(*) as quote_count,
                SUM(total_price) as total_revenue,
                AVG(total_price) as avg_quote_value
            FROM quotations
            WHERE created_at > DATE_SUB(NOW(), INTERVAL %s DAY)
            GROUP BY DATE(created_at)
            ORDER BY date DESC
            """,
            (days,),
            fetch=True
        ) or []
    
    # ==================== DIAGNOSTICS ====================
    
    def run_diagnostics(self) -> bool:
        """Run comprehensive database diagnostics"""
        logger.info("🔍 Running database diagnostics...")
        
        try:
            # Check product counts by category
            results = self.execute_query(
                """
                SELECT base_category, COUNT(*) as count
                FROM products
                GROUP BY base_category
                ORDER BY base_category
                """,
                fetch=True
            )
            
            if results:
                logger.info("✅ Products by category:")
                for row in results:
                    logger.info(f"  {row['base_category']}: {row['count']} products")
            
            # Check critical products
            critical_codes = ["S Plafond 12245", "S Plafond 190", "S Plafond 13869"]
            for code in critical_codes:
                product = self.get_product_by_code(code)
                if product:
                    logger.info(f"✅ Found critical product: {code} - {product['description']}")
                else:
                    logger.error(f"❌ Missing critical product: {code}")
            
            # Check type_ceiling values
            type_ceilings = self.execute_query(
                """
                SELECT product_type, COUNT(DISTINCT type_ceiling) as type_count
                FROM products
                WHERE base_category = 'ceiling'
                AND type_ceiling IS NOT NULL
                AND type_ceiling != ''
                GROUP BY product_type
                """,
                fetch=True
            )
            
            if type_ceilings:
                logger.info("✅ Type ceiling variations:")
                for row in type_ceilings:
                    logger.info(f"  {row['product_type']}: {row['type_count']} types")
            
            # Check acoustic performance products
            acoustic_products = self.get_acoustic_performance_products()
            logger.info(f"✅ Found {len(acoustic_products)} acoustic performance products")
            
            # Check tables
            tables = [
                "users", "products", "quotations", "quote_sessions", 
                "conversation_logs", "ai_chat_contexts", "quote_status_history",
                "admin_messages", "admin_message_recipients", 
                "admin_user_groups", "admin_user_group_members",
                "user_conversation_memory", "user_activity_log",
                "user_custom_groups", "user_custom_group_members"
            ]
            
            for table in tables:
                result = self.execute_query(f"SHOW TABLES LIKE '{table}'", fetch=True)
                if result:
                    logger.info(f"✅ Table '{table}' exists")
                else:
                    logger.error(f"❌ Table '{table}' missing")
            
            # Check database size
            db_size = self.execute_query(
                """
                SELECT 
                    table_schema AS 'Database',
                    SUM(data_length + index_length) / 1024 / 1024 AS 'Size (MB)'
                FROM information_schema.TABLES 
                WHERE table_schema = DATABASE()
                GROUP BY table_schema
                """,
                fetch=True
            )
            
            if db_size:
                logger.info(f"📊 Database size: {db_size[0]['Size (MB)']:.2f} MB")
            
            # Check if we have any products at all
            if not results:
                logger.error("❌ No products found in database!")
                return False
            
            logger.info("✅ Database diagnostics completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error running diagnostics: {e}")
            return False
    
    # ==================== UTILITY METHODS ====================
    
    def backup_database(self, backup_path: str = None) -> bool:
        """Create a database backup (structure only, not data)"""
        try:
            if not backup_path:
                backup_path = f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.sql"
            
            # This is a placeholder - actual implementation would use mysqldump
            logger.info(f"📦 Database backup would be created at: {backup_path}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error creating backup: {e}")
            return False
    
    def clean_old_data(self, days_to_keep: int = 90) -> Dict[str, int]:
        """Clean old data from various tables"""
        cleaned = {}
        
        try:
            # Clean old conversation logs
            result = self.execute_query(
                """
                DELETE FROM conversation_logs 
                WHERE created_at < DATE_SUB(NOW(), INTERVAL %s DAY)
                """,
                (days_to_keep,)
            )
            cleaned["conversation_logs"] = result or 0
            
            # Clean expired sessions
            result = self.execute_query(
                "DELETE FROM quote_sessions WHERE expires_at < NOW()"
            )
            cleaned["expired_sessions"] = result or 0
            
            # Clean old status history (keep last 100 per quote)
            result = self.execute_query(
                """
                DELETE qsh1 FROM quote_status_history qsh1
                INNER JOIN (
                    SELECT quotation_id, changed_at,
                           ROW_NUMBER() OVER (PARTITION BY quotation_id ORDER BY changed_at DESC) as rn
                    FROM quote_status_history
                ) qsh2 ON qsh1.quotation_id = qsh2.quotation_id AND qsh1.changed_at = qsh2.changed_at
                WHERE qsh2.rn > 100
                """
            )
            cleaned["old_status_history"] = result or 0
            
            logger.info(f"🧹 Cleaned old data: {cleaned}")
            return cleaned
            
        except Exception as e:
            logger.error(f"❌ Error cleaning old data: {e}")
            return cleaned

# Add this method to your EnhancedDatabaseManager class in manager.py:

    def _add_dynamics_sync_tables(self):
        """Create Dynamics 365 sync tracking tables"""
        try:
            connection = pymysql.connect(**self.config)
            cursor = connection.cursor()
            
            # Add Dynamics 365 fields to users table
            dynamics_user_fields = [
                ("dynamics_contact_id", "VARCHAR(36)"),
                ("dynamics_account_id", "VARCHAR(36)"),
                ("dynamics_sync_status", "ENUM('pending', 'synced', 'error') DEFAULT 'pending'"),
                ("dynamics_last_sync", "TIMESTAMP NULL"),
                ("dynamics_sync_error", "TEXT")
            ]
            
            for column_name, column_def in dynamics_user_fields:
                cursor.execute(f"""
                    SELECT COLUMN_NAME 
                    FROM INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_SCHEMA = DATABASE() 
                    AND TABLE_NAME = 'users' 
                    AND COLUMN_NAME = '{column_name}'
                """)
                
                if not cursor.fetchone():
                    cursor.execute(f"ALTER TABLE users ADD COLUMN {column_name} {column_def}")
                    logger.info(f"✅ Added column {column_name} to users table")
            
            # Add Dynamics 365 fields to quotations table
            dynamics_quote_fields = [
                ("dynamics_quote_id", "VARCHAR(36)"),
                ("dynamics_sync_status", "ENUM('pending', 'synced', 'error') DEFAULT 'pending'"),
                ("dynamics_last_sync", "TIMESTAMP NULL"),
                ("dynamics_sync_error", "TEXT")
            ]
            
            for column_name, column_def in dynamics_quote_fields:
                cursor.execute(f"""
                    SELECT COLUMN_NAME 
                    FROM INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_SCHEMA = DATABASE() 
                    AND TABLE_NAME = 'quotations' 
                    AND COLUMN_NAME = '{column_name}'
                """)
                
                if not cursor.fetchone():
                    cursor.execute(f"ALTER TABLE quotations ADD COLUMN {column_name} {column_def}")
                    logger.info(f"✅ Added column {column_name} to quotations table")
            
            # Create sync log table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS dynamics_sync_log (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    entity_type ENUM('user', 'quote', 'account', 'contact') NOT NULL,
                    entity_id VARCHAR(255) NOT NULL,
                    dynamics_id VARCHAR(36),
                    action VARCHAR(50),
                    status ENUM('success', 'error') NOT NULL,
                    error_message TEXT,
                    sync_data JSON,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_entity (entity_type, entity_id),
                    INDEX idx_dynamics_id (dynamics_id),
                    INDEX idx_created_at (created_at)
                )
            """)
            logger.info("✅ Created/verified dynamics_sync_log table")
            
            connection.commit()
            cursor.close()
            connection.close()
            
            logger.info("✅ Dynamics 365 sync tables created/updated successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error creating Dynamics sync tables: {e}")
            return False

    # Add these methods to EnhancedDatabaseManager:
    
    def update_user_dynamics_id(self, user_id: int, contact_id: str = None, account_id: str = None, status: str = 'synced', error: str = None) -> bool:
        """Update user's Dynamics 365 IDs"""
        try:
            query = """
                UPDATE users 
                SET dynamics_sync_status = %s,
                    dynamics_last_sync = NOW()
            """
            params = [status]
            
            if contact_id:
                query += ", dynamics_contact_id = %s"
                params.append(contact_id)
            
            if account_id:
                query += ", dynamics_account_id = %s"
                params.append(account_id)
            
            if error:
                query += ", dynamics_sync_error = %s"
                params.append(error)
            
            query += " WHERE user_id = %s"
            params.append(user_id)
            
            result = self.execute_query(query, params)
            
            # Log sync
            self.log_dynamics_sync('user', str(user_id), contact_id or account_id, 
                                 'create_update', 'success' if status == 'synced' else 'error', error)
            
            return bool(result)
            
        except Exception as e:
            logger.error(f"Error updating user Dynamics ID: {e}")
            return False
    
    def update_quote_dynamics_id(self, quote_id: int, dynamics_quote_id: str, status: str = 'synced', error: str = None) -> bool:
        """Update quote's Dynamics 365 ID"""
        try:
            query = """
                UPDATE quotations 
                SET dynamics_quote_id = %s,
                    dynamics_sync_status = %s,
                    dynamics_last_sync = NOW()
            """
            params = [dynamics_quote_id, status]
            
            if error:
                query += ", dynamics_sync_error = %s"
                params.append(error)
            
            query += " WHERE id = %s"
            params.append(quote_id)
            
            result = self.execute_query(query, params)
            
            # Log sync
            self.log_dynamics_sync('quote', str(quote_id), dynamics_quote_id, 
                                 'create', 'success' if status == 'synced' else 'error', error)
            
            return bool(result)
            
        except Exception as e:
            logger.error(f"Error updating quote Dynamics ID: {e}")
            return False
    
    def get_user_dynamics_ids(self, user_id: int) -> Dict:
        """Get user's Dynamics 365 IDs"""
        result = self.execute_query(
            """
            SELECT dynamics_contact_id, dynamics_account_id, dynamics_sync_status
            FROM users
            WHERE user_id = %s
            """,
            (user_id,),
            fetch=True
        )
        
        if result and result[0]:
            return {
                'contact_id': result[0].get('dynamics_contact_id'),
                'account_id': result[0].get('dynamics_account_id'),
                'sync_status': result[0].get('dynamics_sync_status')
            }
        
        return {'contact_id': None, 'account_id': None, 'sync_status': 'pending'}
    
    def get_pending_dynamics_syncs(self, entity_type: str = 'user', limit: int = 50) -> List[Dict]:
        """Get entities pending Dynamics sync"""
        if entity_type == 'user':
            return self.execute_query(
                """
                SELECT user_id, first_name, last_name, email, phone, 
                       is_company, company_name, vat_number, address,
                       dynamics_sync_error
                FROM users
                WHERE dynamics_sync_status = 'pending' 
                   OR dynamics_sync_status = 'error'
                ORDER BY created_at DESC
                LIMIT %s
                """,
                (limit,),
                fetch=True
            ) or []
        elif entity_type == 'quote':
            return self.execute_query(
                """
                SELECT q.*, u.dynamics_contact_id, u.dynamics_account_id
                FROM quotations q
                JOIN users u ON q.user_id = u.user_id
                WHERE q.dynamics_sync_status = 'pending' 
                   OR q.dynamics_sync_status = 'error'
                ORDER BY q.created_at DESC
                LIMIT %s
                """,
                (limit,),
                fetch=True
            ) or []
        
        return []
    
    def log_dynamics_sync(self, entity_type: str, entity_id: str, dynamics_id: str, 
                         action: str, status: str, error_message: str = None, sync_data: Dict = None) -> bool:
        """Log Dynamics 365 sync activity"""
        try:
            result = self.execute_query(
                """
                INSERT INTO dynamics_sync_log 
                (entity_type, entity_id, dynamics_id, action, status, error_message, sync_data)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (entity_type, entity_id, dynamics_id, action, status, error_message,
                 json.dumps(sync_data) if sync_data else None)
            )
            return bool(result)
        except Exception as e:
            logger.error(f"Error logging Dynamics sync: {e}")
            return False

# ==================== ADMIN MESSAGING TABLES ====================

def add_admin_messaging_tables(db: EnhancedDatabaseManager) -> bool:
    """Create admin messaging tables if they don't exist"""
    logger = logging.getLogger(__name__)
    
    try:
        # Create admin_messages table
        db.execute_query("""
            CREATE TABLE IF NOT EXISTS admin_messages (
                id INT AUTO_INCREMENT PRIMARY KEY,
                message_id VARCHAR(255) UNIQUE,
                admin_id BIGINT NOT NULL,
                recipient_id BIGINT,
                message_type ENUM('individual', 'broadcast', 'group') NOT NULL,
                message_text TEXT NOT NULL,
                status ENUM('sent', 'delivered', 'read', 'failed') DEFAULT 'sent',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                delivered_at TIMESTAMP NULL,
                read_at TIMESTAMP NULL,
                INDEX idx_admin_id (admin_id),
                INDEX idx_recipient_id (recipient_id),
                INDEX idx_created_at (created_at)
            )
        """)
        logger.info("✅ Created/verified admin_messages table")
        
        # Create admin_message_recipients table for group/broadcast messages
        db.execute_query("""
            CREATE TABLE IF NOT EXISTS admin_message_recipients (
                id INT AUTO_INCREMENT PRIMARY KEY,
                message_id VARCHAR(255) NOT NULL,
                recipient_id BIGINT NOT NULL,
                status ENUM('sent', 'delivered', 'read', 'failed') DEFAULT 'sent',
                delivered_at TIMESTAMP NULL,
                read_at TIMESTAMP NULL,
                INDEX idx_message_id (message_id),
                INDEX idx_recipient_id (recipient_id)
            )
        """)
        logger.info("✅ Created/verified admin_message_recipients table")
        
        # Create admin_user_groups table
        db.execute_query("""
            CREATE TABLE IF NOT EXISTS admin_user_groups (
                id INT AUTO_INCREMENT PRIMARY KEY,
                group_name VARCHAR(100) NOT NULL UNIQUE,
                description TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                created_by BIGINT NOT NULL
            )
        """)
        logger.info("✅ Created/verified admin_user_groups table")
        
        # Create admin_user_group_members table
        db.execute_query("""
            CREATE TABLE IF NOT EXISTS admin_user_group_members (
                id INT AUTO_INCREMENT PRIMARY KEY,
                group_id INT NOT NULL,
                user_id BIGINT NOT NULL,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                added_by BIGINT NOT NULL,
                FOREIGN KEY (group_id) REFERENCES admin_user_groups(id) ON DELETE CASCADE,
                UNIQUE KEY unique_group_user (group_id, user_id),
                INDEX idx_group_id (group_id),
                INDEX idx_user_id (user_id)
            )
        """)
        logger.info("✅ Created/verified admin_user_group_members table")
        
        logger.info("✅ Admin messaging tables created/updated")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error creating admin messaging tables: {e}")
        return False