"""
Database Update Script for Dynamics 365 Integration
Run this script once to add necessary tables and columns
"""
import logging
from database import EnhancedDatabaseManager
import pymysql

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def update_database_for_dynamics():
    """Add Dynamics 365 sync tables and columns to existing database"""
    
    # Initialize database manager
    db = EnhancedDatabaseManager()
    
    try:
        connection = pymysql.connect(**db.config)
        cursor = connection.cursor()
        
        logger.info("🔄 Starting database update for Dynamics 365 integration...")
        
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
            else:
                logger.info(f"⏭️  Column {column_name} already exists in users table")
        
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
            else:
                logger.info(f"⏭️  Column {column_name} already exists in quotations table")
        
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
        
        # Create last sync tracking table for bidirectional sync
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dynamics_sync_tracking (
                id INT AUTO_INCREMENT PRIMARY KEY,
                entity_type VARCHAR(50) UNIQUE,
                last_sync_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                sync_status VARCHAR(50),
                sync_details JSON
            )
        """)
        logger.info("✅ Created/verified dynamics_sync_tracking table")
        
        # Initialize sync tracking for each entity type
        entity_types = ['contact', 'account', 'quote']
        for entity_type in entity_types:
            cursor.execute("""
                INSERT IGNORE INTO dynamics_sync_tracking (entity_type)
                VALUES (%s)
            """, (entity_type,))
        
        connection.commit()
        cursor.close()
        connection.close()
        
        logger.info("✅ Database update completed successfully!")
        logger.info("📋 Added the following capabilities:")
        logger.info("  - User sync tracking (contact_id, account_id, sync status)")
        logger.info("  - Quote sync tracking (quote_id, sync status)")
        logger.info("  - Sync error logging and history")
        logger.info("  - Bidirectional sync tracking")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Error updating database: {e}")
        return False

if __name__ == "__main__":
    # Run the update
    success = update_database_for_dynamics()
    
    if success:
        logger.info("\n✅ Database is ready for Dynamics 365 integration!")
        logger.info("📝 Next steps:")
        logger.info("1. Add Dynamics 365 configuration to your .env file")
        logger.info("2. Add the integration files to your project")
        logger.info("3. Update your bot.py with the integration code")
        logger.info("4. Restart your bot")
    else:
        logger.error("\n❌ Database update failed. Please check the errors above.")