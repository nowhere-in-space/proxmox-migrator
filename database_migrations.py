import sqlite3
import os
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class DatabaseMigration:
    """Database migration manager"""
    
    def __init__(self, db_path):
        self.db_path = db_path
        self.migrations = [
            self._migration_001_add_ssh_port,
            self._migration_002_add_is_first_login,
        ]
    
    def get_current_version(self):
        """Get current database version"""
        if not os.path.exists(self.db_path):
            return 0
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Check if migration_version table exists
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name='migration_version'
            """)
            
            if not cursor.fetchone():
                # Create migration_version table
                cursor.execute("""
                    CREATE TABLE migration_version (
                        id INTEGER PRIMARY KEY,
                        version INTEGER NOT NULL,
                        applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                cursor.execute("INSERT INTO migration_version (version) VALUES (0)")
                conn.commit()
                version = 0
            else:
                cursor.execute("SELECT MAX(version) FROM migration_version")
                result = cursor.fetchone()
                version = result[0] if result[0] is not None else 0
            
            conn.close()
            return version
            
        except Exception as e:
            logger.error(f"Error getting database version: {e}")
            return 0
    
    def set_version(self, version):
        """Set database version"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO migration_version (version) VALUES (?)", 
                (version,)
            )
            conn.commit()
            conn.close()
            logger.info(f"Database version set to {version}")
        except Exception as e:
            logger.error(f"Error setting database version: {e}")
    
    def run_migrations(self):
        """Run all pending migrations"""
        if not os.path.exists(os.path.dirname(self.db_path)):
            os.makedirs(os.path.dirname(self.db_path))
        
        current_version = self.get_current_version()
        target_version = len(self.migrations)
        
        if current_version >= target_version:
            logger.info(f"Database is up to date (version {current_version})")
            return
        
        logger.info(f"Running database migrations from version {current_version} to {target_version}")
        
        for i in range(current_version, target_version):
            migration_func = self.migrations[i]
            migration_number = i + 1
            
            try:
                logger.info(f"Running migration {migration_number}: {migration_func.__name__}")
                migration_func()
                self.set_version(migration_number)
                logger.info(f"Migration {migration_number} completed successfully")
            except Exception as e:
                logger.error(f"Migration {migration_number} failed: {e}")
                raise
        
        logger.info("All database migrations completed successfully")
    
    def _migration_001_add_ssh_port(self):
        """Migration 001: Add ssh_port column to cluster table"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Check if ssh_port column already exists
            cursor.execute("PRAGMA table_info(cluster)")
            columns = [column[1] for column in cursor.fetchall()]
            
            if 'ssh_port' not in columns:
                cursor.execute("ALTER TABLE cluster ADD COLUMN ssh_port INTEGER DEFAULT 22")
                logger.info("Added ssh_port column to cluster table")
            else:
                logger.info("ssh_port column already exists in cluster table")
            
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()
    
    def _migration_002_add_is_first_login(self):
        """Migration 002: Add is_first_login column to admin_user table"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Check if is_first_login column already exists
            cursor.execute("PRAGMA table_info(admin_user)")
            columns = [column[1] for column in cursor.fetchall()]
            
            if 'is_first_login' not in columns:
                cursor.execute("ALTER TABLE admin_user ADD COLUMN is_first_login BOOLEAN DEFAULT 1")
                logger.info("Added is_first_login column to admin_user table")
            else:
                logger.info("is_first_login column already exists in admin_user table")
            
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()

def run_database_migrations(db_path):
    """Run database migrations - to be called from app startup"""
    try:
        migration = DatabaseMigration(db_path)
        migration.run_migrations()
        return True
    except Exception as e:
        logger.error(f"Database migration failed: {e}")
        return False
