"""
Script for updating the database schema
Adds missing columns to the existing database
"""
import sqlite3
import os

def migrate_database():
    # Database path
    db_path = os.path.join(os.path.dirname(__file__), 'instance', 'proxmox_clusters.db')
    
    if not os.path.exists(db_path):
        print("Database not found, it will be created automatically when the application starts")
        return
    
    # Connect to the database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # Check if the is_first_login column exists in the admin_user table
        cursor.execute("PRAGMA table_info(admin_user)")
        columns = [column[1] for column in cursor.fetchall()]
        
        if 'is_first_login' not in columns:
            print("Adding is_first_login column to the admin_user table...")
            cursor.execute("ALTER TABLE admin_user ADD COLUMN is_first_login BOOLEAN DEFAULT 1")
            print("Column added successfully!")
        else:
            print("The is_first_login column already exists")
        
        conn.commit()
        print("Migration completed successfully!")
        
    except Exception as e:
        print(f"Migration error: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    migrate_database()
