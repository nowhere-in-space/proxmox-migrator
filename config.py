import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'your-secret-key-here-change-in-production'
    
    # Get absolute path to database
    basedir = os.path.abspath(os.path.dirname(__file__))
    instance_dir = os.path.join(basedir, 'instance')
    
    # Ensure instance directory exists
    os.makedirs(instance_dir, exist_ok=True)
    
    # Database configuration with proper path handling
    db_path = os.path.join(instance_dir, 'proxmox_clusters.db')
    SQLALCHEMY_DATABASE_URI = os.environ.get('DATABASE_URL') or f'sqlite:///{db_path}'
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    
    # Application settings
    DEBUG = True
    
    # Session settings
    SESSION_COOKIE_SECURE = False  # Set to True in production with HTTPS
    SESSION_COOKIE_HTTPONLY = True
    SESSION_COOKIE_SAMESITE = 'Lax'
    PERMANENT_SESSION_LIFETIME = 3600  # 1 hour
    
    # Migration settings
    temp_migration_dir = os.path.join(basedir, 'temp_migration')
    os.makedirs(temp_migration_dir, exist_ok=True)
    TEMP_MIGRATION_DIR = temp_migration_dir
    
    # Logs directory
    logs_dir = os.path.join(basedir, 'logs')
    os.makedirs(logs_dir, exist_ok=True)
    LOG_FILE = os.path.join(logs_dir, 'proxmox_migrator.log')
    
    MAX_LOG_ENTRIES = 20
    MIGRATION_POLL_INTERVAL = 1000  # milliseconds
