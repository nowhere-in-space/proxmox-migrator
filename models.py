from flask_sqlalchemy import SQLAlchemy
from werkzeug.security import generate_password_hash, check_password_hash

db = SQLAlchemy()

class Cluster(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    api_host = db.Column(db.String(200), nullable=False)
    api_token_id = db.Column(db.String(100), nullable=False)  # Legacy field from database
    api_token_secret = db.Column(db.String(500), nullable=False)
    ssh_password = db.Column(db.String(200), nullable=False)
    
    def __init__(self, **kwargs):
        # Normalize api_host before saving
        if 'api_host' in kwargs:
            kwargs['api_host'] = self.normalize_api_host(kwargs['api_host'])
        super().__init__(**kwargs)
    
    @staticmethod
    def normalize_api_host(api_host):
        """Normalize and clean API host"""
        if not api_host:
            return api_host
            
        # Remove any protocol prefix
        host = api_host.strip()
        if host.startswith('https://'):
            host = host[8:]
        elif host.startswith('http://'):
            host = host[7:]
        
        # Remove trailing slash
        host = host.rstrip('/')
        
        return host
    
    # Add properties for compatibility with new code
    @property
    def api_user(self):
        # Extract user with realm from api_token_id (format: user@realm!token_name)
        if '@' in self.api_token_id and '!' in self.api_token_id:
            # Take everything before '!' - this is user@realm
            return self.api_token_id.split('!')[0]
        return 'root@pam'  # Default value
    
    @property 
    def api_token_name(self):
        # Extract token name from api_token_id (format: user@realm!token_name)
        if '!' in self.api_token_id:
            return self.api_token_id.split('!')[-1]
        return self.api_token_id
    
    def __repr__(self):
        return f'<Cluster {self.name}>'

class AdminUser(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    password_hash = db.Column(db.String(200))
    is_first_login = db.Column(db.Boolean, default=True)
    
    def set_password(self, password):
        self.password_hash = generate_password_hash(password)
        self.is_first_login = False
    
    def check_password(self, password):
        if self.password_hash is None:
            return False
        return check_password_hash(self.password_hash, password)
