from functools import wraps
from flask import session, redirect, url_for
from models import AdminUser, db

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'logged_in' not in session:
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

def init_admin_user():
    """Initialize admin user if not exists"""
    admin = AdminUser.query.first()
    if not admin:
        admin = AdminUser()
        db.session.add(admin)
        db.session.commit()
    return admin
