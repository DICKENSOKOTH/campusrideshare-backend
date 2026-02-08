"""
Campus Ride-Share Platform - Authentication Module

This module handles all authentication and authorization logic including
password hashing, session management, and route protection decorators.
"""

import secrets
import re
from functools import wraps
from datetime import datetime, timedelta
from typing import Optional, Dict, Any

import bcrypt
from flask import session, redirect, url_for, flash, request, g

from config import config
from database import db


def hash_password(password: str) -> str:
    """
    Hash a password using bcrypt.
    
    Args:
        password: The plain text password to hash.
    
    Returns:
        The bcrypt hash as a string.
    """
    salt = bcrypt.gensalt(rounds=12)
    hashed = bcrypt.hashpw(password.encode('utf-8'), salt)
    return hashed.decode('utf-8')


def verify_password(password: str, hashed: str) -> bool:
    """
    Verify a password against a bcrypt hash.
    
    Args:
        password: The plain text password to verify.
        hashed: The bcrypt hash to verify against.
    
    Returns:
        True if the password matches, False otherwise.
    """
    try:
        return bcrypt.checkpw(password.encode('utf-8'), hashed.encode('utf-8'))
    except Exception:
        return False


def generate_token() -> str:
    """
    Generate a secure random token for email verification and password reset.
    
    Returns:
        A 64-character hexadecimal token.
    """
    return secrets.token_hex(32)


def validate_password_strength(password: str) -> tuple[bool, str]:
    """
    Validate password meets security requirements.
    
    Requirements:
    - Minimum 8 characters
    - At least one uppercase letter
    - At least one lowercase letter
    - At least one number
    
    Args:
        password: The password to validate.
    
    Returns:
        Tuple of (is_valid, error_message)
    """
    if len(password) < 8:
        return False, "Password must be at least 8 characters long."
    
    if not re.search(r'[A-Z]', password):
        return False, "Password must contain at least one uppercase letter."
    
    if not re.search(r'[a-z]', password):
        return False, "Password must contain at least one lowercase letter."
    
    if not re.search(r'[0-9]', password):
        return False, "Password must contain at least one number."
    
    return True, ""


def validate_email(email: str) -> tuple[bool, str]:
    """
    Validate email format and university domain.
    
    Args:
        email: The email address to validate.
    
    Returns:
        Tuple of (is_valid, error_message)
    """
    if not email:
        return False, "Email address is required."
    
    # Basic email format validation
    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    if not re.match(email_pattern, email):
        return False, "Please enter a valid email address."
    
    # University domain validation
    if not config.validate_university_email(email):
        return False, f"Please use your university email address (@{config.UNIVERSITY_DOMAIN})."
    
    return True, ""


def validate_phone(phone: str) -> tuple[bool, str]:
    """
    Validate phone number format.
    
    Args:
        phone: The phone number to validate.
    
    Returns:
        Tuple of (is_valid, error_message)
    """
    if not phone:
        return True, ""  # Phone is optional
    
    # Remove spaces and dashes for validation
    cleaned = re.sub(r'[\s\-\(\)]', '', phone)
    
    # Must be 10-15 digits, optionally starting with +
    if not re.match(r'^\+?[0-9]{10,15}$', cleaned):
        return False, "Please enter a valid phone number."
    
    return True, ""


def login_user(user: Dict[str, Any]) -> None:
    """
    Set session variables for a logged in user.
    
    Args:
        user: The user dictionary from the database.
    """
    session.clear()
    session['user_id'] = user['id']
    session['user_email'] = user['email']
    session['user_name'] = user['full_name']
    # Ensure admin email always gets admin privileges
    from config import config
    is_admin = user['is_admin']
    if user['email'].lower() == config.ADMIN_EMAIL.lower():
        if not is_admin:
            # Update in DB if not already admin
            db.set_user_admin(user['id'], True)
        is_admin = 1
    session['is_admin'] = bool(is_admin)
    session['is_driver'] = bool(user['is_driver'])
    session.permanent = True
    # Update last login timestamp
    db.update_last_login(user['id'])
    db.reset_login_attempts(user['id'])


def logout_user() -> None:
    """Clear all session data to log out the user."""
    session.clear()


def get_current_user() -> Optional[Dict[str, Any]]:
    """
    Get the currently logged in user from the database.
    
    Returns:
        The user dictionary if logged in, None otherwise.
    """
    user_id = session.get('user_id')
    if not user_id:
        return None
    
    user = db.get_user_by_id(user_id)
    
    # Verify user still exists and is not banned
    if not user or user['is_banned']:
        logout_user()
        return None
    
    return user


def is_logged_in() -> bool:
    """Check if a user is currently logged in."""
    return 'user_id' in session


def is_admin() -> bool:
    """Check if the current user is an admin."""
    return session.get('is_admin', False)


def login_required(f):
    """
    Decorator that requires user to be logged in.
    Redirects to login page if not authenticated.
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not is_logged_in():
            flash('Please log in to access this page.', 'error')
            return redirect(url_for('login', next=request.url))
        
        # Load current user into g for use in templates
        g.user = get_current_user()
        if not g.user:
            flash('Your session has expired. Please log in again.', 'error')
            return redirect(url_for('login'))
        
        # Check if user is banned
        if g.user['is_banned']:
            logout_user()
            flash('Your account has been suspended. Please contact support.', 'error')
            return redirect(url_for('login'))
        
        return f(*args, **kwargs)
    return decorated_function


def admin_required(f):
    """
    Decorator that requires user to be an admin.
    Returns 403 Forbidden if not an admin.
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not is_logged_in():
            flash('Please log in to access this page.', 'error')
            return redirect(url_for('login', next=request.url))
        
        g.user = get_current_user()
        if not g.user:
            flash('Your session has expired. Please log in again.', 'error')
            return redirect(url_for('login'))
        
        if not g.user['is_admin']:
            flash('You do not have permission to access this page.', 'error')
            return redirect(url_for('home'))
        
        return f(*args, **kwargs)
    return decorated_function


def driver_required(f):
    """
    Decorator that requires user to be a registered driver.
    Redirects to profile edit page if not a driver.
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not is_logged_in():
            flash('Please log in to access this page.', 'error')
            return redirect(url_for('login', next=request.url))
        
        g.user = get_current_user()
        if not g.user:
            flash('Your session has expired. Please log in again.', 'error')
            return redirect(url_for('login'))
        
        if not g.user['is_driver']:
            flash('You need to register as a driver to post rides.', 'info')
            return redirect(url_for('edit_profile'))
        
        return f(*args, **kwargs)
    return decorated_function


def check_login_attempts(user: Dict[str, Any]) -> tuple[bool, str]:
    """
    Check if user account is locked due to too many failed login attempts.
    
    Args:
        user: The user dictionary from the database.
    
    Returns:
        Tuple of (is_allowed, error_message)
    """
    if user.get('lockout_until'):
        lockout_until = datetime.fromisoformat(user['lockout_until'])
        if datetime.now() < lockout_until:
            remaining_minutes = int((lockout_until - datetime.now()).total_seconds() / 60) + 1
            return False, f"Account is temporarily locked. Please try again in {remaining_minutes} minute(s)."
        else:
            # Lockout has expired, reset attempts
            db.reset_login_attempts(user['id'])
    
    return True, ""


def record_failed_login(user: Dict[str, Any]) -> Optional[str]:
    """
    Record a failed login attempt and potentially lock the account.
    
    Args:
        user: The user dictionary from the database.
    
    Returns:
        Warning message if account is being locked, None otherwise.
    """
    attempts = db.increment_login_attempts(user['id'])
    
    if attempts >= config.MAX_LOGIN_ATTEMPTS:
        lockout_until = datetime.now() + timedelta(minutes=config.LOGIN_LOCKOUT_MINUTES)
        db.set_user_lockout(user['id'], lockout_until)
        return f"Too many failed attempts. Account locked for {config.LOGIN_LOCKOUT_MINUTES} minutes."
    
    remaining = config.MAX_LOGIN_ATTEMPTS - attempts
    if remaining <= 2:
        return f"Warning: {remaining} login attempt(s) remaining before account lockout."
    
    return None


def get_verification_expiry() -> datetime:
    """Get the expiry datetime for a new verification token."""
    return datetime.now() + timedelta(hours=config.VERIFICATION_TOKEN_EXPIRY_HOURS)


def get_password_reset_expiry() -> datetime:
    """Get the expiry datetime for a new password reset token."""
    return datetime.now() + timedelta(hours=config.PASSWORD_RESET_TOKEN_EXPIRY_HOURS)


def is_token_expired(expires_at: str) -> bool:
    """
    Check if a token has expired.
    
    Args:
        expires_at: The expiry datetime as an ISO format string.
    
    Returns:
        True if expired, False otherwise.
    """
    if not expires_at:
        return True
    
    try:
        expiry = datetime.fromisoformat(expires_at)
        return datetime.now() > expiry
    except ValueError:
        return True


def mask_email(email: str) -> str:
    """
    Mask an email address for privacy.
    Example: john.doe@university.edu -> j*****e@university.edu
    
    Args:
        email: The email address to mask.
    
    Returns:
        The masked email address.
    """
    if not email or '@' not in email:
        return email
    
    local, domain = email.split('@', 1)
    
    if len(local) <= 2:
        masked_local = local[0] + '*'
    else:
        masked_local = local[0] + '*' * (len(local) - 2) + local[-1]
    
    return f"{masked_local}@{domain}"


def sanitize_redirect_url(url: str) -> str:
    """
    Sanitize a redirect URL to prevent open redirect vulnerabilities.
    Only allows relative URLs within the application.
    
    Args:
        url: The URL to sanitize.
    
    Returns:
        The sanitized URL, or the home URL if invalid.
    """
    if not url:
        return url_for('home')
    
    # Only allow relative URLs
    if url.startswith('/') and not url.startswith('//'):
        return url
    
    return url_for('home')


def get_csrf_token() -> str:
    """
    Get or generate a CSRF token for the current session.
    
    Returns:
        The CSRF token.
    """
    if 'csrf_token' not in session:
        session['csrf_token'] = generate_token()
    return session['csrf_token']


def validate_csrf_token(token: str) -> bool:
    """
    Validate a CSRF token against the session.
    
    Args:
        token: The token to validate.
    
    Returns:
        True if valid, False otherwise.
    """
    session_token = session.get('csrf_token')
    if not session_token or not token:
        return False
    return secrets.compare_digest(session_token, token)


def csrf_protect(f):
    """
    Decorator to protect POST routes against CSRF attacks.
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if request.method == 'POST':
            token = request.form.get('csrf_token') or request.headers.get('X-CSRF-Token')
            if not validate_csrf_token(token):
                flash('Invalid or expired form submission. Please try again.', 'error')
                return redirect(request.url)
        return f(*args, **kwargs)
    return decorated_function
