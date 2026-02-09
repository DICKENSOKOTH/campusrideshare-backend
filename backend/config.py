"""
Campus Ride-Share Platform - Configuration Module

This module loads all configuration from environment variables.
It validates that required variables are present and provides
sensible defaults for optional configuration.
"""

import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class ConfigurationError(Exception):
    """Raised when a required configuration value is missing."""
    pass


def get_required(key: str) -> str:
    """
    Get a required environment variable.
    Raises ConfigurationError if the variable is not set or empty.
    """
    value = os.getenv(key)
    if not value or value.strip() == '' or value.startswith('your-'):
        raise ConfigurationError(
            f"Required environment variable '{key}' is not set. "
            f"Please check your .env file."
        )
    return value.strip()


def get_optional(key: str, default: str = '') -> str:
    """
    Get an optional environment variable with a default value.
    """
    value = os.getenv(key, default)
    return value.strip() if value else default


def get_int(key: str, default: int) -> int:
    """
    Get an environment variable as an integer.
    """
    value = os.getenv(key)
    if value:
        try:
            return int(value.strip())
        except ValueError:
            return default
    return default


def get_float(key: str, default: float) -> float:
    """
    Get an environment variable as a float.
    """
    value = os.getenv(key)
    if value:
        try:
            return float(value.strip())
        except ValueError:
            return default
    return default


class Config:
    """
    Application configuration loaded from environment variables.
    All configuration values are accessed through this class.
    """
    
    # -------------------------------------------------------------------------
    # Application Settings
    # -------------------------------------------------------------------------
    
    # Flask secret key for session encryption
    SECRET_KEY: str = get_required('SECRET_KEY')
    
    # Application display name
    APP_NAME: str = get_optional('APP_NAME', 'Campus Ride-Share')
    
    # Base URL of the application (for email links)
    APP_URL: str = get_optional('APP_URL', 'http://localhost:5000')
    
    # Debug mode - skips email verification requirement
    DEBUG_MODE: bool = get_optional('DEBUG_MODE', 'true').lower() in ('true', '1', 'yes')
    
    # University email domain for registration validation
    # University email domains for registration validation (robust: supports multiple domains, strips spaces/@, lowercase)
    UNIVERSITY_DOMAIN: str = get_optional('UNIVERSITY_DOMAIN', 'university.edu')
    
    # Price per kilometer for suggested pricing
    PRICE_PER_KM: float = get_float('PRICE_PER_KM', 5.0)
    
    # -------------------------------------------------------------------------
    # Database Settings
    # -------------------------------------------------------------------------
    
    # Path to SQLite database file
    DATABASE_PATH: str = get_optional('DATABASE_PATH', 'rideshare.db')
    
    # -------------------------------------------------------------------------
    # OpenAI API Configuration (Optional)
    # -------------------------------------------------------------------------
    
    # OpenAI API key for chatbot (optional - chatbot disabled if not set)
    OPENAI_API_KEY: str = get_optional('OPENAI_API_KEY', '')
    
    # OpenAI model to use
    OPENAI_MODEL: str = 'gpt-3.5-turbo'
    
    # Maximum tokens for chatbot response
    OPENAI_MAX_TOKENS: int = 500
    
    # -------------------------------------------------------------------------
    # Google Maps API Configuration (Optional)
    # -------------------------------------------------------------------------
    
    # Google Maps JavaScript API key (optional - maps disabled if not set)
    GOOGLE_MAPS_API_KEY: str = get_optional('GOOGLE_MAPS_API_KEY', '')
    
    # -------------------------------------------------------------------------
    # Email (SMTP) Configuration
    # -------------------------------------------------------------------------
    
    # SMTP server hostname
    SMTP_HOST: str = get_optional('SMTP_SERVER', 'smtp.gmail.com')
    # SMTP server port
    SMTP_PORT: int = get_int('SMTP_PORT', 587)
    # SMTP authentication username
    SMTP_USER: str = get_optional('SMTP_USERNAME', '')
    # SMTP authentication password
    SMTP_PASSWORD: str = get_optional('SMTP_PASSWORD', '')
    # Display name for outgoing emails
    EMAIL_FROM_NAME: str = get_optional('SMTP_FROM_NAME', 'Campus Ride-Share')
    # From address for outgoing emails
    EMAIL_FROM_ADDRESS: str = get_optional('SMTP_FROM_EMAIL', '')
    
    # -------------------------------------------------------------------------
    # Admin Configuration
    # -------------------------------------------------------------------------
    
    # Email that gets admin privileges on first registration
    ADMIN_EMAIL: str = get_optional('ADMIN_EMAIL', '')
    
    # -------------------------------------------------------------------------
    # Security Settings
    # -------------------------------------------------------------------------
    
    # Session cookie settings
    SESSION_COOKIE_HTTPONLY: bool = True
    SESSION_COOKIE_SECURE: bool = True   # Changed to True for ngrok HTTPS
    SESSION_COOKIE_SAMESITE: str = 'None'  # Changed to None for cross-origin
    
    # Maximum file upload size in bytes (2MB)
    MAX_UPLOAD_SIZE: int = 2 * 1024 * 1024
    
    # Allowed file extensions for uploads
    ALLOWED_EXTENSIONS: set = {'png', 'jpg', 'jpeg', 'gif', 'pdf'}
    
    # Allowed image extensions for profile photos
    ALLOWED_IMAGE_EXTENSIONS: set = {'png', 'jpg', 'jpeg', 'gif'}
    
    # Login attempt rate limiting
    MAX_LOGIN_ATTEMPTS: int = 5
    LOGIN_LOCKOUT_MINUTES: int = 15
    
    # Chatbot rate limiting
    CHATBOT_RATE_LIMIT: int = 10  # requests per minute
    
    # Token expiry times
    VERIFICATION_TOKEN_EXPIRY_HOURS: int = 24
    PASSWORD_RESET_TOKEN_EXPIRY_HOURS: int = 1
    
    # -------------------------------------------------------------------------
    # Upload Paths
    # -------------------------------------------------------------------------
    
    # Directory for uploaded files
    UPLOAD_FOLDER: str = 'static/uploads'
    
    @classmethod
    def is_email_sending_enabled(cls) -> bool:
        """Check if email sending is properly configured."""
        return bool(
            cls.SMTP_HOST and 
            cls.SMTP_USER and 
            cls.SMTP_PASSWORD and
            cls.EMAIL_FROM_ADDRESS
        )
    
    @classmethod
    def is_openai_enabled(cls) -> bool:
        """Check if OpenAI API is properly configured."""
        return bool(cls.OPENAI_API_KEY and not cls.OPENAI_API_KEY.startswith('your-'))
    
    @classmethod
    def is_google_maps_enabled(cls) -> bool:
        """Check if Google Maps API is properly configured."""
        return bool(cls.GOOGLE_MAPS_API_KEY and not cls.GOOGLE_MAPS_API_KEY.startswith('your-'))
    
    @classmethod
    def validate_university_email(cls, email: str) -> bool:
        """Check if an email belongs to any of the configured university domains (strict match, robust, multi-domain)."""
        if not email or not cls.UNIVERSITY_DOMAIN:
            return False
        # Support multiple domains separated by comma, semicolon, or space
        raw_domains = cls.UNIVERSITY_DOMAIN.replace(';', ',').replace(' ', ',').split(',')
        domains = [d.strip().lstrip('@').lower() for d in raw_domains if d.strip()]
        email_domain = email.strip().lower().split('@')[-1]
        return email_domain in domains


# Create a global config instance for easy importing
config = Config()
