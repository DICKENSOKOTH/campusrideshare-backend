"""
Campus Ride-Share Platform - Main Application

This is the main Flask application file that initializes the app,
registers blueprints, and defines core routes for authentication
and user management.
"""

import os
import uuid
from datetime import timedelta

from flask import (
    Flask, request, session, g, jsonify
)
from werkzeug.utils import secure_filename

from config import config
from database import db
from auth import (
    get_current_user, is_logged_in
)

# Import blueprints
from routes_rides import rides_bp
from routes_bookings import bookings_bp
from routes_messaging import messaging_bp
from routes_admin import admin_bp
from routes_api import api_bp


# =============================================================================
# Application Factory
# =============================================================================

def create_app():
    """Create and configure the Flask application."""
    
    app = Flask(__name__)
    
    # Configure Flask
    app.secret_key = config.SECRET_KEY
    app.config['SESSION_COOKIE_HTTPONLY'] = config.SESSION_COOKIE_HTTPONLY
    app.config['SESSION_COOKIE_SECURE'] = config.SESSION_COOKIE_SECURE
    app.config['SESSION_COOKIE_SAMESITE'] = config.SESSION_COOKIE_SAMESITE
    app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(days=7)
    app.config['MAX_CONTENT_LENGTH'] = config.MAX_UPLOAD_SIZE
    app.config['UPLOAD_FOLDER'] = config.UPLOAD_FOLDER
    
    # Ensure upload folder exists
    os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
    
    # Register blueprints
    app.register_blueprint(rides_bp)
    app.register_blueprint(bookings_bp)
    app.register_blueprint(messaging_bp)
    app.register_blueprint(admin_bp)
    app.register_blueprint(api_bp)
    
    # Register error handlers (JSON responses for API)
    @app.errorhandler(404)
    def not_found_error(error):
        """Handle 404 errors."""
        return jsonify({'error': 'Not found'}), 404
    
    @app.errorhandler(403)
    def forbidden_error(error):
        """Handle 403 errors."""
        return jsonify({'error': 'Forbidden'}), 403
    
    @app.errorhandler(500)
    def internal_error(error):
        """Handle 500 errors."""
        return jsonify({'error': 'Internal server error'}), 500
    
    @app.errorhandler(413)
    def file_too_large(error):
        """Handle file too large errors."""
        return jsonify({'error': 'File is too large. Maximum size is 2MB.'}), 413
    
    # CORS support for API routes
    @app.after_request
    def after_request(response):
        """Add CORS headers to API responses."""
        origin = request.headers.get('Origin', '')
        # Allow requests from Live Server (port 5500) and localhost
        allowed_origins = [
            'http://localhost:5500',
            'http://127.0.0.1:5500',
            'http://localhost:5000',
            'http://127.0.0.1:5000',
            'https://campusrideshare.netlify.app',
            'null'
        ]
        # For API routes, set CORS headers
        if request.path.startswith('/api'):
            if origin in allowed_origins:
                response.headers['Access-Control-Allow-Origin'] = origin
                response.headers['Access-Control-Allow-Credentials'] = 'true'
            else:
                # For unknown origins during development, allow them
                response.headers['Access-Control-Allow-Origin'] = origin if origin else '*'
                if origin:
                    response.headers['Access-Control-Allow-Credentials'] = 'true'
            response.headers['Access-Control-Allow-Methods'] = 'GET, POST, PUT, DELETE, OPTIONS'
            response.headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization, X-Requested-With, Accept'
        return response
    
    # Handle preflight OPTIONS requests for all API paths
    @app.before_request
    def handle_preflight():
        """Handle CORS preflight requests for any path."""
        if request.method == 'OPTIONS' and request.path.startswith('/api'):
            response = app.make_default_options_response()
            origin = request.headers.get('Origin', '*')
            response.headers['Access-Control-Allow-Origin'] = origin
            response.headers['Access-Control-Allow-Credentials'] = 'true'
            response.headers['Access-Control-Allow-Methods'] = 'GET, POST, PUT, DELETE, OPTIONS'
            response.headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization, X-Requested-With, Accept'
            response.headers['Access-Control-Max-Age'] = '86400'
            return response
    
    # Before request handler to set user
    @app.before_request
    def before_request_user():
        """Execute before each request to set user and run periodic cleanup."""
        g.user = None
        if is_logged_in():
            g.user = get_current_user()
        
        # Run ride cleanup periodically (at most once per hour)
        from datetime import datetime
        last_cleanup = getattr(app, '_last_cleanup', None)
        now = datetime.now()
        if last_cleanup is None or (now - last_cleanup).total_seconds() > 3600:
            try:
                db.run_ride_cleanup()
                app._last_cleanup = now
            except Exception:
                pass
    
    return app


# Create the application instance
app = create_app()

# Run ride cleanup on startup
with app.app_context():
    try:
        cleanup_result = db.run_ride_cleanup()
        if cleanup_result['marked_expired'] > 0 or cleanup_result['deleted_rides'] > 0:
            print(f"Ride cleanup: {cleanup_result['marked_expired']} marked expired, {cleanup_result['deleted_rides']} deleted")
    except Exception as e:
        print(f"Warning: Ride cleanup failed - {e}")


# =============================================================================
# Helper Functions
# =============================================================================

def allowed_file(filename: str, allowed_extensions: set) -> bool:
    """Check if a file has an allowed extension."""
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in allowed_extensions


def save_uploaded_file(file, folder: str = '') -> str:
    """
    Save an uploaded file with a unique filename.
    
    Args:
        file: The uploaded file object.
        folder: Optional subfolder within uploads.
    
    Returns:
        The path to the saved file relative to static folder.
    """
    if not file or not file.filename:
        return ''
    
    # Generate unique filename
    ext = file.filename.rsplit('.', 1)[1].lower()
    unique_filename = f"{uuid.uuid4().hex}.{ext}"
    
    # Build save path
    upload_path = os.path.join(app.config['UPLOAD_FOLDER'], folder)
    os.makedirs(upload_path, exist_ok=True)
    
    file_path = os.path.join(upload_path, unique_filename)
    file.save(file_path)
    
    # Return relative path for storage in database
    return os.path.join('uploads', folder, unique_filename).replace('\\', '/')


# =============================================================================
# Run Application
# =============================================================================

if __name__ == '__main__':
    # Initialize database on startup
    print(f"Starting {config.APP_NAME}...")
    print(f"Database: {'PostgreSQL' if db.use_postgres else config.DATABASE_PATH}")
    print(f"Email sending: {'Enabled' if config.is_email_sending_enabled() else 'Disabled'}")
    
    # Run the development server
    app.run(
        host='0.0.0.0',
        port=5000,
        debug=True
    )