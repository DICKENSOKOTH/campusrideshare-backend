"""
Campus Ride-Share Platform - API Routes

This module provides JSON API endpoints for the frontend.
All routes return JSON responses and handle CORS for the frontend.
"""

import secrets
from datetime import datetime, timedelta
from functools import wraps

from flask import Blueprint, jsonify, request, session

from config import config
from database import db
from auth import (
    hash_password, verify_password, generate_token, login_user, logout_user,
    get_current_user, is_logged_in, validate_password_strength, validate_email,
    check_login_attempts, record_failed_login, get_verification_expiry,
    get_password_reset_expiry
)
from email_utils import send_verification_email, send_password_reset_email

# Create blueprint
api_bp = Blueprint('api', __name__, url_prefix='/api')


def generate_api_token(user_id: int) -> str:
    """Generate an API token for a user and store it in the database."""
    token = secrets.token_urlsafe(32)
    expires_at = datetime.now() + timedelta(days=7)
    db.create_api_token(token, user_id, expires_at)
    # Occasionally clean up expired tokens
    db.cleanup_expired_tokens()
    return token


def get_user_from_token() -> dict:
    """Get user from Authorization header token."""
    auth_header = request.headers.get('Authorization', '')
    if auth_header.startswith('Bearer '):
        token = auth_header[7:]
        return db.get_user_by_api_token(token)
    return None


def invalidate_token() -> bool:
    """Invalidate the current request's token (for logout)."""
    auth_header = request.headers.get('Authorization', '')
    if auth_header.startswith('Bearer '):
        token = auth_header[7:]
        return db.delete_api_token(token)
    return False


def api_login_required(f):
    """Decorator to require login for API endpoints."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        user = get_user_from_token()
        if not user:
            # Fallback to session-based auth
            if not is_logged_in():
                return jsonify({'error': 'Authentication required'}), 401
        return f(*args, **kwargs)
    return decorated_function


def api_admin_required(f):
    """Decorator to require admin role for API endpoints."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        user = get_user_from_token()
        if not user:
            if not is_logged_in():
                return jsonify({'error': 'Authentication required'}), 401
            user = get_current_user()
        if not user or not user.get('is_admin'):
            return jsonify({'error': 'Admin access required'}), 403
        return f(*args, **kwargs)
    return decorated_function


def get_api_user():
    """Get current user from token or session."""
    user = get_user_from_token()
    if not user:
        user = get_current_user()
    return user


# =============================================================================
# Public Stats
# =============================================================================

@api_bp.route('/stats')
def get_stats():
    """Get public platform statistics."""
    stats = db.get_platform_statistics()
    return jsonify({
        'total_rides': stats.get('total_rides', 0),
        'total_users': stats.get('total_users', 0),
        'total_bookings': stats.get('total_bookings', 0),
        'active_rides': stats.get('active_rides', 0),
        'average_price': stats.get('average_price', 0)
    })


# =============================================================================
# Authentication API
# =============================================================================

@api_bp.route('/login', methods=['POST'])
def api_login():
    """Handle user login."""
    data = request.get_json() or {}
    email = data.get('email', '').strip().lower()
    password = data.get('password', '')
    
    if not email or not password:
        return jsonify({'error': 'Email and password are required'}), 400
    
    # Get user first to check login attempts
    user = db.get_user_by_email(email)
    
    # Check login attempts (only if user exists)
    if user:
        is_allowed, lockout_message = check_login_attempts(user)
        if not is_allowed:
            return jsonify({'error': lockout_message}), 429
    
    # Verify credentials
    if not user or not verify_password(password, user['password_hash']):
        if user:
            record_failed_login(user)
        return jsonify({'error': 'Invalid email or password'}), 401
    
    # Check if verified (skip in debug mode)
    if not config.DEBUG_MODE and not user.get('is_verified'):
        return jsonify({
            'error': 'Please verify your email address',
            'needs_verification': True
        }), 403
    
    # Check if banned
    if user.get('is_banned'):
        return jsonify({'error': 'Your account has been suspended'}), 403
    
    # Log in user (login_user handles last_login and reset_attempts)
    login_user(user)
    
    # Generate API token for frontend
    api_token = generate_api_token(user['id'])
    
    return jsonify({
        'success': True,
        'token': api_token,
        'user': {
            'id': user['id'],
            'email': user['email'],
            'full_name': user['full_name'],
            'is_admin': user.get('is_admin', False)
        }
    })


@api_bp.route('/register', methods=['POST'])
def api_register():
    """Handle user registration."""
    data = request.get_json() or {}
    
    email = data.get('email', '').strip().lower()
    password = data.get('password', '')
    confirm_password = data.get('confirm_password', password)  # Default to password if not provided
    phone = data.get('phone', '').strip()
    
    # Support both full_name and first_name/last_name
    full_name = data.get('full_name', '').strip()
    if not full_name:
        first_name = data.get('first_name', '').strip()
        last_name = data.get('last_name', '').strip()
        full_name = f"{first_name} {last_name}".strip()
    
    # Validation
    if not all([email, password, full_name]):
        return jsonify({'error': 'Email, password, and name are required'}), 400
    
    if password != confirm_password:
        return jsonify({'error': 'Passwords do not match'}), 400
    

    if not validate_email(email):
        return jsonify({'error': 'Invalid email format'}), 400
    # Strict domain lock
    if not config.validate_university_email(email):
        return jsonify({'error': f'Registration is restricted to {config.UNIVERSITY_DOMAIN} emails only.'}), 400
    
    # Check for existing user
    existing = db.get_user_by_email(email)
    if existing:
        return jsonify({'error': 'An account with this email already exists'}), 400
    
    # Validate password
    is_valid, password_error = validate_password_strength(password)
    if not is_valid:
        return jsonify({'error': password_error}), 400
    
    # Create user (skip verification logic, always verified)
    password_hash = hash_password(password)
    user_id = db.create_user(
        email=email,
        password_hash=password_hash,
        full_name=full_name,
        phone=phone if phone else None,
        verification_token=None,
        verification_expires_at=None
    )
    # Mark user as verified immediately
    with db.get_connection() as conn:
        cursor = conn.cursor()
        if getattr(db, 'use_postgres', False):
            cursor.execute('UPDATE users SET is_verified = 1 WHERE id = %s', (user_id,))
        else:
            cursor.execute('UPDATE users SET is_verified = 1 WHERE id = ?', (user_id,))
    return jsonify({
        'success': True,
        'message': 'Registration successful! You can now log in.',
        'user_id': user_id,
        'email_sent': False
    })


@api_bp.route('/logout', methods=['POST'])
def api_logout():
    """Handle user logout."""
    # Invalidate the API token from database
    invalidate_token()
    # Also clear session
    logout_user()
    return jsonify({'success': True})


@api_bp.route('/check-auth')
def check_auth():
    """Check if user is authenticated via token or session."""
    # First check token auth
    user = get_user_from_token()
    if user:
        return jsonify({
            'authenticated': True,
            'user': {
                'id': user['id'],
                'email': user['email'],
                'full_name': user['full_name'],
                'is_admin': user.get('is_admin', False)
            }
        })
    # Fallback to session auth
    if is_logged_in():
        user = get_current_user()
        if user:
            return jsonify({
                'authenticated': True,
                'user': {
                    'id': user['id'],
                    'email': user['email'],
                    'full_name': user['full_name'],
                    'is_admin': user.get('is_admin', False)
                }
            })
    return jsonify({'authenticated': False})


@api_bp.route('/profile')
@api_login_required
def api_get_profile():
    """Get current user's profile."""
    user = get_api_user()
    if not user:
        return jsonify({'error': 'User not found'}), 404
    
    # Get additional stats
    rating = db.get_user_average_rating(user['id'])
    reviews = db.get_reviews_for_user(user['id'])
    
    # Parse first_name and last_name from full_name
    full_name = user['full_name'] or ''
    name_parts = full_name.split(' ', 1)
    first_name = name_parts[0] if name_parts else ''
    last_name = name_parts[1] if len(name_parts) > 1 else ''
    
    return jsonify({
        'id': user['id'],
        'email': user['email'],
        'full_name': user['full_name'],
        'first_name': first_name,
        'last_name': last_name,
        'name': user['full_name'],  # Alias for frontend compatibility
        'phone': user.get('phone'),
        'bio': user.get('bio'),
        'profile_photo': user.get('profile_photo'),
        'is_admin': user.get('is_admin', False),
        'is_verified': user.get('is_verified', False),
        'is_driver': user.get('is_driver', False),
        'vehicle_make': user.get('vehicle_make'),
        'vehicle_model': user.get('vehicle_model'),
        'license_plate': user.get('license_plate'),
        'drivers_license': user.get('drivers_license'),
        'emergency_contact_name': user.get('emergency_contact_name'),
        'emergency_contact_phone': user.get('emergency_contact_phone'),
        'created_at': user.get('created_at'),
        'avg_rating': rating,
        'total_reviews': len(reviews) if reviews else 0
    })


@api_bp.route('/profile', methods=['PUT'])
@api_login_required
def api_update_profile():
    """Update current user's profile."""
    data = request.get_json() or {}
    user = get_api_user()
    
    # Update allowed fields
    allowed_fields = [
        'full_name', 'phone', 'bio', 'is_driver',
        'vehicle_make', 'vehicle_model', 'license_plate',
        'drivers_license', 'emergency_contact_name', 'emergency_contact_phone'
    ]
    
    update_data = {k: v for k, v in data.items() if k in allowed_fields}
    
    if update_data:
        db.update_user(user['id'], **update_data)
    
    return jsonify({'success': True, 'message': 'Profile updated'})


@api_bp.route('/users/<int:user_id>')
@api_login_required
def api_get_user(user_id):
    """Get a user's public profile."""
    user = db.get_user_by_id(user_id)
    if not user:
        return jsonify({'error': 'User not found'}), 404
    
    # Get additional stats
    rating = db.get_user_average_rating(user_id)
    reviews = db.get_reviews_for_user(user_id)
    
    # Parse first_name and last_name from full_name
    full_name = user['full_name'] or ''
    name_parts = full_name.split(' ', 1)
    first_name = name_parts[0] if name_parts else ''
    last_name = name_parts[1] if len(name_parts) > 1 else ''
    
    return jsonify({
        'id': user['id'],
        'full_name': user['full_name'],
        'first_name': first_name,
        'last_name': last_name,
        'bio': user.get('bio'),
        'profile_photo': user.get('profile_photo'),
        'is_verified': user.get('is_verified', False),
        'is_driver': user.get('is_driver', False),
        'vehicle_make': user.get('vehicle_make'),
        'vehicle_model': user.get('vehicle_model'),
        'created_at': user.get('created_at'),
        'avg_rating': rating,
        'total_reviews': len(reviews) if reviews else 0,
        'email': user['email'],
        'phone': user.get('phone')
    })


@api_bp.route('/users/<int:user_id>/reviews')
@api_login_required
def api_get_user_reviews(user_id):
    """Get reviews for a user."""
    reviews = db.get_reviews_for_user(user_id)
    return jsonify(reviews or [])


# =============================================================================
# Password Reset
# =============================================================================

@api_bp.route('/forgot-password', methods=['POST'])
def api_forgot_password():
    """Request password reset."""
    data = request.get_json() or {}
    email = data.get('email', '').strip().lower()
    
    if not email:
        return jsonify({'error': 'Email is required'}), 400
    
    user = db.get_user_by_email(email)
    if user:
        token = generate_token()
        expires = get_password_reset_expiry()
        db.create_password_reset_token(user['id'], token, expires)
        
        if config.is_email_sending_enabled():
            send_password_reset_email(email, user['full_name'], token, user['id'])
    
    # Always return success to prevent email enumeration
    return jsonify({
        'success': True,
        'message': 'If an account exists with this email, a reset link has been sent.'
    })


@api_bp.route('/reset-password', methods=['POST'])
def api_reset_password():
    """Reset password with token."""
    data = request.get_json() or {}
    token = data.get('token', '')
    password = data.get('password', '')
    confirm_password = data.get('confirm_password', '')
    
    if not token:
        return jsonify({'error': 'Invalid reset token'}), 400
    
    if password != confirm_password:
        return jsonify({'error': 'Passwords do not match'}), 400
    
    is_valid, password_error = validate_password_strength(password)
    if not is_valid:
        return jsonify({'error': password_error}), 400
    
    user = db.get_user_by_reset_token(token)
    if not user:
        return jsonify({'error': 'Invalid or expired reset token'}), 400
    
    password_hash = hash_password(password)
    db.reset_password(user['id'], password_hash)
    
    return jsonify({'success': True, 'message': 'Password reset successful'})


# =============================================================================
# Email Verification
# =============================================================================

@api_bp.route('/verify-email', methods=['POST'])
def api_verify_email():
    """Verify email with token."""
    data = request.get_json() or {}
    token = data.get('token', '')
    
    if not token:
        return jsonify({'error': 'Invalid verification token'}), 400
    
    user = db.get_user_by_verification_token(token)
    if not user:
        return jsonify({'error': 'Invalid or expired verification token'}), 400
    
    db.set_user_verified(user['id'])
    
    return jsonify({'success': True, 'message': 'Email verified successfully'})


@api_bp.route('/resend-verification', methods=['POST'])
def api_resend_verification():
    """Resend verification email."""
    data = request.get_json() or {}
    email = data.get('email', '').strip().lower()
    
    if not email:
        return jsonify({'error': 'Email is required'}), 400
    
    user = db.get_user_by_email(email)
    if user and not user.get('is_verified'):
        token = generate_token()
        expires = get_verification_expiry()
        db.update_verification_token(user['id'], token, expires)
        
        if config.is_email_sending_enabled():
            send_verification_email(email, user['full_name'], token, user['id'])
    
    return jsonify({
        'success': True,
        'message': 'If an unverified account exists, a new verification email has been sent.'
    })


# =============================================================================
# Rides API
# =============================================================================

@api_bp.route('/rides')
def api_get_rides():
    """Get rides with optional filters."""
    origin = request.args.get('origin', '')
    destination = request.args.get('destination', '')
    date = request.args.get('date', '')
    date_from = request.args.get('date_from', '') or date
    date_to = request.args.get('date_to', '')
    max_price = request.args.get('max_price', type=float)
    seats = request.args.get('seats', type=int)
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 50, type=int)
    
    rides, total = db.search_rides(
        origin=origin if origin else None,
        destination=destination if destination else None,
        date_from=date_from if date_from else None,
        date_to=date_to if date_to else None,
        max_price=max_price,
        min_seats=seats,
        page=page,
        per_page=per_page
    )
    
    # Return array directly for frontend compatibility
    result = [{
        'id': r['id'],
        'origin': r['origin'],
        'destination': r['destination'],
        'departure_date': r['departure_date'],
        'departure_time': r.get('departure_time', ''),
        'price_per_seat': r['price_per_seat'],
        'available_seats': r.get('total_seats', 4) - r.get('seats_taken', 0),
        'seats_available': r.get('total_seats', 4) - r.get('seats_taken', 0),
        'total_seats': r.get('total_seats', 4),
        'driver_id': r['driver_id'],
        'driver_name': r.get('driver_name', 'Driver'),
        'driver_rating': r.get('driver_rating'),
        'status': r.get('status', 'active')
    } for r in rides]
    
    return jsonify(result)


@api_bp.route('/rides/<int:ride_id>')
def api_get_ride(ride_id):
    """Get ride details."""
    ride = db.get_ride_by_id(ride_id)
    if not ride:
        return jsonify({'error': 'Ride not found'}), 404
    
    # Get bookings for this ride
    bookings = db.get_bookings_by_ride(ride_id)
    
    # Calculate available seats
    total_seats = ride.get('total_seats', 4)
    seats_taken = ride.get('seats_taken', 0)
    available_seats = total_seats - seats_taken
    
    return jsonify({
        'id': ride['id'],
        'origin': ride['origin'],
        'destination': ride['destination'],
        'departure_date': ride['departure_date'],
        'departure_time': ride['departure_time'],
        'price_per_seat': ride['price_per_seat'],
        'available_seats': available_seats,
        'seats_available': available_seats,
        'total_seats': total_seats,
        'driver_id': ride['driver_id'],
        'driver_name': ride.get('driver_name', 'Driver'),
        'driver_email': ride.get('driver_email'),
        'driver_phone': ride.get('driver_phone'),
        'driver_photo': ride.get('driver_photo'),
        'driver_rating': ride.get('driver_rating'),
        'notes': ride.get('notes', ''),
        'description': ride.get('notes', ''),
        'status': ride.get('status', 'active'),
        'origin_lat': ride.get('origin_lat'),
        'origin_lng': ride.get('origin_lng'),
        'destination_lat': ride.get('destination_lat'),
        'destination_lng': ride.get('destination_lng'),
        'distance_km': ride.get('distance_km'),
        'estimated_duration_minutes': ride.get('estimated_duration_minutes'),
        'created_at': ride.get('created_at'),
        # New preference fields
        'luggage_allowed': bool(ride.get('luggage_allowed', 1)),
        'pets_allowed': bool(ride.get('pets_allowed', 0)),
        'smoking_allowed': bool(ride.get('smoking_allowed', 0)),
        'music_allowed': bool(ride.get('music_allowed', 1)),
        'ac_available': bool(ride.get('ac_available', 1)),
        'vehicle_type': ride.get('vehicle_type'),
        'vehicle_color': ride.get('vehicle_color'),
        'pickup_flexibility': ride.get('pickup_flexibility', 'exact'),
        'return_trip': bool(ride.get('return_trip', 0)),
        'bookings': [{
            'id': b['id'],
            'passenger_id': b['passenger_id'],
            'passenger_name': b.get('passenger_name'),
            'status': b['status']
        } for b in bookings]
    })


@api_bp.route('/rides', methods=['POST'])
@api_login_required
def api_create_ride():
    """Create a new ride."""
    data = request.get_json() or {}
    user = get_api_user()
    
    if not user:
        return jsonify({'error': 'Authentication required'}), 401
    
    # Accept both frontend field names (date, time, seats) and backend names
    origin = data.get('origin', '')
    destination = data.get('destination', '')
    departure_date = data.get('departure_date') or data.get('date', '')
    departure_time = data.get('departure_time') or data.get('time', '')
    price_per_seat = data.get('price_per_seat', 0)
    total_seats = data.get('total_seats') or data.get('seats', 4)
    notes = data.get('notes') or data.get('description', '')
    
    if not all([origin, destination, departure_date, departure_time]):
        return jsonify({'error': 'Origin, destination, date and time are required'}), 400
    
    ride_id = db.create_ride(
        driver_id=user['id'],
        origin=origin,
        destination=destination,
        departure_date=departure_date,
        departure_time=departure_time,
        price_per_seat=float(price_per_seat),
        total_seats=int(total_seats),
        origin_lat=data.get('origin_lat'),
        origin_lng=data.get('origin_lng'),
        destination_lat=data.get('destination_lat'),
        destination_lng=data.get('destination_lng'),
        distance_km=data.get('distance_km'),
        estimated_duration_minutes=data.get('estimated_duration_minutes'),
        notes=notes,
        luggage_allowed=data.get('luggage_allowed', True),
        pets_allowed=data.get('pets_allowed', False),
        smoking_allowed=data.get('smoking_allowed', False),
        music_allowed=data.get('music_allowed', True),
        ac_available=data.get('ac_available', True),
        vehicle_type=data.get('vehicle_type'),
        vehicle_color=data.get('vehicle_color'),
        pickup_flexibility=data.get('pickup_flexibility', 'exact'),
        return_trip=data.get('return_trip', False)
    )
    
    return jsonify({'success': True, 'id': ride_id, 'ride_id': ride_id}), 201


@api_bp.route('/rides/<int:ride_id>', methods=['PUT'])
@api_login_required
def api_update_ride(ride_id):
    """Update a ride."""
    user = get_api_user()
    ride = db.get_ride_by_id(ride_id)
    
    if not ride:
        return jsonify({'error': 'Ride not found'}), 404
    
    if ride['driver_id'] != user['id'] and not user.get('is_admin'):
        return jsonify({'error': 'Not authorized'}), 403
    
    data = request.get_json() or {}
    
    # Map frontend field names to backend names
    if 'date' in data and 'departure_date' not in data:
        data['departure_date'] = data.pop('date')
    if 'time' in data and 'departure_time' not in data:
        data['departure_time'] = data.pop('time')
    if 'seats' in data and 'total_seats' not in data:
        data['total_seats'] = data.pop('seats')
    if 'description' in data and 'notes' not in data:
        data['notes'] = data.pop('description')
    
    allowed_fields = ['origin', 'destination', 'departure_date', 'departure_time', 
                      'price_per_seat', 'total_seats', 'notes', 'luggage_allowed',
                      'pets_allowed', 'smoking_allowed', 'music_allowed', 'ac_available',
                      'vehicle_type', 'vehicle_color', 'pickup_flexibility', 'return_trip']
    update_data = {k: v for k, v in data.items() if k in allowed_fields}
    
    if update_data:
        db.update_ride(ride_id, **update_data)
    
    return jsonify({'success': True})


@api_bp.route('/rides/<int:ride_id>/cancel', methods=['POST'])
@api_login_required
def api_cancel_ride(ride_id):
    """Cancel a ride."""
    user = get_api_user()
    ride = db.get_ride_by_id(ride_id)
    
    if not ride:
        return jsonify({'error': 'Ride not found'}), 404
    
    if ride['driver_id'] != user['id'] and not user.get('is_admin'):
        return jsonify({'error': 'Not authorized'}), 403
    
    db.cancel_ride(ride_id)
    
    return jsonify({'success': True, 'message': 'Ride cancelled'})


@api_bp.route('/rides/<int:ride_id>/complete', methods=['POST'])
@api_login_required
def api_complete_ride(ride_id):
    """Mark a ride as completed."""
    user = get_api_user()
    ride = db.get_ride_by_id(ride_id)
    
    if not ride:
        return jsonify({'error': 'Ride not found'}), 404
    
    if ride['driver_id'] != user['id'] and not user.get('is_admin'):
        return jsonify({'error': 'Not authorized'}), 403
    
    if ride['status'] == 'completed':
        return jsonify({'error': 'Ride already completed'}), 400
    
    if ride['status'] == 'cancelled':
        return jsonify({'error': 'Cannot complete a cancelled ride'}), 400
    
    db.mark_ride_completed(ride_id)
    
    return jsonify({'success': True, 'message': 'Ride marked as completed'})


@api_bp.route('/rides/<int:ride_id>/quick-book', methods=['POST'])
@api_login_required
def api_quick_book_ride(ride_id):
    """Quick book a ride with default 1 seat."""
    user = get_api_user()
    ride = db.get_ride_by_id(ride_id)
    
    if not ride:
        return jsonify({'error': 'Ride not found'}), 404
    
    if ride['driver_id'] == user['id']:
        return jsonify({'error': 'Cannot book your own ride'}), 400
    
    if ride['status'] != 'active':
        return jsonify({'error': 'Ride is not available for booking'}), 400
    
    available_seats = ride['total_seats'] - ride['seats_taken']
    if available_seats < 1:
        return jsonify({'error': 'No seats available'}), 400
    
    # Check if user already has a booking for this ride
    if db.check_existing_booking(ride_id, user['id']):
        return jsonify({'error': 'You already have a booking for this ride'}), 400
    
    # Create the booking
    booking_id = db.create_booking(
        ride_id=ride_id,
        passenger_id=user['id']
    )
    
    if not booking_id:
        return jsonify({'error': 'Could not create booking'}), 500
    
    return jsonify({
        'success': True,
        'message': 'Booking request sent',
        'booking_id': booking_id
    })


@api_bp.route('/rides/<int:ride_id>', methods=['DELETE'])
@api_login_required
def api_delete_ride(ride_id):
    """Delete a ride."""
    user = get_api_user()
    ride = db.get_ride_by_id(ride_id)
    
    if not ride:
        return jsonify({'error': 'Ride not found'}), 404
    
    if ride['driver_id'] != user['id'] and not user.get('is_admin'):
        return jsonify({'error': 'Not authorized'}), 403
    
    db.delete_ride(ride_id)
    
    return jsonify({'success': True, 'message': 'Ride deleted'})


@api_bp.route('/my-rides')
@api_bp.route('/rides/my')  # Alias for frontend compatibility
@api_login_required
def api_get_my_rides():
    """Get current user's rides as driver."""
    user = get_api_user()
    if not user:
        return jsonify({'error': 'Authentication required'}), 401
    
    status = request.args.get('status', '')
    limit = request.args.get('limit', type=int)
    
    rides = db.get_rides_by_driver(user['id'], status=status if status else None)
    
    # Limit results if requested
    if limit:
        rides = rides[:limit]
    
    result = []
    for r in rides:
        # Get pending booking count for this ride
        bookings = db.get_bookings_by_ride(r['id'])
        pending_count = sum(1 for b in bookings if b.get('status') == 'pending')
        confirmed_count = sum(1 for b in bookings if b.get('status') == 'confirmed')
        
        result.append({
            'id': r['id'],
            'origin': r['origin'],
            'destination': r['destination'],
            'departure_date': r['departure_date'],
            'departure_time': r['departure_time'],
            'price_per_seat': r['price_per_seat'],
            'seats_available': r.get('total_seats', 4) - r.get('seats_taken', 0),
            'total_seats': r.get('total_seats', 4),
            'seats_taken': r.get('seats_taken', 0),
            'status': r.get('status', 'active'),
            'created_at': r.get('created_at'),
            'pending_bookings': pending_count,
            'confirmed_bookings': confirmed_count
        })
    
    # Return array directly for compatibility
    return jsonify(result)


@api_bp.route('/my-rides/pending-count')
@api_bp.route('/rides/my/pending-count')
@api_login_required
def api_get_pending_booking_count():
    """Get total pending booking requests count for the driver's rides."""
    user = get_api_user()
    if not user:
        return jsonify({'error': 'Authentication required'}), 401
    
    # Get all active rides for this driver
    rides = db.get_rides_by_driver(user['id'], status='active')
    
    total_pending = 0
    rides_with_pending = []
    
    for r in rides:
        bookings = db.get_bookings_by_ride(r['id'])
        pending_count = sum(1 for b in bookings if b.get('status') == 'pending')
        if pending_count > 0:
            total_pending += pending_count
            rides_with_pending.append({
                'ride_id': r['id'],
                'origin': r['origin'],
                'destination': r['destination'],
                'pending_count': pending_count
            })
    
    return jsonify({
        'total_pending': total_pending,
        'rides_with_pending': rides_with_pending
    })


# =============================================================================
# Bookings API
# =============================================================================

@api_bp.route('/bookings')
@api_login_required
def api_get_bookings():
    """Get current user's bookings as passenger."""
    user = get_api_user()
    if not user:
        return jsonify({'error': 'Authentication required'}), 401
    
    status = request.args.get('status', '')
    limit = request.args.get('limit', type=int)
    
    bookings = db.get_bookings_by_passenger(user['id'], status=status if status else None)
    
    # Limit results if requested
    if limit:
        bookings = bookings[:limit]
    
    result = [{
        'id': b['id'],
        'ride_id': b['ride_id'],
        'origin': b.get('origin', ''),
        'destination': b.get('destination', ''),
        'departure_date': b.get('departure_date', ''),
        'departure_time': b.get('departure_time', ''),
        'price_per_seat': b.get('price_per_seat', 0),
        'status': b.get('status', 'pending'),
        'ride_status': b.get('ride_status', 'active'),
        'driver_name': b.get('driver_name', 'Driver'),
        'driver_id': b.get('driver_id'),
        'driver_phone': b.get('driver_phone'),
        'created_at': b.get('created_at')
    } for b in bookings]
    
    # Return array directly for compatibility
    return jsonify(result)


@api_bp.route('/bookings/<int:booking_id>')
@api_login_required
def api_get_booking(booking_id):
    """Get booking details."""
    user = get_api_user()
    booking = db.get_booking_by_id(booking_id)
    
    if not booking:
        return jsonify({'error': 'Booking not found'}), 404
    
    # Check authorization
    if booking['passenger_id'] != user['id'] and booking.get('driver_id') != user['id'] and not user.get('is_admin'):
        return jsonify({'error': 'Not authorized'}), 403
    
    # Ensure departure_time is always a string for JSON serialization
    dep_time = booking.get('departure_time', '')
    if hasattr(dep_time, 'strftime'):
        dep_time = dep_time.strftime('%H:%M:%S')
    return jsonify({
        'id': booking['id'],
        'ride_id': booking['ride_id'],
        'origin': booking.get('origin', ''),
        'destination': booking.get('destination', ''),
        'departure_date': booking.get('departure_date', ''),
        'departure_time': dep_time,
        'price_per_seat': booking.get('price_per_seat', 0),
        'status': booking.get('status', 'pending'),
        'ride_status': booking.get('ride_status', 'active'),
        'driver_name': booking.get('driver_name', 'Driver'),
        'driver_id': booking.get('driver_id'),
        'driver_email': booking.get('driver_email'),
        'driver_phone': booking.get('driver_phone'),
        'passenger_name': booking.get('passenger_name', 'Passenger'),
        'passenger_id': booking.get('passenger_id'),
        'passenger_email': booking.get('passenger_email'),
        'passenger_phone': booking.get('passenger_phone'),
        'passenger_photo': booking.get('passenger_photo'),
        'created_at': booking.get('created_at')
    })


@api_bp.route('/bookings', methods=['POST'])
@api_login_required
def api_create_booking():
    """Create a booking for a ride."""
    data = request.get_json() or {}
    user = get_api_user()
    
    ride_id = data.get('ride_id')
    
    if not ride_id:
        return jsonify({'error': 'ride_id is required'}), 400
    
    ride = db.get_ride_by_id(ride_id)
    if not ride:
        return jsonify({'error': 'Ride not found'}), 404
    
    if ride['driver_id'] == user['id']:
        return jsonify({'error': 'Cannot book your own ride'}), 400
    
    # Check if already booked
    if db.check_existing_booking(ride_id, user['id']):
        return jsonify({'error': 'You have already booked this ride'}), 400
    
    # Check available seats
    seats_available = ride.get('total_seats', 4) - ride.get('seats_taken', 0)
    if seats_available < 1:
        return jsonify({'error': 'No seats available'}), 400
    
    booking_id = db.create_booking(ride_id=ride_id, passenger_id=user['id'])
    
    if not booking_id:
        return jsonify({'error': 'Could not create booking'}), 500
    
    return jsonify({'success': True, 'booking_id': booking_id, 'id': booking_id}), 201


@api_bp.route('/bookings/<int:booking_id>/cancel', methods=['POST'])
@api_login_required
def api_cancel_booking(booking_id):
    """Cancel a booking."""
    user = get_api_user()
    booking = db.get_booking_by_id(booking_id)
    
    if not booking:
        return jsonify({'error': 'Booking not found'}), 404
    
    if booking['passenger_id'] != user['id'] and not user.get('is_admin'):
        return jsonify({'error': 'Not authorized'}), 403
    
    db.cancel_booking(booking_id)
    
    return jsonify({'success': True, 'message': 'Booking cancelled'})


@api_bp.route('/bookings/<int:booking_id>/approve', methods=['POST'])
@api_login_required  
def api_approve_booking(booking_id):
    """Approve a booking (driver only)."""
    user = get_api_user()
    booking = db.get_booking_by_id(booking_id)
    
    if not booking:
        return jsonify({'error': 'Booking not found', 'success': False}), 404
    
    if booking.get('driver_id') != user['id'] and not user.get('is_admin'):
        return jsonify({'error': 'Not authorized', 'success': False}), 403
    
    if booking.get('status') != 'pending':
        return jsonify({'error': 'Booking is no longer pending', 'success': False}), 400
    
    result = db.approve_booking(booking_id)
    
    if result:
        return jsonify({'success': True, 'message': 'Booking approved'})
    else:
        return jsonify({'error': 'Could not approve booking. Ride may be full.', 'success': False}), 400


@api_bp.route('/bookings/<int:booking_id>/reject', methods=['POST'])
@api_login_required  
def api_reject_booking(booking_id):
    """Reject a booking (driver only)."""
    user = get_api_user()
    booking = db.get_booking_by_id(booking_id)
    
    if not booking:
        return jsonify({'error': 'Booking not found', 'success': False}), 404
    
    if booking.get('driver_id') != user['id'] and not user.get('is_admin'):
        return jsonify({'error': 'Not authorized', 'success': False}), 403
    
    if booking.get('status') != 'pending':
        return jsonify({'error': 'Booking is no longer pending', 'success': False}), 400
    
    result = db.reject_booking(booking_id)
    
    if result:
        return jsonify({'success': True, 'message': 'Booking rejected'})
    else:
        return jsonify({'error': 'Could not reject booking', 'success': False}), 400


# =============================================================================
# Messages API
# =============================================================================

@api_bp.route('/conversations')
@api_login_required
def api_get_conversations():
    """Get user's conversations."""
    user = get_api_user()
    if not user:
        return jsonify({'error': 'Authentication required'}), 401
    
    conversations = db.get_conversations_for_user(user['id'])
    
    # Return as array for consistency
    return jsonify([{
        'id': c.get('other_user_id'),
        'user_id': c.get('other_user_id'),
        'other_user_name': c.get('other_user_name', 'User'),
        'other_user_photo': c.get('other_user_photo'),
        'last_message': c.get('last_message', ''),
        'last_message_time': c.get('last_message_time'),
        'unread_count': c.get('unread_count', 0),
        'unread': c.get('unread_count', 0) > 0
    } for c in conversations])


@api_bp.route('/conversations/<int:user_id>')
@api_login_required
def api_get_conversation(user_id):
    """Get messages in a conversation with another user."""
    user = get_api_user()
    if not user:
        return jsonify({'error': 'Authentication required'}), 401
    
    # Get other user info first
    other_user = db.get_user_by_id(user_id)
    if not other_user:
        return jsonify({'error': 'User not found'}), 404
    
    messages = db.get_messages_in_conversation(user['id'], user_id)
    
    # Mark as read
    db.mark_messages_read(user['id'], user_id)
    
    return jsonify({
        'other_user': {
            'id': other_user['id'],
            'name': other_user['full_name'],
            'full_name': other_user['full_name'],
            'profile_photo': other_user.get('profile_photo')
        },
        'messages': [{
            'id': m['id'],
            'sender_id': m['sender_id'],
            'content': m['content'],
            'created_at': m['created_at'],
            'is_mine': m['sender_id'] == user['id'],
            'is_read': m.get('is_read', False)
        } for m in messages]
    })


@api_bp.route('/messages')
@api_login_required
def api_get_messages():
    """Get recent messages/conversations for home page."""
    user = get_api_user()
    if not user:
        return jsonify({'error': 'Authentication required'}), 401
    
    limit = request.args.get('limit', 10, type=int)
    
    # Get conversations and return as message previews
    conversations = db.get_conversations_for_user(user['id'])
    
    # Format for home page display
    result = [{
        'conversation_id': c.get('other_user_id'),
        'sender_name': c.get('other_user_name', 'User'),
        'preview': c.get('last_message', 'No messages')[:50] + ('...' if len(c.get('last_message', '')) > 50 else ''),
        'last_message_time': c.get('last_message_time'),
        'unread_count': c.get('unread_count', 0)
    } for c in conversations[:limit]]
    
    return jsonify(result)


@api_bp.route('/messages', methods=['POST'])
@api_login_required
def api_send_message():
    """Send a message."""
    data = request.get_json() or {}
    user = get_api_user()
    if not user:
        return jsonify({'error': 'Authentication required'}), 401
    
    recipient_id = data.get('recipient_id')
    content = data.get('content', '').strip()
    
    if not recipient_id or not content:
        return jsonify({'error': 'recipient_id and content are required'}), 400
    
    # Check if blocked
    if db.is_user_blocked(recipient_id, user['id']) or db.is_blocked_by(user['id'], recipient_id):
        return jsonify({'error': 'Cannot send message to this user'}), 403
    
    message_id = db.create_message(
        sender_id=user['id'],
        receiver_id=recipient_id,
        content=content,
        ride_id=data.get('ride_id')  # Optional
    )
    
    return jsonify({'success': True, 'message_id': message_id, 'conversation_id': recipient_id}), 201


@api_bp.route('/messages/unread-count')
@api_login_required
def api_unread_count():
    """Get unread message count."""
    user = get_api_user()
    count = db.get_unread_count(user['id'])
    return jsonify({'unread_count': count})


# =============================================================================
# Ratings API
# =============================================================================

@api_bp.route('/ratings', methods=['POST'])
@api_login_required
def api_create_rating():
    """Create a rating for a user."""
    data = request.get_json() or {}
    user = get_api_user()
    
    reviewed_user_id = data.get('user_id')
    ride_id = data.get('ride_id')
    rating = data.get('rating')
    
    if not reviewed_user_id or not rating or not ride_id:
        return jsonify({'error': 'user_id, ride_id and rating are required'}), 400
    
    if reviewed_user_id == user['id']:
        return jsonify({'error': 'Cannot rate yourself'}), 400
    
    if rating < 1 or rating > 5:
        return jsonify({'error': 'Rating must be between 1 and 5'}), 400
    
    # Check if can review
    can_review, reason = db.can_review_user(user['id'], reviewed_user_id, ride_id)
    if not can_review:
        return jsonify({'error': reason}), 400
    
    review_id = db.create_review(
        reviewer_id=user['id'],
        reviewed_user_id=reviewed_user_id,
        ride_id=ride_id,
        rating=rating,
        comment=data.get('comment', '')
    )
    
    return jsonify({'success': True, 'review_id': review_id}), 201


# =============================================================================
# Block Users API
# =============================================================================

@api_bp.route('/users/<int:user_id>/block', methods=['POST'])
@api_login_required
def api_block_user(user_id):
    """Block a user."""
    user = get_api_user()
    
    if user_id == user['id']:
        return jsonify({'error': 'Cannot block yourself'}), 400
    
    db.block_user(user['id'], user_id)
    return jsonify({'success': True, 'message': 'User blocked'})


@api_bp.route('/users/<int:user_id>/unblock', methods=['POST'])
@api_login_required
def api_unblock_user(user_id):
    """Unblock a user."""
    user = get_api_user()
    
    db.unblock_user(user['id'], user_id)
    return jsonify({'success': True, 'message': 'User unblocked'})


@api_bp.route('/blocked-users')
@api_login_required
def api_get_blocked_users():
    """Get list of blocked users."""
    user = get_api_user()
    blocked = db.get_blocked_users(user['id'])
    return jsonify({'blocked_users': blocked})


# =============================================================================
# Admin API
# =============================================================================

@api_bp.route('/admin/stats')
@api_admin_required
def api_admin_stats():
    """Get admin dashboard statistics."""
    stats = db.get_platform_statistics()
    return jsonify(stats)


@api_bp.route('/admin/users')
@api_admin_required
def api_admin_users():
    """Get all users for admin."""
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 20, type=int)
    search = request.args.get('search', '')
    
    users, total = db.get_all_users(page=page, per_page=per_page, search=search if search else None)
    
    return jsonify({
        'users': users,
        'total': total,
        'page': page,
        'per_page': per_page
    })


@api_bp.route('/admin/users/<int:user_id>')
@api_admin_required
def api_admin_user_detail(user_id):
    """Get user details for admin."""
    user = db.get_user_by_id(user_id)
    if not user:
        return jsonify({'error': 'User not found'}), 404
    
    return jsonify(dict(user))


@api_bp.route('/admin/users/<int:user_id>/ban', methods=['POST'])
@api_admin_required
def api_admin_ban_user(user_id):
    """Ban a user."""
    db.ban_user(user_id)
    return jsonify({'success': True, 'message': 'User banned'})


@api_bp.route('/admin/users/<int:user_id>/unban', methods=['POST'])
@api_admin_required
def api_admin_unban_user(user_id):
    """Unban a user."""
    db.unban_user(user_id)
    return jsonify({'success': True, 'message': 'User unbanned'})


@api_bp.route('/admin/users/<int:user_id>', methods=['DELETE'])
@api_admin_required
def api_admin_delete_user(user_id):
    """Delete a user."""
    # We'll just ban them instead of deleting to preserve data integrity
    db.ban_user(user_id)
    return jsonify({'success': True, 'message': 'User removed'})


@api_bp.route('/admin/rides')
@api_admin_required
def api_admin_rides():
    """Get all rides for admin."""
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 20, type=int)
    status = request.args.get('status', '')
    
    rides, total = db.get_all_rides(page=page, per_page=per_page, status=status if status else None)
    
    return jsonify({
        'rides': rides,
        'total': total,
        'page': page,
        'per_page': per_page
    })


@api_bp.route('/admin/rides/<int:ride_id>/cancel', methods=['POST'])
@api_admin_required
def api_admin_cancel_ride(ride_id):
    """Admin cancel a ride."""
    db.cancel_ride(ride_id)
    return jsonify({'success': True, 'message': 'Ride cancelled'})


@api_bp.route('/admin/reports')
@api_admin_required
def api_admin_reports():
    """Get all user reports for admin."""
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 20, type=int)
    status = request.args.get('status', '')
    
    reports, total = db.get_reports(
        status=status if status else None,
        page=page,
        per_page=per_page
    )
    
    return jsonify({
        'reports': reports,
        'total': total,
        'page': page,
        'per_page': per_page
    })


@api_bp.route('/admin/reports/<int:report_id>/resolve', methods=['POST'])
@api_admin_required
def api_admin_resolve_report(report_id):
    """Resolve a user report."""
    data = request.get_json() or {}
    admin_notes = data.get('admin_notes', '')
    
    success = db.update_report_status(report_id, 'resolved', admin_notes)
    
    if not success:
        return jsonify({'error': 'Report not found'}), 404
    
    return jsonify({'success': True, 'message': 'Report resolved'})


@api_bp.route('/admin/reports/<int:report_id>/dismiss', methods=['POST'])
@api_admin_required
def api_admin_dismiss_report(report_id):
    """Dismiss a user report."""
    data = request.get_json() or {}
    admin_notes = data.get('admin_notes', '')
    
    success = db.update_report_status(report_id, 'dismissed', admin_notes)
    
    if not success:
        return jsonify({'error': 'Report not found'}), 404
    
    return jsonify({'success': True, 'message': 'Report dismissed'})


# =============================================================================
# Feature Flags
# =============================================================================

@api_bp.route('/features')
def api_features():
    """Get enabled features."""
    return jsonify({
        'google_maps': config.is_google_maps_enabled(),
        'ai_chatbot': config.is_openai_enabled(),
        'email_verification': config.is_email_sending_enabled()
    })
