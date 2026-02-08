"""
Campus Ride-Share Platform - Rides Routes

This module contains all routes related to ride management including
posting, searching, viewing, editing, and managing rides.
"""

from datetime import datetime, date
from flask import (
    Blueprint, render_template, request, redirect, url_for,
    flash, g, abort, jsonify
)

from database import db
from auth import login_required, driver_required, get_csrf_token, validate_csrf_token
from config import config


# Create blueprint
rides_bp = Blueprint('rides', __name__, url_prefix='/rides')


# =============================================================================
# Helper Functions
# =============================================================================

def validate_ride_data(form_data: dict, is_edit: bool = False) -> tuple[bool, list, dict]:
    """
    Validate ride form data.
    
    Args:
        form_data: The form data dictionary.
        is_edit: Whether this is an edit operation.
    
    Returns:
        Tuple of (is_valid, errors, cleaned_data)
    """
    errors = []
    cleaned = {}
    
    # Origin
    origin = form_data.get('origin', '').strip()
    if not origin:
        errors.append('Please enter the pickup location.')
    else:
        cleaned['origin'] = origin
    
    # Destination
    destination = form_data.get('destination', '').strip()
    if not destination:
        errors.append('Please enter the destination.')
    else:
        cleaned['destination'] = destination
    
    # Departure date
    departure_date_str = form_data.get('departure_date', '').strip()
    if not departure_date_str:
        errors.append('Please select a departure date.')
    else:
        try:
            departure_date = datetime.strptime(departure_date_str, '%Y-%m-%d').date()
            if departure_date < date.today():
                errors.append('Departure date cannot be in the past.')
            else:
                cleaned['departure_date'] = departure_date_str
        except ValueError:
            errors.append('Invalid departure date format.')
    
    # Departure time
    departure_time = form_data.get('departure_time', '').strip()
    if not departure_time:
        errors.append('Please select a departure time.')
    else:
        try:
            datetime.strptime(departure_time, '%H:%M')
            cleaned['departure_time'] = departure_time
        except ValueError:
            errors.append('Invalid departure time format.')
    
    # Total seats
    total_seats_str = form_data.get('total_seats', '').strip()
    if not total_seats_str:
        errors.append('Please enter the number of available seats.')
    else:
        try:
            total_seats = int(total_seats_str)
            if total_seats < 1:
                errors.append('You must offer at least 1 seat.')
            elif total_seats > 7:
                errors.append('Maximum 7 seats allowed.')
            else:
                cleaned['total_seats'] = total_seats
        except ValueError:
            errors.append('Invalid number of seats.')
    
    # Price per seat
    price_str = form_data.get('price_per_seat', '').strip()
    if not price_str:
        errors.append('Please enter the price per seat.')
    else:
        try:
            price = float(price_str)
            if price < 0:
                errors.append('Price cannot be negative.')
            elif price > 100000:
                errors.append('Price seems too high. Please check.')
            else:
                cleaned['price_per_seat'] = price
        except ValueError:
            errors.append('Invalid price format.')
    
    # Optional fields
    origin_lat = form_data.get('origin_lat', '').strip()
    if origin_lat:
        try:
            cleaned['origin_lat'] = float(origin_lat)
        except ValueError:
            pass
    
    origin_lng = form_data.get('origin_lng', '').strip()
    if origin_lng:
        try:
            cleaned['origin_lng'] = float(origin_lng)
        except ValueError:
            pass
    
    destination_lat = form_data.get('destination_lat', '').strip()
    if destination_lat:
        try:
            cleaned['destination_lat'] = float(destination_lat)
        except ValueError:
            pass
    
    destination_lng = form_data.get('destination_lng', '').strip()
    if destination_lng:
        try:
            cleaned['destination_lng'] = float(destination_lng)
        except ValueError:
            pass
    
    distance_km = form_data.get('distance_km', '').strip()
    if distance_km:
        try:
            cleaned['distance_km'] = float(distance_km)
        except ValueError:
            pass
    
    duration_minutes = form_data.get('estimated_duration_minutes', '').strip()
    if duration_minutes:
        try:
            cleaned['estimated_duration_minutes'] = int(duration_minutes)
        except ValueError:
            pass
    
    notes = form_data.get('notes', '').strip()
    if notes:
        if len(notes) > 500:
            errors.append('Notes must be under 500 characters.')
        else:
            cleaned['notes'] = notes
    
    return len(errors) == 0, errors, cleaned


# =============================================================================
# Ride Routes
# =============================================================================

@rides_bp.route('/post', methods=['GET', 'POST'])
@login_required
@driver_required
def post_ride():
    """Post a new ride."""
    if request.method == 'POST':
        # Validate CSRF
        csrf_token = request.form.get('csrf_token')
        if not validate_csrf_token(csrf_token):
            flash('Invalid form submission. Please try again.', 'error')
            return redirect(url_for('rides.post_ride'))
        
        # Validate ride data
        is_valid, errors, cleaned_data = validate_ride_data(request.form)
        
        if not is_valid:
            for error in errors:
                flash(error, 'error')
            return render_template('rides/post_ride.html',
                form_data=request.form,
                price_per_km=config.PRICE_PER_KM
            )
        
        # Create ride
        try:
            ride_id = db.create_ride(
                driver_id=g.user['id'],
                origin=cleaned_data['origin'],
                destination=cleaned_data['destination'],
                departure_date=cleaned_data['departure_date'],
                departure_time=cleaned_data['departure_time'],
                total_seats=cleaned_data['total_seats'],
                price_per_seat=cleaned_data['price_per_seat'],
                origin_lat=cleaned_data.get('origin_lat'),
                origin_lng=cleaned_data.get('origin_lng'),
                destination_lat=cleaned_data.get('destination_lat'),
                destination_lng=cleaned_data.get('destination_lng'),
                distance_km=cleaned_data.get('distance_km'),
                estimated_duration_minutes=cleaned_data.get('estimated_duration_minutes'),
                notes=cleaned_data.get('notes')
            )
            
            flash('Your ride has been posted successfully!', 'success')
            return redirect(url_for('rides.ride_detail', ride_id=ride_id))
            
        except Exception as e:
            flash('An error occurred while posting your ride. Please try again.', 'error')
            return render_template('rides/post_ride.html',
                form_data=request.form,
                price_per_km=config.PRICE_PER_KM
            )
    
    return render_template('rides/post_ride.html',
        form_data={},
        price_per_km=config.PRICE_PER_KM
    )


@rides_bp.route('/search')
@login_required
def search():
    """Search for rides."""
    # Get search parameters
    destination = request.args.get('destination', '').strip()
    date_from = request.args.get('date_from', '').strip()
    date_to = request.args.get('date_to', '').strip()
    max_price = request.args.get('max_price', '').strip()
    min_rating = request.args.get('min_rating', '').strip()
    sort_by = request.args.get('sort_by', 'departure_date')
    page = request.args.get('page', '1')
    
    # Parse numeric values
    try:
        page = max(1, int(page))
    except ValueError:
        page = 1
    
    try:
        max_price_val = float(max_price) if max_price else None
    except ValueError:
        max_price_val = None
    
    try:
        min_rating_val = float(min_rating) if min_rating else None
    except ValueError:
        min_rating_val = None
    
    # Search rides
    rides, total = db.search_rides(
        destination=destination if destination else None,
        date_from=date_from if date_from else None,
        date_to=date_to if date_to else None,
        max_price=max_price_val,
        min_rating=min_rating_val,
        sort_by=sort_by,
        page=page,
        per_page=12
    )
    
    # Filter out user's own rides from search results
    rides = [r for r in rides if r['driver_id'] != g.user['id']]
    
    # Calculate pagination
    total_pages = (total + 11) // 12
    
    return render_template('rides/search.html',
        rides=rides,
        total=total,
        page=page,
        total_pages=total_pages,
        destination=destination,
        date_from=date_from,
        date_to=date_to,
        max_price=max_price,
        min_rating=min_rating,
        sort_by=sort_by
    )


@rides_bp.route('/<int:ride_id>')
@login_required
def ride_detail(ride_id):
    """View ride details."""
    ride = db.get_ride_by_id(ride_id)
    
    if not ride:
        abort(404)
    
    # Get bookings for this ride
    bookings = db.get_bookings_by_ride(ride_id)
    
    # Check if current user has already booked
    user_booking = None
    for booking in bookings:
        if booking['passenger_id'] == g.user['id']:
            user_booking = booking
            break
    
    # Check if current user is the driver
    is_driver = ride['driver_id'] == g.user['id']
    
    # Get driver info
    driver = db.get_user_by_id(ride['driver_id'])
    driver_rating = db.get_user_average_rating(ride['driver_id'])
    driver_reviews = db.get_reviews_for_user(ride['driver_id'])
    
    # Check if blocked
    is_blocked = False
    is_blocked_by = False
    if not is_driver:
        is_blocked = db.is_user_blocked(g.user['id'], ride['driver_id'])
        is_blocked_by = db.is_blocked_by(g.user['id'], ride['driver_id'])
    
    # Calculate available seats
    available_seats = ride['total_seats'] - ride['seats_taken']
    
    # Get pending bookings count for driver
    pending_bookings = [b for b in bookings if b['status'] == 'pending']
    confirmed_bookings = [b for b in bookings if b['status'] == 'confirmed']
    
    return render_template('rides/ride_detail.html',
        ride=ride,
        driver=driver,
        driver_rating=driver_rating,
        driver_review_count=len(driver_reviews),
        bookings=bookings,
        pending_bookings=pending_bookings,
        confirmed_bookings=confirmed_bookings,
        user_booking=user_booking,
        is_driver=is_driver,
        available_seats=available_seats,
        is_blocked=is_blocked,
        is_blocked_by=is_blocked_by,
        can_book=(
            not is_driver and 
            not user_booking and 
            ride['status'] == 'active' and 
            available_seats > 0 and
            not is_blocked and
            not is_blocked_by
        )
    )


@rides_bp.route('/<int:ride_id>/edit', methods=['GET', 'POST'])
@login_required
def edit_ride(ride_id):
    """Edit a ride."""
    ride = db.get_ride_by_id(ride_id)
    
    if not ride:
        abort(404)
    
    # Only driver can edit their own ride
    if ride['driver_id'] != g.user['id']:
        flash('You can only edit your own rides.', 'error')
        return redirect(url_for('rides.ride_detail', ride_id=ride_id))
    
    # Cannot edit completed or cancelled rides
    if ride['status'] in ('completed', 'cancelled'):
        flash('You cannot edit a completed or cancelled ride.', 'error')
        return redirect(url_for('rides.ride_detail', ride_id=ride_id))
    
    # Check for confirmed bookings
    bookings = db.get_bookings_by_ride(ride_id)
    has_confirmed_bookings = any(b['status'] == 'confirmed' for b in bookings)
    
    if request.method == 'POST':
        # Validate CSRF
        csrf_token = request.form.get('csrf_token')
        if not validate_csrf_token(csrf_token):
            flash('Invalid form submission. Please try again.', 'error')
            return redirect(url_for('rides.edit_ride', ride_id=ride_id))
        
        # Validate ride data
        is_valid, errors, cleaned_data = validate_ride_data(request.form, is_edit=True)
        
        if not is_valid:
            for error in errors:
                flash(error, 'error')
            return render_template('rides/edit_ride.html',
                ride=ride,
                form_data=request.form,
                has_confirmed_bookings=has_confirmed_bookings,
                price_per_km=config.PRICE_PER_KM
            )
        
        # If there are confirmed bookings, cannot reduce seats below current taken
        if has_confirmed_bookings:
            confirmed_count = sum(1 for b in bookings if b['status'] == 'confirmed')
            if cleaned_data['total_seats'] < confirmed_count:
                flash(f'Cannot reduce seats below {confirmed_count} as there are confirmed bookings.', 'error')
                return render_template('rides/edit_ride.html',
                    ride=ride,
                    form_data=request.form,
                    has_confirmed_bookings=has_confirmed_bookings,
                    price_per_km=config.PRICE_PER_KM
                )
        
        # Update ride
        try:
            db.update_ride(
                ride_id,
                origin=cleaned_data['origin'],
                destination=cleaned_data['destination'],
                departure_date=cleaned_data['departure_date'],
                departure_time=cleaned_data['departure_time'],
                total_seats=cleaned_data['total_seats'],
                price_per_seat=cleaned_data['price_per_seat'],
                origin_lat=cleaned_data.get('origin_lat'),
                origin_lng=cleaned_data.get('origin_lng'),
                destination_lat=cleaned_data.get('destination_lat'),
                destination_lng=cleaned_data.get('destination_lng'),
                distance_km=cleaned_data.get('distance_km'),
                estimated_duration_minutes=cleaned_data.get('estimated_duration_minutes'),
                notes=cleaned_data.get('notes')
            )
            
            flash('Ride updated successfully!', 'success')
            return redirect(url_for('rides.ride_detail', ride_id=ride_id))
            
        except Exception as e:
            flash('An error occurred while updating your ride. Please try again.', 'error')
            return render_template('rides/edit_ride.html',
                ride=ride,
                form_data=request.form,
                has_confirmed_bookings=has_confirmed_bookings,
                price_per_km=config.PRICE_PER_KM
            )
    
    # Pre-populate form with ride data
    form_data = {
        'origin': ride['origin'],
        'destination': ride['destination'],
        'departure_date': ride['departure_date'],
        'departure_time': ride['departure_time'],
        'total_seats': ride['total_seats'],
        'price_per_seat': ride['price_per_seat'],
        'origin_lat': ride.get('origin_lat', ''),
        'origin_lng': ride.get('origin_lng', ''),
        'destination_lat': ride.get('destination_lat', ''),
        'destination_lng': ride.get('destination_lng', ''),
        'distance_km': ride.get('distance_km', ''),
        'estimated_duration_minutes': ride.get('estimated_duration_minutes', ''),
        'notes': ride.get('notes', '')
    }
    
    return render_template('rides/edit_ride.html',
        ride=ride,
        form_data=form_data,
        has_confirmed_bookings=has_confirmed_bookings,
        price_per_km=config.PRICE_PER_KM
    )


@rides_bp.route('/<int:ride_id>/cancel', methods=['POST'])
@login_required
def cancel_ride(ride_id):
    """Cancel a ride."""
    ride = db.get_ride_by_id(ride_id)
    
    if not ride:
        abort(404)
    
    # Only driver can cancel their own ride
    if ride['driver_id'] != g.user['id']:
        flash('You can only cancel your own rides.', 'error')
        return redirect(url_for('rides.ride_detail', ride_id=ride_id))
    
    # Cannot cancel already completed or cancelled rides
    if ride['status'] in ('completed', 'cancelled'):
        flash('This ride is already completed or cancelled.', 'error')
        return redirect(url_for('rides.ride_detail', ride_id=ride_id))
    
    # Validate CSRF
    csrf_token = request.form.get('csrf_token')
    if not validate_csrf_token(csrf_token):
        flash('Invalid form submission. Please try again.', 'error')
        return redirect(url_for('rides.ride_detail', ride_id=ride_id))
    
    # Cancel the ride (this also cancels all bookings)
    db.cancel_ride(ride_id)
    
    # Send notifications to confirmed passengers
    from email_utils import send_booking_cancelled_notification
    
    bookings = db.get_bookings_by_ride(ride_id)
    for booking in bookings:
        if booking['status'] in ('confirmed', 'pending'):
            passenger = db.get_user_by_id(booking['passenger_id'])
            if passenger:
                send_booking_cancelled_notification(
                    to_email=passenger['email'],
                    user_name=passenger['full_name'],
                    cancelled_by=f"Driver ({g.user['full_name']})",
                    origin=ride['origin'],
                    destination=ride['destination'],
                    departure_date=ride['departure_date'],
                    departure_time=ride['departure_time'],
                    recipient_id=passenger['id']
                )
    
    flash('Ride cancelled successfully. All passengers have been notified.', 'success')
    return redirect(url_for('rides.my_rides'))


@rides_bp.route('/<int:ride_id>/complete', methods=['POST'])
@login_required
def complete_ride(ride_id):
    """Mark a ride as completed."""
    ride = db.get_ride_by_id(ride_id)
    
    if not ride:
        abort(404)
    
    # Only driver can complete their own ride
    if ride['driver_id'] != g.user['id']:
        flash('You can only complete your own rides.', 'error')
        return redirect(url_for('rides.ride_detail', ride_id=ride_id))
    
    # Can only complete active or full rides
    if ride['status'] not in ('active', 'full'):
        flash('This ride cannot be marked as completed.', 'error')
        return redirect(url_for('rides.ride_detail', ride_id=ride_id))
    
    # Validate CSRF
    csrf_token = request.form.get('csrf_token')
    if not validate_csrf_token(csrf_token):
        flash('Invalid form submission. Please try again.', 'error')
        return redirect(url_for('rides.ride_detail', ride_id=ride_id))
    
    # Mark ride as completed
    db.mark_ride_completed(ride_id)
    
    # Send rating request emails
    from email_utils import send_rating_request
    
    bookings = db.get_bookings_by_ride(ride_id)
    confirmed_bookings = [b for b in bookings if b['status'] == 'completed']
    
    # Send rating request to driver about each passenger
    for booking in confirmed_bookings:
        passenger = db.get_user_by_id(booking['passenger_id'])
        if passenger:
            # Driver rates passenger
            send_rating_request(
                to_email=g.user['email'],
                user_name=g.user['full_name'],
                other_user_name=passenger['full_name'],
                origin=ride['origin'],
                destination=ride['destination'],
                departure_date=ride['departure_date'],
                ride_id=ride_id,
                other_user_id=passenger['id'],
                recipient_id=g.user['id']
            )
            
            # Passenger rates driver
            send_rating_request(
                to_email=passenger['email'],
                user_name=passenger['full_name'],
                other_user_name=g.user['full_name'],
                origin=ride['origin'],
                destination=ride['destination'],
                departure_date=ride['departure_date'],
                ride_id=ride_id,
                other_user_id=g.user['id'],
                recipient_id=passenger['id']
            )
    
    flash('Ride marked as completed! Rating requests have been sent.', 'success')
    return redirect(url_for('rides.my_rides'))


@rides_bp.route('/my')
@login_required
def my_rides():
    """View my posted rides as a driver."""
    # Get all rides by current user
    rides = db.get_rides_by_driver(g.user['id'])
    
    # Separate by status
    active_rides = [r for r in rides if r['status'] == 'active']
    full_rides = [r for r in rides if r['status'] == 'full']
    completed_rides = [r for r in rides if r['status'] == 'completed']
    cancelled_rides = [r for r in rides if r['status'] == 'cancelled']
    
    # Get pending booking counts for each active ride
    for ride in active_rides + full_rides:
        bookings = db.get_bookings_by_ride(ride['id'])
        ride['pending_count'] = sum(1 for b in bookings if b['status'] == 'pending')
        ride['confirmed_count'] = sum(1 for b in bookings if b['status'] == 'confirmed')
        ride['available_seats'] = ride['total_seats'] - ride['seats_taken']
    
    return render_template('rides/my_rides.html',
        active_rides=active_rides,
        full_rides=full_rides,
        completed_rides=completed_rides,
        cancelled_rides=cancelled_rides,
        is_driver=g.user['is_driver']
    )


# =============================================================================
# API Endpoints
# =============================================================================

@rides_bp.route('/api/suggest-price', methods=['POST'])
@login_required
def suggest_price():
    """API endpoint to suggest a price based on distance."""
    try:
        data = request.get_json()
        distance_km = float(data.get('distance_km', 0))
        
        if distance_km <= 0:
            return jsonify({'error': 'Invalid distance'}), 400
        
        suggested_price = round(distance_km * config.PRICE_PER_KM, 2)
        
        return jsonify({
            'success': True,
            'suggested_price': suggested_price,
            'price_per_km': config.PRICE_PER_KM
        })
        
    except (ValueError, TypeError):
        return jsonify({'error': 'Invalid data'}), 400


@rides_bp.route('/api/search', methods=['GET'])
@login_required
def api_search():
    """API endpoint for searching rides (for AJAX)."""
    destination = request.args.get('destination', '').strip()
    date_from = request.args.get('date_from', '').strip()
    date_to = request.args.get('date_to', '').strip()
    max_price = request.args.get('max_price', '').strip()
    page = request.args.get('page', '1')
    
    try:
        page = max(1, int(page))
    except ValueError:
        page = 1
    
    try:
        max_price_val = float(max_price) if max_price else None
    except ValueError:
        max_price_val = None
    
    rides, total = db.search_rides(
        destination=destination if destination else None,
        date_from=date_from if date_from else None,
        date_to=date_to if date_to else None,
        max_price=max_price_val,
        page=page,
        per_page=12
    )
    
    # Filter out user's own rides
    rides = [r for r in rides if r['driver_id'] != g.user['id']]
    
    # Convert to JSON-serializable format
    rides_data = []
    for ride in rides:
        rides_data.append({
            'id': ride['id'],
            'origin': ride['origin'],
            'destination': ride['destination'],
            'departure_date': ride['departure_date'],
            'departure_time': ride['departure_time'],
            'price_per_seat': ride['price_per_seat'],
            'available_seats': ride['available_seats'],
            'driver_name': ride['driver_name'],
            'driver_rating': ride['driver_rating'],
            'origin_lat': ride.get('origin_lat'),
            'origin_lng': ride.get('origin_lng'),
            'destination_lat': ride.get('destination_lat'),
            'destination_lng': ride.get('destination_lng')
        })
    
    return jsonify({
        'success': True,
        'rides': rides_data,
        'total': total,
        'page': page,
        'total_pages': (total + 11) // 12
    })
