"""
Campus Ride-Share Platform - Bookings Routes

This module contains all routes related to booking management including
requesting, approving, rejecting, and cancelling ride bookings.
"""

from flask import (
    Blueprint, render_template, request, redirect, url_for,
    flash, g, abort
)

from database import db
from auth import login_required, validate_csrf_token
from email_utils import (
    send_booking_request_notification,
    send_booking_confirmed_notification,
    send_booking_rejected_notification,
    send_booking_cancelled_notification
)


# Create blueprint
bookings_bp = Blueprint('bookings', __name__, url_prefix='/bookings')


# =============================================================================
# Booking Routes
# =============================================================================

@bookings_bp.route('/book/<int:ride_id>', methods=['POST'])
@login_required
def book_ride(ride_id):
    """Request to join a ride."""
    # Validate CSRF
    csrf_token = request.form.get('csrf_token')
    if not validate_csrf_token(csrf_token):
        flash('Invalid form submission. Please try again.', 'error')
        return redirect(url_for('rides.ride_detail', ride_id=ride_id))
    
    # Get the ride
    ride = db.get_ride_by_id(ride_id)
    
    if not ride:
        abort(404)
    
    # Check if user is the driver
    if ride['driver_id'] == g.user['id']:
        flash('You cannot book your own ride.', 'error')
        return redirect(url_for('rides.ride_detail', ride_id=ride_id))
    
    # Check ride status
    if ride['status'] != 'active':
        flash('This ride is no longer available for booking.', 'error')
        return redirect(url_for('rides.ride_detail', ride_id=ride_id))
    
    # Check available seats
    available_seats = ride['total_seats'] - ride['seats_taken']
    if available_seats <= 0:
        flash('This ride is full.', 'error')
        return redirect(url_for('rides.ride_detail', ride_id=ride_id))
    
    # Check if already booked
    if db.check_existing_booking(ride_id, g.user['id']):
        flash('You have already requested to join this ride.', 'info')
        return redirect(url_for('rides.ride_detail', ride_id=ride_id))
    
    # Check if user is blocked by driver or has blocked driver
    if db.is_user_blocked(g.user['id'], ride['driver_id']):
        flash('You cannot book this ride.', 'error')
        return redirect(url_for('rides.ride_detail', ride_id=ride_id))
    
    if db.is_blocked_by(g.user['id'], ride['driver_id']):
        flash('You cannot book this ride.', 'error')
        return redirect(url_for('rides.ride_detail', ride_id=ride_id))
    
    # Create the booking
    booking_id = db.create_booking(ride_id, g.user['id'])
    
    if not booking_id:
        flash('Unable to create booking. Please try again.', 'error')
        return redirect(url_for('rides.ride_detail', ride_id=ride_id))
    
    # Get driver info for email
    driver = db.get_user_by_id(ride['driver_id'])
    
    # Send notification email to driver
    if driver:
        send_booking_request_notification(
            to_email=driver['email'],
            driver_name=driver['full_name'],
            passenger_name=g.user['full_name'],
            origin=ride['origin'],
            destination=ride['destination'],
            departure_date=ride['departure_date'],
            departure_time=ride['departure_time'],
            ride_id=ride_id,
            recipient_id=driver['id']
        )
    
    flash('Your booking request has been sent! The driver will review it shortly.', 'success')
    return redirect(url_for('bookings.my_bookings'))


@bookings_bp.route('/my')
@login_required
def my_bookings():
    """View my bookings as a passenger."""
    # Get all bookings by current user
    bookings = db.get_bookings_by_passenger(g.user['id'])
    
    # Separate by status
    pending_bookings = [b for b in bookings if b['status'] == 'pending']
    confirmed_bookings = [b for b in bookings if b['status'] == 'confirmed']
    completed_bookings = [b for b in bookings if b['status'] == 'completed']
    cancelled_bookings = [b for b in bookings if b['status'] in ('cancelled', 'rejected')]
    
    # Add driver ratings
    for booking in pending_bookings + confirmed_bookings + completed_bookings:
        booking['driver_rating'] = db.get_user_average_rating(booking['driver_id'])
    
    return render_template('bookings/my_bookings.html',
        pending_bookings=pending_bookings,
        confirmed_bookings=confirmed_bookings,
        completed_bookings=completed_bookings,
        cancelled_bookings=cancelled_bookings
    )


@bookings_bp.route('/<int:booking_id>')
@login_required
def booking_detail(booking_id):
    """View booking details."""
    booking = db.get_booking_by_id(booking_id)
    
    if not booking:
        abort(404)
    
    # Check authorization - must be passenger or driver
    is_passenger = booking['passenger_id'] == g.user['id']
    is_driver = booking['driver_id'] == g.user['id']
    
    if not is_passenger and not is_driver:
        flash('You do not have permission to view this booking.', 'error')
        return redirect(url_for('home'))
    
    # Get ride details
    ride = db.get_ride_by_id(booking['ride_id'])
    
    # Get user ratings
    if is_passenger:
        other_user = db.get_user_by_id(booking['driver_id'])
        other_user_rating = db.get_user_average_rating(booking['driver_id'])
    else:
        other_user = db.get_user_by_id(booking['passenger_id'])
        other_user_rating = db.get_user_average_rating(booking['passenger_id'])
    
    # Check if can rate
    can_rate = False
    if booking['status'] == 'completed':
        if is_passenger:
            can_rate = not db.check_review_exists(g.user['id'], booking['driver_id'], booking['ride_id'])
        else:
            can_rate = not db.check_review_exists(g.user['id'], booking['passenger_id'], booking['ride_id'])
    
    return render_template('bookings/booking_detail.html',
        booking=booking,
        ride=ride,
        is_passenger=is_passenger,
        is_driver=is_driver,
        other_user=other_user,
        other_user_rating=other_user_rating,
        can_rate=can_rate
    )


@bookings_bp.route('/<int:booking_id>/approve', methods=['POST'])
@login_required
def approve_booking(booking_id):
    """Approve a booking request (driver only)."""
    # Validate CSRF
    csrf_token = request.form.get('csrf_token')
    if not validate_csrf_token(csrf_token):
        flash('Invalid form submission. Please try again.', 'error')
        return redirect(url_for('bookings.booking_detail', booking_id=booking_id))
    
    booking = db.get_booking_by_id(booking_id)
    
    if not booking:
        abort(404)
    
    # Check if user is the driver
    if booking['driver_id'] != g.user['id']:
        flash('Only the driver can approve bookings.', 'error')
        return redirect(url_for('bookings.booking_detail', booking_id=booking_id))
    
    # Check booking status
    if booking['status'] != 'pending':
        flash('This booking is no longer pending.', 'error')
        return redirect(url_for('bookings.booking_detail', booking_id=booking_id))
    
    # Get ride to check available seats
    ride = db.get_ride_by_id(booking['ride_id'])
    if not ride:
        abort(404)
    
    available_seats = ride['total_seats'] - ride['seats_taken']
    if available_seats <= 0:
        flash('No more seats available on this ride.', 'error')
        return redirect(url_for('rides.ride_detail', ride_id=booking['ride_id']))
    
    # Approve the booking
    success = db.approve_booking(booking_id)
    
    if not success:
        flash('Unable to approve booking. Please try again.', 'error')
        return redirect(url_for('bookings.booking_detail', booking_id=booking_id))
    
    # Get passenger info for email
    passenger = db.get_user_by_id(booking['passenger_id'])
    
    # Send confirmation email to passenger
    if passenger:
        send_booking_confirmed_notification(
            to_email=passenger['email'],
            passenger_name=passenger['full_name'],
            driver_name=g.user['full_name'],
            driver_phone=g.user.get('phone', ''),
            origin=ride['origin'],
            destination=ride['destination'],
            departure_date=ride['departure_date'],
            departure_time=ride['departure_time'],
            price=ride['price_per_seat'],
            ride_id=ride['id'],
            recipient_id=passenger['id']
        )
    
    flash(f'Booking approved! {passenger["full_name"] if passenger else "The passenger"} has been notified.', 'success')
    return redirect(url_for('rides.ride_detail', ride_id=booking['ride_id']))


@bookings_bp.route('/<int:booking_id>/reject', methods=['POST'])
@login_required
def reject_booking(booking_id):
    """Reject a booking request (driver only)."""
    # Validate CSRF
    csrf_token = request.form.get('csrf_token')
    if not validate_csrf_token(csrf_token):
        flash('Invalid form submission. Please try again.', 'error')
        return redirect(url_for('bookings.booking_detail', booking_id=booking_id))
    
    booking = db.get_booking_by_id(booking_id)
    
    if not booking:
        abort(404)
    
    # Check if user is the driver
    if booking['driver_id'] != g.user['id']:
        flash('Only the driver can reject bookings.', 'error')
        return redirect(url_for('bookings.booking_detail', booking_id=booking_id))
    
    # Check booking status
    if booking['status'] != 'pending':
        flash('This booking is no longer pending.', 'error')
        return redirect(url_for('bookings.booking_detail', booking_id=booking_id))
    
    # Reject the booking
    success = db.reject_booking(booking_id)
    
    if not success:
        flash('Unable to reject booking. Please try again.', 'error')
        return redirect(url_for('bookings.booking_detail', booking_id=booking_id))
    
    # Get passenger and ride info for email
    passenger = db.get_user_by_id(booking['passenger_id'])
    ride = db.get_ride_by_id(booking['ride_id'])
    
    # Send rejection email to passenger
    if passenger and ride:
        send_booking_rejected_notification(
            to_email=passenger['email'],
            passenger_name=passenger['full_name'],
            origin=ride['origin'],
            destination=ride['destination'],
            departure_date=ride['departure_date'],
            recipient_id=passenger['id']
        )
    
    flash('Booking rejected. The passenger has been notified.', 'success')
    return redirect(url_for('rides.ride_detail', ride_id=booking['ride_id']))


@bookings_bp.route('/<int:booking_id>/cancel', methods=['POST'])
@login_required
def cancel_booking(booking_id):
    """Cancel a booking (passenger or driver)."""
    # Validate CSRF
    csrf_token = request.form.get('csrf_token')
    if not validate_csrf_token(csrf_token):
        flash('Invalid form submission. Please try again.', 'error')
        return redirect(url_for('bookings.booking_detail', booking_id=booking_id))
    
    booking = db.get_booking_by_id(booking_id)
    
    if not booking:
        abort(404)
    
    # Check authorization
    is_passenger = booking['passenger_id'] == g.user['id']
    is_driver = booking['driver_id'] == g.user['id']
    
    if not is_passenger and not is_driver:
        flash('You do not have permission to cancel this booking.', 'error')
        return redirect(url_for('home'))
    
    # Check booking status
    if booking['status'] not in ('pending', 'confirmed'):
        flash('This booking cannot be cancelled.', 'error')
        return redirect(url_for('bookings.booking_detail', booking_id=booking_id))
    
    # Cancel the booking
    success = db.cancel_booking(booking_id)
    
    if not success:
        flash('Unable to cancel booking. Please try again.', 'error')
        return redirect(url_for('bookings.booking_detail', booking_id=booking_id))
    
    # Get ride info
    ride = db.get_ride_by_id(booking['ride_id'])
    
    # Send notification to the other party
    if is_passenger:
        # Notify driver
        driver = db.get_user_by_id(booking['driver_id'])
        if driver and ride:
            send_booking_cancelled_notification(
                to_email=driver['email'],
                user_name=driver['full_name'],
                cancelled_by=f"Passenger ({g.user['full_name']})",
                origin=ride['origin'],
                destination=ride['destination'],
                departure_date=ride['departure_date'],
                departure_time=ride['departure_time'],
                recipient_id=driver['id']
            )
        flash('Your booking has been cancelled.', 'success')
        return redirect(url_for('bookings.my_bookings'))
    else:
        # Notify passenger
        passenger = db.get_user_by_id(booking['passenger_id'])
        if passenger and ride:
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
        flash('Booking cancelled. The passenger has been notified.', 'success')
        return redirect(url_for('rides.ride_detail', ride_id=booking['ride_id']))
