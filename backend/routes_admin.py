"""
Campus Ride-Share Platform - Admin Routes

This module contains all routes for the admin dashboard including
user management, ride management, and report handling.
"""

from flask import (
    Blueprint, render_template, request, redirect, url_for,
    flash, g, abort, jsonify
)

from database import db
from auth import admin_required, validate_csrf_token


# Create blueprint
admin_bp = Blueprint('admin', __name__, url_prefix='/admin')


# =============================================================================
# Admin Dashboard
# =============================================================================

@admin_bp.route('/')
@admin_required
def dashboard():
    """Admin dashboard with platform statistics."""
    # Get platform statistics
    stats = db.get_platform_statistics()
    
    # Get recent users
    recent_users, _ = db.get_all_users(page=1, per_page=5)
    
    # Get recent rides
    recent_rides, _ = db.get_all_rides(page=1, per_page=5)
    
    # Get pending reports
    pending_reports, pending_count = db.get_reports(status='pending', page=1, per_page=5)
    
    return render_template('admin/dashboard.html',
        stats=stats,
        recent_users=recent_users,
        recent_rides=recent_rides,
        pending_reports=pending_reports,
        pending_report_count=pending_count
    )


# =============================================================================
# User Management
# =============================================================================

@admin_bp.route('/users')
@admin_required
def users():
    """List all users with search and filters."""
    # Get query parameters
    page = request.args.get('page', '1')
    search = request.args.get('search', '').strip()
    filter_banned = request.args.get('banned', '')
    filter_verified = request.args.get('verified', '')
    
    # Parse page number
    try:
        page = max(1, int(page))
    except ValueError:
        page = 1
    
    # Parse filters
    banned = None
    if filter_banned == 'true':
        banned = True
    elif filter_banned == 'false':
        banned = False
    
    verified = None
    if filter_verified == 'true':
        verified = True
    elif filter_verified == 'false':
        verified = False
    
    # Get users
    users_list, total = db.get_all_users(
        page=page,
        per_page=20,
        search=search if search else None,
        filter_banned=banned,
        filter_verified=verified
    )
    
    # Add ratings to users
    for user in users_list:
        user['rating'] = db.get_user_average_rating(user['id'])
    
    # Calculate pagination
    total_pages = (total + 19) // 20
    
    return render_template('admin/users.html',
        users=users_list,
        total=total,
        page=page,
        total_pages=total_pages,
        search=search,
        filter_banned=filter_banned,
        filter_verified=filter_verified
    )


@admin_bp.route('/users/<int:user_id>/ban', methods=['POST'])
@admin_required
def ban_user(user_id):
    """Ban a user."""
    # Validate CSRF
    csrf_token = request.form.get('csrf_token')
    if not validate_csrf_token(csrf_token):
        flash('Invalid form submission. Please try again.', 'error')
        return redirect(url_for('admin.users'))
    
    # Get user
    user = db.get_user_by_id(user_id)
    
    if not user:
        abort(404)
    
    # Cannot ban yourself
    if user_id == g.user['id']:
        flash('You cannot ban yourself.', 'error')
        return redirect(url_for('admin.users'))
    
    # Cannot ban other admins
    if user['is_admin']:
        flash('You cannot ban another admin.', 'error')
        return redirect(url_for('admin.users'))
    
    # Ban the user
    db.ban_user(user_id)
    
    flash(f'{user["full_name"]} has been banned.', 'success')
    return redirect(url_for('admin.users'))


@admin_bp.route('/users/<int:user_id>/unban', methods=['POST'])
@admin_required
def unban_user(user_id):
    """Unban a user."""
    # Validate CSRF
    csrf_token = request.form.get('csrf_token')
    if not validate_csrf_token(csrf_token):
        flash('Invalid form submission. Please try again.', 'error')
        return redirect(url_for('admin.users'))
    
    # Get user
    user = db.get_user_by_id(user_id)
    
    if not user:
        abort(404)
    
    # Unban the user
    db.unban_user(user_id)
    
    flash(f'{user["full_name"]} has been unbanned.', 'success')
    return redirect(url_for('admin.users'))


@admin_bp.route('/users/<int:user_id>/verify', methods=['POST'])
@admin_required
def verify_user(user_id):
    """Manually verify a user's email."""
    # Validate CSRF
    csrf_token = request.form.get('csrf_token')
    if not validate_csrf_token(csrf_token):
        flash('Invalid form submission. Please try again.', 'error')
        return redirect(url_for('admin.users'))
    
    # Get user
    user = db.get_user_by_id(user_id)
    
    if not user:
        abort(404)
    
    if user['is_verified']:
        flash(f'{user["full_name"]} is already verified.', 'info')
        return redirect(url_for('admin.users'))
    
    # Verify the user
    db.set_user_verified(user_id)
    
    flash(f'{user["full_name"]} has been verified.', 'success')
    return redirect(url_for('admin.users'))


@admin_bp.route('/users/<int:user_id>/details')
@admin_required
def user_detail(user_id):
    """View detailed user information."""
    user = db.get_user_by_id(user_id)
    
    if not user:
        abort(404)
    
    # Get user stats
    rating = db.get_user_average_rating(user_id)
    reviews = db.get_reviews_for_user(user_id)
    rides = db.get_rides_by_driver(user_id)
    bookings = db.get_bookings_by_passenger(user_id)
    
    # Count by status
    active_rides = len([r for r in rides if r['status'] == 'active'])
    completed_rides = len([r for r in rides if r['status'] == 'completed'])
    
    confirmed_bookings = len([b for b in bookings if b['status'] == 'confirmed'])
    completed_bookings = len([b for b in bookings if b['status'] == 'completed'])
    
    return render_template('admin/user_detail.html',
        user=user,
        rating=rating,
        reviews=reviews,
        review_count=len(reviews),
        active_rides=active_rides,
        completed_rides=completed_rides,
        confirmed_bookings=confirmed_bookings,
        completed_bookings=completed_bookings,
        total_rides=len(rides),
        total_bookings=len(bookings)
    )


# =============================================================================
# Ride Management
# =============================================================================

@admin_bp.route('/rides')
@admin_required
def rides():
    """List all rides with filters."""
    # Get query parameters
    page = request.args.get('page', '1')
    status = request.args.get('status', '')
    
    # Parse page number
    try:
        page = max(1, int(page))
    except ValueError:
        page = 1
    
    # Get rides
    rides_list, total = db.get_all_rides(
        page=page,
        per_page=20,
        status=status if status else None
    )
    
    # Calculate pagination
    total_pages = (total + 19) // 20
    
    return render_template('admin/rides.html',
        rides=rides_list,
        total=total,
        page=page,
        total_pages=total_pages,
        status=status
    )


@admin_bp.route('/rides/<int:ride_id>/cancel', methods=['POST'])
@admin_required
def cancel_ride(ride_id):
    """Admin cancels a ride."""
    # Validate CSRF
    csrf_token = request.form.get('csrf_token')
    if not validate_csrf_token(csrf_token):
        flash('Invalid form submission. Please try again.', 'error')
        return redirect(url_for('admin.rides'))
    
    # Get ride
    ride = db.get_ride_by_id(ride_id)
    
    if not ride:
        abort(404)
    
    if ride['status'] in ('completed', 'cancelled'):
        flash('This ride is already completed or cancelled.', 'error')
        return redirect(url_for('admin.rides'))
    
    # Cancel the ride
    db.cancel_ride(ride_id)
    
    # Notify driver and passengers
    from email_utils import send_booking_cancelled_notification
    
    driver = db.get_user_by_id(ride['driver_id'])
    if driver:
        send_booking_cancelled_notification(
            to_email=driver['email'],
            user_name=driver['full_name'],
            cancelled_by="Platform Administrator",
            origin=ride['origin'],
            destination=ride['destination'],
            departure_date=ride['departure_date'],
            departure_time=ride['departure_time'],
            recipient_id=driver['id']
        )
    
    bookings = db.get_bookings_by_ride(ride_id)
    for booking in bookings:
        if booking['status'] in ('pending', 'confirmed'):
            passenger = db.get_user_by_id(booking['passenger_id'])
            if passenger:
                send_booking_cancelled_notification(
                    to_email=passenger['email'],
                    user_name=passenger['full_name'],
                    cancelled_by="Platform Administrator",
                    origin=ride['origin'],
                    destination=ride['destination'],
                    departure_date=ride['departure_date'],
                    departure_time=ride['departure_time'],
                    recipient_id=passenger['id']
                )
    
    flash('Ride cancelled. Driver and passengers have been notified.', 'success')
    return redirect(url_for('admin.rides'))


# =============================================================================
# Report Management
# =============================================================================

@admin_bp.route('/reports')
@admin_required
def reports():
    """View all user reports."""
    # Get query parameters
    page = request.args.get('page', '1')
    status = request.args.get('status', '')
    
    # Parse page number
    try:
        page = max(1, int(page))
    except ValueError:
        page = 1
    
    # Get reports
    reports_list, total = db.get_reports(
        status=status if status else None,
        page=page,
        per_page=20
    )
    
    # Calculate pagination
    total_pages = (total + 19) // 20
    
    return render_template('admin/reports.html',
        reports=reports_list,
        total=total,
        page=page,
        total_pages=total_pages,
        status=status
    )


@admin_bp.route('/reports/<int:report_id>/resolve', methods=['POST'])
@admin_required
def resolve_report(report_id):
    """Mark a report as resolved."""
    # Validate CSRF
    csrf_token = request.form.get('csrf_token')
    if not validate_csrf_token(csrf_token):
        flash('Invalid form submission. Please try again.', 'error')
        return redirect(url_for('admin.reports'))
    
    # Get admin notes
    admin_notes = request.form.get('admin_notes', '').strip()
    action = request.form.get('action', 'resolve')
    
    # Update report status
    new_status = 'resolved' if action == 'resolve' else 'reviewed'
    db.update_report_status(report_id, new_status, admin_notes if admin_notes else None)
    
    flash('Report has been updated.', 'success')
    return redirect(url_for('admin.reports'))


@admin_bp.route('/reports/<int:report_id>/ban-user', methods=['POST'])
@admin_required
def report_ban_user(report_id):
    """Ban the reported user and resolve the report."""
    # Validate CSRF
    csrf_token = request.form.get('csrf_token')
    if not validate_csrf_token(csrf_token):
        flash('Invalid form submission. Please try again.', 'error')
        return redirect(url_for('admin.reports'))
    
    # Get reports to find reported user
    reports_list, _ = db.get_reports(page=1, per_page=1000)
    report = None
    for r in reports_list:
        if r['id'] == report_id:
            report = r
            break
    
    if not report:
        abort(404)
    
    reported_user = db.get_user_by_id(report['reported_user_id'])
    
    if not reported_user:
        abort(404)
    
    # Cannot ban admins
    if reported_user['is_admin']:
        flash('You cannot ban an admin.', 'error')
        return redirect(url_for('admin.reports'))
    
    # Ban the user
    db.ban_user(report['reported_user_id'])
    
    # Update report
    admin_notes = request.form.get('admin_notes', '').strip()
    notes = f"User banned. {admin_notes}" if admin_notes else "User banned."
    db.update_report_status(report_id, 'resolved', notes)
    
    flash(f'{reported_user["full_name"]} has been banned and the report resolved.', 'success')
    return redirect(url_for('admin.reports'))


# =============================================================================
# API Endpoints
# =============================================================================

@admin_bp.route('/api/stats')
@admin_required
def api_stats():
    """API endpoint to get platform statistics."""
    stats = db.get_platform_statistics()
    return jsonify({
        'success': True,
        'stats': stats
    })


@admin_bp.route('/api/users/search')
@admin_required
def api_search_users():
    """API endpoint to search users."""
    search = request.args.get('q', '').strip()
    
    if not search or len(search) < 2:
        return jsonify({'success': True, 'users': []})
    
    users_list, _ = db.get_all_users(
        page=1,
        per_page=10,
        search=search
    )
    
    # Format for response
    users_data = []
    for user in users_list:
        users_data.append({
            'id': user['id'],
            'email': user['email'],
            'full_name': user['full_name'],
            'is_verified': bool(user['is_verified']),
            'is_banned': bool(user['is_banned']),
            'is_admin': bool(user['is_admin'])
        })
    
    return jsonify({
        'success': True,
        'users': users_data
    })
