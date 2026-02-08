"""
Campus Ride-Share Platform - Messaging Routes

This module contains all routes related to messaging between users
and the AI chatbot functionality.
"""

from flask import (
    Blueprint, render_template, request, redirect, url_for,
    flash, g, abort, jsonify
)

from database import db
from auth import login_required, validate_csrf_token
from chatbot import get_chat_response, get_quick_suggestions, get_initial_greeting


# Create blueprint
messaging_bp = Blueprint('messaging', __name__, url_prefix='/messages')


# =============================================================================
# Messaging Routes
# =============================================================================

@messaging_bp.route('/')
@login_required
def messages():
    """View all conversations."""
    conversations = db.get_conversations_for_user(g.user['id'])
    
    return render_template('messaging/messages.html',
        conversations=conversations
    )


@messaging_bp.route('/conversation/<int:user_id>', methods=['GET', 'POST'])
@messaging_bp.route('/conversation/<int:user_id>/ride/<int:ride_id>', methods=['GET', 'POST'])
@login_required
def conversation(user_id, ride_id=None):
    """View or send messages in a specific conversation."""
    # Get the other user
    other_user = db.get_user_by_id(user_id)
    if not other_user:
        abort(404)
    
    # Get the ride if ride_id is provided
    ride = None
    if ride_id:
        ride = db.get_ride_by_id(ride_id)
        if not ride:
            abort(404)
        
        # Check if user can message this person about this ride
        if not db.can_message_user(g.user['id'], user_id, ride_id):
            flash('You cannot message this user about this ride.', 'error')
            return redirect(url_for('messaging.messages'))
    else:
        # If no ride_id, try to find the most recent conversation/ride between users
        # This allows messaging from profile pages, etc.
        conversations = db.get_conversations_for_user(g.user['id'])
        for conv in conversations:
            if conv['user_id'] == user_id:
                ride_id = conv.get('ride_id')
                if ride_id:
                    ride = db.get_ride_by_id(ride_id)
                break
        
        # If still no ride found, check if user can message at all
        # (e.g., they've had a booking together or a ride together)
        if not ride_id:
            flash('Please start a conversation from a specific ride.', 'error')
            return redirect(url_for('messaging.messages'))
    
    # Check if blocked
    if db.is_user_blocked(g.user['id'], user_id):
        flash('You have blocked this user.', 'error')
        return redirect(url_for('messaging.messages'))
    
    if db.is_blocked_by(g.user['id'], user_id):
        flash('You cannot message this user.', 'error')
        return redirect(url_for('messaging.messages'))
    
    # Handle POST (send message)
    if request.method == 'POST':
        # Validate CSRF
        csrf_token = request.form.get('csrf_token')
        if not validate_csrf_token(csrf_token):
            flash('Invalid form submission. Please try again.', 'error')
            return redirect(url_for('messaging.conversation', user_id=user_id, ride_id=ride_id))
        
        content = request.form.get('content', '').strip()
        
        if not content:
            flash('Please enter a message.', 'error')
            return redirect(url_for('messaging.conversation', user_id=user_id, ride_id=ride_id))
        
        if len(content) > 1000:
            flash('Message is too long. Maximum 1000 characters.', 'error')
            return redirect(url_for('messaging.conversation', user_id=user_id, ride_id=ride_id))
        
        # Create message
        db.create_message(
            sender_id=g.user['id'],
            receiver_id=user_id,
            ride_id=ride_id,
            content=content
        )
        
        # Redirect to clear form
        return redirect(url_for('messaging.conversation', user_id=user_id, ride_id=ride_id))
    
    # Get messages
    messages_list = db.get_messages_in_conversation(g.user['id'], user_id, ride_id)
    
    # Mark messages as read
    db.mark_messages_read(g.user['id'], user_id, ride_id)
    
    # Determine relationship to ride
    is_driver = ride['driver_id'] == g.user['id']
    
    return render_template('messaging/conversation.html',
        other_user=other_user,
        ride=ride,
        messages=messages_list,
        is_driver=is_driver
    )


@messaging_bp.route('/send', methods=['POST'])
@login_required
def send_message():
    """API endpoint to send a message (for AJAX)."""
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({'success': False, 'error': 'Invalid request'}), 400
        
        receiver_id = data.get('receiver_id')
        ride_id = data.get('ride_id')
        content = data.get('content', '').strip()
        
        # Validate input
        if not receiver_id or not ride_id:
            return jsonify({'success': False, 'error': 'Missing required fields'}), 400
        
        if not content:
            return jsonify({'success': False, 'error': 'Message cannot be empty'}), 400
        
        if len(content) > 1000:
            return jsonify({'success': False, 'error': 'Message too long'}), 400
        
        receiver_id = int(receiver_id)
        ride_id = int(ride_id)
        
        # Get receiver and ride
        receiver = db.get_user_by_id(receiver_id)
        ride = db.get_ride_by_id(ride_id)
        
        if not receiver or not ride:
            return jsonify({'success': False, 'error': 'Invalid recipient or ride'}), 404
        
        # Check if user can message
        if not db.can_message_user(g.user['id'], receiver_id, ride_id):
            return jsonify({'success': False, 'error': 'Cannot message this user'}), 403
        
        # Check blocks
        if db.is_user_blocked(g.user['id'], receiver_id) or db.is_blocked_by(g.user['id'], receiver_id):
            return jsonify({'success': False, 'error': 'Cannot message this user'}), 403
        
        # Create message
        message_id = db.create_message(
            sender_id=g.user['id'],
            receiver_id=receiver_id,
            ride_id=ride_id,
            content=content
        )
        
        return jsonify({
            'success': True,
            'message_id': message_id,
            'message': {
                'id': message_id,
                'content': content,
                'sender_id': g.user['id'],
                'sender_name': g.user['full_name'],
                'is_mine': True
            }
        })
        
    except (ValueError, TypeError) as e:
        return jsonify({'success': False, 'error': 'Invalid data'}), 400
    except Exception as e:
        return jsonify({'success': False, 'error': 'Server error'}), 500


@messaging_bp.route('/unread-count')
@login_required
def unread_count():
    """API endpoint to get unread message count."""
    count = db.get_unread_count(g.user['id'])
    return jsonify({'count': count})


# =============================================================================
# Chatbot Routes
# =============================================================================

@messaging_bp.route('/chat')
@login_required
def chat():
    """AI Chatbot page."""
    # Get quick suggestions based on current data
    suggestions = get_quick_suggestions()
    
    # Get recent chat history
    history = db.get_chat_history_for_user(g.user['id'], limit=10)
    
    return render_template('chat/chat.html',
        suggestions=suggestions,
        history=history
    )


@messaging_bp.route('/api/chat', methods=['POST'])
@login_required
def api_chat():
    """API endpoint for chatbot conversations."""
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({
                'success': False,
                'error': 'Invalid request',
                'response': 'Please send a valid message.'
            }), 400
        
        message = data.get('message', '').strip()
        history = data.get('history', [])
        
        if not message:
            return jsonify({
                'success': False,
                'error': 'Message is required',
                'response': 'Please enter a message.'
            }), 400
        
        # Get response from chatbot
        result = get_chat_response(
            user_id=g.user['id'],
            message=message,
            history=history
        )
        
        if result['success']:
            return jsonify({
                'success': True,
                'response': result['response'],
                'tokens_used': result.get('tokens_used')
            })
        else:
            return jsonify({
                'success': False,
                'error': result.get('error', 'Unknown error'),
                'response': result['response']
            })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e),
            'response': 'I am temporarily unavailable. Please use the Search page to find rides, or try again in a moment.'
        }), 500


@messaging_bp.route('/api/chat/suggestions')
@login_required
def api_chat_suggestions():
    """API endpoint to get chatbot suggestions."""
    suggestions = get_quick_suggestions()
    return jsonify({
        'success': True,
        'suggestions': suggestions
    })


@messaging_bp.route('/api/chat/history')
@login_required
def api_chat_history():
    """API endpoint to get chat history."""
    limit = request.args.get('limit', '10')
    
    try:
        limit = min(50, max(1, int(limit)))
    except ValueError:
        limit = 10
    
    history = db.get_chat_history_for_user(g.user['id'], limit=limit)
    
    # Format history for response
    formatted_history = []
    for entry in history:
        formatted_history.append({
            'user_message': entry['user_message'],
            'bot_response': entry['bot_response'],
            'created_at': entry['created_at']
        })
    
    return jsonify({
        'success': True,
        'history': formatted_history
    })


@messaging_bp.route('/api/chat/greeting')
@login_required
def api_chat_greeting():
    """
    API endpoint to get the initial greeting for the chat.
    Returns a professional, data-driven greeting.
    """
    try:
        greeting = get_initial_greeting()
        return jsonify({
            'success': True,
            'greeting': greeting
        })
    except Exception as e:
        # Fallback greeting if there's an error
        return jsonify({
            'success': True,
            'greeting': 'I can help you find rides, check availability, or answer questions about the platform. What would you like to know?'
        })
