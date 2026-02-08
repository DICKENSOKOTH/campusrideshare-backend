"""
Campus Ride-Share Platform - AI Chatbot Module

Professional, secure AI assistant using OpenAI's GPT API.
Security-first design: Never sends personal data (names, phones, emails) to OpenAI.
Only sanitized ride data (IDs, routes, dates, times, prices) is included in prompts.
"""

from datetime import datetime
from typing import Optional, List, Dict, Any

from config import config
from database import db

# Only import OpenAI if configured
if config.is_openai_enabled():
    from openai import OpenAI


class RideShareChatbot:
    """
    Secure AI Assistant for the Campus Ride-Share platform.
    
    Security Features:
    - NEVER sends driver names to OpenAI
    - NEVER sends phone numbers to OpenAI
    - NEVER sends email addresses to OpenAI
    - NEVER sends license plates to OpenAI
    - Only sends: Ride IDs, routes, dates, times, prices, seat counts
    
    Users get full ride details by clicking Ride IDs which link to the
    ride detail page where data is fetched directly from the database.
    """
    
    def __init__(self):
        """Initialize the chatbot with OpenAI client if configured."""
        self.enabled = config.is_openai_enabled()
        if self.enabled:
            self.client = OpenAI(api_key=config.OPENAI_API_KEY)
        else:
            self.client = None
        self.model = config.OPENAI_MODEL
        self.max_tokens = config.OPENAI_MAX_TOKENS
        self.rate_limit = config.CHATBOT_RATE_LIMIT
    
    def _get_current_date(self) -> str:
        """Get current date formatted for system prompt."""
        now = datetime.now()
        return now.strftime("%A, %B %d, %Y")
    
    def _format_time_ampm(self, time_str: str) -> str:
        """
        Convert 24-hour time string to 12-hour AM/PM format.
        
        Args:
            time_str: Time in HH:MM format (e.g., "14:30")
        
        Returns:
            Time in 12-hour format (e.g., "2:30 PM")
        """
        try:
            hours, minutes = time_str.split(':')
            hour = int(hours)
            ampm = 'PM' if hour >= 12 else 'AM'
            display_hour = hour % 12 or 12
            return f"{display_hour}:{minutes} {ampm}"
        except (ValueError, AttributeError):
            return time_str
    
    def _sanitize_ride_for_prompt(self, ride: Dict[str, Any]) -> str:
        """
        Create a SANITIZED ride summary for the system prompt.
        
        SECURITY: This method intentionally EXCLUDES:
        - Driver's full name (uses Driver ID only)
        - Driver's phone number
        - Driver's email address
        - License plate number
        - Any personal identifiable information
        
        INCLUDES ONLY:
        - Ride ID
        - Origin city
        - Destination city
        - Date
        - Time
        - Price per seat
        - Available seats
        - Vehicle type (if available)
        
        Args:
            ride: The ride dictionary from the database.
        
        Returns:
            Sanitized string representation safe for OpenAI.
        """
        vehicle_type = ride.get('vehicle_type', 'vehicle')
        if not vehicle_type:
            vehicle_type = 'vehicle'
        
        # Format time to AM/PM
        time_str = ride['departure_time']
        formatted_time = self._format_time_ampm(time_str)
            
        return f"Ride #{ride['id']}: {ride['origin']} to {ride['destination']}, {ride['departure_date']} at {formatted_time}, KSh {int(ride['price_per_seat'])}/seat, {ride['available_seats']} seats available"
    
    def _build_system_prompt(self) -> str:
        """
        Build the system prompt with SANITIZED real-time data.
        
        SECURITY: No personal information is included in this prompt.
        OpenAI only receives: ride IDs, routes, dates, times, prices, seat counts.
        
        Returns:
            The complete system prompt string.
        """
        current_date = self._get_current_date()
        
        # Get all active rides from database
        active_rides = db.get_all_active_rides()
        
        # Get platform statistics (no personal data)
        stats = db.get_platform_statistics()
        
        # Build sanitized rides list
        if active_rides:
            rides_list = "\n".join([
                self._sanitize_ride_for_prompt(ride) 
                for ride in active_rides
            ])
            rides_section = f"AVAILABLE RIDES:\n{rides_list}"
        else:
            rides_section = "AVAILABLE RIDES:\nNo rides currently available."
        
        # Get unique destinations for context
        destinations = list(set([r['destination'] for r in active_rides])) if active_rides else []
        origins = list(set([r['origin'] for r in active_rides])) if active_rides else []
        
        # Build the strict professional system prompt
        system_prompt = f"""You are the AI Assistant for Campus Ride-Share, a university student carpooling platform. You help students find rides, check availability, and understand how the platform works. You are professional, efficient, and data-driven.

TODAY'S DATE: {current_date}

PLATFORM STATS:
- Active rides: {len(active_rides)}
- Total users: {stats['total_users']}
- Average price: KSh {int(stats['average_price'])}

ROUTES WITH AVAILABLE RIDES:
Origins: {', '.join(origins[:8]) if origins else 'None'}
Destinations: {', '.join(destinations[:8]) if destinations else 'None'}

{rides_section}

STRICT OPERATIONAL RULES:

1. Never use emojis in any response. Not one. Ever.

2. Only recommend rides from the AVAILABLE RIDES list above. Never invent rides.

3. Always cite Ride ID when recommending a ride. Format: "Ride #47" not "ride 47".

4. Never make up ride information. If data is not in the list, do not guess.

5. If no rides match the query:
   - State this clearly: "No rides to [destination] are currently available."
   - Suggest nearby destinations that DO have rides
   - Suggest different dates if applicable
   - Mention they can post their own ride if they are a driver

6. Keep responses concise. Under 100 words unless listing multiple rides.

7. Use professional language. No casual greetings like "Hi there!" or "Hey!"

8. When listing multiple rides, use this exact format:
   Ride #[ID]: [Origin] to [Destination], [Date] at [Time], KSh [Price]/seat, [X] seats available

9. Never share driver names, phone numbers, emails, or personal details. You do not have this information.

10. Direct users to "view Ride #[ID] for full details and booking" - the website shows complete information.

11. For platform questions, provide clear, brief answers.

12. If asked something unrelated to ride-sharing, respond: "I can only assist with finding rides and platform-related questions. What ride are you looking for?"

13. Never suggest Uber, Bolt, matatus, buses, taxis, or any external service.

14. Do not start responses with "I" - vary your sentence structure.

RESPONSE EXAMPLES:

User: "I need a ride to Mombasa this Friday"
Response: "Found 2 rides to Mombasa on Friday, February 7th:

Ride #47: Departs 08:00, KSh 1200/seat, 3 seats available
Ride #58: Departs 15:30, KSh 1100/seat, 2 seats available

View a Ride ID above for full details and booking."

User: "What's the cheapest ride available?"
Response: "The most affordable option is Ride #34: Nairobi to Nakuru on February 6th at 10:00 for KSh 400/seat with 4 seats available. View Ride #34 for full details."

User: "Find me something under 500 shillings"
Response: "Three rides under KSh 500:

Ride #34: Nairobi to Nakuru, Feb 6 at 10:00, KSh 400/seat
Ride #61: Nakuru to Nairobi, Feb 11 at 09:00, KSh 450/seat
Ride #72: Thika to Nairobi, Feb 8 at 07:30, KSh 300/seat

Select a Ride ID to view details and book."

User: "Any rides to Kisii?"
Response: "No rides to Kisii are currently available. However, there are 2 rides to Kisumu (nearby):

Ride #52: Feb 10 at 14:30, KSh 800/seat
Ride #66: Feb 12 at 09:00, KSh 750/seat

Would you like to view these, or post your own ride to Kisii?"

User: "How does booking work?"
Response: "To book a ride: Find a ride using search or through my recommendations. Click the Ride ID to view full details including driver information. Click Request to Book. The driver reviews and approves or declines your request. You receive an email notification with their decision. Once approved, message the driver through the platform to coordinate pickup."

User: "What's the weather like?"
Response: "I can only assist with finding rides and platform-related questions. For ride searches, tell me your destination and preferred travel date."

User: "Hello"
Response: "I can help you find rides, check availability, or answer questions about the platform. What would you like to know?"

REMEMBER: You have NO access to driver names, contact details, or personal information. Only provide what is in the AVAILABLE RIDES data above. For complete ride details, users must view the Ride ID on the website."""

        return system_prompt
    
    def _build_messages(
        self,
        user_message: str,
        conversation_history: List[Dict[str, str]]
    ) -> List[Dict[str, str]]:
        """
        Build the messages array for the OpenAI API call.
        
        Args:
            user_message: The current user message.
            conversation_history: Previous messages in the conversation.
        
        Returns:
            List of message dictionaries for the API.
        """
        messages = [
            {"role": "system", "content": self._build_system_prompt()}
        ]
        
        # Add conversation history (last 10 messages for context)
        for msg in conversation_history[-10:]:
            messages.append({
                "role": msg.get("role", "user"),
                "content": msg.get("content", "")
            })
        
        # Add current user message
        messages.append({
            "role": "user",
            "content": user_message
        })
        
        return messages
    
    def check_rate_limit(self, user_id: int) -> tuple:
        """
        Check if the user has exceeded the rate limit.
        
        Args:
            user_id: The ID of the user making the request.
        
        Returns:
            Tuple of (is_allowed, error_message)
        """
        request_count = db.get_user_chat_count_last_minute(user_id)
        
        if request_count >= self.rate_limit:
            return False, f"Rate limit reached. Please wait before sending another message."
        
        return True, ""
    
    def get_response(
        self,
        user_id: int,
        user_message: str,
        conversation_history: Optional[List[Dict[str, str]]] = None
    ) -> Dict[str, Any]:
        """
        Get a response from the AI Assistant.
        
        Flow:
        1. Check rate limiting
        2. Query database for current rides (sanitized - no personal data)
        3. Build system prompt with sanitized data only
        4. Call OpenAI API
        5. Log interaction
        6. Return response
        
        SECURITY: Personal data (names, phones, emails) is NEVER sent to OpenAI.
        
        Args:
            user_id: The ID of the user making the request.
            user_message: The user's message.
            conversation_history: Optional list of previous messages.
        
        Returns:
            Dictionary with 'success', 'response', and optionally 'error'.
        """
        # Check if OpenAI is enabled
        if not self.enabled:
            return {
                "success": False,
                "error": "AI assistant is not configured.",
                "response": "The AI Assistant is currently unavailable. Please use the search page to find rides, or try again later."
            }
        
        # Check rate limit
        is_allowed, rate_limit_error = self.check_rate_limit(user_id)
        if not is_allowed:
            return {
                "success": False,
                "error": rate_limit_error,
                "response": rate_limit_error
            }
        
        # Validate input
        if not user_message or not user_message.strip():
            return {
                "success": False,
                "error": "Please enter a message.",
                "response": "Please enter a message."
            }
        
        # Limit message length
        if len(user_message) > 1000:
            return {
                "success": False,
                "error": "Message too long.",
                "response": "Message exceeds 1000 character limit. Please shorten your query."
            }
        
        conversation_history = conversation_history or []
        
        try:
            # Build messages with fresh, sanitized database data
            messages = self._build_messages(user_message, conversation_history)
            
            # Call OpenAI API
            response = self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                max_tokens=self.max_tokens,
                temperature=0.3,  # Lower temperature for more consistent, professional responses
                presence_penalty=0.1,
                frequency_penalty=0.1
            )
            
            # Extract response
            bot_response = response.choices[0].message.content.strip()
            tokens_used = response.usage.total_tokens if response.usage else None
            
            # Log the interaction
            db.log_chat_interaction(
                user_id=user_id,
                user_message=user_message,
                bot_response=bot_response,
                tokens_used=tokens_used
            )
            
            return {
                "success": True,
                "response": bot_response,
                "tokens_used": tokens_used
            }
            
        except Exception as e:
            error_message = str(e)
            fallback_response = self._get_fallback_response()
            
            # Log the error interaction
            db.log_chat_interaction(
                user_id=user_id,
                user_message=user_message,
                bot_response=f"[ERROR] {fallback_response}",
                tokens_used=0
            )
            
            return {
                "success": False,
                "error": error_message,
                "response": fallback_response
            }
    
    def _get_fallback_response(self) -> str:
        """
        Get a professional fallback response when the API fails.
        
        Returns:
            A helpful fallback message without emojis.
        """
        try:
            active_rides = db.get_all_active_rides()
            ride_count = len(active_rides)
            
            if ride_count > 0:
                destinations = list(set([r['destination'] for r in active_rides[:5]]))
                dest_list = ", ".join(destinations[:3])
                return f"Experiencing technical difficulties. {ride_count} rides are currently available to destinations including {dest_list}. Please use the Search page to find and book rides."
            else:
                return "Experiencing technical difficulties. Please use the Search page to browse available rides, or try again in a moment."
        except Exception:
            return "Experiencing technical difficulties. Please use the Search page or try again in a moment."
    
    def get_quick_suggestions(self) -> List[str]:
        """
        Get professional quick action buttons based on current data.
        
        Returns:
            List of professional suggested queries (no emojis).
        """
        suggestions = []
        
        try:
            active_rides = db.get_all_active_rides()
            
            if active_rides:
                # Get most common destinations
                destinations = {}
                for ride in active_rides:
                    dest = ride['destination']
                    destinations[dest] = destinations.get(dest, 0) + 1
                
                # Sort by popularity
                sorted_dests = sorted(destinations.items(), key=lambda x: x[1], reverse=True)
                
                # Add top destination suggestion
                if sorted_dests:
                    suggestions.append(f"Search {sorted_dests[0][0]} rides")
                
                suggestions.append("Check weekend availability")
                suggestions.append("View pricing guide")
            else:
                suggestions.append("How to post a ride")
            
            suggestions.append("Platform help")
            
        except Exception:
            suggestions = [
                "Search available rides",
                "How booking works",
                "Platform help"
            ]
        
        return suggestions[:4]  # Limit to 4 suggestions
    
    def get_initial_greeting(self) -> str:
        """
        Get the professional initial greeting for the chat interface.
        
        Returns:
            Professional opening message without emojis.
        """
        try:
            active_rides = db.get_all_active_rides()
            ride_count = len(active_rides)
            
            if ride_count > 0:
                destinations = list(set([r['destination'] for r in active_rides]))[:3]
                dest_text = ", ".join(destinations)
                return f"I can help you find rides, check availability, or answer questions about the platform. Currently {ride_count} rides available to destinations including {dest_text}. What would you like to know?"
            else:
                return "I can help you find rides, check availability, or answer questions about the platform. What would you like to know?"
        except Exception:
            return "I can help you find rides, check availability, or answer questions about the platform. What would you like to know?"


# Global chatbot instance
chatbot = RideShareChatbot()


def get_chat_response(
    user_id: int,
    message: str,
    history: Optional[List[Dict[str, str]]] = None
) -> Dict[str, Any]:
    """
    Convenience function to get a chatbot response.
    
    Args:
        user_id: The ID of the user.
        message: The user's message.
        history: Optional conversation history.
    
    Returns:
        Response dictionary from the chatbot.
    """
    return chatbot.get_response(user_id, message, history)


def get_quick_suggestions() -> List[str]:
    """
    Convenience function to get quick suggestions.
    
    Returns:
        List of suggested queries (no emojis).
    """
    return chatbot.get_quick_suggestions()


def get_initial_greeting() -> str:
    """
    Convenience function to get the initial greeting.
    
    Returns:
        Professional greeting message.
    """
    return chatbot.get_initial_greeting()
