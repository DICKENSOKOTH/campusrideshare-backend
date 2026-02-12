"""
Campus Ride-Share Platform - Email Utilities Module

This module handles all email sending operations using SMTP.
All emails are logged to the database for tracking and debugging.
"""

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from typing import Optional

from config import config
from database import db


class EmailSender:
    """
    Handles all email sending operations for the platform.
    Uses SMTP with TLS for secure email delivery.
    """
    
    def __init__(self):
        """Initialize the email sender with configuration from environment."""
        self.host = config.SMTP_HOST
        self.port = config.SMTP_PORT
        self.user = config.SMTP_USER
        self.password = config.SMTP_PASSWORD
        self.from_name = config.EMAIL_FROM_NAME
        self.from_address = config.EMAIL_FROM_ADDRESS
        self.app_name = config.APP_NAME
        self.app_url = config.APP_URL.rstrip('/')
    
    def _create_message(
        self,
        to_email: str,
        subject: str,
        body: str
    ) -> MIMEMultipart:
        """
        Create an email message.
        
        Args:
            to_email: Recipient email address.
            subject: Email subject line.
            body: Plain text email body.
        
        Returns:
            MIMEMultipart message object.
        """
        message = MIMEMultipart('alternative')
        message['Subject'] = subject
        message['From'] = f"{self.from_name} <{self.from_address}>"
        message['To'] = to_email
        
        # Plain text body
        text_part = MIMEText(body, 'plain', 'utf-8')
        message.attach(text_part)
        
        return message
    
    def _send(
        self,
        to_email: str,
        subject: str,
        body: str,
        email_type: str,
        recipient_id: Optional[int] = None
    ) -> bool:
        """
        Send an email and log the result.
        
        Args:
            to_email: Recipient email address.
            subject: Email subject line.
            body: Plain text email body.
            email_type: Type of email for logging.
            recipient_id: Optional user ID of recipient.
        
        Returns:
            True if sent successfully, False otherwise.
        """
        # Log email before sending
        log_id = db.log_email(
            recipient_email=to_email,
            subject=subject,
            email_type=email_type,
            recipient_id=recipient_id
        )
        
        # Check if email sending is configured
        if not config.is_email_sending_enabled():
            db.update_email_status(log_id, 'failed', 'Email sending is not configured')
            return False
        
        try:
            message = self._create_message(to_email, subject, body)
            
            with smtplib.SMTP(self.host, self.port) as server:
                server.starttls()
                server.login(self.user, self.password)
                server.sendmail(self.from_address, to_email, message.as_string())
            
            db.update_email_status(log_id, 'sent')
            return True
            
        except smtplib.SMTPAuthenticationError as e:
            error_msg = f"SMTP authentication failed: {str(e)}"
            db.update_email_status(log_id, 'failed', error_msg)
            return False
            
        except smtplib.SMTPRecipientsRefused as e:
            error_msg = f"Recipient refused: {str(e)}"
            db.update_email_status(log_id, 'failed', error_msg)
            return False
            
        except smtplib.SMTPException as e:
            error_msg = f"SMTP error: {str(e)}"
            db.update_email_status(log_id, 'failed', error_msg)
            return False
            
        except Exception as e:
            error_msg = f"Unexpected error: {str(e)}"
            db.update_email_status(log_id, 'failed', error_msg)
            return False
    
    def send_verification_email(
        self,
        to_email: str,
        user_name: str,
        token: str,
        recipient_id: Optional[int] = None
    ) -> bool:
        """
        Send an email verification email.
        
        Args:
            to_email: Recipient email address.
            user_name: Name of the user.
            token: Verification token.
            recipient_id: Optional user ID.
        
        Returns:
            True if sent successfully, False otherwise.
        """
        subject = f"Verify your {self.app_name} account"
        
        verification_url = f"{self.app_url}/verify-email/{token}"
        
        body = f"""Hello {user_name},

Welcome to {self.app_name}!

Please verify your email address by clicking the link below:

{verification_url}

This link will expire in 24 hours.

If you did not create an account, please ignore this email.

Best regards,
The {self.app_name} Team

---
This is an automated message. Please do not reply to this email.
"""
        
        return self._send(to_email, subject, body, 'verification', recipient_id)
    
    def send_password_reset_email(
        self,
        to_email: str,
        user_name: str,
        token: str,
        recipient_id: Optional[int] = None
    ) -> bool:
        """
        Send a password reset email.
        
        Args:
            to_email: Recipient email address.
            user_name: Name of the user.
            token: Password reset token.
            recipient_id: Optional user ID.
        
        Returns:
            True if sent successfully, False otherwise.
        """
        subject = f"Reset your {self.app_name} password"
        
        reset_url = f"{self.app_url}/reset-password/{token}"
        
        body = f"""Hello {user_name},

We received a request to reset your password for your {self.app_name} account.

Click the link below to set a new password:

{reset_url}

This link will expire in 1 hour.

If you did not request a password reset, please ignore this email. Your password will remain unchanged.

Best regards,
The {self.app_name} Team

---
This is an automated message. Please do not reply to this email.
"""
        
        return self._send(to_email, subject, body, 'password_reset', recipient_id)
    
    def send_booking_request_notification(
        self,
        to_email: str,
        driver_name: str,
        passenger_name: str,
        origin: str,
        destination: str,
        departure_date: str,
        departure_time: str,
        ride_id: int,
        recipient_id: Optional[int] = None
    ) -> bool:
        """
        Send a notification to the driver about a new booking request.
        
        Args:
            to_email: Driver's email address.
            driver_name: Name of the driver.
            passenger_name: Name of the passenger.
            origin: Ride origin.
            destination: Ride destination.
            departure_date: Date of departure.
            departure_time: Time of departure.
            ride_id: ID of the ride.
            recipient_id: Optional user ID.
        
        Returns:
            True if sent successfully, False otherwise.
        """
        subject = f"New booking request for your ride to {destination}"
        
        ride_url = f"{self.app_url}/rides/{ride_id}"
        
        body = f"""Hello {driver_name},

You have a new booking request for your ride!

Ride Details:
- From: {origin}
- To: {destination}
- Date: {departure_date}
- Time: {departure_time}

Passenger: {passenger_name}

Please log in to review and respond to this request:

{ride_url}

Best regards,
The {self.app_name} Team

---
This is an automated message. Please do not reply to this email.
"""
        
        return self._send(to_email, subject, body, 'booking_request', recipient_id)
    
    def send_booking_confirmed_notification(
        self,
        to_email: str,
        passenger_name: str,
        driver_name: str,
        driver_phone: str,
        origin: str,
        destination: str,
        departure_date: str,
        departure_time: str,
        price: float,
        ride_id: int,
        recipient_id: Optional[int] = None
    ) -> bool:
        """
        Send a confirmation notification to the passenger.
        
        Args:
            to_email: Passenger's email address.
            passenger_name: Name of the passenger.
            driver_name: Name of the driver.
            driver_phone: Driver's phone number.
            origin: Ride origin.
            destination: Ride destination.
            departure_date: Date of departure.
            departure_time: Time of departure.
            price: Price per seat.
            ride_id: ID of the ride.
            recipient_id: Optional user ID.
        
        Returns:
            True if sent successfully, False otherwise.
        """
        subject = f"Your booking to {destination} has been confirmed!"
        
        ride_url = f"{self.app_url}/rides/{ride_id}"
        
        phone_info = f"- Driver Phone: {driver_phone}" if driver_phone else "- Driver Phone: Contact via messages"
        
        body = f"""Hello {passenger_name},

Great news! Your booking request has been confirmed.

Ride Details:
- From: {origin}
- To: {destination}
- Date: {departure_date}
- Time: {departure_time}
- Price: KSh {price:.2f}

Driver Information:
- Driver Name: {driver_name}
{phone_info}

You can view ride details and message the driver here:

{ride_url}

Please be at the pickup location on time. Have a safe trip!

Best regards,
The {self.app_name} Team

---
This is an automated message. Please do not reply to this email.
"""
        
        return self._send(to_email, subject, body, 'booking_confirmed', recipient_id)
    
    def send_booking_rejected_notification(
        self,
        to_email: str,
        passenger_name: str,
        origin: str,
        destination: str,
        departure_date: str,
        recipient_id: Optional[int] = None
    ) -> bool:
        """
        Send a rejection notification to the passenger.
        
        Args:
            to_email: Passenger's email address.
            passenger_name: Name of the passenger.
            origin: Ride origin.
            destination: Ride destination.
            departure_date: Date of departure.
            recipient_id: Optional user ID.
        
        Returns:
            True if sent successfully, False otherwise.
        """
        subject = f"Update on your booking request"
        
        search_url = f"{self.app_url}/rides/search"
        
        body = f"""Hello {passenger_name},

We regret to inform you that your booking request for the following ride was not accepted:

- From: {origin}
- To: {destination}
- Date: {departure_date}

This could be due to various reasons such as the driver having prior commitments or the ride becoming full.

Please browse other available rides:

{search_url}

We hope you find a suitable ride soon!

Best regards,
The {self.app_name} Team

---
This is an automated message. Please do not reply to this email.
"""
        
        return self._send(to_email, subject, body, 'booking_rejected', recipient_id)
    
    def send_booking_cancelled_notification(
        self,
        to_email: str,
        user_name: str,
        cancelled_by: str,
        origin: str,
        destination: str,
        departure_date: str,
        departure_time: str,
        recipient_id: Optional[int] = None
    ) -> bool:
        """
        Send a cancellation notification.
        
        Args:
            to_email: Recipient email address.
            user_name: Name of the recipient.
            cancelled_by: Who cancelled ("driver", "passenger", or the name).
            origin: Ride origin.
            destination: Ride destination.
            departure_date: Date of departure.
            departure_time: Time of departure.
            recipient_id: Optional user ID.
        
        Returns:
            True if sent successfully, False otherwise.
        """
        subject = f"Booking cancellation notice"
        
        search_url = f"{self.app_url}/rides/search"
        
        body = f"""Hello {user_name},

A booking has been cancelled for the following ride:

- From: {origin}
- To: {destination}
- Date: {departure_date}
- Time: {departure_time}

Cancelled by: {cancelled_by}

If you need to find an alternative ride, please browse available rides:

{search_url}

We apologize for any inconvenience.

Best regards,
The {self.app_name} Team

---
This is an automated message. Please do not reply to this email.
"""
        
        return self._send(to_email, subject, body, 'booking_cancelled', recipient_id)
    
    def send_ride_reminder(
        self,
        to_email: str,
        user_name: str,
        is_driver: bool,
        origin: str,
        destination: str,
        departure_date: str,
        departure_time: str,
        ride_id: int,
        recipient_id: Optional[int] = None
    ) -> bool:
        """
        Send a ride reminder 24 hours before departure.
        
        Args:
            to_email: Recipient email address.
            user_name: Name of the recipient.
            is_driver: Whether the recipient is the driver.
            origin: Ride origin.
            destination: Ride destination.
            departure_date: Date of departure.
            departure_time: Time of departure.
            ride_id: ID of the ride.
            recipient_id: Optional user ID.
        
        Returns:
            True if sent successfully, False otherwise.
        """
        role_text = "your ride" if is_driver else "the ride you booked"
        subject = f"Reminder: {role_text} to {destination} is tomorrow"
        
        ride_url = f"{self.app_url}/rides/{ride_id}"
        
        driver_reminder = """
As the driver, please remember to:
- Ensure your vehicle is ready
- Be at the pickup location on time
- Contact passengers if there are any changes
""" if is_driver else """
As a passenger, please remember to:
- Be at the pickup location on time
- Have your payment ready
- Contact the driver if you need to cancel
"""
        
        body = f"""Hello {user_name},

This is a friendly reminder that {role_text} is scheduled for tomorrow.

Ride Details:
- From: {origin}
- To: {destination}
- Date: {departure_date}
- Time: {departure_time}
{driver_reminder}
View ride details:

{ride_url}

Have a safe trip!

Best regards,
The {self.app_name} Team

---
This is an automated message. Please do not reply to this email.
"""
        
        return self._send(to_email, subject, body, 'ride_reminder', recipient_id)
    
    def send_rating_request(
        self,
        to_email: str,
        user_name: str,
        other_user_name: str,
        origin: str,
        destination: str,
        departure_date: str,
        ride_id: int,
        other_user_id: int,
        recipient_id: Optional[int] = None
    ) -> bool:
        """
        Send a request to rate another user after a completed trip.
        
        Args:
            to_email: Recipient email address.
            user_name: Name of the recipient.
            other_user_name: Name of the user to rate.
            origin: Ride origin.
            destination: Ride destination.
            departure_date: Date of the trip.
            ride_id: ID of the ride.
            other_user_id: ID of the user to rate.
            recipient_id: Optional user ID.
        
        Returns:
            True if sent successfully, False otherwise.
        """
        subject = f"How was your ride? Rate {other_user_name}"
        
        rate_url = f"{self.app_url}/rate/{other_user_id}/ride/{ride_id}"
        
        body = f"""Hello {user_name},

Thank you for using {self.app_name}!

We hope you had a great trip:
- From: {origin}
- To: {destination}
- Date: {departure_date}

Your feedback helps build trust in our community. Please take a moment to rate {other_user_name}:

{rate_url}

Your rating helps other users make informed decisions.

Best regards,
The {self.app_name} Team

---
This is an automated message. Please do not reply to this email.
"""
        
        return self._send(to_email, subject, body, 'rating_request', recipient_id)


# Global email sender instance
email_sender = EmailSender()


# Convenience functions for direct import

def send_verification_email(
    to_email: str,
    user_name: str,
    token: str,
    recipient_id: Optional[int] = None
) -> bool:
    """Send verification email."""
    return email_sender.send_verification_email(to_email, user_name, token, recipient_id)


def send_password_reset_email(
    to_email: str,
    user_name: str,
    token: str,
    recipient_id: Optional[int] = None
) -> bool:
    """Send password reset email."""
    return email_sender.send_password_reset_email(to_email, user_name, token, recipient_id)


def send_booking_request_notification(
    to_email: str,
    driver_name: str,
    passenger_name: str,
    origin: str,
    destination: str,
    departure_date: str,
    departure_time: str,
    ride_id: int,
    recipient_id: Optional[int] = None
) -> bool:
    """Send booking request notification to driver."""
    return email_sender.send_booking_request_notification(
        to_email, driver_name, passenger_name, origin, destination,
        departure_date, departure_time, ride_id, recipient_id
    )


def send_booking_confirmed_notification(
    to_email: str,
    passenger_name: str,
    driver_name: str,
    driver_phone: str,
    origin: str,
    destination: str,
    departure_date: str,
    departure_time: str,
    price: float,
    ride_id: int,
    recipient_id: Optional[int] = None
) -> bool:
    """Send booking confirmed notification to passenger."""
    return email_sender.send_booking_confirmed_notification(
        to_email, passenger_name, driver_name, driver_phone, origin,
        destination, departure_date, departure_time, price, ride_id, recipient_id
    )


def send_booking_rejected_notification(
    to_email: str,
    passenger_name: str,
    origin: str,
    destination: str,
    departure_date: str,
    recipient_id: Optional[int] = None
) -> bool:
    """Send booking rejected notification to passenger."""
    return email_sender.send_booking_rejected_notification(
        to_email, passenger_name, origin, destination, departure_date, recipient_id
    )


def send_booking_cancelled_notification(
    to_email: str,
    user_name: str,
    cancelled_by: str,
    origin: str,
    destination: str,
    departure_date: str,
    departure_time: str,
    recipient_id: Optional[int] = None
) -> bool:
    """Send booking cancelled notification."""
    return email_sender.send_booking_cancelled_notification(
        to_email, user_name, cancelled_by, origin, destination,
        departure_date, departure_time, recipient_id
    )


def send_ride_reminder(
    to_email: str,
    user_name: str,
    is_driver: bool,
    origin: str,
    destination: str,
    departure_date: str,
    departure_time: str,
    ride_id: int,
    recipient_id: Optional[int] = None
) -> bool:
    """Send ride reminder."""
    return email_sender.send_ride_reminder(
        to_email, user_name, is_driver, origin, destination,
        departure_date, departure_time, ride_id, recipient_id
    )


def send_rating_request(
    to_email: str,
    user_name: str,
    other_user_name: str,
    origin: str,
    destination: str,
    departure_date: str,
    ride_id: int,
    other_user_id: int,
    recipient_id: Optional[int] = None
) -> bool:
    """Send rating request email."""
    return email_sender.send_rating_request(
        to_email, user_name, other_user_name, origin, destination,
        departure_date, ride_id, other_user_id, recipient_id
    )


def send_admin_warning(
    to_email: str,
    user_name: str,
    message: str,
    recipient_id: Optional[int] = None
) -> bool:
    """Send an admin warning email to a user."""
    subject = f"Important message from {email_sender.app_name}"
    body = (
        f"Hello {user_name},\n\n{message}\n\nIf you have questions, please contact support.\n\n"
        f"Best regards,\nThe {email_sender.app_name} Team\n\n---\nThis is an automated message. Please do not reply to this email."
    )
    return email_sender._send(to_email, subject, body, 'admin_warning', recipient_id)
