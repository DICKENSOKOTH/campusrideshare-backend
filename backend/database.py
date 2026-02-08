

"""
Campus Ride-Share Platform - Database Module

This module handles all database operations using SQLite3 or PostgreSQL.
The database is self-initializing - it creates all tables, indexes,
and constraints on first run if they don't exist.
"""

import sqlite3
import os
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Tuple
from contextlib import contextmanager

from config import config

# PostgreSQL support
try:
    import psycopg2
    import psycopg2.extras
    HAS_POSTGRES = True
except ImportError:
    HAS_POSTGRES = False


class Database:
    """
    Database handler for the Campus Ride-Share platform.
    
    All methods use parameterized queries to prevent SQL injection.
    The database is automatically initialized on first use.
    Supports both SQLite (local) and PostgreSQL (production).
    """
    
    def __init__(self, db_path: Optional[str] = None):
        """
        Initialize the database connection.
        
        Args:
            db_path: Path to the SQLite database file. 
                     Defaults to config.DATABASE_PATH.
                     If DATABASE_URL environment variable is set, uses PostgreSQL instead.
        """
        # Check for PostgreSQL connection string
        database_url = os.environ.get('DATABASE_URL')
        
        if database_url and HAS_POSTGRES:
            self.use_postgres = True
            self.db_url = database_url
            # Render/Heroku use postgres:// but psycopg2 needs postgresql://
            if self.db_url.startswith('postgres://'):
                self.db_url = self.db_url.replace('postgres://', 'postgresql://', 1)
            self.db_path = None
        else:
            self.use_postgres = False
            self.db_path = db_path or config.DATABASE_PATH
            self.db_url = None
        
        self._init_database()
    
    @contextmanager
    def get_connection(self):
        """
        Context manager for database connections.
        Ensures connections are properly closed after use.
        Handles both PostgreSQL and SQLite.
        """
        if self.use_postgres:
            conn = psycopg2.connect(self.db_url)
            conn.set_session(autocommit=False)
            try:
                yield conn
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.close()
        else:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA foreign_keys = ON")
            try:
                yield conn
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.close()
    
    def _get_cursor(self, conn):
        """Get a cursor with proper row factory for both databases."""
        if self.use_postgres:
            return conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        else:
            return conn.cursor()
    
    def _placeholder(self):
        """Get the correct placeholder for parameterized queries."""
        return '%s' if self.use_postgres else '?'
    
    def _init_database(self):
        """
        Initialize the database schema if it doesn't exist.
        Creates all tables, constraints, and indexes.
        """
        with self.get_connection() as conn:
            cursor = self._get_cursor(conn)
            
            # Users table
            if self.use_postgres:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS users (
                        id SERIAL PRIMARY KEY,
                        email TEXT UNIQUE NOT NULL,
                        password_hash TEXT NOT NULL,
                        full_name TEXT NOT NULL,
                        phone TEXT,
                        profile_photo TEXT,
                        is_verified INTEGER DEFAULT 0,
                        verification_token TEXT,
                        verification_expires_at TIMESTAMP,
                        is_driver INTEGER DEFAULT 0,
                        drivers_license TEXT,
                        vehicle_make TEXT,
                        vehicle_model TEXT,
                        license_plate TEXT,
                        emergency_contact_name TEXT,
                        emergency_contact_phone TEXT,
                        bio TEXT,
                        is_active INTEGER DEFAULT 1,
                        is_admin INTEGER DEFAULT 0,
                        is_banned INTEGER DEFAULT 0,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        last_login TIMESTAMP,
                        password_reset_token TEXT,
                        password_reset_expires_at TIMESTAMP,
                        login_attempts INTEGER DEFAULT 0,
                        lockout_until TIMESTAMP
                    )
                """)
            else:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS users (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        email TEXT UNIQUE NOT NULL,
                        password_hash TEXT NOT NULL,
                        full_name TEXT NOT NULL,
                        phone TEXT,
                        profile_photo TEXT,
                        is_verified INTEGER DEFAULT 0,
                        verification_token TEXT,
                        verification_expires_at DATETIME,
                        is_driver INTEGER DEFAULT 0,
                        drivers_license TEXT,
                        vehicle_make TEXT,
                        vehicle_model TEXT,
                        license_plate TEXT,
                        emergency_contact_name TEXT,
                        emergency_contact_phone TEXT,
                        bio TEXT,
                        is_active INTEGER DEFAULT 1,
                        is_admin INTEGER DEFAULT 0,
                        is_banned INTEGER DEFAULT 0,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        last_login DATETIME,
                        password_reset_token TEXT,
                        password_reset_expires_at DATETIME,
                        login_attempts INTEGER DEFAULT 0,
                        lockout_until DATETIME
                    )
                """)
            
            # Rides table
            if self.use_postgres:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS rides (
                        id SERIAL PRIMARY KEY,
                        driver_id INTEGER NOT NULL,
                        origin TEXT NOT NULL,
                        destination TEXT NOT NULL,
                        origin_lat REAL,
                        origin_lng REAL,
                        destination_lat REAL,
                        destination_lng REAL,
                        departure_date DATE NOT NULL,
                        departure_time TIME NOT NULL,
                        total_seats INTEGER NOT NULL CHECK (total_seats >= 1 AND total_seats <= 7),
                        seats_taken INTEGER DEFAULT 0,
                        price_per_seat REAL NOT NULL,
                        distance_km REAL,
                        estimated_duration_minutes INTEGER,
                        notes TEXT,
                        luggage_allowed INTEGER DEFAULT 0,
                        pets_allowed INTEGER DEFAULT 0,
                        smoking_allowed INTEGER DEFAULT 0,
                        music_allowed INTEGER DEFAULT 1,
                        ac_available INTEGER DEFAULT 1,
                        vehicle_type TEXT,
                        vehicle_color TEXT,
                        pickup_flexibility TEXT DEFAULT 'exact',
                        return_trip INTEGER DEFAULT 0,
                        status TEXT DEFAULT 'active' CHECK (status IN ('active', 'full', 'completed', 'cancelled')),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP,
                        FOREIGN KEY (driver_id) REFERENCES users(id)
                    )
                """)
            else:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS rides (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        driver_id INTEGER NOT NULL,
                        origin TEXT NOT NULL,
                        destination TEXT NOT NULL,
                        origin_lat REAL,
                        origin_lng REAL,
                        destination_lat REAL,
                        destination_lng REAL,
                        departure_date DATE NOT NULL,
                        departure_time TIME NOT NULL,
                        total_seats INTEGER NOT NULL CHECK (total_seats >= 1 AND total_seats <= 7),
                        seats_taken INTEGER DEFAULT 0,
                        price_per_seat REAL NOT NULL,
                        distance_km REAL,
                        estimated_duration_minutes INTEGER,
                        notes TEXT,
                        luggage_allowed INTEGER DEFAULT 0,
                        pets_allowed INTEGER DEFAULT 0,
                        smoking_allowed INTEGER DEFAULT 0,
                        music_allowed INTEGER DEFAULT 1,
                        ac_available INTEGER DEFAULT 1,
                        vehicle_type TEXT,
                        vehicle_color TEXT,
                        pickup_flexibility TEXT DEFAULT 'exact',
                        return_trip INTEGER DEFAULT 0,
                        status TEXT DEFAULT 'active' CHECK (status IN ('active', 'full', 'completed', 'cancelled')),
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        updated_at DATETIME,
                        FOREIGN KEY (driver_id) REFERENCES users(id)
                    )
                """)
            
            # Bookings table
            if self.use_postgres:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS bookings (
                        id SERIAL PRIMARY KEY,
                        ride_id INTEGER NOT NULL,
                        passenger_id INTEGER NOT NULL,
                        status TEXT DEFAULT 'pending' CHECK (status IN ('pending', 'confirmed', 'cancelled', 'completed', 'rejected')),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP,
                        FOREIGN KEY (ride_id) REFERENCES rides(id),
                        FOREIGN KEY (passenger_id) REFERENCES users(id),
                        UNIQUE (ride_id, passenger_id)
                    )
                """)
            else:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS bookings (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        ride_id INTEGER NOT NULL,
                        passenger_id INTEGER NOT NULL,
                        status TEXT DEFAULT 'pending' CHECK (status IN ('pending', 'confirmed', 'cancelled', 'completed', 'rejected')),
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        updated_at DATETIME,
                        FOREIGN KEY (ride_id) REFERENCES rides(id),
                        FOREIGN KEY (passenger_id) REFERENCES users(id),
                        UNIQUE (ride_id, passenger_id)
                    )
                """)
            
            # Reviews table
            if self.use_postgres:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS reviews (
                        id SERIAL PRIMARY KEY,
                        reviewer_id INTEGER NOT NULL,
                        reviewed_user_id INTEGER NOT NULL,
                        ride_id INTEGER NOT NULL,
                        rating INTEGER NOT NULL CHECK (rating >= 1 AND rating <= 5),
                        comment TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (reviewer_id) REFERENCES users(id),
                        FOREIGN KEY (reviewed_user_id) REFERENCES users(id),
                        FOREIGN KEY (ride_id) REFERENCES rides(id),
                        UNIQUE (reviewer_id, reviewed_user_id, ride_id)
                    )
                """)
            else:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS reviews (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        reviewer_id INTEGER NOT NULL,
                        reviewed_user_id INTEGER NOT NULL,
                        ride_id INTEGER NOT NULL,
                        rating INTEGER NOT NULL CHECK (rating >= 1 AND rating <= 5),
                        comment TEXT,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (reviewer_id) REFERENCES users(id),
                        FOREIGN KEY (reviewed_user_id) REFERENCES users(id),
                        FOREIGN KEY (ride_id) REFERENCES rides(id),
                        UNIQUE (reviewer_id, reviewed_user_id, ride_id)
                    )
                """)
            
            # Messages table
            if self.use_postgres:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS messages (
                        id SERIAL PRIMARY KEY,
                        sender_id INTEGER NOT NULL,
                        receiver_id INTEGER NOT NULL,
                        ride_id INTEGER,
                        content TEXT NOT NULL,
                        is_read INTEGER DEFAULT 0,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (sender_id) REFERENCES users(id),
                        FOREIGN KEY (receiver_id) REFERENCES users(id),
                        FOREIGN KEY (ride_id) REFERENCES rides(id)
                    )
                """)
            else:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS messages (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        sender_id INTEGER NOT NULL,
                        receiver_id INTEGER NOT NULL,
                        ride_id INTEGER,
                        content TEXT NOT NULL,
                        is_read INTEGER DEFAULT 0,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (sender_id) REFERENCES users(id),
                        FOREIGN KEY (receiver_id) REFERENCES users(id),
                        FOREIGN KEY (ride_id) REFERENCES rides(id)
                    )
                """)
            
            # User blocks table
            if self.use_postgres:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS user_blocks (
                        id SERIAL PRIMARY KEY,
                        blocker_id INTEGER NOT NULL,
                        blocked_id INTEGER NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (blocker_id) REFERENCES users(id),
                        FOREIGN KEY (blocked_id) REFERENCES users(id),
                        UNIQUE (blocker_id, blocked_id)
                    )
                """)
            else:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS user_blocks (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        blocker_id INTEGER NOT NULL,
                        blocked_id INTEGER NOT NULL,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (blocker_id) REFERENCES users(id),
                        FOREIGN KEY (blocked_id) REFERENCES users(id),
                        UNIQUE (blocker_id, blocked_id)
                    )
                """)
            
            # User reports table
            if self.use_postgres:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS user_reports (
                        id SERIAL PRIMARY KEY,
                        reporter_id INTEGER NOT NULL,
                        reported_user_id INTEGER NOT NULL,
                        ride_id INTEGER,
                        reason TEXT NOT NULL,
                        status TEXT DEFAULT 'pending' CHECK (status IN ('pending', 'reviewed', 'resolved')),
                        admin_notes TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP,
                        FOREIGN KEY (reporter_id) REFERENCES users(id),
                        FOREIGN KEY (reported_user_id) REFERENCES users(id),
                        FOREIGN KEY (ride_id) REFERENCES rides(id)
                    )
                """)
            else:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS user_reports (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        reporter_id INTEGER NOT NULL,
                        reported_user_id INTEGER NOT NULL,
                        ride_id INTEGER,
                        reason TEXT NOT NULL,
                        status TEXT DEFAULT 'pending' CHECK (status IN ('pending', 'reviewed', 'resolved')),
                        admin_notes TEXT,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        updated_at DATETIME,
                        FOREIGN KEY (reporter_id) REFERENCES users(id),
                        FOREIGN KEY (reported_user_id) REFERENCES users(id),
                        FOREIGN KEY (ride_id) REFERENCES rides(id)
                    )
                """)
            
            # Chat logs table
            if self.use_postgres:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS chat_logs (
                        id SERIAL PRIMARY KEY,
                        user_id INTEGER NOT NULL,
                        user_message TEXT NOT NULL,
                        bot_response TEXT NOT NULL,
                        tokens_used INTEGER,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (user_id) REFERENCES users(id)
                    )
                """)
            else:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS chat_logs (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        user_id INTEGER NOT NULL,
                        user_message TEXT NOT NULL,
                        bot_response TEXT NOT NULL,
                        tokens_used INTEGER,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (user_id) REFERENCES users(id)
                    )
                """)
            
            # Email logs table
            if self.use_postgres:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS email_logs (
                        id SERIAL PRIMARY KEY,
                        recipient_id INTEGER,
                        recipient_email TEXT NOT NULL,
                        subject TEXT NOT NULL,
                        email_type TEXT NOT NULL CHECK (email_type IN (
                            'verification', 'password_reset', 'booking_request',
                            'booking_confirmed', 'booking_rejected', 'booking_cancelled',
                            'ride_reminder', 'rating_request'
                        )),
                        sent_at TIMESTAMP,
                        status TEXT DEFAULT 'pending' CHECK (status IN ('pending', 'sent', 'failed')),
                        error_message TEXT,
                        FOREIGN KEY (recipient_id) REFERENCES users(id)
                    )
                """)
            else:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS email_logs (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        recipient_id INTEGER,
                        recipient_email TEXT NOT NULL,
                        subject TEXT NOT NULL,
                        email_type TEXT NOT NULL CHECK (email_type IN (
                            'verification', 'password_reset', 'booking_request',
                            'booking_confirmed', 'booking_rejected', 'booking_cancelled',
                            'ride_reminder', 'rating_request'
                        )),
                        sent_at DATETIME,
                        status TEXT DEFAULT 'pending' CHECK (status IN ('pending', 'sent', 'failed')),
                        error_message TEXT,
                        FOREIGN KEY (recipient_id) REFERENCES users(id)
                    )
                """)
            
            # API Tokens table
            if self.use_postgres:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS api_tokens (
                        id SERIAL PRIMARY KEY,
                        token TEXT UNIQUE NOT NULL,
                        user_id INTEGER NOT NULL,
                        expires_at TIMESTAMP NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
                    )
                """)
            else:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS api_tokens (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        token TEXT UNIQUE NOT NULL,
                        user_id INTEGER NOT NULL,
                        expires_at DATETIME NOT NULL,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
                    )
                """)
            
            # Create indexes for performance
            indexes = [
                "CREATE INDEX IF NOT EXISTS idx_rides_driver_id ON rides(driver_id)",
                "CREATE INDEX IF NOT EXISTS idx_rides_destination ON rides(destination)",
                "CREATE INDEX IF NOT EXISTS idx_rides_departure_date ON rides(departure_date)",
                "CREATE INDEX IF NOT EXISTS idx_rides_status ON rides(status)",
                "CREATE INDEX IF NOT EXISTS idx_bookings_ride_id ON bookings(ride_id)",
                "CREATE INDEX IF NOT EXISTS idx_bookings_passenger_id ON bookings(passenger_id)",
                "CREATE INDEX IF NOT EXISTS idx_bookings_status ON bookings(status)",
                "CREATE INDEX IF NOT EXISTS idx_messages_receiver_id ON messages(receiver_id)",
                "CREATE INDEX IF NOT EXISTS idx_messages_is_read ON messages(is_read)",
                "CREATE INDEX IF NOT EXISTS idx_messages_ride_id ON messages(ride_id)",
                "CREATE INDEX IF NOT EXISTS idx_reviews_reviewed_user_id ON reviews(reviewed_user_id)",
                "CREATE INDEX IF NOT EXISTS idx_chat_logs_user_id ON chat_logs(user_id)",
                "CREATE INDEX IF NOT EXISTS idx_api_tokens_token ON api_tokens(token)",
                "CREATE INDEX IF NOT EXISTS idx_api_tokens_user_id ON api_tokens(user_id)",
            ]
            
            for index_sql in indexes:
                try:
                    cursor.execute(index_sql)
                except Exception:
                    pass
    
    # =========================================================================
    # User Operations
    # =========================================================================

    def create_user(
        self,
        email: str,
        password_hash: str,
        full_name: str,
        phone: Optional[str] = None,
        is_driver: bool = False,
        vehicle_make: Optional[str] = None,
        vehicle_model: Optional[str] = None,
        license_plate: Optional[str] = None,
        verification_token: Optional[str] = None,
        verification_expires_at: Optional[datetime] = None
    ) -> int:
        """
        Create a new user account.

        Returns:
            The ID of the newly created user.
        """
        with self.get_connection() as conn:
            cursor = self._get_cursor(conn)

            # Check if this is the admin email
            is_admin = 1 if email.lower() == config.ADMIN_EMAIL.lower() else 0

            p = self._placeholder()
            if self.use_postgres:
                cursor.execute(f"""
                    INSERT INTO users (
                        email, password_hash, full_name, phone, is_driver,
                        vehicle_make, vehicle_model, license_plate,
                        verification_token, verification_expires_at, is_admin
                    ) VALUES ({p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p})
                    RETURNING id
                """, (
                    email.lower(), password_hash, full_name, phone,
                    1 if is_driver else 0, vehicle_make, vehicle_model,
                    license_plate, verification_token, verification_expires_at,
                    is_admin
                ))

                row = cursor.fetchone()
                return row["id"] if isinstance(row, dict) else row[0]

            else:
                cursor.execute(f"""
                    INSERT INTO users (
                        email, password_hash, full_name, phone, is_driver,
                        vehicle_make, vehicle_model, license_plate,
                        verification_token, verification_expires_at, is_admin
                    ) VALUES ({p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p})
                """, (
                    email.lower(), password_hash, full_name, phone,
                    1 if is_driver else 0, vehicle_make, vehicle_model,
                    license_plate, verification_token, verification_expires_at,
                    is_admin
                ))

                return cursor.lastrowid
    
    def get_user_by_id(self, user_id: int) -> Optional[Dict[str, Any]]:
        """Get a user by their ID."""
        with self.get_connection() as conn:
            cursor = self._get_cursor(conn)
            p = self._placeholder()
            cursor.execute(f"SELECT * FROM users WHERE id = {p}", (user_id,))
            row = cursor.fetchone()
            return dict(row) if row else None
    
    def get_user_by_email(self, email: str) -> Optional[Dict[str, Any]]:
        """Get a user by their email address."""
        with self.get_connection() as conn:
            cursor = self._get_cursor(conn)
            p = self._placeholder()
            cursor.execute(f"SELECT * FROM users WHERE email = {p}", (email.lower(),))
            row = cursor.fetchone()
            return dict(row) if row else None
    
    def get_user_by_verification_token(self, token: str) -> Optional[Dict[str, Any]]:
        """Get a user by their verification token."""
        with self.get_connection() as conn:
            cursor = self._get_cursor(conn)
            p = self._placeholder()
            cursor.execute(f"SELECT * FROM users WHERE verification_token = {p}", (token,))
            row = cursor.fetchone()
            return dict(row) if row else None
    
    def get_user_by_reset_token(self, token: str) -> Optional[Dict[str, Any]]:
        """Get a user by their password reset token."""
        with self.get_connection() as conn:
            cursor = self._get_cursor(conn)
            p = self._placeholder()
            cursor.execute(f"SELECT * FROM users WHERE password_reset_token = {p}", (token,))
            row = cursor.fetchone()
            return dict(row) if row else None
    
    def update_user(self, user_id: int, **kwargs) -> bool:
        """
        Update user fields.
        
        Args:
            user_id: The ID of the user to update.
            **kwargs: Field names and values to update.
        
        Returns:
            True if the update was successful.
        """
        if not kwargs:
            return False
        
        allowed_fields = {
            'full_name', 'phone', 'bio', 'is_driver', 'vehicle_make',
            'vehicle_model', 'license_plate', 'emergency_contact_name',
            'emergency_contact_phone', 'profile_photo', 'drivers_license'
        }
        
        # Filter to only allowed fields
        fields = {k: v for k, v in kwargs.items() if k in allowed_fields}
        
        if not fields:
            return False
        
        p = self._placeholder()
        set_clause = ', '.join([f"{k} = {p}" for k in fields.keys()])
        values = list(fields.values()) + [user_id]
        
        with self.get_connection() as conn:
            cursor = self._get_cursor(conn)
            cursor.execute(f"UPDATE users SET {set_clause} WHERE id = {p}", tuple(values))
            return cursor.rowcount > 0
    
    def update_user_photo(self, user_id: int, photo_path: str) -> bool:
        """Update user's profile photo."""
        with self.get_connection() as conn:
            cursor = self._get_cursor(conn)
            p = self._placeholder()
            cursor.execute(f"UPDATE users SET profile_photo = {p} WHERE id = {p}", (photo_path, user_id))
            return cursor.rowcount > 0
    
    def set_user_verified(self, user_id: int) -> bool:
        """Mark a user as email verified."""
        with self.get_connection() as conn:
            cursor = self._get_cursor(conn)
            p = self._placeholder()
            cursor.execute(f"""
                UPDATE users 
                SET is_verified = 1, verification_token = NULL, verification_expires_at = NULL 
                WHERE id = {p}
            """, (user_id,))
            return cursor.rowcount > 0
    
    def update_verification_token(self, user_id: int, token: str, expires_at: datetime) -> bool:
        """Update the verification token for a user."""
        with self.get_connection() as conn:
            cursor = self._get_cursor(conn)
            p = self._placeholder()
            cursor.execute(f"""
                UPDATE users 
                SET verification_token = {p}, verification_expires_at = {p}
                WHERE id = {p}
            """, (token, expires_at, user_id))
            return cursor.rowcount > 0
    
    def update_password(self, user_id: int, password_hash: str) -> bool:
        """Update a user's password hash."""
        with self.get_connection() as conn:
            cursor = self._get_cursor(conn)
            p = self._placeholder()
            cursor.execute(f"UPDATE users SET password_hash = {p} WHERE id = {p}", (password_hash, user_id))
            return cursor.rowcount > 0
    
    def create_password_reset_token(self, user_id: int, token: str, expires_at: datetime) -> bool:
        """Create a password reset token for a user."""
        with self.get_connection() as conn:
            cursor = self._get_cursor(conn)
            p = self._placeholder()
            cursor.execute(f"""
                UPDATE users 
                SET password_reset_token = {p}, password_reset_expires_at = {p}
                WHERE id = {p}
            """, (token, expires_at, user_id))
            return cursor.rowcount > 0
    
    def reset_password(self, user_id: int, password_hash: str) -> bool:
        """Reset a user's password and clear the reset token."""
        with self.get_connection() as conn:
            cursor = self._get_cursor(conn)
            p = self._placeholder()
            cursor.execute(f"""
                UPDATE users 
                SET password_hash = {p}, password_reset_token = NULL, password_reset_expires_at = NULL
                WHERE id = {p}
            """, (password_hash, user_id))
            return cursor.rowcount > 0
    
    def update_last_login(self, user_id: int) -> bool:
        """Update the last login timestamp for a user."""
        with self.get_connection() as conn:
            cursor = self._get_cursor(conn)
            p = self._placeholder()
            cursor.execute(f"UPDATE users SET last_login = CURRENT_TIMESTAMP WHERE id = {p}", (user_id,))
            return cursor.rowcount > 0
    
    def increment_login_attempts(self, user_id: int) -> int:
        """Increment failed login attempts and return the new count."""
        with self.get_connection() as conn:
            cursor = self._get_cursor(conn)
            p = self._placeholder()
            cursor.execute(f"UPDATE users SET login_attempts = login_attempts + 1 WHERE id = {p}", (user_id,))
            cursor.execute(f"SELECT login_attempts FROM users WHERE id = {p}", (user_id,))
            row = cursor.fetchone()
            return row['login_attempts'] if row else 0
    
    def reset_login_attempts(self, user_id: int) -> bool:
        """Reset login attempts after successful login."""
        with self.get_connection() as conn:
            cursor = self._get_cursor(conn)
            p = self._placeholder()
            cursor.execute(f"UPDATE users SET login_attempts = 0, lockout_until = NULL WHERE id = {p}", (user_id,))
            return cursor.rowcount > 0
    
    def set_user_lockout(self, user_id: int, until: datetime) -> bool:
        """Lock a user account until the specified time."""
        with self.get_connection() as conn:
            cursor = self._get_cursor(conn)
            p = self._placeholder()
            cursor.execute(f"UPDATE users SET lockout_until = {p} WHERE id = {p}", (until, user_id))
            return cursor.rowcount > 0
    
    def ban_user(self, user_id: int) -> bool:
        """Ban a user from the platform."""
        with self.get_connection() as conn:
            cursor = self._get_cursor(conn)
            p = self._placeholder()
            cursor.execute(f"UPDATE users SET is_banned = 1 WHERE id = {p}", (user_id,))
            return cursor.rowcount > 0
    
    def unban_user(self, user_id: int) -> bool:
        """Unban a user."""
        with self.get_connection() as conn:
            cursor = self._get_cursor(conn)
            p = self._placeholder()
            cursor.execute(f"UPDATE users SET is_banned = 0 WHERE id = {p}", (user_id,))
            return cursor.rowcount > 0
    
    def get_all_users(
        self,
        page: int = 1,
        per_page: int = 20,
        search: Optional[str] = None,
        filter_banned: Optional[bool] = None,
        filter_verified: Optional[bool] = None
    ) -> Tuple[List[Dict[str, Any]], int]:
        """
        Get all users with pagination and filters.
        
        Returns:
            Tuple of (list of users, total count)
        """
        with self.get_connection() as conn:
            cursor = self._get_cursor(conn)
            p = self._placeholder()
            
            conditions = []
            params = []
            
            if search:
                conditions.append(f"(full_name LIKE {p} OR email LIKE {p})")
                search_term = f"%{search}%"
                params.extend([search_term, search_term])
            
            if filter_banned is not None:
                conditions.append(f"is_banned = {p}")
                params.append(1 if filter_banned else 0)
            
            if filter_verified is not None:
                conditions.append(f"is_verified = {p}")
                params.append(1 if filter_verified else 0)
            
            where_clause = " AND ".join(conditions) if conditions else "1=1"
            
            # Get total count
            cursor.execute(f"SELECT COUNT(*) as count FROM users WHERE {where_clause}", tuple(params))
            row = cursor.fetchone()
            total = row['count'] if self.use_postgres else row[0]
            
            # Get paginated results
            offset = (page - 1) * per_page
            cursor.execute(f"""
                SELECT id, email, full_name, phone, is_verified, is_driver,
                       is_admin, is_banned, created_at, last_login
                FROM users 
                WHERE {where_clause}
                ORDER BY created_at DESC
                LIMIT {p} OFFSET {p}
            """, tuple(params + [per_page, offset]))
            
            users = [dict(row) for row in cursor.fetchall()]
            return users, total
    
    # =========================================================================
    # Ride Operations
    # =========================================================================
    
    def create_ride(
        self,
        driver_id: int,
        origin: str,
        destination: str,
        departure_date: str,
        departure_time: str,
        total_seats: int,
        price_per_seat: float,
        origin_lat: Optional[float] = None,
        origin_lng: Optional[float] = None,
        destination_lat: Optional[float] = None,
        destination_lng: Optional[float] = None,
        distance_km: Optional[float] = None,
        estimated_duration_minutes: Optional[int] = None,
        notes: Optional[str] = None,
        luggage_allowed: bool = True,
        pets_allowed: bool = False,
        smoking_allowed: bool = False,
        music_allowed: bool = True,
        ac_available: bool = True,
        vehicle_type: Optional[str] = None,
        vehicle_color: Optional[str] = None,
        pickup_flexibility: str = 'exact',
        return_trip: bool = False
    ) -> int:
        """
        Create a new ride.
        
        Returns:
            The ID of the newly created ride.
        """
        with self.get_connection() as conn:
            cursor = self._get_cursor(conn)
            p = self._placeholder()
            
            cursor.execute(f"""
                INSERT INTO rides (
                    driver_id, origin, destination, departure_date, departure_time,
                    total_seats, price_per_seat, origin_lat, origin_lng,
                    destination_lat, destination_lng, distance_km,
                    estimated_duration_minutes, notes, luggage_allowed, pets_allowed,
                    smoking_allowed, music_allowed, ac_available, vehicle_type,
                    vehicle_color, pickup_flexibility, return_trip
                ) VALUES ({p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p})
            """, (
                driver_id, origin, destination, departure_date, departure_time,
                total_seats, price_per_seat, origin_lat, origin_lng,
                destination_lat, destination_lng, distance_km,
                estimated_duration_minutes, notes,
                1 if luggage_allowed else 0,
                1 if pets_allowed else 0,
                1 if smoking_allowed else 0,
                1 if music_allowed else 0,
                1 if ac_available else 0,
                vehicle_type, vehicle_color, pickup_flexibility,
                1 if return_trip else 0
            ))
            
            if self.use_postgres:
                cursor.execute("SELECT lastval()")
                result = cursor.fetchone()
                if result is None:
                    return None
                if isinstance(result, dict):
                    # For psycopg2 with RealDictCursor
                    return result.get('id') or result.get('user_id') or result.get('ride_id') or next(iter(result.values()), None)
                return result[0]
            else:
                return cursor.lastrowid
    
    def get_ride_by_id(self, ride_id: int) -> Optional[Dict[str, Any]]:
        """Get a ride by its ID, including driver information."""
        with self.get_connection() as conn:
            cursor = self._get_cursor(conn)
            p = self._placeholder()
            cursor.execute(f"""
                SELECT r.*, u.full_name as driver_name, u.phone as driver_phone,
                       u.profile_photo as driver_photo, u.email as driver_email
                FROM rides r
                JOIN users u ON r.driver_id = u.id
                WHERE r.id = {p}
            """, (ride_id,))
            row = cursor.fetchone()
            if row:
                ride = dict(row)
                ride['driver_rating'] = self.get_user_average_rating(ride['driver_id'])
                for k, v in ride.items():
                    import datetime
                    if isinstance(v, (datetime.datetime, datetime.date)):
                        ride[k] = v.isoformat()
                    elif isinstance(v, datetime.time):
                        ride[k] = v.strftime('%H:%M:%S')
                return ride
            return None
    
    def get_rides_by_driver(
        self,
        driver_id: int,
        status: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get all rides posted by a specific driver."""
        with self.get_connection() as conn:
            cursor = self._get_cursor(conn)
            p = self._placeholder()
            
            if status:
                cursor.execute(f"""
                    SELECT * FROM rides 
                    WHERE driver_id = {p} AND status = {p}
                    ORDER BY departure_date DESC, departure_time DESC
                """, (driver_id, status))
            else:
                cursor.execute(f"""
                    SELECT * FROM rides 
                    WHERE driver_id = {p}
                    ORDER BY departure_date DESC, departure_time DESC
                """, (driver_id,))
            
            rides = []
            for row in cursor.fetchall():
                ride = dict(row)
                for k, v in ride.items():
                    import datetime
                    if isinstance(v, (datetime.datetime, datetime.date)):
                        ride[k] = v.isoformat()
                    elif isinstance(v, datetime.time):
                        ride[k] = v.strftime('%H:%M:%S')
                rides.append(ride)
            return rides
    
    def search_rides(
        self,
        origin: Optional[str] = None,
        destination: Optional[str] = None,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        max_price: Optional[float] = None,
        min_seats: Optional[int] = None,
        min_rating: Optional[float] = None,
        sort_by: str = 'departure_date',
        page: int = 1,
        per_page: int = 50
    ) -> Tuple[List[Dict[str, Any]], int]:
        """
        Search for rides with filters.
        
        Returns:
            Tuple of (list of rides, total count)
        """
        with self.get_connection() as conn:
            cursor = self._get_cursor(conn)
            p = self._placeholder()
            
            conditions = ["r.status = 'active'", "r.seats_taken < r.total_seats"]
            params = []
            
            # Only show future rides
            if self.use_postgres:
                conditions.append("(r.departure_date > CURRENT_DATE OR (r.departure_date = CURRENT_DATE AND r.departure_time > CURRENT_TIME))")
            else:
                conditions.append("(r.departure_date > date('now') OR (r.departure_date = date('now') AND r.departure_time > time('now')))")
            
            if origin:
                conditions.append(f"r.origin LIKE {p}")
                params.append(f"%{origin}%")
            
            if destination:
                conditions.append(f"r.destination LIKE {p}")
                params.append(f"%{destination}%")
            
            if date_from:
                conditions.append(f"r.departure_date >= {p}")
                params.append(date_from)
            
            if date_to:
                conditions.append(f"r.departure_date <= {p}")
                params.append(date_to)
            
            if max_price:
                conditions.append(f"r.price_per_seat <= {p}")
                params.append(max_price)
            
            if min_seats:
                conditions.append(f"(r.total_seats - r.seats_taken) >= {p}")
                params.append(min_seats)
            
            where_clause = " AND ".join(conditions)
            
            # Get total count first
            cursor.execute(f"SELECT COUNT(*) as count FROM rides r WHERE {where_clause}", tuple(params))
            row = cursor.fetchone()
            total = row['count'] if self.use_postgres else row[0]
            
            # Determine sort order
            sort_options = {
                'departure_date': 'r.departure_date ASC, r.departure_time ASC',
                'price_low': 'r.price_per_seat ASC',
                'price_high': 'r.price_per_seat DESC',
                'seats': '(r.total_seats - r.seats_taken) DESC'
            }
            order_by = sort_options.get(sort_by, sort_options['departure_date'])
            
            # Get paginated results
            offset = (page - 1) * per_page
            cursor.execute(f"""
                SELECT r.*, u.full_name as driver_name, u.profile_photo as driver_photo
                FROM rides r
                JOIN users u ON r.driver_id = u.id
                WHERE {where_clause}
                ORDER BY {order_by}
                LIMIT {p} OFFSET {p}
            """, tuple(params + [per_page, offset]))
            
            rides = []
            for row in cursor.fetchall():
                ride = dict(row)
                ride['driver_rating'] = self.get_user_average_rating(ride['driver_id'])
                ride['available_seats'] = ride['total_seats'] - ride['seats_taken']
                
                # Apply min_rating filter (after fetching because rating is calculated)
                if min_rating and ride['driver_rating'] < min_rating:
                    continue
                    
                rides.append(ride)
            
            return rides, total
    
    def get_all_active_rides(self) -> List[Dict[str, Any]]:
        """
        Get all active rides with available seats.
        Used by the chatbot to recommend rides.
        """
        with self.get_connection() as conn:
            cursor = self._get_cursor(conn)
            if self.use_postgres:
                cursor.execute("""
                    SELECT r.*, u.full_name as driver_name
                    FROM rides r
                    JOIN users u ON r.driver_id = u.id
                    WHERE r.status = 'active' 
                    AND r.seats_taken < r.total_seats
                    AND (r.departure_date > CURRENT_DATE 
                         OR (r.departure_date = CURRENT_DATE AND r.departure_time > CURRENT_TIME))
                    ORDER BY r.departure_date ASC, r.departure_time ASC
                """)
            else:
                cursor.execute("""
                    SELECT r.*, u.full_name as driver_name
                    FROM rides r
                    JOIN users u ON r.driver_id = u.id
                    WHERE r.status = 'active' 
                    AND r.seats_taken < r.total_seats
                    AND (r.departure_date > date('now') 
                         OR (r.departure_date = date('now') AND r.departure_time > time('now')))
                    ORDER BY r.departure_date ASC, r.departure_time ASC
                """)
            rides = []
            for row in cursor.fetchall():
                ride = dict(row)
                ride['driver_rating'] = self.get_user_average_rating(ride['driver_id'])
                ride['available_seats'] = ride['total_seats'] - ride['seats_taken']
                for k, v in ride.items():
                    import datetime
                    if isinstance(v, (datetime.datetime, datetime.date)):
                        ride[k] = v.isoformat()
                    elif isinstance(v, datetime.time):
                        ride[k] = v.strftime('%H:%M:%S')
                rides.append(ride)
            return rides
    
    def update_ride(self, ride_id: int, **kwargs) -> bool:
        """Update ride fields."""
        allowed_fields = {
            'origin', 'destination', 'departure_date', 'departure_time',
            'total_seats', 'price_per_seat', 'origin_lat', 'origin_lng',
            'destination_lat', 'destination_lng', 'distance_km',
            'estimated_duration_minutes', 'notes', 'status'
        }
        
        fields = {k: v for k, v in kwargs.items() if k in allowed_fields}
        
        if not fields:
            return False
        
        fields['updated_at'] = datetime.now() if self.use_postgres else datetime.now().isoformat()
        
        p = self._placeholder()
        set_clause = ', '.join([f"{k} = {p}" for k in fields.keys()])
        values = list(fields.values()) + [ride_id]
        
        with self.get_connection() as conn:
            cursor = self._get_cursor(conn)
            cursor.execute(f"UPDATE rides SET {set_clause} WHERE id = {p}", tuple(values))
            return cursor.rowcount > 0
    
    def delete_ride(self, ride_id: int) -> bool:
        """Delete a ride (only if it has no confirmed bookings)."""
        with self.get_connection() as conn:
            cursor = self._get_cursor(conn)
            p = self._placeholder()
            
            # Check for confirmed bookings
            cursor.execute(f"""
                SELECT COUNT(*) as count FROM bookings 
                WHERE ride_id = {p} AND status = 'confirmed'
            """, (ride_id,))
            
            row = cursor.fetchone()
            count = row['count'] if self.use_postgres else row[0]
            if count > 0:
                return False
            
            cursor.execute(f"DELETE FROM rides WHERE id = {p}", (ride_id,))
            return cursor.rowcount > 0
    
    def mark_ride_full(self, ride_id: int) -> bool:
        """Mark a ride as full."""
        return self.update_ride(ride_id, status='full')
    
    def mark_ride_completed(self, ride_id: int) -> bool:
        """Mark a ride as completed."""
        with self.get_connection() as conn:
            cursor = self._get_cursor(conn)
            p = self._placeholder()
            cursor.execute(f"UPDATE rides SET status = 'completed', updated_at = CURRENT_TIMESTAMP WHERE id = {p}", (ride_id,))
            
            # Also mark all confirmed bookings as completed
            cursor.execute(f"""
                UPDATE bookings 
                SET status = 'completed', updated_at = CURRENT_TIMESTAMP 
                WHERE ride_id = {p} AND status = 'confirmed'
            """, (ride_id,))
            
            return cursor.rowcount > 0
    
    def cancel_ride(self, ride_id: int) -> bool:
        """Cancel a ride and all its pending/confirmed bookings."""
        with self.get_connection() as conn:
            cursor = self._get_cursor(conn)
            p = self._placeholder()
            cursor.execute(f"UPDATE rides SET status = 'cancelled', updated_at = CURRENT_TIMESTAMP WHERE id = {p}", (ride_id,))
            
            # Cancel all bookings for this ride
            cursor.execute(f"""
                UPDATE bookings 
                SET status = 'cancelled', updated_at = CURRENT_TIMESTAMP 
                WHERE ride_id = {p} AND status IN ('pending', 'confirmed')
            """, (ride_id,))
            
            return cursor.rowcount > 0
    
    def update_seats_taken(self, ride_id: int, seats_taken: int) -> bool:
        """Update the number of seats taken for a ride."""
        with self.get_connection() as conn:
            cursor = self._get_cursor(conn)
            p = self._placeholder()
            cursor.execute(f"UPDATE rides SET seats_taken = {p}, updated_at = CURRENT_TIMESTAMP WHERE id = {p}", (seats_taken, ride_id))
            return cursor.rowcount > 0
    
    def get_all_rides(
        self,
        page: int = 1,
        per_page: int = 20,
        status: Optional[str] = None
    ) -> Tuple[List[Dict[str, Any]], int]:
        """Get all rides with pagination (for admin)."""
        with self.get_connection() as conn:
            cursor = self._get_cursor(conn)
            p = self._placeholder()
            
            conditions = []
            params = []
            
            if status:
                conditions.append(f"r.status = {p}")
                params.append(status)
            
            where_clause = " AND ".join(conditions) if conditions else "1=1"
            
            # Get total count
            cursor.execute(f"SELECT COUNT(*) as count FROM rides r WHERE {where_clause}", tuple(params))
            row = cursor.fetchone()
            total = row['count'] if self.use_postgres else row[0]
            
            # Get paginated results
            offset = (page - 1) * per_page
            cursor.execute(f"""
                SELECT r.*, u.full_name as driver_name, u.email as driver_email
                FROM rides r
                JOIN users u ON r.driver_id = u.id
                WHERE {where_clause}
                ORDER BY r.created_at DESC
                LIMIT {p} OFFSET {p}
            """, tuple(params + [per_page, offset]))
            
            rides = []
            for row in cursor.fetchall():
                ride = dict(row)
                for k, v in ride.items():
                    import datetime
                    if isinstance(v, (datetime.datetime, datetime.date)):
                        ride[k] = v.isoformat()
                    elif isinstance(v, datetime.time):
                        ride[k] = v.strftime('%H:%M:%S')
                rides.append(ride)
            return rides, total
    
    # =========================================================================
    # Booking Operations
    # =========================================================================
    
    def create_booking(self, ride_id: int, passenger_id: int) -> Optional[int]:
        """
        Create a new booking request.
        
        Returns:
            The booking ID, or None if booking already exists.
        """
        with self.get_connection() as conn:
            cursor = self._get_cursor(conn)
            p = self._placeholder()
            
            try:
                cursor.execute(f"INSERT INTO bookings (ride_id, passenger_id) VALUES ({p}, {p})", (ride_id, passenger_id))
                
                if self.use_postgres:
                    cursor.execute("SELECT lastval()")
                    result = cursor.fetchone()
                    if result is None:
                        return None
                    if isinstance(result, dict):
                        return result.get('id') or result.get('user_id') or result.get('ride_id') or next(iter(result.values()), None)
                    return result[0]
                else:
                    return cursor.lastrowid
            except (sqlite3.IntegrityError if not self.use_postgres else psycopg2.IntegrityError):
                return None
    
    def get_booking_by_id(self, booking_id: int) -> Optional[Dict[str, Any]]:
        """Get a booking by its ID with related ride and user info."""
        with self.get_connection() as conn:
            cursor = self._get_cursor(conn)
            p = self._placeholder()
            cursor.execute(f"""
                SELECT b.*, 
                       r.origin, r.destination, r.departure_date, r.departure_time,
                       r.price_per_seat, r.driver_id, r.status as ride_status,
                       p.full_name as passenger_name, p.email as passenger_email,
                       p.phone as passenger_phone, p.profile_photo as passenger_photo,
                       d.full_name as driver_name, d.email as driver_email,
                       d.phone as driver_phone
                FROM bookings b
                JOIN rides r ON b.ride_id = r.id
                JOIN users p ON b.passenger_id = p.id
                JOIN users d ON r.driver_id = d.id
                WHERE b.id = {p}
            """, (booking_id,))
            row = cursor.fetchone()
            return dict(row) if row else None
    
    def get_bookings_by_passenger(
        self,
        passenger_id: int,
        status: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get all bookings for a passenger."""
        with self.get_connection() as conn:
            cursor = self._get_cursor(conn)
            p = self._placeholder()
            
            if status:
                cursor.execute(f"""
                    SELECT b.*, r.origin, r.destination, r.departure_date, 
                           r.departure_time, r.price_per_seat, r.driver_id,
                           r.status as ride_status, u.full_name as driver_name,
                           u.phone as driver_phone
                    FROM bookings b
                    JOIN rides r ON b.ride_id = r.id
                    JOIN users u ON r.driver_id = u.id
                    WHERE b.passenger_id = {p} AND b.status = {p}
                    ORDER BY r.departure_date DESC, r.departure_time DESC
                """, (passenger_id, status))
            else:
                cursor.execute(f"""
                    SELECT b.*, r.origin, r.destination, r.departure_date, 
                           r.departure_time, r.price_per_seat, r.driver_id,
                           r.status as ride_status, u.full_name as driver_name,
                           u.phone as driver_phone
                    FROM bookings b
                    JOIN rides r ON b.ride_id = r.id
                    JOIN users u ON r.driver_id = u.id
                    WHERE b.passenger_id = {p}
                    ORDER BY r.departure_date DESC, r.departure_time DESC
                """, (passenger_id,))
            
            bookings = []
            for row in cursor.fetchall():
                booking = dict(row)
                for k, v in booking.items():
                    import datetime
                    if isinstance(v, (datetime.datetime, datetime.date)):
                        booking[k] = v.isoformat()
                    elif isinstance(v, datetime.time):
                        booking[k] = v.strftime('%H:%M:%S')
                bookings.append(booking)
            return bookings
    
    def get_bookings_by_ride(self, ride_id: int) -> List[Dict[str, Any]]:
        """Get all bookings for a specific ride."""
        with self.get_connection() as conn:
            cursor = self._get_cursor(conn)
            p = self._placeholder()
            cursor.execute(f"""
                SELECT b.*, u.full_name as passenger_name, u.email as passenger_email,
                       u.phone as passenger_phone, u.profile_photo as passenger_photo
                FROM bookings b
                JOIN users u ON b.passenger_id = u.id
                WHERE b.ride_id = {p}
                ORDER BY b.created_at ASC
            """, (ride_id,))
            
            bookings = []
            for row in cursor.fetchall():
                booking = dict(row)
                booking['passenger_rating'] = self.get_user_average_rating(booking['passenger_id'])
                for k, v in booking.items():
                    import datetime
                    if isinstance(v, (datetime.datetime, datetime.date)):
                        booking[k] = v.isoformat()
                    elif isinstance(v, datetime.time):
                        booking[k] = v.strftime('%H:%M:%S')
                bookings.append(booking)
            return bookings
    
    def approve_booking(self, booking_id: int) -> bool:
        """Approve a booking request."""
        with self.get_connection() as conn:
            cursor = self._get_cursor(conn)
            p = self._placeholder()
            
            # Get booking to check ride seats
            cursor.execute(f"""
                SELECT b.ride_id, r.total_seats, r.seats_taken
                FROM bookings b
                JOIN rides r ON b.ride_id = r.id
                WHERE b.id = {p} AND b.status = 'pending'
            """, (booking_id,))
            
            row = cursor.fetchone()
            if not row:
                return False
            
            ride_id = row['ride_id']
            total_seats = row['total_seats']
            seats_taken = row['seats_taken']
            
            if seats_taken >= total_seats:
                return False
            
            # Update booking status
            cursor.execute(f"""
                UPDATE bookings 
                SET status = 'confirmed', updated_at = CURRENT_TIMESTAMP 
                WHERE id = {p}
            """, (booking_id,))
            
            # Increment seats taken
            new_seats_taken = seats_taken + 1
            cursor.execute(f"""
                UPDATE rides 
                SET seats_taken = {p}, updated_at = CURRENT_TIMESTAMP 
                WHERE id = {p}
            """, (new_seats_taken, ride_id))
            
            # Mark ride as full if all seats are taken
            if new_seats_taken >= total_seats:
                cursor.execute(f"""
                    UPDATE rides 
                    SET status = 'full', updated_at = CURRENT_TIMESTAMP 
                    WHERE id = {p}
                """, (ride_id,))
            
            return True
    
    def reject_booking(self, booking_id: int) -> bool:
        """Reject a booking request."""
        with self.get_connection() as conn:
            cursor = self._get_cursor(conn)
            p = self._placeholder()
            cursor.execute(f"""
                UPDATE bookings 
                SET status = 'rejected', updated_at = CURRENT_TIMESTAMP 
                WHERE id = {p} AND status = 'pending'
            """, (booking_id,))
            return cursor.rowcount > 0
    
    def cancel_booking(self, booking_id: int) -> bool:
        """Cancel a booking (by passenger)."""
        with self.get_connection() as conn:
            cursor = self._get_cursor(conn)
            p = self._placeholder()
            
            # Get booking info first
            cursor.execute(f"""
                SELECT b.status, b.ride_id, r.seats_taken
                FROM bookings b
                JOIN rides r ON b.ride_id = r.id
                WHERE b.id = {p}
            """, (booking_id,))
            
            row = cursor.fetchone()
            if not row:
                return False
            
            old_status = row['status']
            ride_id = row['ride_id']
            seats_taken = row['seats_taken']
            
            # Update booking status
            cursor.execute(f"""
                UPDATE bookings 
                SET status = 'cancelled', updated_at = CURRENT_TIMESTAMP 
                WHERE id = {p}
            """, (booking_id,))
            
            # If was confirmed, decrement seats and potentially reactivate ride
            if old_status == 'confirmed':
                new_seats_taken = max(0, seats_taken - 1)
                cursor.execute(f"""
                    UPDATE rides 
                    SET seats_taken = {p}, 
                        status = CASE WHEN status = 'full' THEN 'active' ELSE status END,
                        updated_at = CURRENT_TIMESTAMP 
                    WHERE id = {p}
                """, (new_seats_taken, ride_id))
            
            return True
    
    def complete_booking(self, booking_id: int) -> bool:
        """Mark a booking as completed."""
        with self.get_connection() as conn:
            cursor = self._get_cursor(conn)
            p = self._placeholder()
            cursor.execute(f"""
                UPDATE bookings 
                SET status = 'completed', updated_at = CURRENT_TIMESTAMP 
                WHERE id = {p} AND status = 'confirmed'
            """, (booking_id,))
            return cursor.rowcount > 0
    
    def check_existing_booking(self, ride_id: int, passenger_id: int) -> bool:
        """Check if a booking already exists for this ride and passenger."""
        with self.get_connection() as conn:
            cursor = self._get_cursor(conn)
            p = self._placeholder()
            cursor.execute(f"""
                SELECT id FROM bookings 
                WHERE ride_id = {p} AND passenger_id = {p} 
                AND status NOT IN ('cancelled', 'rejected')
            """, (ride_id, passenger_id))
            return cursor.fetchone() is not None
    
    # =========================================================================
    # Review Operations
    # =========================================================================
    
    def create_review(
        self,
        reviewer_id: int,
        reviewed_user_id: int,
        ride_id: int,
        rating: int,
        comment: Optional[str] = None
    ) -> Optional[int]:
        """
        Create a new review.
        
        Returns:
            The review ID, or None if review already exists.
        """
        with self.get_connection() as conn:
            cursor = self._get_cursor(conn)
            p = self._placeholder()
            
            try:
                cursor.execute(f"""
                    INSERT INTO reviews (reviewer_id, reviewed_user_id, ride_id, rating, comment)
                    VALUES ({p}, {p}, {p}, {p}, {p})
                """, (reviewer_id, reviewed_user_id, ride_id, rating, comment))
                
                if self.use_postgres:
                    cursor.execute("SELECT lastval()")
                    result = cursor.fetchone()
                    if result is None:
                        return None
                    if isinstance(result, dict):
                        return result.get('id') or result.get('user_id') or result.get('ride_id') or next(iter(result.values()), None)
                    return result[0]
                else:
                    return cursor.lastrowid
            except (sqlite3.IntegrityError if not self.use_postgres else psycopg2.IntegrityError):
                return None
    
    def get_reviews_for_user(self, user_id: int) -> List[Dict[str, Any]]:
        """Get all reviews for a user."""
        with self.get_connection() as conn:
            cursor = self._get_cursor(conn)
            p = self._placeholder()
            cursor.execute(f"""
                SELECT r.*, u.full_name as reviewer_name, u.profile_photo as reviewer_photo,
                       rd.origin, rd.destination, rd.departure_date
                FROM reviews r
                JOIN users u ON r.reviewer_id = u.id
                JOIN rides rd ON r.ride_id = rd.id
                WHERE r.reviewed_user_id = {p}
                ORDER BY r.created_at DESC
            """, (user_id,))
            reviews = []
            for row in cursor.fetchall():
                review = dict(row)
                for k, v in review.items():
                    import datetime
                    if isinstance(v, (datetime.datetime, datetime.date)):
                        review[k] = v.isoformat()
                    elif isinstance(v, datetime.time):
                        review[k] = v.strftime('%H:%M:%S')
                reviews.append(review)
            return reviews
    
    def get_user_average_rating(self, user_id: int) -> float:
        """Get the average rating for a user."""
        with self.get_connection() as conn:
            cursor = self._get_cursor(conn)
            p = self._placeholder()
            cursor.execute(f"""
                SELECT AVG(rating) as avg_rating, COUNT(*) as count
                FROM reviews
                WHERE reviewed_user_id = {p}
            """, (user_id,))
            row = cursor.fetchone()
            if row and row['avg_rating']:
                return round(float(row['avg_rating']), 1)
            return 0.0
    
    def check_review_exists(
        self,
        reviewer_id: int,
        reviewed_user_id: int,
        ride_id: int
    ) -> bool:
        """Check if a review already exists."""
        with self.get_connection() as conn:
            cursor = self._get_cursor(conn)
            p = self._placeholder()
            cursor.execute(f"""
                SELECT id FROM reviews 
                WHERE reviewer_id = {p} AND reviewed_user_id = {p} AND ride_id = {p}
            """, (reviewer_id, reviewed_user_id, ride_id))
            return cursor.fetchone() is not None
    
    def can_review_user(
        self,
        reviewer_id: int,
        reviewed_user_id: int,
        ride_id: int
    ) -> bool:
        """
        Check if a user can review another user for a specific ride.
        User must have been on the same completed ride.
        """
        with self.get_connection() as conn:
            cursor = self._get_cursor(conn)
            p = self._placeholder()
            
            # Check if ride is completed
            cursor.execute(f"SELECT driver_id, status FROM rides WHERE id = {p}", (ride_id,))
            ride = cursor.fetchone()
            
            if not ride or ride['status'] != 'completed':
                return False
            
            driver_id = ride['driver_id']
            
            # If reviewer is the driver, reviewed must be a confirmed passenger
            if reviewer_id == driver_id:
                cursor.execute(f"""
                    SELECT id FROM bookings 
                    WHERE ride_id = {p} AND passenger_id = {p} AND status = 'completed'
                """, (ride_id, reviewed_user_id))
                return cursor.fetchone() is not None
            
            # If reviewer is a passenger, they can review the driver
            # or another passenger (if they were both confirmed)
            cursor.execute(f"""
                SELECT id FROM bookings 
                WHERE ride_id = {p} AND passenger_id = {p} AND status = 'completed'
            """, (ride_id, reviewer_id))
            
            if not cursor.fetchone():
                return False
            
            # Reviewed is the driver
            if reviewed_user_id == driver_id:
                return True
            
            # Reviewed is another passenger
            cursor.execute(f"""
                SELECT id FROM bookings 
                WHERE ride_id = {p} AND passenger_id = {p} AND status = 'completed'
            """, (ride_id, reviewed_user_id))
            return cursor.fetchone() is not None
    
    # =========================================================================
    # Message Operations
    # =========================================================================
    
    def create_message(
        self,
        sender_id: int,
        receiver_id: int,
        content: str,
        ride_id: int = None
    ) -> int:
        """Create a new message. ride_id is optional for general user-to-user chat."""
        with self.get_connection() as conn:
            cursor = self._get_cursor(conn)
            p = self._placeholder()
            cursor.execute(f"""
                INSERT INTO messages (sender_id, receiver_id, ride_id, content)
                VALUES ({p}, {p}, {p}, {p})
            """, (sender_id, receiver_id, ride_id, content))
            
            if self.use_postgres:
                cursor.execute("SELECT lastval()")
                result = cursor.fetchone()
                if result is None:
                    return None
                if isinstance(result, dict):
                    return result.get('id') or result.get('user_id') or result.get('ride_id') or next(iter(result.values()), None)
                return result[0]
            else:
                return cursor.lastrowid
    
    def get_conversations_for_user(self, user_id: int) -> List[Dict[str, Any]]:
        """Get all unique conversations for a user (grouped by other user, not ride)."""
        with self.get_connection() as conn:
            cursor = self._get_cursor(conn)
            p = self._placeholder()
            
            # Get unique conversation partners
            cursor.execute(f"""
                SELECT DISTINCT
                    CASE WHEN sender_id = {p} THEN receiver_id ELSE sender_id END as other_user_id
                FROM messages
                WHERE sender_id = {p} OR receiver_id = {p}
            """, (user_id, user_id, user_id))
            
            conversations = []
            for row in cursor.fetchall():
                other_user_id = row['other_user_id']
                
                # Get other user info
                cursor.execute(f"SELECT id, full_name, profile_photo FROM users WHERE id = {p}", (other_user_id,))
                other_user = cursor.fetchone()
                
                if not other_user:
                    continue
                
                # Get last message between these two users
                cursor.execute(f"""
                    SELECT content, created_at, sender_id, ride_id
                    FROM messages
                    WHERE (sender_id = {p} AND receiver_id = {p}) OR (sender_id = {p} AND receiver_id = {p})
                    ORDER BY created_at DESC
                    LIMIT 1
                """, (user_id, other_user_id, other_user_id, user_id))
                last_message = cursor.fetchone()
                
                # Get unread count
                cursor.execute(f"""
                    SELECT COUNT(*) as count
                    FROM messages
                    WHERE receiver_id = {p} AND sender_id = {p} AND is_read = 0
                """, (user_id, other_user_id))
                row = cursor.fetchone()
                unread = row['count'] if self.use_postgres else row[0]
                
                conversations.append({
                    'id': other_user_id,
                    'other_user_id': other_user_id,
                    'other_user_name': other_user['full_name'] if other_user else 'Unknown',
                    'other_user_photo': other_user['profile_photo'] if other_user else None,
                    'last_message': last_message['content'] if last_message else '',
                    'last_message_time': last_message['created_at'] if last_message else None,
                    'last_message_is_mine': last_message['sender_id'] == user_id if last_message else False,
                    'unread_count': unread
                })
            
            # Sort by last message time
            conversations.sort(
                key=lambda x: x['last_message_time'] or '',
                reverse=True
            )
            
            return conversations
    
    def get_messages_in_conversation(
        self,
        user_id: int,
        other_user_id: int
    ) -> List[Dict[str, Any]]:
        """Get all messages between two users."""
        with self.get_connection() as conn:
            cursor = self._get_cursor(conn)
            p = self._placeholder()
            cursor.execute(f"""
                SELECT m.*, 
                       s.full_name as sender_name, s.profile_photo as sender_photo
                FROM messages m
                JOIN users s ON m.sender_id = s.id
                WHERE (m.sender_id = {p} AND m.receiver_id = {p}) 
                   OR (m.sender_id = {p} AND m.receiver_id = {p})
                ORDER BY m.created_at ASC
            """, (user_id, other_user_id, other_user_id, user_id))
            return [dict(row) for row in cursor.fetchall()]
    
    def mark_messages_read(
        self,
        receiver_id: int,
        sender_id: int
    ) -> int:
        """Mark all messages from sender to receiver as read."""
        with self.get_connection() as conn:
            cursor = self._get_cursor(conn)
            p = self._placeholder()
            cursor.execute(f"""
                UPDATE messages 
                SET is_read = 1 
                WHERE receiver_id = {p} AND sender_id = {p} AND is_read = 0
            """, (receiver_id, sender_id))
            return cursor.rowcount
    
    def get_unread_count(self, user_id: int) -> int:
        """Get total unread message count for a user."""
        with self.get_connection() as conn:
            cursor = self._get_cursor(conn)
            p = self._placeholder()
            cursor.execute(f"""
                SELECT COUNT(*) as count 
                FROM messages 
                WHERE receiver_id = {p} AND is_read = 0
            """, (user_id,))
            row = cursor.fetchone()
            return row['count'] if self.use_postgres else row[0]
    
    def can_message_user(
        self,
        sender_id: int,
        receiver_id: int,
        ride_id: int
    ) -> bool:
        """
        Check if a user can message another user about a specific ride.
        Users must be involved in the same ride.
        """
        with self.get_connection() as conn:
            cursor = self._get_cursor(conn)
            p = self._placeholder()
            
            # Get ride info
            cursor.execute(f"SELECT driver_id, status FROM rides WHERE id = {p}", (ride_id,))
            ride = cursor.fetchone()
            
            if not ride:
                return False
            
            driver_id = ride['driver_id']
            
            # Check if sender is driver or has a booking
            sender_is_driver = sender_id == driver_id
            if not sender_is_driver:
                cursor.execute(f"""
                    SELECT id FROM bookings 
                    WHERE ride_id = {p} AND passenger_id = {p} 
                    AND status IN ('pending', 'confirmed', 'completed')
                """, (ride_id, sender_id))
                if not cursor.fetchone():
                    return False
            
            # Check if receiver is driver or has a booking
            receiver_is_driver = receiver_id == driver_id
            if not receiver_is_driver:
                cursor.execute(f"""
                    SELECT id FROM bookings 
                    WHERE ride_id = {p} AND passenger_id = {p} 
                    AND status IN ('pending', 'confirmed', 'completed')
                """, (ride_id, receiver_id))
                if not cursor.fetchone():
                    return False
            
            return True
    
    # =========================================================================
    # Block Operations
    # =========================================================================
    
    def block_user(self, blocker_id: int, blocked_id: int) -> bool:
        """Block a user."""
        with self.get_connection() as conn:
            cursor = self._get_cursor(conn)
            p = self._placeholder()
            try:
                cursor.execute(f"INSERT INTO user_blocks (blocker_id, blocked_id) VALUES ({p}, {p})", (blocker_id, blocked_id))
                return True
            except (sqlite3.IntegrityError if not self.use_postgres else psycopg2.IntegrityError):
                return False
    
    def unblock_user(self, blocker_id: int, blocked_id: int) -> bool:
        """Unblock a user."""
        with self.get_connection() as conn:
            cursor = self._get_cursor(conn)
            p = self._placeholder()
            cursor.execute(f"DELETE FROM user_blocks WHERE blocker_id = {p} AND blocked_id = {p}", (blocker_id, blocked_id))
            return cursor.rowcount > 0
    
    def is_user_blocked(self, blocker_id: int, blocked_id: int) -> bool:
        """Check if a user has blocked another user."""
        with self.get_connection() as conn:
            cursor = self._get_cursor(conn)
            p = self._placeholder()
            cursor.execute(f"SELECT id FROM user_blocks WHERE blocker_id = {p} AND blocked_id = {p}", (blocker_id, blocked_id))
            return cursor.fetchone() is not None
    
    def is_blocked_by(self, user_id: int, other_user_id: int) -> bool:
        """Check if user_id is blocked by other_user_id."""
        return self.is_user_blocked(other_user_id, user_id)
    
    def get_blocked_users(self, user_id: int) -> List[Dict[str, Any]]:
        """Get all users blocked by a user."""
        with self.get_connection() as conn:
            cursor = self._get_cursor(conn)
            p = self._placeholder()
            cursor.execute(f"""
                SELECT u.id, u.full_name, u.profile_photo, ub.created_at as blocked_at
                FROM user_blocks ub
                JOIN users u ON ub.blocked_id = u.id
                WHERE ub.blocker_id = {p}
                ORDER BY ub.created_at DESC
            """, (user_id,))
            return [dict(row) for row in cursor.fetchall()]
    
    # =========================================================================
    # Report Operations
    # =========================================================================
    
    def create_report(
        self,
        reporter_id: int,
        reported_user_id: int,
        reason: str,
        ride_id: Optional[int] = None
    ) -> int:
        """Create a new user report."""
        with self.get_connection() as conn:
            cursor = self._get_cursor(conn)
            p = self._placeholder()
            cursor.execute(f"""
                INSERT INTO user_reports (reporter_id, reported_user_id, ride_id, reason)
                VALUES ({p}, {p}, {p}, {p})
            """, (reporter_id, reported_user_id, ride_id, reason))
            
            if self.use_postgres:
                cursor.execute("SELECT lastval()")
                result = cursor.fetchone()
                if result is None:
                    return None
                if isinstance(result, dict):
                    return result.get('id') or result.get('user_id') or result.get('ride_id') or next(iter(result.values()), None)
                return result[0]
            else:
                return cursor.lastrowid
    
    def get_reports(
        self,
        status: Optional[str] = None,
        page: int = 1,
        per_page: int = 20
    ) -> Tuple[List[Dict[str, Any]], int]:
        """Get all reports with pagination."""
        with self.get_connection() as conn:
            cursor = self._get_cursor(conn)
            p = self._placeholder()
            
            conditions = []
            params = []
            
            if status:
                conditions.append(f"ur.status = {p}")
                params.append(status)
            
            where_clause = " AND ".join(conditions) if conditions else "1=1"
            
            # Get total count
            cursor.execute(f"SELECT COUNT(*) as count FROM user_reports ur WHERE {where_clause}", tuple(params))
            row = cursor.fetchone()
            total = row['count'] if self.use_postgres else row[0]
            
            # Get paginated results
            offset = (page - 1) * per_page
            cursor.execute(f"""
                SELECT ur.*, 
                       reporter.full_name as reporter_name,
                       reported.full_name as reported_user_name,
                       r.origin, r.destination
                FROM user_reports ur
                JOIN users reporter ON ur.reporter_id = reporter.id
                JOIN users reported ON ur.reported_user_id = reported.id
                LEFT JOIN rides r ON ur.ride_id = r.id
                WHERE {where_clause}
                ORDER BY ur.created_at DESC
                LIMIT {p} OFFSET {p}
            """, tuple(params + [per_page, offset]))
            
            reports = [dict(row) for row in cursor.fetchall()]
            return reports, total
    
    def update_report_status(
        self,
        report_id: int,
        status: str,
        admin_notes: Optional[str] = None
    ) -> bool:
        """Update the status of a report."""
        with self.get_connection() as conn:
            cursor = self._get_cursor(conn)
            p = self._placeholder()
            cursor.execute(f"""
                UPDATE user_reports 
                SET status = {p}, admin_notes = {p}, updated_at = CURRENT_TIMESTAMP 
                WHERE id = {p}
            """, (status, admin_notes, report_id))
            return cursor.rowcount > 0
    
    # =========================================================================
    # Chat Log Operations
    # =========================================================================
    
    def log_chat_interaction(
        self,
        user_id: int,
        user_message: str,
        bot_response: str,
        tokens_used: Optional[int] = None
    ) -> int:
        """Log a chatbot interaction."""
        with self.get_connection() as conn:
            cursor = self._get_cursor(conn)
            p = self._placeholder()
            cursor.execute(f"""
                INSERT INTO chat_logs (user_id, user_message, bot_response, tokens_used)
                VALUES ({p}, {p}, {p}, {p})
            """, (user_id, user_message, bot_response, tokens_used))
            
            if self.use_postgres:
                cursor.execute("SELECT lastval()")
                result = cursor.fetchone()
                if result is None:
                    return None
                if isinstance(result, dict):
                    return result.get('id') or result.get('user_id') or result.get('ride_id') or next(iter(result.values()), None)
                return result[0]
            else:
                return cursor.lastrowid
    
    def get_chat_history_for_user(
        self,
        user_id: int,
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """Get recent chat history for a user."""
        with self.get_connection() as conn:
            cursor = self._get_cursor(conn)
            p = self._placeholder()
            cursor.execute(f"""
                SELECT user_message, bot_response, created_at
                FROM chat_logs
                WHERE user_id = {p}
                ORDER BY created_at DESC
                LIMIT {p}
            """, (user_id, limit))
            
            # Return in chronological order
            rows = cursor.fetchall()
            return [dict(row) for row in reversed(rows)]
    
    def get_user_chat_count_last_minute(self, user_id: int) -> int:
        """Get the number of chat requests from a user in the last minute."""
        with self.get_connection() as conn:
            cursor = self._get_cursor(conn)
            p = self._placeholder()
            one_minute_ago = datetime.now() - timedelta(minutes=1)
            
            cursor.execute(f"""
                SELECT COUNT(*) as count
                FROM chat_logs
                WHERE user_id = {p} AND created_at > {p}
            """, (user_id, one_minute_ago))
            row = cursor.fetchone()
            return row['count'] if self.use_postgres else row[0]
    
    # =========================================================================
    # Email Log Operations
    # =========================================================================
    
    def log_email(
        self,
        recipient_email: str,
        subject: str,
        email_type: str,
        recipient_id: Optional[int] = None
    ) -> int:
        """Log an email before sending."""
        with self.get_connection() as conn:
            cursor = self._get_cursor(conn)
            p = self._placeholder()
            cursor.execute(f"""
                INSERT INTO email_logs (recipient_id, recipient_email, subject, email_type)
                VALUES ({p}, {p}, {p}, {p})
            """, (recipient_id, recipient_email, subject, email_type))
            
            if self.use_postgres:
                cursor.execute("SELECT lastval()")
                return cursor.fetchone()[0]
            else:
                return cursor.lastrowid
    
    def update_email_status(
        self,
        email_log_id: int,
        status: str,
        error_message: Optional[str] = None
    ) -> bool:
        """Update the status of an email log."""
        with self.get_connection() as conn:
            cursor = self._get_cursor(conn)
            p = self._placeholder()
            cursor.execute(f"""
                UPDATE email_logs 
                SET status = {p}, sent_at = CURRENT_TIMESTAMP, error_message = {p}
                WHERE id = {p}
            """, (status, error_message, email_log_id))
            return cursor.rowcount > 0
    
    # =========================================================================
    # Ride Auto-Cleanup Operations
    # =========================================================================
    
    def mark_expired_rides_inactive(self) -> int:
        """
        Mark rides as 'expired' if they are past their scheduled departure time by 30 minutes.
        Only affects 'active' or 'full' rides.
        
        Returns:
            Number of rides marked as expired.
        """
        with self.get_connection() as conn:
            cursor = self._get_cursor(conn)
            now = datetime.now()
            p = self._placeholder()
            
            if self.use_postgres:
                cursor.execute(f"""
                    UPDATE rides 
                    SET status = 'expired', 
                        updated_at = CURRENT_TIMESTAMP
                    WHERE status IN ('active', 'full')
                    AND (departure_date || ' ' || departure_time)::timestamp + interval '30 minutes' < {p}::timestamp
                """, (now,))
            else:
                cursor.execute(f"""
                    UPDATE rides 
                    SET status = 'expired', 
                        updated_at = CURRENT_TIMESTAMP
                    WHERE status IN ('active', 'full')
                    AND datetime(departure_date || ' ' || departure_time, '+30 minutes') < datetime({p})
                """, (now.isoformat(),))
            
            return cursor.rowcount
    
    def delete_old_expired_rides(self) -> Dict[str, Any]:
        """
        Delete rides that have been expired for more than 1 day.
        Preserves ride counts by storing driver statistics before deletion.
        
        Returns:
            Dictionary with count of deleted rides and affected drivers.
        """
        with self.get_connection() as conn:
            cursor = self._get_cursor(conn)
            p = self._placeholder()
            
            # Get rides to delete (expired for more than 1 day)
            one_day_ago = datetime.now() - timedelta(days=1)
            
            cursor.execute(f"""
                SELECT id, driver_id 
                FROM rides 
                WHERE status = 'expired' 
                AND updated_at < {p}
            """, (one_day_ago,))
            
            rides_to_delete = cursor.fetchall()
            
            if not rides_to_delete:
                return {'deleted_rides': 0, 'affected_drivers': []}
            
            ride_ids = [r['id'] for r in rides_to_delete]
            driver_ids = list(set(r['driver_id'] for r in rides_to_delete))
            
            # Delete associated records first (due to foreign key constraints)
            if self.use_postgres:
                cursor.execute(f"DELETE FROM bookings WHERE ride_id = ANY({p})", (ride_ids,))
                cursor.execute(f"DELETE FROM messages WHERE ride_id = ANY({p})", (ride_ids,))
                cursor.execute(f"DELETE FROM reviews WHERE ride_id = ANY({p})", (ride_ids,))
                cursor.execute(f"DELETE FROM rides WHERE id = ANY({p})", (ride_ids,))
            else:
                placeholders = ','.join(['?'] * len(ride_ids))
                cursor.execute(f"DELETE FROM bookings WHERE ride_id IN ({placeholders})", ride_ids)
                cursor.execute(f"DELETE FROM messages WHERE ride_id IN ({placeholders})", ride_ids)
                cursor.execute(f"DELETE FROM reviews WHERE ride_id IN ({placeholders})", ride_ids)
                cursor.execute(f"DELETE FROM rides WHERE id IN ({placeholders})", ride_ids)
            
            deleted_count = cursor.rowcount
            
            return {
                'deleted_rides': deleted_count,
                'affected_drivers': driver_ids
            }
    
    def run_ride_cleanup(self) -> Dict[str, Any]:
        """
        Run the full ride cleanup process:
        1. Mark expired rides as 'expired'
        2. Delete rides that have been expired for > 1 day
        
        Returns:
            Summary of cleanup operations.
        """
        marked_count = self.mark_expired_rides_inactive()
        delete_result = self.delete_old_expired_rides()
        
        return {
            'marked_expired': marked_count,
            'deleted_rides': delete_result['deleted_rides'],
            'affected_drivers': delete_result['affected_drivers']
        }
    
    # =========================================================================
    # Statistics Operations (for admin and chatbot)
    # =========================================================================
    
    def get_platform_statistics(self) -> Dict[str, Any]:
        """Get platform-wide statistics."""
        with self.get_connection() as conn:
            cursor = self._get_cursor(conn)
            
            stats = {}
            
            # Total users
            cursor.execute("SELECT COUNT(*) as count FROM users WHERE is_active = 1")
            row = cursor.fetchone()
            stats['total_users'] = row['count'] if self.use_postgres else row[0]
            
            # Total drivers
            cursor.execute("SELECT COUNT(*) as count FROM users WHERE is_driver = 1 AND is_active = 1")
            row = cursor.fetchone()
            stats['total_drivers'] = row['count'] if self.use_postgres else row[0]
            
            # Total rides
            cursor.execute("SELECT COUNT(*) as count FROM rides")
            row = cursor.fetchone()
            stats['total_rides'] = row['count'] if self.use_postgres else row[0]
            
            # Active rides
            cursor.execute("SELECT COUNT(*) as count FROM rides WHERE status = 'active'")
            row = cursor.fetchone()
            stats['active_rides'] = row['count'] if self.use_postgres else row[0]
            
            # Completed rides
            cursor.execute("SELECT COUNT(*) as count FROM rides WHERE status = 'completed'")
            row = cursor.fetchone()
            stats['completed_rides'] = row['count'] if self.use_postgres else row[0]
            
            # Total bookings
            cursor.execute("SELECT COUNT(*) as count FROM bookings")
            row = cursor.fetchone()
            stats['total_bookings'] = row['count'] if self.use_postgres else row[0]
            
            # Confirmed bookings
            cursor.execute("SELECT COUNT(*) as count FROM bookings WHERE status = 'confirmed'")
            row = cursor.fetchone()
            stats['confirmed_bookings'] = row['count'] if self.use_postgres else row[0]
            
            # Completed bookings
            cursor.execute("SELECT COUNT(*) as count FROM bookings WHERE status = 'completed'")
            row = cursor.fetchone()
            stats['completed_bookings'] = row['count'] if self.use_postgres else row[0]
            
            # Average price per seat
            cursor.execute("SELECT AVG(price_per_seat) as avg_price FROM rides WHERE status != 'cancelled'")
            row = cursor.fetchone()
            stats['average_price'] = round(float(row['avg_price']), 2) if row['avg_price'] else 0
            
            # Pending reports
            cursor.execute("SELECT COUNT(*) as count FROM user_reports WHERE status = 'pending'")
            row = cursor.fetchone()
            stats['pending_reports'] = row['count'] if self.use_postgres else row[0]
            
            return stats

    # =========================================================================
    # API Token Operations (for persistent authentication)
    # =========================================================================
    
    def create_api_token(self, token: str, user_id: int, expires_at: datetime) -> int:
        """
        Store an API token in the database.
        
        Args:
            token: The token string
            user_id: The user ID this token belongs to
            expires_at: When the token expires
            
        Returns:
            The ID of the created token record
        """
        with self.get_connection() as conn:
            cursor = self._get_cursor(conn)
            p = self._placeholder()
            cursor.execute(f"""
                INSERT INTO api_tokens (token, user_id, expires_at)
                VALUES ({p}, {p}, {p})
            """, (token, user_id, expires_at))

            if self.use_postgres:
                cursor.execute("SELECT lastval()")
                row = cursor.fetchone()
                if row is None:
                    return None
                # psycopg2 returns tuple, not dict
                return row[0] if isinstance(row, tuple) else row.get('id', None)
            else:
                return cursor.lastrowid
    
    def get_user_by_api_token(self, token: str) -> Optional[Dict[str, Any]]:
        """
        Get the user associated with an API token if valid.
        
        Returns:
            User dict if token is valid and not expired, None otherwise
        """
        with self.get_connection() as conn:
            cursor = self._get_cursor(conn)
            p = self._placeholder()
            
            if self.use_postgres:
                cursor.execute(f"""
                    SELECT u.* FROM users u
                    INNER JOIN api_tokens t ON u.id = t.user_id
                    WHERE t.token = {p} AND t.expires_at > CURRENT_TIMESTAMP
                """, (token,))
            else:
                cursor.execute(f"""
                    SELECT u.* FROM users u
                    INNER JOIN api_tokens t ON u.id = t.user_id
                    WHERE t.token = {p} AND t.expires_at > datetime('now')
                """, (token,))
            
            row = cursor.fetchone()
            return dict(row) if row else None
    
    def delete_api_token(self, token: str) -> bool:
        """Delete a specific API token (for logout)."""
        with self.get_connection() as conn:
            cursor = self._get_cursor(conn)
            p = self._placeholder()
            cursor.execute(f"DELETE FROM api_tokens WHERE token = {p}", (token,))
            return cursor.rowcount > 0
    
    def delete_user_tokens(self, user_id: int) -> int:
        """Delete all API tokens for a user (for logout all devices)."""
        with self.get_connection() as conn:
            cursor = self._get_cursor(conn)
            p = self._placeholder()
            cursor.execute(f"DELETE FROM api_tokens WHERE user_id = {p}", (user_id,))
            return cursor.rowcount
    
    def cleanup_expired_tokens(self) -> int:
        """Remove all expired tokens from the database."""
        with self.get_connection() as conn:
            cursor = self._get_cursor(conn)
            
            if self.use_postgres:
                cursor.execute("DELETE FROM api_tokens WHERE expires_at < CURRENT_TIMESTAMP")
            else:
                cursor.execute("DELETE FROM api_tokens WHERE expires_at < datetime('now')")
            
            return cursor.rowcount


# Global database instance
db = Database()
