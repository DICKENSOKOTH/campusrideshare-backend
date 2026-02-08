"""
Admin utility to reset a user's password.
Usage: python reset_password.py <email> <new_password>
"""
import sys
from database import db
from auth import hash_password

if len(sys.argv) != 3:
    print("Usage: python reset_password.py <email> <new_password>")
    print("Example: python reset_password.py user@example.com NewPassword123!")
    sys.exit(1)

email = sys.argv[1].lower()
new_password = sys.argv[2]

# Check if user exists
user = db.get_user_by_email(email)
if not user:
    print(f"Error: No user found with email '{email}'")
    sys.exit(1)

# Update password and ensure verified
with db.get_connection() as conn:
    cursor = conn.cursor()
    cursor.execute(
        'UPDATE users SET password_hash = ?, is_verified = 1 WHERE email = ?',
        (hash_password(new_password), email)
    )

print(f"Password reset successfully for {email}")
print(f"User can now login with the new password")
