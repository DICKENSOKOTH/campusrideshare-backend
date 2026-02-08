"""
Simple server starter script.
Run this from command line: python start_server.py
"""
import sys
import os

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

if __name__ == '__main__':
    print("=" * 60)
    print("          Campus Ride-Share Backend Server")
    print("=" * 60)
    print()
    print("Loading application...")
    
    from app import app
    
    print("Application loaded successfully!")
    print()
    print("Server URL:     http://127.0.0.1:5000")
    print("API Base:       http://127.0.0.1:5000/api/")
    print()
    print("Test endpoints:")
    print("  GET  /api/stats    - Platform statistics")
    print("  POST /api/login    - User login")
    print("  POST /api/register - User registration")
    print()
    print("=" * 60)
    print("Press CTRL+C to stop the server")
    print("=" * 60)
    print()
    
    # Run without reloader to avoid issues on Windows
    app.run(
        host='127.0.0.1',
        port=5000,
        debug=False,
        use_reloader=False,
        threaded=True
    )
