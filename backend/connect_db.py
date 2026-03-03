#!/usr/bin/env python3
"""
DB setup script: MySQL (XAMPP)
Ensures the MySQL database and tables exist (creates them if missing).
"""

import os
import sys
from pathlib import Path
from urllib.parse import urlparse

# Add backend directory to path
sys.path.insert(0, str(Path(__file__).parent))

from dotenv import load_dotenv

# Load .env
# IMPORTANT: In Docker, environment variables should win.
# Set DOTENV_OVERRIDE=1 only if you explicitly want backend/.env to override existing env vars.
_override = (os.getenv('DOTENV_OVERRIDE') or '').strip().lower() in ('1', 'true', 'yes', 'on')
load_dotenv(os.path.join(os.path.dirname(__file__), '.env'), override=_override)

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import OperationalError

def check_and_create_mysql_db():
    """Kiểm tra và tạo database MySQL"""
    mysql_url = os.getenv('DATABASE_URL', 'mysql+pymysql://root:@localhost:3306/ai_translation')
    
    # Parse URL
    parsed = urlparse(mysql_url)
    user = parsed.username or 'root'
    password = parsed.password or ''
    host = parsed.hostname or 'localhost'
    port = parsed.port or 3306
    db_name = parsed.path.lstrip('/') or 'ai_translation'
    
    print(f"🔍 Checking MySQL connection...")
    print(f"   Host: {host}:{port}")
    print(f"   User: {user}")
    print(f"   Database: {db_name}")
    
    # Connect to MySQL server (without database)
    try:
        server_engine = create_engine(
            f"mysql+pymysql://{user}:{password}@{host}:{port}/",
            echo=False
        )
        with server_engine.connect() as conn:
            # Check if database exists
            result = conn.execute(text(f"SHOW DATABASES LIKE '{db_name}'"))
            if not result.fetchone():
                print(f"📝 Creating database '{db_name}'...")
                conn.execute(text(f"CREATE DATABASE {db_name} DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"))
                conn.commit()
                print(f"✅ Database '{db_name}' created")
            else:
                print(f"✅ Database '{db_name}' already exists")
        return True
    except OperationalError as e:
        print(f"❌ MySQL connection failed: {e}")
        print("   Make sure XAMPP MySQL is running on localhost:3306")
        print("   Check .env: DATABASE_URL should be 'mysql+pymysql://root:@localhost:3306/ai_translation'")
        return False

def create_tables():
    """Tạo bảng trong MySQL"""
    print(f"\n📦 Creating tables...")
    try:
        from app import create_app
        app = create_app()
        with app.app_context():
            from app.models import db
            db.create_all()
            print("✅ Tables created successfully")
        return True
    except Exception as e:
        print(f"❌ Failed to create tables: {e}")
        return False

def main():
    print("=" * 60)
    print("🚀 MySQL DB setup (XAMPP)")
    print("=" * 60)
    
    # Step 1: Check/Create database
    if not check_and_create_mysql_db():
        return False
    
    # Step 2: Create tables
    if not create_tables():
        return False
    
    print("\n" + "=" * 60)
    print("✨ Database and tables are ready!")
    print("=" * 60)
    print("\nNext steps:")
    print("  1. Run: python run.py")
    print("  2. Access: http://127.0.0.1:5000")
    print("  3. Check MySQL: SHOW TABLES IN ai_translation;")
    
    return True

if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)
