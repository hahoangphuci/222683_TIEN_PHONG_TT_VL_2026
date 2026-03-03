import os
from dotenv import load_dotenv

# Load .env từ thư mục backend (config.py nằm trong backend/)
# IMPORTANT: In Docker, environment variables (e.g. DATABASE_URL) should win.
# Set DOTENV_OVERRIDE=1 only if you explicitly want backend/.env to override existing env vars.
_override = (os.getenv('DOTENV_OVERRIDE') or '').strip().lower() in ('1', 'true', 'yes', 'on')
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env'), override=_override)

class Config:
    SECRET_KEY = os.getenv('SECRET_KEY', 'dev-secret-key')
    JWT_SECRET_KEY = os.getenv('JWT_SECRET_KEY', 'jwt-secret-key')
    
    # Database
    SQLALCHEMY_DATABASE_URI = os.getenv('DATABASE_URL', 'mysql+pymysql://root:@localhost:3306/ai_translation')
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    
    # Google OAuth
    GOOGLE_CLIENT_ID = os.getenv('GOOGLE_CLIENT_ID')
    GOOGLE_CLIENT_SECRET = os.getenv('GOOGLE_CLIENT_SECRET')
    GOOGLE_REDIRECT_URI = os.getenv('GOOGLE_REDIRECT_URI', 'http://127.0.0.1:5000/api/auth/google/callback')
    
    # AI Services
    OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
    DEEPL_API_KEY = os.getenv('DEEPL_API_KEY')

    # OpenRouter / AI Integration
    OPENROUTER_API_KEY = os.getenv('OPENROUTER_API_KEY')
    AI_PROVIDER = os.getenv('AI_PROVIDER', 'openrouter')
    AI_MODEL = os.getenv('AI_MODEL', 'nvidia/nemotron-3-nano-30b-a3b:free')
    AI_VISION_MODEL = os.getenv('AI_VISION_MODEL', '')  # Vision-capable model for OCR (auto-detect if empty)
    AI_AVAILABLE_MODELS = os.getenv('AI_AVAILABLE_MODELS', '')
    AI_ENDPOINTS = {
        'chat': os.getenv('AI_ENDPOINT_CHAT'),
        'models': os.getenv('AI_ENDPOINT_MODELS')
    }
    AI_HEADERS = {
        'HTTP-Referer': os.getenv('AI_HEADER_HTTP_REFERER'),
        'X-Title': os.getenv('AI_HEADER_X_TITLE')
    }
    
    # Payment
    SEPAY_API_KEY = os.getenv('SEPAY_API_KEY')
    SEPAY_SECRET = os.getenv('SEPAY_SECRET')
    
    # File Upload
    UPLOAD_FOLDER = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'uploads')
    DOWNLOAD_FOLDER = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'downloads')
    MAX_CONTENT_LENGTH = 50 * 1024 * 1024  # 50MB
    
    # Frontend URL
    FRONTEND_URL = os.getenv('FRONTEND_URL', 'http://localhost:3000')

class DevelopmentConfig(Config):
    DEBUG = True

class ProductionConfig(Config):
    DEBUG = False