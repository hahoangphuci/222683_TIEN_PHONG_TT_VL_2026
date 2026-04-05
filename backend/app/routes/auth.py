from flask import Blueprint, request, jsonify, redirect
from flask_jwt_extended import create_access_token, jwt_required, get_jwt_identity
from app.models import db, User, Translation
import google.auth.transport.requests
import google.oauth2.id_token
import google.oauth2.service_account
import os
import secrets
from pathlib import Path
from urllib.parse import urlencode

from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired

auth_bp = Blueprint('auth', __name__)


def _backend_dotenv_path() -> Path:
    # app/routes/auth.py → parents[2] = backend/
    return Path(__file__).resolve().parents[2] / ".env"


def _google_oauth_credentials():
    """Lấy Client ID/Secret tin cậy cho mọi request.

    Tránh lệ thuộc giá trị chụp một lần trên class `Config` (import sớm) hoặc biến môi trường
    hệ thống rỗng chặn dotenv: đọc trực tiếp `backend/.env` nếu cần.
    """
    from flask import current_app
    from dotenv import dotenv_values

    cid = (
        (os.getenv("GOOGLE_CLIENT_ID") or "").strip()
        or (current_app.config.get("GOOGLE_CLIENT_ID") or "").strip()
    )
    sec = (
        (os.getenv("GOOGLE_CLIENT_SECRET") or "").strip()
        or (current_app.config.get("GOOGLE_CLIENT_SECRET") or "").strip()
    )
    if cid and sec:
        return cid, sec

    vals = {}
    p = _backend_dotenv_path()
    if p.is_file():
        try:
            vals = dotenv_values(p) or {}
        except Exception:
            vals = {}
    cid = (vals.get("GOOGLE_CLIENT_ID") or cid or "").strip()
    sec = (vals.get("GOOGLE_CLIENT_SECRET") or sec or "").strip()
    return cid, sec

_OAUTH_STATE_SALT = "google-oauth-state-v1"
_OAUTH_STATE_MAX_AGE = 900  # 15 phút


def _oauth_state_sign(secret: str) -> str:
    ser = URLSafeTimedSerializer(secret, salt=_OAUTH_STATE_SALT)
    return ser.dumps({"n": secrets.token_urlsafe(24)})


def _oauth_state_verify(secret: str, state: str) -> bool:
    if not state or not isinstance(state, str):
        return False
    ser = URLSafeTimedSerializer(secret, salt=_OAUTH_STATE_SALT)
    try:
        ser.loads(state, max_age=_OAUTH_STATE_MAX_AGE)
        return True
    except (BadSignature, SignatureExpired):
        return False

@auth_bp.route('/config', methods=['GET'])
def auth_config():
    from flask import current_app

    rid = current_app.config.get('GOOGLE_REDIRECT_URI') or ''
    cid, csec = _google_oauth_credentials()
    return jsonify({
        'google_client_id': cid or None,
        'google_redirect_uri': rid,
        # True chỉ khi có cả ID và secret (secret không gửi ra ngoài, chỉ cờ)
        'google_oauth_ready': bool(cid and csec),
        'oauth_console_hint': (
            'Google Cloud → APIs & Services → Credentials → OAuth 2.0 Client ID → '
            'thêm đúng URI vào "Authorized redirect URIs". '
            'Client secret chỉ đặt trong backend/.env (GOOGLE_CLIENT_SECRET), không để trống.'
        ),
    }), 200

@auth_bp.route('/google/authorize', methods=['POST', 'OPTIONS'])
def google_authorize():
    """Initiate Google OAuth flow - backend creates the authorization URL"""
    from flask import current_app

    if request.method == 'OPTIONS':
        return '', 204
    
    client_id, client_secret = _google_oauth_credentials()
    if not client_id:
        return jsonify({"error": "Google Client ID not configured"}), 500

    if not client_secret:
        return jsonify({
            "error": "missing_client_secret",
            "message": (
                "Chưa cấu hình GOOGLE_CLIENT_SECRET trong backend/.env. "
                "Google Cloud → Credentials → OAuth client (đúng Client ID) → Client secret → copy vào .env, rồi restart server."
            ),
        }), 500
    
    # State ký bằng SECRET_KEY — không phụ thuộc cookie session (tránh localhost vs 127.0.0.1).
    sk = current_app.config.get('SECRET_KEY') or ''
    if not sk:
        return jsonify({"error": "SECRET_KEY not configured"}), 500
    state = _oauth_state_sign(str(sk))
    
    # Use configured redirect URI to avoid Google blocking for private IPs
    redirect_uri = current_app.config.get('GOOGLE_REDIRECT_URI') or f"{request.scheme}://{request.host}/api/auth/google/callback"
    
    print(f"[DEBUG] Building auth URL:")
    print(f"[DEBUG]   Client ID: {client_id[:20]}...")
    print(f"[DEBUG]   Redirect URI: {redirect_uri}")

    auth_url = (
        'https://accounts.google.com/o/oauth2/v2/auth?'
        + urlencode({
            'client_id': client_id,
            'redirect_uri': redirect_uri,
            'response_type': 'code',
            'scope': 'openid email profile',
            'state': state,
        })
    )
    
    print(f"[DEBUG] Full Auth URL: {auth_url[:150]}...")
    return jsonify({'auth_url': auth_url}), 200

@auth_bp.route('/google/callback', methods=['GET'])
def google_callback():
    """Handle Google OAuth callback - exchange code for tokens"""
    from flask import current_app
    import requests
    
    code = request.args.get('code')
    state = request.args.get('state')
    error = request.args.get('error')
    
    print(f"[DEBUG] Callback received: code={code[:20] if code else 'None'}, state={state}, error={error}")
    
    if error:
        return redirect(f'/auth?error={error}')
    
    if not code:
        print(f"[WARN] OAuth callback without code; args={dict(request.args)}")
        return redirect('/auth?error=missing_code')
    
    sk = str(current_app.config.get('SECRET_KEY') or '')
    if not _oauth_state_verify(sk, state or ''):
        print(f"[ERROR] OAuth state invalid or expired (signed state check failed)")
        return redirect('/auth?error=state_mismatch')
    
    client_id, client_secret = _google_oauth_credentials()
    if not client_id or not client_secret:
        print(
            f"[ERROR] OAuth missing_credentials id_set={bool(client_id)} "
            f"secret_set={bool(client_secret)}"
        )
        return redirect('/auth?error=missing_credentials')
    
    # Exchange code for tokens using configured redirect URI
    redirect_uri = current_app.config.get('GOOGLE_REDIRECT_URI') or f"{request.scheme}://{request.host}/api/auth/google/callback"
    
    print(f"[DEBUG] Token exchange:")
    print(f"[DEBUG]   Code: {code[:20] if code else 'None'}...")
    print(f"[DEBUG]   Redirect URI: {redirect_uri}")
    
    token_url = 'https://oauth2.googleapis.com/token'
    
    try:
        response = requests.post(token_url, data={
            'client_id': client_id,
            'client_secret': client_secret,
            'code': code,
            'grant_type': 'authorization_code',
            'redirect_uri': redirect_uri
        })
        
        token_data = response.json()
        print(f"[DEBUG] Token response status: {response.status_code}")
        
        if response.status_code != 200:
            print(f"[ERROR] Token exchange failed: {token_data}")
            return redirect(f'/auth?error=token_exchange_failed')
        
        id_token = token_data.get('id_token')
        if not id_token:
            print(f"[ERROR] No id_token in response")
            return redirect('/auth?error=no_id_token')
        
        # Verify and decode ID token
        try:
            idinfo = google.oauth2.id_token.verify_oauth2_token(
                id_token,
                google.auth.transport.requests.Request(),
                client_id
            )
            
            user_id = idinfo.get('sub')
            email = idinfo.get('email')
            name = idinfo.get('name')
            
            print(f"[DEBUG] Token verified for user: {user_id}, {email}")
            
            # Create or get user
            user = User.query.filter_by(google_id=user_id).first()
            if not user:
                print(f"[DEBUG] Creating new user: {user_id}")
                user = User(google_id=user_id, email=email, name=name)
                db.session.add(user)
                db.session.commit()
            
            # Create JWT for our app
            access_token = create_access_token(identity=user_id)
            print(f"[DEBUG] JWT created for user: {user_id}")
            
            # Redirect to dashboard with token
            return redirect(f'/dashboard?token={access_token}')
            
        except ValueError as e:
            print(f"[ERROR] ID token verification failed: {str(e)}")
            return redirect(f'/auth?error=token_verification_failed')
            
    except Exception as e:
        print(f"[ERROR] Token exchange exception: {str(e)}")
        import traceback
        traceback.print_exc()
        return redirect(f'/auth?error=server_error')

@auth_bp.route('/google', methods=['POST'])
def google_auth():
    try:
        data = request.get_json()
        if not data or not data.get('token'):
            return jsonify({"error": "Missing token"}), 400
        
        token = data.get('token')
        print(f"[DEBUG] Received token: {token[:50]}...")
        
        gcid, _ = _google_oauth_credentials()
        if not gcid:
            return jsonify({"error": "Google Client ID not configured"}), 500
        idinfo = google.oauth2.id_token.verify_oauth2_token(
            token,
            google.auth.transport.requests.Request(),
            gcid,
        )
        
        user_id = idinfo.get('sub')
        email = idinfo.get('email')
        name = idinfo.get('name')
        
        print(f"[DEBUG] Verified user: {user_id}, {email}, {name}")
        
        if not user_id or not email:
            return jsonify({"error": "Missing user info in token"}), 400
        
        user = User.query.filter_by(google_id=user_id).first()
        if not user:
            print(f"[DEBUG] Creating new user: {user_id}")
            user = User(google_id=user_id, email=email, name=name)
            db.session.add(user)
            db.session.commit()
        
        print(f"[DEBUG] Creating access token for user: {user_id}")
        access_token = create_access_token(identity=user_id)
        print(f"[DEBUG] Token created: {access_token[:50]}...")
        
        return jsonify(access_token=access_token), 200
        
    except ValueError as e:
        print(f"[ERROR] Token verification failed: {str(e)}")
        return jsonify({"error": f"Invalid token: {str(e)}"}), 400
    except Exception as e:
        print(f"[ERROR] Unexpected error in google_auth: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": f"Server error: {str(e)}"}), 500

@auth_bp.route('/profile', methods=['GET'])
@jwt_required()
def get_profile():
    user_id = get_jwt_identity()
    user = User.query.filter_by(google_id=user_id).first()
    if user:
        # Compute today's translation count
        from datetime import datetime, timedelta
        today = datetime.utcnow().date()
        count_today = 0
        try:
            count_today = Translation.query.filter(
                Translation.user_id == user.id,
                db.func.date(Translation.created_at) == today
            ).count()
        except Exception:
            count_today = 0

        # Determine plan info (daily quota is used for current enforcement/UI)
        # Pricing copy on Home is monthly; map to a reasonable daily quota approximation here.
        plan = user.plan or 'free'
        plan_info = {
            'free': {'name': 'Free', 'daily_quota': 170},      # ~5,000 words/month
            'pro': {'name': 'Pro', 'daily_quota': 4000},       # ~120,000 words/month
            'promax': {'name': 'ProMax', 'daily_quota': 10000} # ~300,000 words/month
        }.get(plan, {'name': plan, 'daily_quota': 170})

        return jsonify({
            'id': user.id,
            'email': user.email,
            'name': user.name,
            'avatar_url': getattr(user, 'avatar_url', None),
            'plan': plan,
            'plan_name': plan_info['name'],
            'daily_quota': plan_info['daily_quota'],
            'used_today': count_today
        }), 200
    return jsonify({"error": "User not found"}), 404


@auth_bp.route('/profile', methods=['PATCH'])
@jwt_required()
def update_profile():
    """Update mutable profile fields (currently: name)."""
    user_google_id = get_jwt_identity()
    user = User.query.filter_by(google_id=user_google_id).first()
    if not user:
        return jsonify({"error": "User not found"}), 404

    data = request.get_json(silent=True) or {}
    name = data.get('name')
    avatar_url = data.get('avatar_url')

    if name is not None:
        name = str(name).strip()
        if len(name) > 255:
            return jsonify({"error": "Name too long"}), 400
        user.name = name

    if avatar_url is not None:
        avatar_url = str(avatar_url).strip()
        if avatar_url and len(avatar_url) > 500:
            return jsonify({"error": "avatar_url too long"}), 400
        # Allow clearing by sending empty string
        user.avatar_url = avatar_url or None

    db.session.commit()

    return jsonify({
        'id': user.id,
        'email': user.email,
        'name': user.name,
        'avatar_url': getattr(user, 'avatar_url', None),
        'plan': user.plan or 'free',
    }), 200