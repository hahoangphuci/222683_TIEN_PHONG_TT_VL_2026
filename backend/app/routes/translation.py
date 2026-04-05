from flask import Blueprint, request, jsonify, current_app
from flask_jwt_extended import jwt_required, get_jwt_identity
from app.models import db, Translation, User
from app.services.translation_service import TranslationService
from werkzeug.utils import secure_filename
import os
import uuid

translation_bp = Blueprint('translation', __name__)
translation_service = None


def _get_translation_service():
    global translation_service
    if translation_service is None:
        translation_service = TranslationService()
    return translation_service

UPLOAD_FOLDER = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'uploads')
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

@translation_bp.route('/text', methods=['POST'])
@jwt_required(optional=True)
def translate_text():
    svc = _get_translation_service()
    data = request.json
    text = data.get('text')
    source_lang = data.get('source_lang', 'auto')
    target_lang = data.get('target_lang')

    if not text or not str(text).strip():
        return jsonify({"error": "No text provided"}), 400
    if not target_lang or not str(target_lang).strip():
        return jsonify({"error": "target_lang is required"}), 400

    user_id = get_jwt_identity()
    user = User.query.filter_by(google_id=user_id).first() if user_id else None

    # Enforce daily quota if user exists
    if user:
        from datetime import datetime
        today = datetime.utcnow().date()
        try:
            used_today = Translation.query.filter(
                Translation.user_id == user.id,
                db.func.date(Translation.created_at) == today
            ).count()
        except Exception:
            used_today = 0
        plan = (user.plan or 'free')
        plan_quota = {'free': 170, 'pro': 4000, 'promax': 10000}.get(plan, 170)
        if plan_quota > 0 and used_today >= plan_quota:
            return jsonify({"error": "Quota exceeded for today", "quota": plan_quota}), 402

    is_html = data.get('is_html', False)

    try:
        if is_html:
            translated_text = svc.translate_html(text, source_lang, target_lang)
        else:
            translated_text = svc.translate_text(text, source_lang, target_lang)
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except RuntimeError as e:
        return jsonify({"error": str(e)}), 503

    translation = Translation(
        user_id=user.id if user else None,
        original_text=(text[:5000] + '...') if len(text) > 5000 else text,
        translated_text=(translated_text[:5000] + '...') if len(translated_text) > 5000 else translated_text,
        source_lang=source_lang,
        target_lang=target_lang
    )
    db.session.add(translation)
    db.session.commit()

    return jsonify({"translated_text": translated_text, "is_html": bool(is_html)}), 200

@translation_bp.route('/document', methods=['POST'])
@jwt_required(optional=True)
def translate_document():
    svc = _get_translation_service()
    if 'file' not in request.files:
        return jsonify({"error": "No file provided"}), 400
    
    file = request.files['file']
    target_lang = request.form.get('target_lang')

    if not target_lang or not str(target_lang).strip():
        return jsonify({"error": "target_lang is required"}), 400
    
    if file.filename == '':
        return jsonify({"error": "No file selected"}), 400
    
    filename = secure_filename(file.filename)
    ext = os.path.splitext(filename)[1].lower()
    filepath = os.path.join(UPLOAD_FOLDER, filename)
    file.save(filepath)
    
    user_id = get_jwt_identity()
    user = User.query.filter_by(google_id=user_id).first() if user_id else None

    # Create DB record indicating processing started
    translation = Translation(
        user_id=user.id if user else None,
        original_text=f"File: {filename}",
        translated_text=f"Processing file...",
        source_lang='auto',
        target_lang=target_lang,
        file_path=filepath
    )
    db.session.add(translation)
    db.session.commit()

    # Extract optional bilingual / OCR parameters sent by the frontend
    bilingual_mode = (request.form.get('bilingual_mode') or '').strip() or None
    bilingual_delimiter = (request.form.get('bilingual_delimiter') or '').strip() or None
    ocr_images = (request.form.get('ocr_images') or '').strip().lower() in ('1', 'true', 'yes', 'on')
    ocr_mode = (request.form.get('ocr_mode') or '').strip() or None
    ocr_langs = (request.form.get('ocr_langs') or '').strip() or None
    print(f"[translate_document] bilingual_mode={bilingual_mode!r}, bilingual_delimiter={bilingual_delimiter!r}, ext={ext}")

    # Start background job
    job_id = svc.translate_document_background(
        filepath, target_lang, user_id=user_id,
        ocr_images=ocr_images, ocr_langs=ocr_langs, ocr_mode=ocr_mode,
        bilingual_mode=bilingual_mode, bilingual_delimiter=bilingual_delimiter,
    )

    return jsonify({"job_id": job_id, "status_url": f"/api/translation/document/status/{job_id}"}), 202


@translation_bp.route('/image', methods=['POST'])
@jwt_required(optional=True)
def translate_image():
    """Translate text from an uploaded image using OCR."""
    svc = _get_translation_service()
    if 'file' not in request.files:
        return jsonify({"error": "No file provided"}), 400

    file = request.files['file']
    if not file or not file.filename:
        return jsonify({"error": "No file selected"}), 400

    source_lang = (request.form.get('source_lang') or 'auto').strip() or 'auto'
    target_lang = (request.form.get('target_lang') or '').strip()
    ocr_langs = (request.form.get('ocr_langs') or '').strip() or None
    render_overlay = (request.form.get('render') or request.form.get('render_overlay') or '').strip().lower() in ('1', 'true', 'yes', 'on')

    if not target_lang:
        return jsonify({"error": "target_lang is required"}), 400

    # Enforce daily quota if user exists
    user_id = get_jwt_identity()
    user = User.query.filter_by(google_id=user_id).first() if user_id else None
    if user:
        from datetime import datetime
        today = datetime.utcnow().date()
        try:
            used_today = Translation.query.filter(
                Translation.user_id == user.id,
                db.func.date(Translation.created_at) == today
            ).count()
        except Exception:
            used_today = 0
        plan = (user.plan or 'free')
        plan_quota = {'free': 170, 'pro': 4000, 'promax': 10000}.get(plan, 170)
        if plan_quota > 0 and used_today >= plan_quota:
            return jsonify({"error": "Quota exceeded for today", "quota": plan_quota}), 402

    # Basic extension allowlist
    filename = secure_filename(file.filename)
    ext = os.path.splitext(filename)[1].lower()
    allowed = {'.png', '.jpg', '.jpeg', '.bmp', '.tif', '.tiff', '.webp'}
    if ext not in allowed:
        return jsonify({"error": "Unsupported image type. Use png/jpg/jpeg/bmp/tiff/webp."}), 400

    # Save to uploads with unique prefix
    unique_name = f"{uuid.uuid4().hex}_{filename}"
    filepath = os.path.join(UPLOAD_FOLDER, unique_name)
    file.save(filepath)

    try:
        rendered_image_data_url = None

        if render_overlay:
            ocr_text, translated_text, png_bytes = svc.ocr_translate_overlay(
                filepath,
                source_lang=source_lang,
                target_lang=target_lang,
                ocr_langs=ocr_langs,
            )
            if not ocr_text or not str(ocr_text).strip():
                return jsonify({"error": "OCR found no text in the image."}), 400
            import base64
            rendered_image_data_url = "data:image/png;base64," + base64.b64encode(png_bytes).decode('ascii')
        else:
            ocr_text = svc.ocr_image_to_text(filepath, ocr_langs=ocr_langs)
            if not ocr_text or not str(ocr_text).strip():
                return jsonify({"error": "OCR found no text in the image."}), 400
            translated_text = svc.translate_text(ocr_text, source_lang, target_lang)
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except RuntimeError as e:
        return jsonify({"error": str(e)}), 503
    finally:
        # Best-effort cleanup
        try:
            os.remove(filepath)
        except Exception:
            pass

    # Persist translation history
    translation = Translation(
        user_id=user.id if user else None,
        original_text=(ocr_text[:5000] + '...') if len(ocr_text) > 5000 else ocr_text,
        translated_text=(translated_text[:5000] + '...') if len(translated_text) > 5000 else translated_text,
        source_lang=source_lang,
        target_lang=target_lang
    )
    db.session.add(translation)
    db.session.commit()

    payload = {
        "ocr_text": ocr_text,
        "translated_text": translated_text,
        "is_html": False
    }
    if render_overlay:
        payload["rendered_image"] = rendered_image_data_url
    return jsonify(payload), 200

@translation_bp.route('/document/status/<job_id>', methods=['GET'])
@jwt_required(optional=True)
def document_status(job_id):
    svc = _get_translation_service()
    job = svc.get_job(job_id)
    if not job:
        return jsonify({"error": "Job not found"}), 404
    # When completed, include download_url
    download_url = None
    if job.get('status') == 'completed' and job.get('download_path'):
        download_url = f"/downloads/{os.path.basename(job.get('download_path'))}"
    return jsonify({
        'job_id': job_id,
        'status': job.get('status'),
        'progress': job.get('progress'),
        'message': job.get('message'),
        'download_url': download_url,
        'error': job.get('error'),
        'fallback': job.get('fallback', False),
        'fallback_reason': job.get('fallback_reason')
    }), 200


@translation_bp.route('/save', methods=['POST'])
@jwt_required()
def save_translation():
    """Explicitly save a translation from the dashboard UI.

    Note: /text already persists translations; this endpoint exists to keep the
    dashboard "Lưu" button functional and supports idempotent re-saves.
    """
    data = request.get_json(silent=True) or {}
    original_text = data.get('original_text')
    translated_text = data.get('translated_text')
    source_lang = (data.get('source_lang') or 'auto').strip() if data.get('source_lang') else 'auto'
    target_lang = data.get('target_lang')

    if not original_text or not str(original_text).strip():
        return jsonify({'error': 'original_text is required'}), 400
    if not translated_text or not str(translated_text).strip():
        return jsonify({'error': 'translated_text is required'}), 400
    if not target_lang or not str(target_lang).strip():
        return jsonify({'error': 'target_lang is required'}), 400

    user_google_id = get_jwt_identity()
    user = User.query.filter_by(google_id=user_google_id).first()
    if not user:
        return jsonify({'error': 'User not found'}), 404

    # Best-effort idempotency: if the same record was saved very recently, reuse it.
    from datetime import datetime, timedelta
    window_start = datetime.utcnow() - timedelta(minutes=2)
    existing = Translation.query.filter(
        Translation.user_id == user.id,
        Translation.source_lang == source_lang,
        Translation.target_lang == target_lang,
        Translation.created_at >= window_start,
        Translation.original_text == original_text,
        Translation.translated_text == translated_text,
    ).order_by(Translation.created_at.desc()).first()
    if existing:
        return jsonify({'message': 'already_saved', 'id': existing.id}), 200

    translation = Translation(
        user_id=user.id,
        original_text=(str(original_text)[:5000] + '...') if len(str(original_text)) > 5000 else str(original_text),
        translated_text=(str(translated_text)[:5000] + '...') if len(str(translated_text)) > 5000 else str(translated_text),
        source_lang=source_lang,
        target_lang=str(target_lang).strip(),
    )
    db.session.add(translation)
    db.session.commit()
    return jsonify({'message': 'saved', 'id': translation.id}), 201


@translation_bp.route('/history', methods=['GET'])
@jwt_required(optional=True)
def get_history():
    user_google_id = get_jwt_identity()
    user = User.query.filter_by(google_id=user_google_id).first() if user_google_id else None
    if not user:
        return jsonify({'translations': [], 'total': 0, 'pages': 0, 'current_page': 1}), 200

    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 10, type=int)
    filter_type = (request.args.get('type') or 'all').strip().lower()
    date_str = (request.args.get('date') or '').strip()

    q = Translation.query.filter(Translation.user_id == user.id)

    # Filter by type (text/document)
    if filter_type == 'text':
        q = q.filter(Translation.file_path.is_(None))
    elif filter_type == 'document':
        q = q.filter(Translation.file_path.isnot(None))

    # Optional date filter: YYYY-MM-DD (UTC date)
    if date_str:
        from datetime import datetime
        try:
            d = datetime.strptime(date_str, '%Y-%m-%d').date()
            q = q.filter(db.func.date(Translation.created_at) == d)
        except ValueError:
            return jsonify({'error': 'Invalid date format. Use YYYY-MM-DD'}), 400

    q = q.order_by(Translation.created_at.desc())
    pagination = q.paginate(page=page, per_page=per_page, error_out=False)

    history = [{
        'id': t.id,
        'original_text': t.original_text,
        'translated_text': t.translated_text,
        'source_lang': t.source_lang,
        'target_lang': t.target_lang,
        'created_at': t.created_at.isoformat(),
        'has_file': bool(t.file_path)
    } for t in pagination.items]

    return jsonify({
        'translations': history,
        'total': pagination.total,
        'pages': pagination.pages,
        'current_page': pagination.page,
    }), 200