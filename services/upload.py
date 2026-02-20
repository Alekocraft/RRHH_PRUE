\
import os
import re
from datetime import datetime
from werkzeug.utils import secure_filename

UPLOAD_FOLDER = os.path.join(os.getcwd(), "uploads")

def ensure_upload_folder():
    os.makedirs(UPLOAD_FOLDER, exist_ok=True)

def _safe_prefix(prefix: str) -> str:
    prefix = (prefix or "").strip().lower()
    prefix = re.sub(r"[^a-z0-9_-]+", "-", prefix)
    return prefix or "file"

def save_upload(file_storage, prefix: str = "upload") -> dict:
    """
    Guarda el archivo en /uploads y retorna metadata:
    {file_name, storage_path, size_bytes}
    """
    ensure_upload_folder()
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = secure_filename(file_storage.filename or "archivo")
    prefix = _safe_prefix(prefix)
    final_name = f"{prefix}_{ts}_{filename}"
    path = os.path.join(UPLOAD_FOLDER, final_name)
    file_storage.save(path)
    size_bytes = os.path.getsize(path)
    return {"file_name": final_name, "storage_path": path, "size_bytes": size_bytes}
