import os
from dotenv import load_dotenv

 
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

from flask import Flask, redirect, url_for
from flask_login import LoginManager

from models.user import load_user
from blueprints.auth import auth_bp
from blueprints.admin_rrhh import admin_rrhh_bp
from blueprints.modulos import modulos_bp


def create_app():
    app = Flask(__name__)

 
    app.secret_key = os.getenv("SECRET_KEY", "dev-secret-change-me")

    login_manager = LoginManager()
    login_manager.login_view = "auth.login"
    login_manager.init_app(app)

    @login_manager.user_loader
    def _load(user_id):
        return load_user(user_id)

    app.register_blueprint(auth_bp)
    app.register_blueprint(admin_rrhh_bp)
    app.register_blueprint(modulos_bp)


    # -------------------------
    # Filtros de templates (UI)
    # -------------------------
 
    _STATUS_ES = {
        # Workflow (inglés)
        "DRAFT": "Borrador",
        "SUBMITTED": "Enviada",
        "PENDING": "Pendiente",
        "APPROVED": "Aprobada",
        "REJECTED": "Rechazada",
        "SKIPPED": "Omitida",
        "CANCELLED": "Cancelada",
        "CANCELED": "Cancelada",
        "APROBADO": "Aprobado",
        "APROBADA": "Aprobada",
        "RECHAZADO": "Rechazado",
        "RECHAZADA": "Rechazada",
        "PENDIENTE": "Pendiente",
        "CANCELADO": "Cancelado",
        "CANCELADA": "Cancelada",
    }

    @app.template_filter("estado")
    def estado_filter(value):
        """Traduce estados técnicos a etiquetas en español para la interfaz."""
        if value is None:
            return ""
        v = (f"{value}").strip()
        if not v:
            return ""
        up = v.upper()
        return _STATUS_ES.get(up, v)

    # Formato de fechas: evita microsegundos y reduce a lo que necesita el usuario
    # - fecha: YYYY-MM-DD
    # - fecha_hora: YYYY-MM-DD HH:MM
    import re as _re
    from datetime import datetime as _dt, date as _date

    def _to_dt(value):
        if value is None:
            return None
        if isinstance(value, _dt):
            return value
        if isinstance(value, _date):
            return _dt.combine(value, _dt.min.time())
        s = (f"{value}").strip()
        if not s:
            return None
        s = s.replace("T", " ")
        # quita zona horaria tipo Z o +05:00
        s = _re.sub(r"(Z|[+-]\d\d:\d\d)$", "", s)
        # quita microsegundos
        s_no_ms = s.split(".")[0]
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
            try:
                return _dt.strptime(s_no_ms, fmt)
            except Exception:
                pass
        return None

    @app.template_filter("fecha")
    def fecha_filter(value):
        dt = _to_dt(value)
        return dt.strftime("%Y-%m-%d") if dt else ""

    @app.template_filter("fecha_hora")
    def fecha_hora_filter(value):
        dt = _to_dt(value)
        return dt.strftime("%Y-%m-%d %H:%M") if dt else ""

    @app.route("/")
    def index():
        return redirect(url_for("modulos.dashboard"))

    # ------------------------------------------------------------------
    # Helpers para templates: evitan BuildError cuando hay módulos opcionales
    # ------------------------------------------------------------------
    def has_endpoint(endpoint: str) -> bool:
        try:
            return endpoint in app.view_functions
        except Exception:
            return False

    def safe_url_for(endpoint: str, **values):
        try:
            return url_for(endpoint, **values)
        except Exception:
            return "#"

    app.jinja_env.globals["has_endpoint"] = has_endpoint
    app.jinja_env.globals["safe_url_for"] = safe_url_for




    return app


if __name__ == "__main__":
    app = create_app()
    port = int(os.getenv("PORT", "5011"))
    # No exponer debugger en la red
    dev_tls = (os.getenv("RRHH_DEV_TLS", "1").strip().lower() not in ("0","false","no","off"))
    ssl_ctx = "adhoc" if dev_tls else None
    app.run(host="0.0.0.0", port=port, debug=False, ssl_context=ssl_ctx)