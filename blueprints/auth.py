from flask import Blueprint, render_template, request, redirect, url_for, flash
from flask_login import login_user, logout_user, current_user
from werkzeug.routing import BuildError

from models.user import User
from services.ldap_auth import authenticate, test_connection
from services.rrhh_security import (
    normalize_ad_username,
    get_or_create_user,
    ensure_default_empleado_role,
    get_roles,
    ROLE_ADMIN,
    ROLE_RRHH,
)

# Import tolerante (no rompe el arranque si el service aún no se actualizó)
try:
    from services import hr_employee_service as hes
except Exception:
    hes = None

auth_bp = Blueprint("auth", __name__, url_prefix="")


def _u(endpoint: str, **values) -> str:
    """url_for tolerante: intenta con prefijo modulos.* y luego sin prefijo.
    Evita BuildError cuando el dashboard vive en blueprints.modulos.
    """
    candidates = [endpoint]
    if "." not in endpoint:
        candidates = [f"modulos.{endpoint}", endpoint]

    last_err = None
    for ep in candidates:
        try:
            return url_for(ep, **values)
        except BuildError as e:
            last_err = e
    # Si no existe, re-levanta el último error (para depurar)
    if last_err:
        raise last_err
    raise BuildError(endpoint, values, None, None)


def _human_login_error(info: dict) -> str:
    code = (info or {}).get("error") or "LOGIN_FAILED"
    detail = (info or {}).get("detail")

    if code == "EMPTY_CREDENTIALS":
        return "Usuario y contraseña son obligatorios."
    if code == "LDAP_DISABLED":
        return "LDAP está deshabilitado (LDAP_ENABLED=false)."
    if code == "LDAP_SEARCH_BASE_EMPTY":
        return "Falta LDAP_SEARCH_BASE / LDAP_BASE_DN en .env."
    if code == "LDAP_SERVICE_ACCOUNT_EMPTY":
        return "Falta cuenta de servicio LDAP (LDAP_SERVICE_USER / LDAP_SERVICE_PASSWORD) en .env."
    if code == "USER_NOT_FOUND":
        return "Usuario no encontrado en Directorio Activo."
    if code == "INVALID_CREDENTIALS":
        return "Credenciales inválidas."
    if code == "LDAP_ERROR":
        return f"Error LDAP: {detail or 'revise configuración/credenciales'}"
    return "Credenciales inválidas o error de directorio."


def _build_user(rec: dict, ad_username: str, roles: list[str] | None) -> User:
    """Construye el objeto User de forma compatible con versiones viejas/nuevas."""
    roles = roles or []

    # Intenta calcular flags (best effort); si falla, load_user las recalculará por request
    can_wfh = False
    is_mgr = False
    employee_id = rec.get("employee_id")

    if hes is not None:
        try:
            if hasattr(hes, "employee_can_work_from_home"):
                can_wfh = bool(hes.employee_can_work_from_home(employee_id))
        except Exception:
            can_wfh = False
        try:
            if hasattr(hes, "manager_has_subordinates"):
                is_mgr = bool(hes.manager_has_subordinates(employee_id))
        except Exception:
            is_mgr = False

    # NUEVA firma: User(user_id, username, ...)
    try:
        return User(
            user_id=rec["user_id"],
            username=ad_username,
            employee_id=employee_id,
            roles=roles,
            display_name=ad_username,
            email=rec.get("email"),
            puede_trabajo_casa=can_wfh,
            es_jefe=is_mgr,
        )
    except TypeError:
        # FIRMA ANTIGUA (si existe): User({dict})
        return User(
            {
                "user_id": rec["user_id"],
                "ad_username": ad_username,
                "employee_id": employee_id,
                "roles": roles,
                "is_active": True,
                "can_work_from_home": can_wfh,
                "is_manager": is_mgr,
            }
        )


@auth_bp.route("/login", methods=["GET", "POST"])
def login():
    if current_user.is_authenticated:
        return redirect(_u("dashboard"))

    if request.method == "POST":
        usuario = (request.form.get("usuario") or "").strip()
        contrasena = request.form.get("contrasena") or ""

        ok, info = authenticate(usuario, contrasena)
        if ok:
            ad_username = normalize_ad_username(info.get("username") or usuario)

            rec = get_or_create_user(ad_username)
            ensure_default_empleado_role(rec["user_id"])
            roles = get_roles(ad_username)

            user = _build_user(rec, ad_username, roles)

            login_user(user)

            # empleado normal sin employee_id => perfil pendiente
            if (
                (getattr(user, "employee_id", None) is None)
                and (ROLE_ADMIN not in (roles or []))
                and (ROLE_RRHH not in (roles or []))
            ):
                return redirect(_u("perfil_pendiente"))

            next_url = request.args.get("next")
            return redirect(next_url or _u("dashboard"))

        flash(_human_login_error(info), "error")
        return render_template("auth/login.html")

    return render_template("auth/login.html")


@auth_bp.route("/logout")
def logout():
    logout_user()
    return redirect(url_for("auth.login"))


@auth_bp.route("/test-ldap")
def test_ldap():
    ok, msg = test_connection()
    if ok:
        flash("Conexión LDAP OK.", "success")
    else:
        flash(f"Conexión LDAP falló: {msg}", "error")
    return redirect(url_for("auth.login"))
