from __future__ import annotations

from typing import Optional, Dict, List, Iterable, Union

from services.rrhh_db import fetch_one, fetch_all, execute

# --- Catálogo de roles (códigos) ---
ROLE_EMPLEADO = "EMPLEADO"
ROLE_RRHH = "RRHH"
ROLE_ADMIN = "ADMINISTRADOR"
ROLE_COORD_INDEM = "COORDINADOR_INDEMNIZACIONES"
ROLE_GERENTE_INDEM = "GERENTE_INDEMNIZACIONES"


# -------------------------
# Utilidades
# -------------------------

def normalize_ad_username(raw: str) -> str:
    """Normaliza entradas tipo 'DOMINIO\\usuario' o 'usuario@dominio'."""
    raw = (raw or "").strip()
    if "\\" in raw:
        raw = raw.split("\\", 1)[1]
    if "@" in raw:
        raw = raw.split("@", 1)[0]
    return raw.lower()


def _role_name(role_code: str) -> str:
    # Nombres human-friendly (solo para inserción inicial)
    if role_code == ROLE_ADMIN:
        return "Administrador"
    if role_code == ROLE_RRHH:
        return "Recursos Humanos"
    if role_code == ROLE_EMPLEADO:
        return "Empleado"
    if role_code == ROLE_COORD_INDEM:
        return "Coordinador de indemnizaciones"
    if role_code == ROLE_GERENTE_INDEM:
        return "Gerente de indemnizaciones"
    return role_code


def ensure_role_exists(role_code: str, role_name: Optional[str] = None) -> None:
    role_code = (role_code or "").strip().upper()
    if not role_code:
        return
    r = fetch_one("SELECT role_id FROM rrhh.auth_role WHERE role_code=?", (role_code,))
    if not r:
        execute(
            "INSERT INTO rrhh.auth_role(role_code, role_name) VALUES (?,?)",
            (role_code, role_name or _role_name(role_code)),
        )


def _get_role_id(role_code: str) -> Optional[int]:
    role_code = (role_code or "").strip().upper()
    if not role_code:
        return None
    row = fetch_one("SELECT role_id FROM rrhh.auth_role WHERE role_code=?", (role_code,))
    return int(row.role_id) if row else None


def _get_user_id_by_username(ad_username: str) -> Optional[int]:
    u = normalize_ad_username(ad_username)
    row = fetch_one("SELECT user_id FROM rrhh.auth_user WHERE ad_username=?", (u,))
    return int(row.user_id) if row else None


# -------------------------
# Usuarios / Roles
# -------------------------

def get_or_create_user(ad_username: str) -> Dict:
    """Crea (si no existe) el auth_user y lo retorna como dict."""
    u = normalize_ad_username(ad_username)

    row = fetch_one(
        "SELECT user_id, ad_username, employee_id, is_active FROM rrhh.auth_user WHERE ad_username=?",
        (u,),
    )
    if row:
        if int(row.is_active) == 0:
            execute("UPDATE rrhh.auth_user SET is_active=1 WHERE user_id=?", (row.user_id,))
        return {
            "user_id": row.user_id,
            "ad_username": row.ad_username,
            "employee_id": row.employee_id,
            "is_active": row.is_active,
        }

    execute("INSERT INTO rrhh.auth_user(ad_username, employee_id, is_active) VALUES (?, NULL, 1)", (u,))
    row2 = fetch_one(
        "SELECT user_id, ad_username, employee_id, is_active FROM rrhh.auth_user WHERE ad_username=?",
        (u,),
    )
    return {
        "user_id": row2.user_id,
        "ad_username": row2.ad_username,
        "employee_id": row2.employee_id,
        "is_active": row2.is_active,
    }


def ensure_default_empleado_role(user_id: int) -> None:
    """Garantiza que existan los roles base y que el usuario tenga EMPLEADO."""
    ensure_role_exists(ROLE_EMPLEADO)
    ensure_role_exists(ROLE_RRHH)
    ensure_role_exists(ROLE_ADMIN)

    rid = _get_role_id(ROLE_EMPLEADO)
    if rid is None:
        return
    ok = fetch_one("SELECT 1 FROM rrhh.auth_user_role WHERE user_id=? AND role_id=?", (user_id, rid))
    if not ok:
        execute("INSERT INTO rrhh.auth_user_role(user_id, role_id) VALUES (?,?)", (user_id, rid))


def ensure_default_admin_role(user_id: int) -> None:
    """Asegura que el rol ADMINISTRADOR exista y esté asignado al usuario."""
    ensure_role_exists(ROLE_ADMIN)
    rid = _get_role_id(ROLE_ADMIN)
    if rid is None:
        return
    ok = fetch_one("SELECT 1 FROM rrhh.auth_user_role WHERE user_id=? AND role_id=?", (user_id, rid))
    if not ok:
        execute("INSERT INTO rrhh.auth_user_role(user_id, role_id) VALUES (?,?)", (user_id, rid))


def ensure_default_rrhh_role(user_id: int) -> None:
    """(Opcional) Asegura que el rol RRHH exista y esté asignado al usuario."""
    ensure_role_exists(ROLE_RRHH)
    rid = _get_role_id(ROLE_RRHH)
    if rid is None:
        return
    ok = fetch_one("SELECT 1 FROM rrhh.auth_user_role WHERE user_id=? AND role_id=?", (user_id, rid))
    if not ok:
        execute("INSERT INTO rrhh.auth_user_role(user_id, role_id) VALUES (?,?)", (user_id, rid))


def get_roles(ad_username: str) -> List[str]:
    """Roles por username (sam)."""
    u = normalize_ad_username(ad_username)
    rows = fetch_all(
        """
        SELECT r.role_code
        FROM rrhh.auth_user u
        JOIN rrhh.auth_user_role ur ON ur.user_id = u.user_id
        JOIN rrhh.auth_role r ON r.role_id = ur.role_id
        WHERE u.ad_username = ?
        """,
        (u,),
    )
    return [r.role_code for r in rows]


def get_user_roles(user_id: Union[int, str]) -> List[str]:
    """Compatibilidad: algunos módulos llaman get_user_roles(user_id).

    - Si recibe int: interpreta como user_id.
    - Si recibe str: interpreta como ad_username.
    """
    uid: Optional[int]
    if isinstance(user_id, int):
        uid = user_id
    else:
        uid = _get_user_id_by_username(f"{user_id}")

    if not uid:
        return []

    rows = fetch_all(
        """
        SELECT r.role_code
        FROM rrhh.auth_user_role ur
        JOIN rrhh.auth_role r ON r.role_id = ur.role_id
        WHERE ur.user_id = ?
        """,
        (uid,),
    )
    return [r.role_code for r in rows]


def set_user_roles(user_id: int, role_codes: Iterable[str]) -> None:
    """Reemplaza el set de roles del usuario por role_codes."""
    # Normaliza entrada
    role_codes_norm = []
    for rc in role_codes or []:
        rc2 = (rc or "").strip().upper()
        if rc2 and rc2 not in role_codes_norm:
            role_codes_norm.append(rc2)

    # Asegura catálogo
    for rc2 in role_codes_norm:
        ensure_role_exists(rc2)

    # Borra asignaciones actuales
    execute("DELETE FROM rrhh.auth_user_role WHERE user_id=?", (user_id,))

    # Inserta nuevas
    for rc2 in role_codes_norm:
        rid = _get_role_id(rc2)
        if rid is not None:
            execute("INSERT INTO rrhh.auth_user_role(user_id, role_id) VALUES (?,?)", (user_id, rid))


def get_user_row(ad_username: str) -> Optional[Dict]:
    u = normalize_ad_username(ad_username)
    row = fetch_one(
        "SELECT user_id, ad_username, employee_id, is_active FROM rrhh.auth_user WHERE ad_username=?",
        (u,),
    )
    if not row:
        return None
    return {
        "user_id": row.user_id,
        "ad_username": row.ad_username,
        "employee_id": row.employee_id,
        "is_active": row.is_active,
    }


def load_user_by_username(ad_username: str) -> Optional[Dict]:
    rec = get_user_row(ad_username)
    if not rec:
        return None
    rec["roles"] = get_roles(rec["ad_username"])
    return rec
