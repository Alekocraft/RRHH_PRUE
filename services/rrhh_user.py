from __future__ import annotations

from typing import Optional, Dict, Any, List

from services.rrhh_db import fetch_one, fetch_all, execute
from services.rrhh_security import (
    normalize_ad_username,
    get_user_roles,
    set_user_roles,
    ensure_default_empleado_role,
    ROLE_ADMIN,
    ROLE_RRHH,
    ROLE_EMPLEADO,
)


def _user_row_to_dict(u) -> Dict[str, Any]:
    roles = list(get_user_roles(int(u.user_id)))

    first_name = getattr(u, "first_name", None)
    last_name = getattr(u, "last_name", None)
    email = getattr(u, "email", None)
    department = getattr(u, "department", None)
    position_name = getattr(u, "position_name", None)

    display_name = " ".join([x for x in [first_name, last_name] if x]).strip() or u.ad_username

    employee_id = getattr(u, "employee_id", None)
    try:
        employee_id = int(employee_id) if employee_id is not None else None
    except Exception:
        # Si llega como string/Decimal raro, lo dejamos tal cual
        pass

    return {
        "user_id": int(u.user_id),
        "ad_username": u.ad_username,
        "employee_id": employee_id,
        "is_active": bool(u.is_active),
        "created_at": u.created_at,
        "roles": roles,
        "display_name": display_name,
        "first_name": first_name,
        "last_name": last_name,
        "email": email,
        "department": department,
        "position_name": position_name,
        # banderas para UI (menu / accesos)
        "can_work_from_home": bool(getattr(u, "can_work_from_home", 0) or 0),
        "is_manager": bool(getattr(u, "is_manager", 0) or 0),
    }



_SQL_USER_BASE = """
SELECT
  u.user_id,
  u.ad_username,
  u.employee_id AS auth_employee_id,
  COALESCE(u.employee_id, e.employee_id) AS employee_id,
  u.is_active,
  u.created_at,
  e.first_name,
  e.last_name,
  e.email,
  e.department,
  e.position_name,
  ISNULL(e.can_work_from_home, 0) AS can_work_from_home,
  CASE
    WHEN COALESCE(u.employee_id, e.employee_id) IS NULL THEN 0
    WHEN EXISTS (
      SELECT 1
      FROM rrhh.hr_employee_manager m
      WHERE m.manager_employee_id = COALESCE(u.employee_id, e.employee_id)
        AND m.is_primary = 1
        AND m.employee_id <> m.manager_employee_id
        AND m.valid_from <= CAST(GETDATE() AS DATE)
        AND (m.valid_to IS NULL OR m.valid_to >= CAST(GETDATE() AS DATE))
    ) THEN 1
    ELSE 0
  END AS is_manager
FROM rrhh.auth_user u
OUTER APPLY (
  -- Si el usuario no está enlazado (employee_id NULL), intenta resolver por ad_username.
  SELECT TOP (1)
    e2.employee_id,
    e2.first_name,
    e2.last_name,
    e2.email,
    e2.department,
    e2.position_name,
    e2.can_work_from_home
  FROM rrhh.hr_employee e2
  WHERE e2.employee_id = u.employee_id
     OR (
       u.employee_id IS NULL
       AND e2.ad_username IS NOT NULL
       AND LOWER(e2.ad_username) = LOWER(u.ad_username)
     )
  ORDER BY CASE WHEN e2.employee_id = u.employee_id THEN 0 ELSE 1 END, e2.employee_id
) e
"""


def _auto_link_if_needed(u) -> None:
    """Si auth_user.employee_id está vacío pero encontramos employee_id por AD, lo persistimos."""
    auth_emp = getattr(u, "auth_employee_id", None)
    eff_emp = getattr(u, "employee_id", None)
    try:
        auth_emp = int(auth_emp) if auth_emp is not None else None
    except Exception:
        auth_emp = None
    try:
        eff_emp = int(eff_emp) if eff_emp is not None else None
    except Exception:
        eff_emp = None

    if auth_emp is None and eff_emp is not None:
        link_user_to_employee(int(u.user_id), eff_emp)


def get_user_by_id(user_id: int) -> Optional[Dict[str, Any]]:
    u = fetch_one(_SQL_USER_BASE + " WHERE u.user_id = ?", (int(user_id),))
    if not u:
        return None
    _auto_link_if_needed(u)
    return _user_row_to_dict(u)


def get_user_by_ad_username(ad_username: str) -> Optional[Dict[str, Any]]:
    u_norm = normalize_ad_username(ad_username)
    u = fetch_one(_SQL_USER_BASE + " WHERE u.ad_username = ?", (u_norm,))
    if not u:
        return None
    _auto_link_if_needed(u)
    return _user_row_to_dict(u)


def get_or_create_auth_user(ad_username: str) -> Dict[str, Any]:
    """Upsert auth_user por ad_username.

    IMPORTANTE: el rol por defecto debe ser EMPLEADO (no ADMIN).
    """
    u_norm = normalize_ad_username(ad_username)

    row = fetch_one("SELECT user_id, is_active FROM rrhh.auth_user WHERE ad_username = ?", (u_norm,))
    if not row:
        execute(
            "INSERT INTO rrhh.auth_user(ad_username, employee_id, is_active) VALUES (?, NULL, 1)",
            (u_norm,),
        )
        row = fetch_one("SELECT user_id, is_active FROM rrhh.auth_user WHERE ad_username = ?", (u_norm,))
    else:
        # re-activa si estaba inactivo
        if int(row.is_active) == 0:
            execute("UPDATE rrhh.auth_user SET is_active = 1 WHERE user_id = ?", (int(row.user_id),))

    uid = int(row.user_id)

    # Garantiza catálogo y rol base EMPLEADO (sin tocar RRHH/ADMIN si ya existían)
    ensure_default_empleado_role(uid)

    return get_user_by_id(uid)


def link_user_to_employee(user_id: int, employee_id: Optional[int]):
    execute("UPDATE rrhh.auth_user SET employee_id = ? WHERE user_id = ?", (employee_id, int(user_id)))


def set_user_active(user_id: int, is_active: bool):
    execute("UPDATE rrhh.auth_user SET is_active = ? WHERE user_id = ?", (1 if is_active else 0, int(user_id)))


def set_user_is_admin(user_id: int, is_admin: bool):
    """Activa/desactiva rol ADMINISTRADOR preservando roles existentes.

    - Nunca deja al usuario sin rol: siempre garantiza EMPLEADO.
    - Si el usuario tenía RRHH, lo mantiene.
    """
    uid = int(user_id)
    roles = set(rc.upper() for rc in (get_user_roles(uid) or []))

    if is_admin:
        roles.add(ROLE_ADMIN)
    else:
        roles.discard(ROLE_ADMIN)

    # Base mínima
    roles.add(ROLE_EMPLEADO)

    # Normaliza (por si acaso)
    roles_norm = []
    for rc in [ROLE_EMPLEADO, ROLE_RRHH, ROLE_ADMIN]:
        if rc in roles:
            roles_norm.append(rc)
    # agrega otros roles no estándar que existan
    for rc in sorted(roles):
        if rc not in roles_norm:
            roles_norm.append(rc)

    set_user_roles(uid, roles_norm)


def list_users_for_admin() -> List[Dict[str, Any]]:
    rows = fetch_all(
        """
        SELECT
          u.user_id, u.ad_username, u.employee_id, u.is_active, u.created_at,
          e.doc_number, e.first_name, e.last_name, e.department, e.position_name,
          ISNULL(e.can_work_from_home, 0) AS can_work_from_home
        FROM rrhh.auth_user u
        LEFT JOIN rrhh.hr_employee e ON e.employee_id = u.employee_id
        ORDER BY u.created_at DESC
        """
    )
    role_rows = fetch_all(
        "SELECT ur.user_id, r.role_code "
        "FROM rrhh.auth_user_role ur "
        "JOIN rrhh.auth_role r ON r.role_id = ur.role_id"
    )
    roles_map: Dict[int, set] = {}
    for rr in role_rows:
        roles_map.setdefault(int(rr.user_id), set()).add(rr.role_code)

    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "user_id": int(r.user_id),
                "ad_username": r.ad_username,
                "employee_id": r.employee_id,
                "is_active": bool(r.is_active),
                "created_at": r.created_at,
                "doc_number": r.doc_number,
                "first_name": r.first_name,
                "last_name": r.last_name,
                "department": r.department,
                "position_name": r.position_name,
                "can_work_from_home": bool(r.can_work_from_home),
                "roles": sorted(list(roles_map.get(int(r.user_id), set()))),
            }
        )
    return out
