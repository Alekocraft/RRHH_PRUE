"""Helpers compartidos para blueprints/modulos_*.py.

Objetivo: mantener `blueprints/modulos.py` pequeño sin cambiar endpoints ni
reglas de negocio.
"""

from __future__ import annotations

import calendar
import hashlib
from datetime import date, datetime, time
from typing import Optional, Tuple

from flask import flash
from flask_login import current_user

from services.hr_employee_service import (
    employee_can_work_from_home,
    get_manager_for_employee,
    manager_has_subordinates,
)
from services.rrhh_db import fetch_one
from services.rrhh_security import ROLE_ADMIN, ROLE_RRHH


# -----------------------------------------------------------------------------
# Permisos / roles
# -----------------------------------------------------------------------------


def _require_admin() -> bool:
    """Permite acceso a ADMINISTRADOR o RRHH."""
    roles = getattr(current_user, "roles", None) or []
    if (
        getattr(current_user, "is_admin", False)
        or (ROLE_ADMIN in roles)
        or (ROLE_RRHH in roles)
        or ("ADMINISTRADOR" in roles)
        or ("RRHH" in roles)
    ):
        return True

    flash("No tienes permisos para acceder a esta sección.", "warning")
    return False


def _is_admin_or_rrhh() -> bool:
    roles = getattr(current_user, "roles", None) or []
    return bool(
        getattr(current_user, "is_admin", False)
        or (ROLE_ADMIN in roles)
        or (ROLE_RRHH in roles)
        or ("ADMINISTRADOR" in roles)
        or ("RRHH" in roles)
    )


# -----------------------------------------------------------------------------
# Trabajo en casa (WFH)
# -----------------------------------------------------------------------------


def _user_can_request_wfh() -> bool:
    """Valida si el usuario puede solicitar Trabajo en Casa."""
    # Primero el flag inyectado en login; si no está, valida en DB.
    if bool(getattr(current_user, "puede_trabajo_casa", False)):
        return True
    try:
        return employee_can_work_from_home(getattr(current_user, "employee_id", None))
    except Exception:
        return False


def _can_approve_wfh(employee_id: int, ref_date: date) -> bool:
    """Reglas de aprobación WFH.

    - ADMIN/RRHH: ve y aprueba todo.
    - Jefe: solo aprueba solicitudes de su equipo (relación vigente en ref_date).
    - Solicitudes de *jefes* (personas con subordinados) quedan para RRHH/ADMIN.
    """
    if _is_admin_or_rrhh():
        return True

    try:
        if manager_has_subordinates(employee_id, ref_date):
            return False
    except Exception:
        pass

    mgr_id = get_manager_for_employee(employee_id, ref_date)
    return bool(mgr_id and mgr_id == getattr(current_user, "employee_id", None))


# -----------------------------------------------------------------------------
# Utilidades generales
# -----------------------------------------------------------------------------


def _month_range(y: int, m: int) -> Tuple[date, date]:
    first = date(y, m, 1)
    last = date(y, m, calendar.monthrange(y, m)[1])
    return first, last


def _time_to_str(t: Optional[time]) -> str:
    if t is None:
        return ""
    return t.strftime("%H:%M")


def _parse_doc_number(v) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    digits = "".join(ch for ch in s if ch.isdigit())
    return digits or None


def _parse_date(v) -> Optional[date]:
    if v is None:
        return None
    if isinstance(v, date) and not isinstance(v, datetime):
        return v
    if isinstance(v, datetime):
        return v.date()
    s = str(v).strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            pass
    return None


def _parse_time(v) -> Optional[time]:
    if v is None:
        return None
    if isinstance(v, time):
        return v.replace(microsecond=0)
    if isinstance(v, datetime):
        return v.time().replace(microsecond=0)
    s = str(v).strip()
    if not s:
        return None
    for fmt in ("%H:%M:%S", "%H:%M"):
        try:
            return datetime.strptime(s, fmt).time()
        except Exception:
            pass
    return None


def _diff_minutes(t1: time, t2: time) -> int:
    dt1 = datetime.combine(date(2000, 1, 1), t1)
    dt2 = datetime.combine(date(2000, 1, 1), t2)
    return int((dt2 - dt1).total_seconds() // 60)


def _checksum_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _att_import_doc_column() -> str:
    """Soporta DB vieja (employee_code) o nueva (doc_number)."""
    col = fetch_one(
        "SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS "
        "WHERE TABLE_SCHEMA='rrhh' AND TABLE_NAME='att_import_row' AND COLUMN_NAME='doc_number'"
    )
    return "doc_number" if col else "employee_code"
