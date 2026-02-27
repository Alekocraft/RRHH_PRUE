from __future__ import annotations

from datetime import date, datetime

from flask import render_template, request, redirect, url_for, flash
from flask_login import login_required, current_user

from services.hr_employee_service import get_manager_for_employee, manager_has_subordinates
from services.rrhh_db import fetch_all, fetch_one, execute
from services.rrhh_security import ROLE_ADMIN, ROLE_RRHH

from .modulos import modulos_bp


REQUEST_TYPE_CHEQUERA = "CHEQUERA_TIEMPO"
TOTAL_MEDIOS_DIAS = 6  # 6 medios días = 100%


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def _table_exists(schema: str, table: str) -> bool:
    row = fetch_one(
        "SELECT 1 AS ok FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=?",
        (schema, table),
    )
    return row is not None


def _roles() -> set[str]:
    r = getattr(current_user, "roles", None) or []
    # roles puede venir como set
    try:
        return set(r)
    except Exception:
        return set(list(r))


def _is_admin() -> bool:
    roles = _roles()
    return bool(
        getattr(current_user, "is_admin", False)
        or (ROLE_ADMIN in roles)
        or ("ADMINISTRADOR" in roles)
        or ("Administrador" in roles)
    )


def _is_rrhh() -> bool:
    roles = _roles()
    return bool((ROLE_RRHH in roles) or ("RRHH" in roles) or ("Recursos Humanos" in roles))


def _is_backoffice_view() -> bool:
    # Backoffice (visualización): Admin o RRHH
    return _is_admin() or _is_rrhh()


def _get_shift_for(employee_id: int, on_date: date):
    """Retorna el turno vigente para un empleado en una fecha (o None si no tiene)."""
    if not employee_id or not on_date:
        return None
    return fetch_one(
        "SELECT sd.shift_code, sd.start_time, sd.end_time "
        "FROM rrhh.shift_assignment sa "
        "JOIN rrhh.shift_definition sd ON sd.shift_id = sa.shift_id "
        "WHERE sa.employee_id=? "
        "  AND sa.valid_from <= ? "
        "  AND (sa.valid_to IS NULL OR sa.valid_to >= ?) "
        "ORDER BY sa.valid_from DESC",
        (int(employee_id), on_date, on_date),
    )


def _has_shift(employee_id: int, on_date: date) -> bool:
    return _get_shift_for(employee_id, on_date) is not None


def _manager_for(employee_id: int, ref_date: date) -> int | None:
    """Wrapper tolerante para get_manager_for_employee(employee_id, ref_date?)."""
    try:
        return get_manager_for_employee(int(employee_id), ref_date)
    except TypeError:
        # firma antigua
        try:
            return get_manager_for_employee(int(employee_id))
        except Exception:
            return None
    except Exception:
        return None


def _manager_name_for(employee_id: int, ref_date: date) -> str | None:
    mgr_id = _manager_for(employee_id, ref_date)
    if not mgr_id:
        return None
    row = fetch_one(
        "SELECT first_name, last_name FROM rrhh.hr_employee WHERE employee_id=?",
        (int(mgr_id),),
    )
    if not row:
        return None
    fn = (getattr(row, "first_name", "") or "").strip()
    ln = (getattr(row, "last_name", "") or "").strip()
    name = (fn + " " + ln).strip()
    return name or None


def _get_balance(employee_id: int) -> dict:
    """Balance de chequera: total, aprobadas, restantes, porcentaje restante."""
    if not employee_id:
        return {"total": TOTAL_MEDIOS_DIAS, "approved": 0, "remaining": TOTAL_MEDIOS_DIAS, "percent": 100}

    row = fetch_one(
        "SELECT COUNT(1) AS c "
        "FROM rrhh.timebook_request "
        "WHERE employee_id=? AND status='APPROVED'",
        (int(employee_id),),
    )
    approved = int(getattr(row, "c", 0) or 0) if row else 0
    remaining = max(TOTAL_MEDIOS_DIAS - approved, 0)
    percent = int(round((remaining / TOTAL_MEDIOS_DIAS) * 100)) if TOTAL_MEDIOS_DIAS else 0
    return {"total": TOTAL_MEDIOS_DIAS, "approved": approved, "remaining": remaining, "percent": percent}


def _can_decide_request(request_employee_id: int, request_date: date) -> bool:
    """Reglas de decisión (aprobar/rechazar) para Chequera.

    - ADMIN: puede aprobar/rechazar SIEMPRE (incluye sus propias solicitudes).
    - RRHH: SOLO VISUALIZA (no decide).
    - Jefe directo: decide solicitudes de su equipo (relación vigente en request_date).

    Nota: si un empleado no tiene jefe asignado, la solicitud queda decidible por ADMIN.
    """
    if not request_employee_id or not request_date:
        return False

    if _is_admin():
        return True

    if _is_rrhh():
        return False

    my_emp_id = getattr(current_user, "employee_id", None)
    if not my_emp_id:
        return False

    # Evitar auto-aprobación para jefes (solo ADMIN puede)
    if int(my_emp_id) == int(request_employee_id):
        return False

    mgr_id = _manager_for(int(request_employee_id), request_date)
    return bool(mgr_id and int(mgr_id) == int(my_emp_id))


def _pending_for_viewer():
    """Solicitudes pendientes visibles en bandeja.

    Visibilidad:
    - ADMIN/RRHH: ve TODAS las pendientes.
    - Jefe: ve pendientes de su equipo (según vigencia en request_date).
    """
    my_emp_id = getattr(current_user, "employee_id", None)

    if _is_backoffice_view():
        return fetch_all(
            "SELECT r.request_id, r.employee_id, r.request_date, r.slot, r.reason, r.status, r.created_at, "
            "       e.doc_number, e.first_name, e.last_name, e.department, e.position_name, "
            "       mgr.manager_employee_id, me.first_name AS manager_first_name, me.last_name AS manager_last_name "
            "FROM rrhh.timebook_request r "
            "JOIN rrhh.hr_employee e ON e.employee_id = r.employee_id "
            "LEFT JOIN rrhh.hr_employee_manager mgr "
            "  ON mgr.employee_id=r.employee_id "
            " AND mgr.is_primary=1 "
            " AND mgr.valid_from <= r.request_date "
            " AND (mgr.valid_to IS NULL OR mgr.valid_to >= r.request_date) "
            "LEFT JOIN rrhh.hr_employee me ON me.employee_id = mgr.manager_employee_id "
            "WHERE r.status='PENDING' "
            "ORDER BY r.created_at DESC"
        )

    # No backoffice: debe ser jefe
    if not my_emp_id:
        return []

    return fetch_all(
        "SELECT r.request_id, r.employee_id, r.request_date, r.slot, r.reason, r.status, r.created_at, "
        "       e.doc_number, e.first_name, e.last_name, e.department, e.position_name, "
        "       m.manager_employee_id, NULL AS manager_first_name, NULL AS manager_last_name "
        "FROM rrhh.timebook_request r "
        "JOIN rrhh.hr_employee_manager m "
        "  ON m.employee_id = r.employee_id "
        " AND m.manager_employee_id=? "
        " AND m.is_primary=1 "
        " AND m.valid_from <= r.request_date "
        " AND (m.valid_to IS NULL OR m.valid_to >= r.request_date) "
        "JOIN rrhh.hr_employee e ON e.employee_id = r.employee_id "
        "WHERE r.status='PENDING' "
        "  AND r.employee_id <> ? "
        "ORDER BY r.created_at DESC",
        (int(my_emp_id), int(my_emp_id)),
    )


def _decorate_item(r) -> dict:
    req_emp_id = int(getattr(r, "employee_id", 0) or 0)
    req_date = getattr(r, "request_date", None)

    manager_name = None
    mf = (getattr(r, "manager_first_name", None) or "").strip()
    ml = (getattr(r, "manager_last_name", None) or "").strip()
    if mf or ml:
        manager_name = (mf + " " + ml).strip()

    # Si no viene en query (vista jefe), calculamos nombre del jefe real
    if not manager_name and req_date:
        try:
            manager_name = _manager_name_for(req_emp_id, req_date)
        except Exception:
            manager_name = None

    # Pendiente de
    expected = "Jefe Directo" if manager_name else "Administrador"

    can_decide = False
    readonly_hint = None

    if req_date:
        can_decide = _can_decide_request(req_emp_id, req_date)

    if not can_decide:
        if _is_rrhh() and not _is_admin():
            readonly_hint = "RRHH solo visualiza. La aprobación la realiza el Jefe Directo o el Administrador." 
        else:
            readonly_hint = "Solo lectura. Pendiente del aprobador asignado." 

    return {
        "request_id": getattr(r, "request_id", None),
        "employee_id": req_emp_id,
        "doc_number": getattr(r, "doc_number", None),
        "first_name": getattr(r, "first_name", None),
        "last_name": getattr(r, "last_name", None),
        "department": getattr(r, "department", None),
        "position_name": getattr(r, "position_name", None),
        "request_date": req_date,
        "slot": getattr(r, "slot", None),
        "reason": getattr(r, "reason", None),
        "created_at": getattr(r, "created_at", None),
        "status": getattr(r, "status", None),
        "manager_name": manager_name,
        "expected_approver": expected,
        "can_decide": can_decide,
        "readonly_hint": readonly_hint,
    }


# -----------------------------------------------------------------------------
# Routes
# -----------------------------------------------------------------------------


@modulos_bp.route("/chequera")
@login_required
def chequera():
    """Vista de chequera del usuario (saldo + solicitudes)."""
    if not _table_exists("rrhh", "timebook_request"):
        flash("No se encontró la tabla rrhh.timebook_request en la base de datos.", "warning")
        return render_template("modulos/chequera.html", balance=None, items=[], today_shift=None)

    emp_id = getattr(current_user, "employee_id", None)
    balance = _get_balance(int(emp_id)) if emp_id else _get_balance(None)

    today_shift = None
    if emp_id:
        try:
            today_shift = _get_shift_for(int(emp_id), date.today())
        except Exception:
            today_shift = None

    items = []
    if emp_id:
        items = fetch_all(
            "SELECT request_id, employee_id, request_date, slot, reason, status, created_at, "
            "       decided_at, decided_by_user_id, decision_comment "
            "FROM rrhh.timebook_request "
            "WHERE employee_id=? "
            "ORDER BY created_at DESC",
            (int(emp_id),),
        )

    return render_template("modulos/chequera.html", balance=balance, items=items, today_shift=today_shift)


@modulos_bp.route("/chequera/solicitar", methods=["POST"])
@login_required
def chequera_solicitar():
    if not _table_exists("rrhh", "timebook_request"):
        flash("No existe la tabla rrhh.timebook_request en BD.", "danger")
        return redirect(url_for("modulos.chequera"))

    emp_id = getattr(current_user, "employee_id", None)
    if not emp_id:
        flash("Tu usuario no tiene empleado asociado. Contacta a RRHH.", "warning")
        return redirect(url_for("modulos.chequera"))

    req_date_str = (request.form.get("request_date") or "").strip()
    slot = (request.form.get("slot") or "").strip().upper()
    reason = (request.form.get("reason") or "").strip()

    if not req_date_str:
        flash("Debes seleccionar la fecha.", "warning")
        return redirect(url_for("modulos.chequera"))

    try:
        req_date = datetime.strptime(req_date_str, "%Y-%m-%d").date()
    except Exception:
        flash("Fecha inválida.", "warning")
        return redirect(url_for("modulos.chequera"))

    if slot not in ("AM", "PM"):
        flash("Debes seleccionar Mañana (AM) o Tarde (PM).", "warning")
        return redirect(url_for("modulos.chequera"))

    if req_date < date.today():
        flash("No puedes solicitar chequera para fechas pasadas.", "warning")
        return redirect(url_for("modulos.chequera"))

    # Requisito: para usar chequera debe tener turno asignado en ese día
    if not _has_shift(int(emp_id), req_date):
        flash(
            "No tienes turno asignado para la fecha seleccionada. "
            "Para usar la chequera debes tener turno asignado ese día.",
            "warning",
        )
        return redirect(url_for("modulos.chequera"))

    bal = _get_balance(int(emp_id))
    if int(bal["remaining"]) <= 0:
        flash("No tienes saldo disponible en la chequera (6 medios días consumidos).", "warning")
        return redirect(url_for("modulos.chequera"))

    dup = fetch_one(
        "SELECT 1 AS ok FROM rrhh.timebook_request "
        "WHERE employee_id=? AND request_date=? AND slot=? AND status IN ('PENDING','APPROVED')",
        (int(emp_id), req_date, slot),
    )
    if dup:
        flash("Ya tienes una solicitud pendiente/aprobada para esa fecha y franja.", "warning")
        return redirect(url_for("modulos.chequera"))

    execute(
        "INSERT INTO rrhh.timebook_request(employee_id, request_date, slot, reason, status, created_at, created_by_user_id) "
        "VALUES (?, ?, ?, ?, 'PENDING', GETDATE(), ?)",
        (int(emp_id), req_date, slot, reason or None, int(current_user.user_id)),
    )

    mgr_id = _manager_for(int(emp_id), req_date)
    if mgr_id:
        flash("Solicitud enviada a tu jefe para aprobación. RRHH solo visualiza.", "success")
    else:
        flash(
            "Solicitud registrada. No tienes jefe asignado para esa fecha: el Administrador la gestionará.",
            "warning",
        )

    return redirect(url_for("modulos.chequera"))


@modulos_bp.route("/chequera/cancelar/<int:request_id>", methods=["POST"])
@login_required
def chequera_cancelar(request_id: int):
    if not _table_exists("rrhh", "timebook_request"):
        flash("No existe la tabla rrhh.timebook_request en BD.", "danger")
        return redirect(url_for("modulos.chequera"))

    emp_id = getattr(current_user, "employee_id", None)
    if not emp_id:
        return redirect(url_for("modulos.chequera"))

    row = fetch_one(
        "SELECT request_id, employee_id, status FROM rrhh.timebook_request WHERE request_id=?",
        (int(request_id),),
    )
    if not row:
        flash("Solicitud no encontrada.", "warning")
        return redirect(url_for("modulos.chequera"))

    if int(getattr(row, "employee_id", 0) or 0) != int(emp_id):
        flash("No puedes cancelar una solicitud que no es tuya.", "warning")
        return redirect(url_for("modulos.chequera"))

    if (getattr(row, "status", "") or "").upper() != "PENDING":
        flash("Solo puedes cancelar solicitudes en estado PENDIENTE.", "warning")
        return redirect(url_for("modulos.chequera"))

    execute(
        "UPDATE rrhh.timebook_request "
        "SET status='CANCELLED', decided_at=GETDATE(), decided_by_user_id=?, decision_comment='Cancelada por el solicitante' "
        "WHERE request_id=?",
        (int(current_user.user_id), int(request_id)),
    )
    flash("Solicitud cancelada.", "success")
    return redirect(url_for("modulos.chequera"))


@modulos_bp.route("/chequera/aprobaciones")
@login_required
def chequera_aprobaciones():
    """Bandeja de chequera.

    - Empleado normal: no accede.
    - Jefe: ve pendientes de su equipo.
    - RRHH: ve todas (solo lectura).
    - ADMIN: ve todas y puede aprobar/rechazar.
    """

    my_emp_id = getattr(current_user, "employee_id", None)

    if not _is_backoffice_view():
        if not my_emp_id:
            flash("Tu usuario no está asociado a un empleado. Contacta a RRHH.", "warning")
            return redirect(url_for("modulos.dashboard"))
        try:
            is_mgr = bool(manager_has_subordinates(int(my_emp_id)))
        except TypeError:
            is_mgr = bool(manager_has_subordinates(int(my_emp_id), date.today()))
        except Exception:
            is_mgr = False

        if not is_mgr:
            flash("No tienes permisos para acceder a esta sección.", "warning")
            return redirect(url_for("modulos.dashboard"))

    if not _table_exists("rrhh", "timebook_request"):
        flash("No existe la tabla rrhh.timebook_request en BD.", "danger")
        return render_template("modulos/chequera_aprobaciones.html", items=[])

    raw = _pending_for_viewer()
    items = [_decorate_item(r) for r in (raw or [])]

    return render_template(
        "modulos/chequera_aprobaciones.html",
        items=items,
        is_admin=_is_admin(),
        is_rrhh=_is_rrhh(),
    )


@modulos_bp.route("/chequera/aprobar/<int:request_id>", methods=["POST"])
@login_required
def chequera_aprobar(request_id: int):
    return _chequera_decidir(request_id, approve=True)


@modulos_bp.route("/chequera/rechazar/<int:request_id>", methods=["POST"])
@login_required
def chequera_rechazar(request_id: int):
    return _chequera_decidir(request_id, approve=False)


def _chequera_decidir(request_id: int, approve: bool):
    if not _table_exists("rrhh", "timebook_request"):
        flash("No existe la tabla rrhh.timebook_request en BD.", "danger")
        return redirect(url_for("modulos.chequera_aprobaciones"))

    comment = (request.form.get("comment") or "").strip()

    row = fetch_one(
        "SELECT request_id, employee_id, request_date, slot, status "
        "FROM rrhh.timebook_request WHERE request_id=?",
        (int(request_id),),
    )
    if not row:
        flash("Solicitud no encontrada.", "warning")
        return redirect(url_for("modulos.chequera_aprobaciones"))

    status = (getattr(row, "status", "") or "").upper()
    if status != "PENDING":
        flash("La solicitud ya fue gestionada.", "warning")
        return redirect(url_for("modulos.chequera_aprobaciones"))

    req_emp_id = int(getattr(row, "employee_id", 0) or 0)
    req_date = getattr(row, "request_date", None)

    if not req_date or not _can_decide_request(req_emp_id, req_date):
        flash("No tienes permisos para aprobar/rechazar esta solicitud.", "warning")
        return redirect(url_for("modulos.chequera_aprobaciones"))

    if approve:
        # Debe existir turno asignado para la fecha, si no, no se puede descontar chequera
        try:
            if req_date and not _has_shift(req_emp_id, req_date):
                flash(
                    "No se puede aprobar: el empleado no tiene turno asignado para la fecha solicitada.",
                    "warning",
                )
                return redirect(url_for("modulos.chequera_aprobaciones"))
        except Exception:
            pass

        # Validar saldo al momento de aprobar (evita exceder 6)
        bal = _get_balance(req_emp_id)
        if int(bal["remaining"]) <= 0:
            flash("No se puede aprobar: el empleado no tiene saldo disponible.", "warning")
            return redirect(url_for("modulos.chequera_aprobaciones"))

        execute(
            "UPDATE rrhh.timebook_request "
            "SET status='APPROVED', decided_at=GETDATE(), decided_by_user_id=?, decision_comment=? "
            "WHERE request_id=?",
            (int(current_user.user_id), comment or None, int(request_id)),
        )
        flash("Solicitud aprobada. Se descontó 1 medio día del saldo.", "success")

    else:
        execute(
            "UPDATE rrhh.timebook_request "
            "SET status='REJECTED', decided_at=GETDATE(), decided_by_user_id=?, decision_comment=? "
            "WHERE request_id=?",
            (int(current_user.user_id), comment or None, int(request_id)),
        )
        flash("Solicitud rechazada.", "info")

    return redirect(url_for("modulos.chequera_aprobaciones"))
