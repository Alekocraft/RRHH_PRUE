from __future__ import annotations

from datetime import date, datetime, timedelta

from flask import render_template, request, redirect, url_for, flash
from flask_login import login_required, current_user

from services.hr_employee_service import get_manager_for_employee, manager_has_subordinates
from services.rrhh_db import fetch_all, fetch_one, execute

from .modulos import modulos_bp
from .modulos_common import (
    _require_admin,
    _is_admin_or_rrhh,
    _user_can_request_wfh,
    _can_approve_wfh,
)


def _wfh_week_range(d: date) -> tuple[date, date]:
    """Rango de semana lun-dom."""
    week_start = d - timedelta(days=d.weekday())
    return week_start, week_start + timedelta(days=6)


def _wfh_week_has_weekday_holiday(work_date: date) -> bool:
    """True si existe un festivo entre semana (lun-vie) en la semana de work_date."""
    try:
        has_holiday_table = (
            fetch_one(
                "SELECT 1 FROM INFORMATION_SCHEMA.TABLES "
                "WHERE TABLE_SCHEMA='rrhh' AND TABLE_NAME='hr_holiday'"
            )
            is not None
        )
    except Exception:
        return False

    if not has_holiday_table:
        return False

    week_start, week_end = _wfh_week_range(work_date)

    # Si existe columna is_active, intentamos primero con activos, pero
    # hacemos fallback a cualquier registro para evitar que una carga con is_active=0
    # deje sin efecto la regla de festivos.
    holiday_has_is_active = (
        fetch_one(
            "SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_SCHEMA='rrhh' AND TABLE_NAME='hr_holiday' AND COLUMN_NAME='is_active'"
        )
        is not None
    )

    def _query_holidays(active_only: bool) -> list:
        sql = "SELECT holiday_date FROM rrhh.hr_holiday WHERE holiday_date BETWEEN ? AND ?"
        params = [week_start, week_end]
        if active_only and holiday_has_is_active:
            sql += " AND is_active=1"
        return fetch_all(sql, tuple(params)) or []

    hols = _query_holidays(active_only=True)
    if not hols and holiday_has_is_active:
        hols = _query_holidays(active_only=False)

    for h in hols:
        try:
            if h.holiday_date and h.holiday_date.weekday() < 5:
                return True
        except Exception:
            continue

    return False


def _validate_wfh_rules(employee_id: int, work_date: date) -> tuple[bool, str]:
    """Valida reglas de Trabajo en casa.

    Reglas:
      1) No se permiten 2 días seguidos.
      2) Semanas con festivo (lun-vie) solo permiten 1 día de trabajo en casa.
    """

    # Regla 1: no permitir días consecutivos (calendario)
    prev_day = work_date - timedelta(days=1)
    next_day = work_date + timedelta(days=1)
    adj = fetch_one(
        "SELECT TOP (1) work_date AS d "
        "FROM rrhh.wfh_day "
        "WHERE employee_id=? AND work_date IN (?, ?) AND work_date <> ?",
        (int(employee_id), prev_day, next_day, work_date),
    )
    if adj:
        return (
            False,
            "No puedes solicitar Trabajo en casa en días consecutivos. Revisa el día anterior/siguiente.",
        )

    # Regla 2: semana con festivo -> solo 1 día WFH
    if _wfh_week_has_weekday_holiday(work_date):
        week_start, week_end = _wfh_week_range(work_date)
        c_row = fetch_one(
            "SELECT COUNT(1) AS c "
            "FROM rrhh.wfh_day "
            "WHERE employee_id=? "
            "  AND work_date BETWEEN ? AND ? "
            "  AND work_date <> ?",
            (int(employee_id), week_start, week_end, work_date),
        )
        if c_row and int(getattr(c_row, "c", 0) or 0) >= 1:
            return (
                False,
                "En semanas con festivo entre semana solo se permite 1 día de Trabajo en casa.",
            )

    return True, ""


@modulos_bp.route("/trabajo-casa")
@login_required
def trabajo_casa():
    """Vista RRHH/ADMIN: registros aprobados + registrar manual."""
    if not _require_admin():
        return redirect(url_for("modulos.dashboard"))

    exists = (
        fetch_one(
            "SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='rrhh' AND TABLE_NAME='wfh_day'"
        )
        is not None
    )
    if not exists:
        flash("Tabla rrhh.wfh_day no existe. Aplica el script SQL para crearla.", "warning")
        return render_template("modulos/trabajo_casa.html", registros=[], empleados=[])

    registros = fetch_all(
        "SELECT w.employee_id, w.work_date, w.reason, e.doc_number, e.first_name, e.last_name "
        "FROM rrhh.wfh_day w JOIN rrhh.hr_employee e ON e.employee_id = w.employee_id "
        "WHERE w.is_active=1 ORDER BY w.work_date DESC"
    )
    empleados = fetch_all(
        "SELECT employee_id, doc_number, first_name, last_name "
        "FROM rrhh.hr_employee WHERE is_active=1 ORDER BY last_name, first_name"
    )
    return render_template("modulos/trabajo_casa.html", registros=registros, empleados=empleados)


@modulos_bp.route("/trabajo-casa/nuevo", methods=["POST"])
@login_required
def trabajo_casa_nuevo():
    """RRHH/ADMIN registra directamente un día aprobado."""
    if not _require_admin():
        return redirect(url_for("modulos.dashboard"))

    employee_id = int(request.form.get("employee_id"))
    work_date = datetime.strptime(request.form.get("work_date"), "%Y-%m-%d").date()
    reason = (request.form.get("reason") or "Trabajo en casa").strip()

    ok, msg = _validate_wfh_rules(int(employee_id), work_date)
    if not ok:
        flash(msg, "warning")
        return redirect(url_for("modulos.trabajo_casa"))

    try:
        execute(
            "MERGE rrhh.wfh_day AS t "
            "USING (SELECT ? AS employee_id, ? AS work_date) AS s "
            "ON (t.employee_id = s.employee_id AND t.work_date = s.work_date) "
            "WHEN MATCHED THEN UPDATE SET is_active=1, reason=? "
            "WHEN NOT MATCHED THEN INSERT (employee_id, work_date, reason, created_by_user_id, is_active) "
            "VALUES (?, ?, ?, ?, 1);",
            (employee_id, work_date, reason, employee_id, work_date, reason, current_user.user_id),
        )
        flash("Día de trabajo en casa registrado.", "success")
    except Exception as ex:
        flash(f"No se pudo registrar: {ex}", "danger")

    return redirect(url_for("modulos.trabajo_casa"))


@modulos_bp.route("/trabajo-casa/solicitar", methods=["GET", "POST"])
@login_required
def trabajo_casa_solicitar():
    """Empleado solicita WFH (queda pendiente)."""
    if not _user_can_request_wfh():
        flash("No tienes habilitado Trabajo en casa.", "warning")
        return redirect(url_for("modulos.dashboard"))

    employee_id = getattr(current_user, "employee_id", None)
    if not employee_id:
        flash("Tu perfil no está asociado a un empleado. Pide a RRHH que te asocie.", "warning")
        return redirect(url_for("modulos.dashboard"))

    exists = (
        fetch_one(
            "SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='rrhh' AND TABLE_NAME='wfh_day'"
        )
        is not None
    )
    if not exists:
        flash("Tabla rrhh.wfh_day no existe. RRHH debe crearla.", "warning")
        return redirect(url_for("modulos.dashboard"))

    if request.method == "POST":
        work_date = datetime.strptime(request.form.get("work_date"), "%Y-%m-%d").date()
        reason = (request.form.get("reason") or "Trabajo en casa").strip()

        row = fetch_one(
            "SELECT is_active FROM rrhh.wfh_day WHERE employee_id=? AND work_date=?",
            (int(employee_id), work_date),
        )
        if row and int(row.is_active) == 1:
            flash("Ese día ya está aprobado como Trabajo en casa.", "warning")
            return redirect(url_for("modulos.trabajo_casa_solicitar"))

        ok, msg = _validate_wfh_rules(int(employee_id), work_date)
        if not ok:
            flash(msg, "warning")
            return redirect(url_for("modulos.trabajo_casa_solicitar"))

        try:
            if row:
                execute(
                    "UPDATE rrhh.wfh_day SET is_active=0, reason=?, created_by_user_id=? "
                    "WHERE employee_id=? AND work_date=?",
                    (reason, current_user.user_id, int(employee_id), work_date),
                )
            else:
                execute(
                    "INSERT INTO rrhh.wfh_day(employee_id, work_date, reason, created_by_user_id, is_active) "
                    "VALUES (?, ?, ?, ?, 0)",
                    (int(employee_id), work_date, reason, current_user.user_id),
                )

            approval_date = date.today()

            requester_emp_id = getattr(current_user, "employee_id", None)
            requester_is_manager = False
            if requester_emp_id:
                try:
                    requester_is_manager = manager_has_subordinates(int(requester_emp_id), approval_date)
                except Exception:
                    requester_is_manager = False

            if requester_is_manager:
                flash("Solicitud enviada a RRHH para aprobación.", "success")
            else:
                mgr_id = get_manager_for_employee(int(employee_id), approval_date)
                if mgr_id:
                    flash("Solicitud enviada a tu jefe para aprobación.", "success")
                else:
                    flash(
                        "Solicitud registrada, pero no tienes jefe asignado en el sistema. RRHH debe asignarlo para poder aprobar.",
                        "warning",
                    )
        except Exception as ex:
            flash(f"No se pudo registrar la solicitud: {ex}", "danger")

        return redirect(url_for("modulos.trabajo_casa_solicitar"))

    return render_template("modulos/trabajo_casa_solicitar.html")


@modulos_bp.route("/trabajo-casa/aprobaciones")
@login_required
def trabajo_casa_aprobaciones():
    """Bandeja de aprobaciones WFH."""

    is_backoffice = _is_admin_or_rrhh()
    my_emp_id = getattr(current_user, "employee_id", None)

    if not my_emp_id:
        flash("Tu usuario no está asociado a un empleado. Contacta a RRHH.", "error")
        return redirect(url_for("modulos.dashboard"))

    if not is_backoffice:
        if not manager_has_subordinates(int(my_emp_id)):
            flash("No tienes solicitudes de Trabajo en casa para aprobar.", "warning")
            return redirect(url_for("modulos.dashboard"))

    exists = fetch_one(
        "SELECT 1 AS ok FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='rrhh' AND TABLE_NAME='wfh_day'"
    )
    if not exists:
        flash("No existe la tabla rrhh.wfh_day. Revisa el script de base de datos.", "error")
        return redirect(url_for("modulos.dashboard"))

    if is_backoffice:
        pendientes = fetch_all(
            """
            SELECT
              e.employee_id,
              (e.first_name + ' ' + e.last_name) AS employee_name,
              e.doc_number,
              w.work_date,
              w.reason,
              w.is_active,
              w.created_at
            FROM rrhh.wfh_day w
            JOIN rrhh.hr_employee e ON e.employee_id = w.employee_id
            WHERE w.is_active = 0
            ORDER BY w.created_at DESC
            """
        )

        aprobadas = fetch_all(
            """
            SELECT
              e.employee_id,
              (e.first_name + ' ' + e.last_name) AS employee_name,
              e.doc_number,
              w.work_date,
              w.reason,
              w.is_active,
              w.created_at
            FROM rrhh.wfh_day w
            JOIN rrhh.hr_employee e ON e.employee_id = w.employee_id
            WHERE w.is_active = 1
              AND w.created_at >= DATEADD(day, -60, GETDATE())
            ORDER BY w.created_at DESC
            """
        )

    else:
        pendientes = fetch_all(
            """
            SELECT
              e.employee_id,
              (e.first_name + ' ' + e.last_name) AS employee_name,
              e.doc_number,
              w.work_date,
              w.reason,
              w.is_active,
              w.created_at
            FROM rrhh.wfh_day w
            JOIN rrhh.hr_employee e ON e.employee_id = w.employee_id
            JOIN rrhh.hr_employee_manager mm
              ON mm.employee_id = w.employee_id
             AND mm.is_primary = 1
             AND mm.manager_employee_id = ?
             AND mm.valid_from <= w.work_date
             AND (mm.valid_to IS NULL OR mm.valid_to >= w.work_date)
            WHERE w.is_active = 0
              AND w.employee_id <> ?
            ORDER BY w.created_at DESC
            """,
            (int(my_emp_id), int(my_emp_id)),
        )

        aprobadas = fetch_all(
            """
            SELECT
              e.employee_id,
              (e.first_name + ' ' + e.last_name) AS employee_name,
              e.doc_number,
              w.work_date,
              w.reason,
              w.is_active,
              w.created_at
            FROM rrhh.wfh_day w
            JOIN rrhh.hr_employee e ON e.employee_id = w.employee_id
            JOIN rrhh.hr_employee_manager mm
              ON mm.employee_id = w.employee_id
             AND mm.is_primary = 1
             AND mm.manager_employee_id = ?
             AND mm.valid_from <= w.work_date
             AND (mm.valid_to IS NULL OR mm.valid_to >= w.work_date)
            WHERE w.is_active = 1
              AND w.created_at >= DATEADD(day, -60, GETDATE())
              AND w.employee_id <> ?
            ORDER BY w.created_at DESC
            """,
            (int(my_emp_id), int(my_emp_id)),
        )

    return render_template(
        "modulos/trabajo_casa_aprobaciones.html",
        pendientes=pendientes or [],
        aprobadas=aprobadas or [],
        is_backoffice=is_backoffice,
    )


@modulos_bp.route("/trabajo-casa/aprobar", methods=["POST"])
@login_required
def trabajo_casa_aprobar():
    employee_id = int(request.form.get("employee_id"))
    work_date = datetime.strptime(request.form.get("work_date"), "%Y-%m-%d").date()

    if not _can_approve_wfh(employee_id, work_date):
        flash("No tienes permiso para aprobar esta solicitud.", "warning")
        return redirect(url_for("modulos.trabajo_casa_aprobaciones"))

    ok, msg = _validate_wfh_rules(int(employee_id), work_date)
    if not ok:
        flash(f"No se puede aprobar: {msg}", "warning")
        return redirect(url_for("modulos.trabajo_casa_aprobaciones"))

    try:
        rc = execute(
            "UPDATE rrhh.wfh_day SET is_active=1 WHERE employee_id=? AND work_date=? AND is_active=0",
            (employee_id, work_date),
        )
        if rc:
            flash("Solicitud aprobada.", "success")
        else:
            flash("La solicitud no estaba pendiente o no existe.", "warning")
    except Exception as ex:
        flash(f"No se pudo aprobar: {ex}", "danger")

    return redirect(url_for("modulos.trabajo_casa_aprobaciones"))


@modulos_bp.route("/trabajo-casa/rechazar", methods=["POST"])
@login_required
def trabajo_casa_rechazar():
    employee_id = int(request.form.get("employee_id"))
    work_date = datetime.strptime(request.form.get("work_date"), "%Y-%m-%d").date()

    if not _can_approve_wfh(employee_id, work_date):
        flash("No tienes permiso para rechazar esta solicitud.", "warning")
        return redirect(url_for("modulos.trabajo_casa_aprobaciones"))

    try:
        rc = execute(
            "DELETE FROM rrhh.wfh_day WHERE employee_id=? AND work_date=? AND is_active=0",
            (employee_id, work_date),
        )
        if rc:
            flash("Solicitud rechazada.", "success")
        else:
            flash("La solicitud no estaba pendiente o no existe.", "warning")
    except Exception as ex:
        flash(f"No se pudo rechazar: {ex}", "danger")

    return redirect(url_for("modulos.trabajo_casa_aprobaciones"))
