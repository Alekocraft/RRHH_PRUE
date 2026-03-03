from __future__ import annotations

from datetime import date, datetime, timedelta

from flask import render_template, request, redirect, url_for, flash
from flask_login import login_required, current_user

from services.rrhh_db import fetch_one, fetch_all, execute, execute_scalar, call_proc

from .modulos import modulos_bp
from .modulos_common import _is_admin_or_rrhh


REQUEST_TYPE_HORA_FLEXIBLE = "HORA_FLEXIBLE"


def _weekday_label(wd: int) -> str:
    return {
        1: "Lunes",
        2: "Martes",
        3: "Miércoles",
        4: "Jueves",
        5: "Viernes",
        6: "Sábado",
        7: "Domingo",
    }.get(int(wd), f"{wd}")


def _flex_tables_exist() -> bool:
    r1 = fetch_one(
        "SELECT 1 FROM INFORMATION_SCHEMA.TABLES "
        "WHERE TABLE_SCHEMA='rrhh' AND TABLE_NAME='time_flexible_rule'"
    )
    r2 = fetch_one(
        "SELECT 1 FROM INFORMATION_SCHEMA.TABLES "
        "WHERE TABLE_SCHEMA='rrhh' AND TABLE_NAME='time_flexible_rule_request'"
    )
    return bool(r1 and r2)


def _get_active_flex_rule(employee_id: int):
    if not employee_id:
        return None
    if not fetch_one(
        "SELECT 1 FROM INFORMATION_SCHEMA.TABLES "
        "WHERE TABLE_SCHEMA='rrhh' AND TABLE_NAME='time_flexible_rule'"
    ):
        return None

    return fetch_one(
        "SELECT TOP (1) rule_id, employee_id, weekday, slot, minutes, valid_from, valid_to, is_active, created_at "
        "FROM rrhh.time_flexible_rule "
        "WHERE employee_id=? AND is_active=1 AND valid_to IS NULL "
        "ORDER BY rule_id DESC",
        (int(employee_id),),
    )


def _count_open_flex_requests(employee_id: int) -> int:
    if not employee_id:
        return 0
    row = fetch_one(
        "SELECT COUNT(1) AS c "
        "FROM rrhh.wf_request "
        "WHERE employee_id=? AND request_type=? AND status IN ('DRAFT','SUBMITTED')",
        (int(employee_id), REQUEST_TYPE_HORA_FLEXIBLE),
    )
    return int(getattr(row, "c", 0) or 0) if row else 0


def _apply_hora_flexible_rule_from_request(request_id: int, actor_user_id: int):
    """Aplica la regla periódica cuando el workflow queda APPROVED (último paso)."""
    req = fetch_one(
        "SELECT request_id, employee_id, created_by_user_id "
        "FROM rrhh.wf_request "
        "WHERE request_id=? AND request_type=? AND status='APPROVED'",
        (int(request_id), REQUEST_TYPE_HORA_FLEXIBLE),
    )
    if not req:
        raise ValueError("La solicitud no está APPROVED o no es HORA_FLEXIBLE")

    det = fetch_one(
        "SELECT weekday, slot, valid_from "
        "FROM rrhh.time_flexible_rule_request "
        "WHERE request_id=?",
        (int(request_id),),
    )
    if not det:
        raise ValueError("No existe detalle rrhh.time_flexible_rule_request para esta solicitud")

    employee_id = int(req.employee_id)
    weekday = int(det.weekday)
    slot = str(det.slot)
    valid_from = det.valid_from

    old = fetch_one(
        "SELECT TOP (1) rule_id, valid_from "
        "FROM rrhh.time_flexible_rule "
        "WHERE employee_id=? AND is_active=1 AND valid_to IS NULL "
        "ORDER BY rule_id DESC",
        (employee_id,),
    )
    if old:
        close_to = valid_from - timedelta(days=1)
        try:
            if close_to < old.valid_from:
                close_to = old.valid_from
        except Exception:
            pass

        execute(
            "UPDATE rrhh.time_flexible_rule "
            "SET valid_to=?, is_active=0, closed_by_user_id=?, closed_at=GETDATE() "
            "WHERE rule_id=?",
            (close_to, int(actor_user_id), int(old.rule_id)),
        )

    execute(
        "INSERT INTO rrhh.time_flexible_rule("
        "employee_id, weekday, slot, minutes, valid_from, valid_to, is_active, "
        "created_by_user_id, source_request_id"
        ") VALUES (?, ?, ?, 60, ?, NULL, 1, ?, ?)",
        (employee_id, weekday, slot, valid_from, int(req.created_by_user_id), int(request_id)),
    )


@modulos_bp.route("/hora-flexible")
@login_required
def hora_flexible():
    employee_id = getattr(current_user, "employee_id", None)
    if not employee_id:
        flash("Tu usuario no está asociado a un empleado. Contacta a RRHH.", "warning")
        return redirect(url_for("modulos.dashboard"))

    employee_id = int(employee_id)
    rule = _get_active_flex_rule(employee_id)
    open_count = _count_open_flex_requests(employee_id)

    req_history = []
    rule_history = []
    try:
        if _flex_tables_exist():
            req_history = fetch_all(
                "SELECT TOP 50 "
                "  r.request_id, r.status, r.created_at, r.submitted_at, r.closed_at, "
                "  d.weekday, d.slot, d.valid_from "
                "FROM rrhh.wf_request r "
                "LEFT JOIN rrhh.time_flexible_rule_request d ON d.request_id = r.request_id "
                "WHERE r.employee_id=? AND r.request_type=? "
                "ORDER BY r.created_at DESC",
                (employee_id, REQUEST_TYPE_HORA_FLEXIBLE),
            )

        if fetch_one(
            "SELECT 1 FROM INFORMATION_SCHEMA.TABLES "
            "WHERE TABLE_SCHEMA='rrhh' AND TABLE_NAME='time_flexible_rule'"
        ):
            rule_history = fetch_all(
                "SELECT TOP 20 rule_id, weekday, slot, minutes, valid_from, valid_to, created_at, source_request_id "
                "FROM rrhh.time_flexible_rule "
                "WHERE employee_id=? "
                "ORDER BY valid_from DESC, rule_id DESC",
                (employee_id,),
            )
    except Exception:
        req_history = req_history or []
        rule_history = rule_history or []

    return render_template(
        "modulos/hora_flexible.html",
        rule=rule,
        weekday_label=_weekday_label,
        open_request_count=open_count,
        req_history=req_history or [],
        rule_history=rule_history or [],
        is_backoffice=_is_admin_or_rrhh(),
        es_jefe=bool(getattr(current_user, "es_jefe", False)),
    )


@modulos_bp.route("/hora-flexible/nueva", methods=["GET", "POST"])
@login_required
def hora_flexible_nueva():
    employee_id = getattr(current_user, "employee_id", None)
    if not employee_id:
        flash("Tu usuario no está asociado a un empleado. Contacta a RRHH.", "warning")
        return redirect(url_for("modulos.dashboard"))

    if not _flex_tables_exist():
        flash(
            "Falta crear tablas de Hora flexible periódica (time_flexible_rule / time_flexible_rule_request). Ejecuta el script SQL.",
            "danger",
        )
        return redirect(url_for("modulos.hora_flexible"))

    employee_id = int(employee_id)
    rule = _get_active_flex_rule(employee_id)

    today = date.today()
    if rule:
        min_from = today + timedelta(days=1)
        try:
            if rule.valid_from and min_from <= rule.valid_from:
                min_from = rule.valid_from + timedelta(days=1)
        except Exception:
            pass
    else:
        min_from = today

    default_from = min_from

    open_count = _count_open_flex_requests(employee_id)
    if open_count > 0 and request.method == "GET":
        flash(
            "Ya tienes una solicitud de Hora flexible en curso. Espera a que sea aprobada o rechazada para crear otra.",
            "warning",
        )

    if request.method == "POST":
        if _count_open_flex_requests(employee_id) > 0:
            flash("Ya tienes una solicitud de Hora flexible en curso. No puedes crear otra.", "warning")
            return redirect(url_for("modulos.hora_flexible"))

        weekday = int(request.form.get("weekday") or 0)
        slot = (request.form.get("slot") or "").strip().upper()
        valid_from_s = (request.form.get("valid_from") or "").strip()

        if weekday not in (1, 2, 3, 4, 5, 6, 7):
            flash("Día de la semana inválido.", "warning")
            return redirect(url_for("modulos.hora_flexible_nueva"))

        if slot not in ("AM", "PM"):
            flash("Horario inválido (AM/PM).", "warning")
            return redirect(url_for("modulos.hora_flexible_nueva"))

        try:
            valid_from = datetime.strptime(valid_from_s, "%Y-%m-%d").date()
        except Exception:
            flash("Fecha 'Desde' inválida.", "warning")
            return redirect(url_for("modulos.hora_flexible_nueva"))

        if valid_from < today:
            flash("La fecha 'Desde' no puede estar en el pasado.", "warning")
            return redirect(url_for("modulos.hora_flexible_nueva"))

        if rule:
            if valid_from <= today:
                flash(
                    "Si ya tienes Hora flexible aprobada, la modificación debe iniciar con una fecha futura.",
                    "warning",
                )
                return redirect(url_for("modulos.hora_flexible_nueva"))
            try:
                if rule.valid_from and valid_from <= rule.valid_from:
                    flash(
                        f"La modificación debe iniciar después de la vigencia actual (desde {rule.valid_from}).",
                        "warning",
                    )
                    return redirect(url_for("modulos.hora_flexible_nueva"))
            except Exception:
                pass

        request_id = int(
            execute_scalar(
                "INSERT INTO rrhh.wf_request(request_type, employee_id, created_by_user_id) "
                "OUTPUT INSERTED.request_id "
                "VALUES (?, ?, ?)",
                (REQUEST_TYPE_HORA_FLEXIBLE, employee_id, int(current_user.user_id)),
            )
        )

        execute(
            "INSERT INTO rrhh.time_flexible_rule_request(request_id, weekday, slot, valid_from) VALUES (?, ?, ?, ?)",
            (request_id, weekday, slot, valid_from),
        )

        try:
            call_proc("rrhh.sp_submit_request", [request_id, int(current_user.user_id)])
            flash("Solicitud enviada al flujo de aprobaciones.", "success")
        except Exception as ex:
            flash(f"No se pudo enviar la solicitud al workflow: {ex}", "danger")

        return redirect(url_for("modulos.hora_flexible"))

    return render_template(
        "modulos/hora_flexible_nueva.html",
        rule=rule,
        weekday_label=_weekday_label,
        default_from=default_from.isoformat(),
        min_from=min_from.isoformat(),
    )


@modulos_bp.route("/hora-flexible/aprobaciones")
@login_required
def aprobaciones():
    """Bandeja de aprobaciones para Hora flexible (workflow rrhh.wf_*)."""
    is_backoffice = _is_admin_or_rrhh()
    es_jefe = bool(getattr(current_user, "es_jefe", False))

    if not (is_backoffice or es_jefe):
        flash("No tienes permisos para ver la bandeja de aprobaciones.", "warning")
        return redirect(url_for("modulos.dashboard"))

    if not _flex_tables_exist():
        flash(
            "Falta crear tablas de Hora flexible periódica (time_flexible_rule / time_flexible_rule_request).",
            "danger",
        )
        return redirect(url_for("modulos.dashboard"))

    base_sql = (
        "SELECT "
        "  s.step_id, s.request_id, s.step_no, s.assigned_to_user_id, s.status, "
        "  r.employee_id, r.created_at, "
        "  e.doc_number, (e.first_name + ' ' + e.last_name) AS employee_name, "
        "  d.weekday, d.slot, d.valid_from "
        "FROM rrhh.wf_request_step s "
        "JOIN rrhh.wf_request r ON r.request_id = s.request_id "
        "JOIN rrhh.time_flexible_rule_request d ON d.request_id = r.request_id "
        "JOIN rrhh.hr_employee e ON e.employee_id = r.employee_id "
        "WHERE r.request_type=? "
        "  AND r.status='SUBMITTED' "
        "  AND s.status='PENDING' "
        "  AND s.step_no = ("
        "    SELECT MIN(s2.step_no) "
        "    FROM rrhh.wf_request_step s2 "
        "    WHERE s2.request_id = s.request_id AND s2.status='PENDING'"
        "  ) "
    )

    params = [REQUEST_TYPE_HORA_FLEXIBLE]
    if not is_backoffice:
        base_sql += " AND s.assigned_to_user_id=? "
        params.append(int(current_user.user_id))

    base_sql += " ORDER BY r.created_at DESC"
    steps = fetch_all(base_sql, tuple(params))

    requests = []
    if is_backoffice:
        requests = fetch_all(
            "SELECT TOP 200 "
            "  r.request_id, r.status, r.employee_id, r.created_at, r.closed_at, "
            "  e.doc_number, (e.first_name + ' ' + e.last_name) AS employee_name, "
            "  d.weekday, d.slot, d.valid_from "
            "FROM rrhh.wf_request r "
            "JOIN rrhh.time_flexible_rule_request d ON d.request_id = r.request_id "
            "JOIN rrhh.hr_employee e ON e.employee_id = r.employee_id "
            "WHERE r.request_type=? "
            "ORDER BY r.created_at DESC",
            (REQUEST_TYPE_HORA_FLEXIBLE,),
        )

    return render_template(
        "modulos/aprobaciones.html",
        steps=steps or [],
        requests=requests or [],
        is_backoffice=is_backoffice,
        weekday_label=_weekday_label,
    )


@modulos_bp.route("/hora-flexible/aprobaciones/accion", methods=["POST"])
@login_required
def aprobaciones_accion():
    is_backoffice = _is_admin_or_rrhh()

    step_id = int(request.form.get("step_id"))
    action = (request.form.get("action") or "").strip().upper()
    comment = (request.form.get("comment") or "").strip() or None

    step = fetch_one(
        "SELECT step_id, request_id, step_no, assigned_to_user_id, status "
        "FROM rrhh.wf_request_step WHERE step_id=?",
        (int(step_id),),
    )
    if not step or str(step.status).upper() != "PENDING":
        flash("El paso no existe o ya fue gestionado.", "warning")
        return redirect(url_for("modulos.aprobaciones"))

    row_min = fetch_one(
        "SELECT MIN(step_no) AS m FROM rrhh.wf_request_step WHERE request_id=? AND status='PENDING'",
        (int(step.request_id),),
    )
    if row_min and int(step.step_no) != int(getattr(row_min, "m", step.step_no) or step.step_no):
        flash("Este paso no es el paso activo actual para la solicitud.", "warning")
        return redirect(url_for("modulos.aprobaciones"))

    if (not is_backoffice) and int(step.assigned_to_user_id) != int(current_user.user_id):
        flash("No tienes permiso para gestionar este paso.", "warning")
        return redirect(url_for("modulos.aprobaciones"))

    if action not in ("APPROVE", "REJECT"):
        flash("Acción inválida.", "warning")
        return redirect(url_for("modulos.aprobaciones"))

    if action == "APPROVE":
        execute(
            "UPDATE rrhh.wf_request_step SET status='APPROVED', acted_at=GETDATE(), comment=? WHERE step_id=?",
            (comment, int(step_id)),
        )
        execute(
            "INSERT INTO rrhh.wf_action(request_id, step_no, actor_user_id, action, comment) VALUES (?, ?, ?, 'APPROVE', ?)",
            (int(step.request_id), int(step.step_no), int(current_user.user_id), comment),
        )

        row_left = fetch_one(
            "SELECT COUNT(1) AS c FROM rrhh.wf_request_step WHERE request_id=? AND status='PENDING'",
            (int(step.request_id),),
        )
        left = int(getattr(row_left, "c", 0) or 0) if row_left else 0
        if left == 0:
            execute(
                "UPDATE rrhh.wf_request SET status='APPROVED', closed_at=GETDATE() WHERE request_id=?",
                (int(step.request_id),),
            )
            try:
                _apply_hora_flexible_rule_from_request(int(step.request_id), int(current_user.user_id))
            except Exception as ex:
                flash(f"Workflow aprobado, pero no se pudo aplicar la regla periódica: {ex}", "danger")
                return redirect(url_for("modulos.aprobaciones"))

            flash("Solicitud aprobada y regla aplicada.", "success")
        else:
            flash("Paso aprobado.", "success")

        return redirect(url_for("modulos.aprobaciones"))

    # REJECT
    execute(
        "UPDATE rrhh.wf_request_step SET status='REJECTED', acted_at=GETDATE(), comment=? WHERE step_id=?",
        (comment, int(step_id)),
    )
    execute(
        "INSERT INTO rrhh.wf_action(request_id, step_no, actor_user_id, action, comment) VALUES (?, ?, ?, 'REJECT', ?)",
        (int(step.request_id), int(step.step_no), int(current_user.user_id), comment),
    )
    execute(
        "UPDATE rrhh.wf_request SET status='REJECTED', closed_at=GETDATE() WHERE request_id=?",
        (int(step.request_id),),
    )
    execute(
        "UPDATE rrhh.wf_request_step SET status='SKIPPED', acted_at=GETDATE() "
        "WHERE request_id=? AND status='PENDING'",
        (int(step.request_id),),
    )

    flash("Solicitud rechazada.", "success")
    return redirect(url_for("modulos.aprobaciones"))
