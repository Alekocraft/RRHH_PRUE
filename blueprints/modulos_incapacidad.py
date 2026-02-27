from __future__ import annotations

from datetime import datetime

from flask import render_template, request, redirect, url_for, flash
from flask_login import login_required, current_user

from services.rrhh_db import fetch_one, fetch_all, execute, execute_scalar

from .modulos import modulos_bp
from .modulos_common import _is_admin_or_rrhh


REQUEST_TYPE_INCAPACIDAD = "INCAPACIDAD"


def _incap_tables_exist() -> bool:
    try:
        return bool(
            fetch_one(
                "SELECT 1 FROM INFORMATION_SCHEMA.TABLES "
                "WHERE TABLE_SCHEMA='rrhh' AND TABLE_NAME='medical_leave_request'"
            )
        )
    except Exception:
        return False


def _submit_request_hr_only(request_id: int, actor_user_id: int) -> None:
    """Envía un wf_request al flujo asignando SOLO a RRHH (sin paso de jefe)."""

    emp = fetch_one(
        "SELECT employee_id FROM rrhh.wf_request WHERE request_id=? AND status='DRAFT'",
        (int(request_id),),
    )
    if not emp:
        raise Exception("Solicitud no existe o no está en DRAFT")

    hr = fetch_one(
        "SELECT TOP (1) user_id FROM rrhh.hr_approver_pool WHERE is_active=1 ORDER BY user_id"
    )
    if not hr:
        raise Exception("No hay aprobadores RRHH activos en hr_approver_pool")

    execute(
        "UPDATE rrhh.wf_request SET status='SUBMITTED', submitted_at=GETDATE() WHERE request_id=?",
        (int(request_id),),
    )
    execute(
        "INSERT INTO rrhh.wf_action(request_id, step_no, actor_user_id, action, comment) "
        "VALUES (?, NULL, ?, 'SUBMIT', NULL)",
        (int(request_id), int(actor_user_id)),
    )
    execute("DELETE FROM rrhh.wf_request_step WHERE request_id=?", (int(request_id),))
    execute(
        "INSERT INTO rrhh.wf_request_step(request_id, step_no, assigned_to_user_id) VALUES (?, 1, ?)",
        (int(request_id), int(hr.user_id)),
    )


@modulos_bp.route("/incapacidad")
@login_required
def incapacidad():
    """Vista del colaborador: histórico de incapacidad médica."""
    employee_id = getattr(current_user, "employee_id", None)
    if not employee_id:
        flash("Tu usuario no está asociado a un empleado. Contacta a RRHH.", "warning")
        return redirect(url_for("modulos.dashboard"))

    if not _incap_tables_exist():
        flash(
            "Falta crear la tabla rrhh.medical_leave_request. Ejecuta el script SQL del módulo de incapacidad.",
            "danger",
        )
        return redirect(url_for("modulos.dashboard"))

    rows = fetch_all(
        "SELECT TOP 50 "
        "  r.request_id, r.status, r.created_at, r.submitted_at, r.closed_at, "
        "  d.start_date, d.end_date, d.notes "
        "FROM rrhh.wf_request r "
        "JOIN rrhh.medical_leave_request d ON d.request_id=r.request_id "
        "WHERE r.employee_id=? AND r.request_type=? "
        "ORDER BY r.created_at DESC",
        (int(employee_id), REQUEST_TYPE_INCAPACIDAD),
    )

    return render_template(
        "modulos/incapacidad.html",
        items=rows or [],
        is_backoffice=_is_admin_or_rrhh(),
        es_jefe=bool(getattr(current_user, "es_jefe", False)),
    )


@modulos_bp.route("/incapacidad/nueva", methods=["GET", "POST"])
@login_required
def incapacidad_nueva():
    """Registro por colaborador. RRHH aprueba. Jefe solo visualiza."""
    employee_id = getattr(current_user, "employee_id", None)
    if not employee_id:
        flash("Tu usuario no está asociado a un empleado. Contacta a RRHH.", "warning")
        return redirect(url_for("modulos.dashboard"))

    if not _incap_tables_exist():
        flash(
            "Falta crear la tabla rrhh.medical_leave_request. Ejecuta el script SQL del módulo de incapacidad.",
            "danger",
        )
        return redirect(url_for("modulos.dashboard"))

    if request.method == "POST":
        start_s = (request.form.get("start_date") or "").strip()
        end_s = (request.form.get("end_date") or "").strip()
        notes = (request.form.get("notes") or "").strip()

        try:
            start_date = datetime.strptime(start_s, "%Y-%m-%d").date()
            end_date = datetime.strptime(end_s, "%Y-%m-%d").date()
        except Exception:
            flash("Fechas inválidas.", "warning")
            return redirect(url_for("modulos.incapacidad_nueva"))

        if end_date < start_date:
            flash("La fecha fin no puede ser menor a la fecha inicio.", "warning")
            return redirect(url_for("modulos.incapacidad_nueva"))

        try:
            request_id = int(
                execute_scalar(
                    "INSERT INTO rrhh.wf_request(request_type, employee_id, created_by_user_id) "
                    "OUTPUT INSERTED.request_id "
                    "VALUES (?, ?, ?)",
                    (REQUEST_TYPE_INCAPACIDAD, int(employee_id), int(current_user.user_id)),
                )
            )

            execute(
                "INSERT INTO rrhh.medical_leave_request(request_id, start_date, end_date, notes) "
                "VALUES (?, ?, ?, ?)",
                (int(request_id), start_date, end_date, notes if notes else None),
            )

            _submit_request_hr_only(int(request_id), int(current_user.user_id))

            flash("Incapacidad registrada y enviada a RRHH para aprobación.", "success")
            flash("Recuerde enviar el soporte (incapacidad médica) directamente al correo de RRHH.", "warning")
            return redirect(url_for("modulos.incapacidad"))

        except Exception as ex:
            flash(f"No se pudo registrar la incapacidad: {ex}", "danger")
            return redirect(url_for("modulos.incapacidad_nueva"))

    return render_template("modulos/incapacidad_nueva.html")


@modulos_bp.route("/incapacidad/equipo")
@login_required
def incapacidad_equipo():
    """Vista del jefe: solo lectura de incapacidades de su equipo."""
    employee_id = getattr(current_user, "employee_id", None)
    if not employee_id:
        flash("Tu usuario no está asociado a un empleado. Contacta a RRHH.", "warning")
        return redirect(url_for("modulos.dashboard"))

    is_backoffice = _is_admin_or_rrhh()
    es_jefe = bool(getattr(current_user, "es_jefe", False))

    if not (is_backoffice or es_jefe):
        flash("No tienes permisos para ver incapacidades de equipo.", "warning")
        return redirect(url_for("modulos.dashboard"))

    if not _incap_tables_exist():
        flash(
            "Falta crear la tabla rrhh.medical_leave_request. Ejecuta el script SQL del módulo de incapacidad.",
            "danger",
        )
        return redirect(url_for("modulos.dashboard"))

    if is_backoffice:
        rows = fetch_all(
            "SELECT TOP 200 "
            "  r.request_id, r.status, r.created_at, r.submitted_at, r.closed_at, "
            "  d.start_date, d.end_date, d.notes, "
            "  e.employee_id, e.doc_number, (e.first_name + ' ' + e.last_name) AS employee_name "
            "FROM rrhh.wf_request r "
            "JOIN rrhh.medical_leave_request d ON d.request_id=r.request_id "
            "JOIN rrhh.hr_employee e ON e.employee_id=r.employee_id "
            "WHERE r.request_type=? "
            "ORDER BY r.created_at DESC",
            (REQUEST_TYPE_INCAPACIDAD,),
        )
    else:
        rows = fetch_all(
            "SELECT TOP 200 "
            "  r.request_id, r.status, r.created_at, r.submitted_at, r.closed_at, "
            "  d.start_date, d.end_date, d.notes, "
            "  e.employee_id, e.doc_number, (e.first_name + ' ' + e.last_name) AS employee_name "
            "FROM rrhh.wf_request r "
            "JOIN rrhh.medical_leave_request d ON d.request_id=r.request_id "
            "JOIN rrhh.hr_employee e ON e.employee_id=r.employee_id "
            "JOIN rrhh.hr_employee_manager mm ON mm.employee_id = r.employee_id "
            "WHERE r.request_type=? "
            "  AND mm.manager_employee_id=? "
            "  AND mm.is_primary=1 "
            "  AND mm.valid_from <= d.start_date "
            "  AND (mm.valid_to IS NULL OR mm.valid_to >= d.start_date) "
            "ORDER BY r.created_at DESC",
            (REQUEST_TYPE_INCAPACIDAD, int(employee_id)),
        )

    return render_template(
        "modulos/incapacidad_equipo.html",
        items=rows or [],
        is_backoffice=is_backoffice,
    )


@modulos_bp.route("/incapacidad/aprobaciones")
@login_required
def incapacidad_aprobaciones():
    """Bandeja RRHH/Admin: pasos pendientes de incapacidad médica."""
    if not _is_admin_or_rrhh():
        flash("No tienes permisos para aprobar incapacidades.", "warning")
        return redirect(url_for("modulos.dashboard"))

    if not _incap_tables_exist():
        flash(
            "Falta crear la tabla rrhh.medical_leave_request. Ejecuta el script SQL del módulo de incapacidad.",
            "danger",
        )
        return redirect(url_for("modulos.dashboard"))

    steps = fetch_all(
        "SELECT s.step_id, s.request_id, s.step_no, s.assigned_to_user_id, s.status AS step_status, "
        "       r.status AS request_status, r.created_at, r.submitted_at, "
        "       e.employee_id, e.doc_number, (e.first_name + ' ' + e.last_name) AS employee_name, "
        "       d.start_date, d.end_date, d.notes "
        "FROM rrhh.wf_request_step s "
        "JOIN rrhh.wf_request r ON r.request_id=s.request_id "
        "JOIN rrhh.medical_leave_request d ON d.request_id=r.request_id "
        "JOIN rrhh.hr_employee e ON e.employee_id=r.employee_id "
        "WHERE r.request_type=? AND r.status='SUBMITTED' AND s.status='PENDING' "
        "ORDER BY r.submitted_at DESC, r.request_id DESC",
        (REQUEST_TYPE_INCAPACIDAD,),
    )

    return render_template("modulos/incapacidad_aprobaciones.html", steps=steps or [])


@modulos_bp.route("/incapacidad/aprobaciones/accion", methods=["POST"])
@login_required
def incapacidad_aprobaciones_accion():
    if not _is_admin_or_rrhh():
        flash("No tienes permisos para aprobar incapacidades.", "warning")
        return redirect(url_for("modulos.dashboard"))

    step_id = int(request.form.get("step_id") or 0)
    action = (request.form.get("action") or "").strip().upper()
    comment = (request.form.get("comment") or "").strip()
    if action not in ("APPROVE", "REJECT"):
        flash("Acción inválida.", "warning")
        return redirect(url_for("modulos.incapacidad_aprobaciones"))

    step = fetch_one(
        "SELECT s.step_id, s.request_id, s.step_no, s.status, r.request_type "
        "FROM rrhh.wf_request_step s "
        "JOIN rrhh.wf_request r ON r.request_id=s.request_id "
        "WHERE s.step_id=?",
        (int(step_id),),
    )
    if not step or step.request_type != REQUEST_TYPE_INCAPACIDAD:
        flash("Paso no encontrado.", "warning")
        return redirect(url_for("modulos.incapacidad_aprobaciones"))
    if step.status != "PENDING":
        flash("El paso ya fue gestionado.", "warning")
        return redirect(url_for("modulos.incapacidad_aprobaciones"))

    if action == "APPROVE":
        execute(
            "UPDATE rrhh.wf_request_step SET status='APPROVED', acted_at=GETDATE(), comment=? WHERE step_id=?",
            (comment if comment else None, int(step_id)),
        )
        execute(
            "INSERT INTO rrhh.wf_action(request_id, step_no, actor_user_id, action, comment) "
            "VALUES (?, ?, ?, 'APPROVE', ?)",
            (
                int(step.request_id),
                int(step.step_no),
                int(current_user.user_id),
                comment if comment else None,
            ),
        )
        execute(
            "UPDATE rrhh.wf_request SET status='APPROVED', closed_at=GETDATE() WHERE request_id=?",
            (int(step.request_id),),
        )
        execute(
            "UPDATE rrhh.wf_request_step SET status='SKIPPED', acted_at=GETDATE() "
            "WHERE request_id=? AND status='PENDING'",
            (int(step.request_id),),
        )
        flash("Incapacidad aprobada.", "success")
    else:
        execute(
            "UPDATE rrhh.wf_request_step SET status='REJECTED', acted_at=GETDATE(), comment=? WHERE step_id=?",
            (comment if comment else None, int(step_id)),
        )
        execute(
            "INSERT INTO rrhh.wf_action(request_id, step_no, actor_user_id, action, comment) "
            "VALUES (?, ?, ?, 'REJECT', ?)",
            (
                int(step.request_id),
                int(step.step_no),
                int(current_user.user_id),
                comment if comment else None,
            ),
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
        flash("Incapacidad rechazada.", "success")

    return redirect(url_for("modulos.incapacidad_aprobaciones"))
