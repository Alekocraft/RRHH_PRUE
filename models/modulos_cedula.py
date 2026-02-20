from flask import Blueprint, render_template, redirect, url_for, request, flash
from flask_login import login_required, current_user

from services.rrhh_db import fetch_all, execute, execute_scalar, call_proc
from services.upload import save_upload

modulos_bp = Blueprint("modulos", __name__, url_prefix="")


def _ensure_doc_number_column():
    """Asegura que exista rrhh.hr_employee.doc_number (cédula) para dejar de depender de employee_code."""
    try:
        execute("IF COL_LENGTH('rrhh.hr_employee','doc_number') IS NULL ALTER TABLE rrhh.hr_employee ADD doc_number NVARCHAR(20) NULL;")
    except Exception:
        # En ambientes sin permisos DDL, la columna debe crearse por script de migración.
        pass


def _requiere_perfil_empleado_si_aplica():
    if (current_user.employee_id is None) and (not current_user.es_administrador) and (not current_user.es_rrhh):
        return redirect(url_for("perfil_pendiente"))
    return None

def _require_admin():
    if not (current_user.es_administrador or current_user.es_rrhh):
        flash("No tienes permisos para esta acción.", "error")
        return False
    return True

@modulos_bp.route("/asistencia")
@login_required
def asistencia():
    redir = _requiere_perfil_empleado_si_aplica()
    if redir:
        return redir

    batches = fetch_all(
        "SELECT TOP (50) batch_id, year_no, month_no, file_name, uploaded_at, status "
        "FROM rrhh.att_import_batch ORDER BY batch_id DESC"
    )
    return render_template(
        "modulos/asistencia.html",
        puede_admin=(current_user.es_administrador or current_user.es_rrhh),
        batches=batches
    )

@modulos_bp.route("/asistencia/cargar", methods=["GET", "POST"])
@login_required
def asistencia_cargar():
    redir = _requiere_perfil_empleado_si_aplica()
    if redir:
        return redir
    if not _require_admin():
        return redirect(url_for("modulos.asistencia"))

    if request.method == "POST":
        try:
            year_no = int(request.form.get("year_no"))
            month_no = int(request.form.get("month_no"))
            f = request.files.get("archivo")
            if not f or not f.filename:
                flash("Debes seleccionar un archivo.", "error")
                return render_template("modulos/asistencia_cargar.html")

            storage_path = save_upload(f, prefix="asistencia_")

            file_id = execute_scalar(
                "INSERT INTO rrhh.sys_attachment(file_name, mime_type, size_bytes, storage_path, uploaded_by_user_id) "
                "OUTPUT INSERTED.file_id "
                "VALUES (?, ?, ?, ?, ?)",
                (f.filename, f.mimetype or "application/octet-stream", int(request.content_length or 0), storage_path, int(current_user.user_db_id))
            )

            batch_id = execute_scalar(
                "INSERT INTO rrhh.att_import_batch(year_no, month_no, file_name, file_id, uploaded_by_user_id, status) "
                "OUTPUT INSERTED.batch_id "
                "VALUES (?, ?, ?, ?, ?, 'UPLOADED')",
                (year_no, month_no, f.filename, int(file_id), int(current_user.user_db_id))
            )

            flash(f"Archivo cargado. Batch creado: {batch_id}. (Falta parseo de filas, siguiente paso)", "success")
            return redirect(url_for("modulos.asistencia"))

        except Exception as e:
            flash(f"Error cargando archivo: {e}", "error")

    return render_template("modulos/asistencia_cargar.html")

@modulos_bp.route("/asistencia/aplicar/<int:batch_id>", methods=["POST"])
@login_required
def asistencia_aplicar(batch_id: int):
    redir = _requiere_perfil_empleado_si_aplica()
    if redir:
        return redir
    if not _require_admin():
        return redirect(url_for("modulos.asistencia"))

    try:
        call_proc("EXEC rrhh.sp_apply_attendance_batch ?, ?", (batch_id, int(current_user.user_db_id)))
        flash(f"Batch {batch_id} aplicado (estado APPLIED).", "success")
    except Exception as e:
        flash(f"No se pudo aplicar batch {batch_id}: {e}", "error")

    return redirect(url_for("modulos.asistencia"))

@modulos_bp.route("/asistencia/ajuste", methods=["GET", "POST"])
@login_required
def asistencia_ajuste():
    redir = _requiere_perfil_empleado_si_aplica()
    if redir:
        return redir
    if not _require_admin():
        return redirect(url_for("modulos.asistencia"))

    _ensure_doc_number_column()

    empleados = fetch_all(
        "SELECT employee_id, doc_number, first_name, last_name FROM rrhh.hr_employee WHERE is_active=1 ORDER BY doc_number, first_name"
    )

    if request.method == "POST":
        try:
            employee_id = int(request.form.get("employee_id"))
            work_date = request.form.get("work_date")
            first_in = request.form.get("first_in") or None
            last_out = request.form.get("last_out") or None
            total_minutes = request.form.get("total_minutes") or None
            reason = (request.form.get("reason") or "").strip() or "Ajuste RRHH"
            comment = (request.form.get("comment") or "").strip() or None

            execute(
                "INSERT INTO rrhh.att_manual_override(employee_id, work_date, first_in, last_out, total_minutes, reason, comment, created_by_user_id, is_active) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)",
                (employee_id, work_date, first_in, last_out, total_minutes, reason, comment, int(current_user.user_db_id))
            )

            flash("Ajuste guardado. (Se refleja en vw_attendance_effective)", "success")
            return redirect(url_for("modulos.asistencia"))
        except Exception as e:
            flash(f"Error guardando ajuste: {e}", "error")

    return render_template("modulos/asistencia_ajuste.html", empleados=empleados)

@modulos_bp.route("/asistencia/exportar", methods=["POST"])
@login_required
def asistencia_exportar():
    redir = _requiere_perfil_empleado_si_aplica()
    if redir:
        return redir
    if not _require_admin():
        return redirect(url_for("modulos.asistencia"))

    try:
        report_code = "ASISTENCIA_MENSUAL"
        params_json = request.form.get("params_json") or "{}"
        fmt = request.form.get("format") or "XLSX"

        execute(
            "INSERT INTO rrhh.report_export_log(report_code, params_json, requested_by_user_id, format, status) "
            "VALUES (?, ?, ?, ?, 'CREATED')",
            (report_code, params_json, int(current_user.user_db_id), fmt)
        )
        flash(f"Exportación registrada en log (formato {fmt}). Generación XLSX/PDF: siguiente paso.", "success")
    except Exception as e:
        flash(f"Error registrando exportación: {e}", "error")

    return redirect(url_for("modulos.asistencia"))

@modulos_bp.route("/turnos")
@login_required
def turnos():
    redir = _requiere_perfil_empleado_si_aplica()
    if redir:
        return redir
    return render_template("modulos/turnos.html", puede_admin=(current_user.es_administrador or current_user.es_rrhh))

@modulos_bp.route("/turnos/asignar", methods=["GET", "POST"])
@login_required
def turnos_asignar():
    redir = _requiere_perfil_empleado_si_aplica()
    if redir:
        return redir
    if not _require_admin():
        return redirect(url_for("modulos.turnos"))

    _ensure_doc_number_column()

    empleados = fetch_all(
        "SELECT employee_id, doc_number, first_name, last_name FROM rrhh.hr_employee WHERE is_active=1 ORDER BY doc_number, first_name"
    )
    turnos = fetch_all(
        "SELECT shift_id, shift_code, start_time, end_time, shift_group FROM rrhh.shift_definition WHERE is_active=1 ORDER BY shift_code"
    )

    if request.method == "POST":
        try:
            employee_id = int(request.form.get("employee_id"))
            shift_id = int(request.form.get("shift_id"))
            valid_from = request.form.get("valid_from")
            valid_to = request.form.get("valid_to") or None
            reason = (request.form.get("reason") or "").strip() or None

            call_proc(
                "EXEC rrhh.sp_set_shift_assignment ?, ?, ?, ?, ?, ?",
                (employee_id, shift_id, valid_from, valid_to, int(current_user.user_db_id), reason)
            )
            flash("Turno asignado por vigencia y notificación registrada.", "success")
            return redirect(url_for("modulos.turnos"))
        except Exception as e:
            flash(f"Error asignando turno: {e}", "error")

    return render_template("modulos/turnos_asignar.html", empleados=empleados, turnos=turnos)

@modulos_bp.route("/hora-flexible")
@login_required
def hora_flexible():
    redir = _requiere_perfil_empleado_si_aplica()
    if redir:
        return redir
    return render_template("modulos/hora_flexible.html", puede_admin=(current_user.es_administrador or current_user.es_rrhh))

@modulos_bp.route("/hora-flexible/nueva", methods=["GET", "POST"])
@login_required
def hora_flexible_nueva():
    redir = _requiere_perfil_empleado_si_aplica()
    if redir:
        return redir

    if current_user.employee_id is None:
        flash("Tu usuario no está asociado a un empleado. RRHH debe asignar el perfil.", "error")
        return redirect(url_for("perfil_pendiente"))

    if request.method == "POST":
        try:
            reduction_date = request.form.get("reduction_date")
            slot = request.form.get("slot")  # AM / PM
            reason = (request.form.get("reason") or "").strip()

            request_id = execute_scalar(
                "INSERT INTO rrhh.wf_request(request_type, employee_id, created_by_user_id, status) "
                "OUTPUT INSERTED.request_id "
                "VALUES ('TIME_REDUCTION', ?, ?, 'DRAFT')",
                (int(current_user.employee_id), int(current_user.user_db_id))
            )

            execute(
                "INSERT INTO rrhh.time_reduction_request(request_id, reduction_date, slot, minutes, reason, comment) "
                "VALUES (?, ?, ?, 60, ?, NULL)",
                (int(request_id), reduction_date, slot, reason)
            )

            call_proc("EXEC rrhh.sp_submit_request ?, ?", (int(request_id), int(current_user.user_db_id)))

            flash(f"Solicitud creada y enviada. ID: {request_id}", "success")
            return redirect(url_for("modulos.hora_flexible"))

        except Exception as e:
            flash(f"Error creando solicitud: {e}", "error")

    return render_template("modulos/hora_flexible_nueva.html")

@modulos_bp.route("/aprobaciones")
@login_required
def aprobaciones():
    sql = (
        "SELECT s.step_id, s.request_id, s.step_no, s.status, "
        "r.request_type, r.employee_id, r.created_at "
        "FROM rrhh.wf_request_step s "
        "JOIN rrhh.wf_request r ON r.request_id = s.request_id "
        "WHERE s.assigned_to_user_id = ? AND s.status = 'PENDING' "
        "ORDER BY r.created_at DESC"
    )
    steps = fetch_all(sql, (int(current_user.user_db_id),))
    return render_template("modulos/aprobaciones.html", steps=steps)

@modulos_bp.route("/aprobaciones/accion", methods=["POST"])
@login_required
def aprobaciones_accion():
    flash("Acción de aprobación aún en construcción. (Siguiente paso)", "error")
    return redirect(url_for("modulos.aprobaciones"))
