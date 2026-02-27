from __future__ import annotations

from datetime import date, datetime

from flask import render_template, request, redirect, url_for, flash
from flask_login import login_required, current_user

from services.rrhh_db import fetch_all, call_proc

from .modulos import modulos_bp
from .modulos_common import _require_admin


@modulos_bp.route("/turnos")
@login_required
def turnos():
    if not _require_admin():
        return redirect(url_for("modulos.dashboard"))

    today = date.today()
    rows = fetch_all(
        "SELECT e.employee_id, e.doc_number, e.first_name, e.last_name, "
        "sd.shift_code, sd.start_time, sd.end_time "
        "FROM rrhh.hr_employee e "
        "OUTER APPLY ("
        "  SELECT TOP (1) sa.shift_id "
        "  FROM rrhh.shift_assignment sa "
        "  WHERE sa.employee_id = e.employee_id "
        "    AND sa.valid_from <= ? "
        "    AND (sa.valid_to IS NULL OR sa.valid_to >= ?) "
        "  ORDER BY sa.valid_from DESC"
        ") x "
        "LEFT JOIN rrhh.shift_definition sd ON sd.shift_id = x.shift_id "
        "WHERE e.is_active = 1 "
        "ORDER BY e.last_name, e.first_name",
        (today, today),
    )

    shifts = fetch_all(
        "SELECT shift_id, shift_code, start_time, end_time "
        "FROM rrhh.shift_definition WHERE is_active=1 ORDER BY shift_id"
    )
    return render_template("modulos/turnos.html", empleados=rows, shifts=shifts)


@modulos_bp.route("/turnos/asignar", methods=["GET", "POST"])
@login_required
def turnos_asignar():
    if not _require_admin():
        return redirect(url_for("modulos.dashboard"))

    empleados = fetch_all(
        "SELECT employee_id, doc_number, first_name, last_name "
        "FROM rrhh.hr_employee WHERE is_active=1 ORDER BY last_name, first_name"
    )
    shifts = fetch_all(
        "SELECT shift_id, shift_code, start_time, end_time "
        "FROM rrhh.shift_definition WHERE is_active=1 ORDER BY shift_id"
    )

    if request.method == "POST":
        employee_id = int(request.form.get("employee_id"))
        shift_id = int(request.form.get("shift_id"))
        valid_from = request.form.get("valid_from")
        valid_to = request.form.get("valid_to") or None

        try:
            vf = datetime.strptime(valid_from, "%Y-%m-%d").date()
            vt = datetime.strptime(valid_to, "%Y-%m-%d").date() if valid_to else None
            call_proc(
                "rrhh.sp_set_shift_assignment",
                [employee_id, shift_id, vf, vt, current_user.user_id, "Asignación manual"],
            )
            flash("Turno asignado correctamente.", "success")
            return redirect(url_for("modulos.turnos"))
        except Exception as ex:
            flash(f"No se pudo asignar el turno: {ex}", "danger")

    return render_template("modulos/turnos_asignar.html", empleados=empleados, shifts=shifts)
