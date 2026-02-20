import calendar
import hashlib
import json
import os
from datetime import date, datetime, timedelta, time
from typing import Dict, List, Tuple, Optional

from flask import Blueprint, render_template, request, redirect, url_for, flash
from flask_login import login_required, current_user
from openpyxl import load_workbook

from services.rrhh_db import fetch_all, fetch_one, execute, execute_scalar, call_proc
from services.rrhh_security import ROLE_ADMIN, ROLE_RRHH
from services.hr_employee_service import (
    employee_can_work_from_home,
    get_manager_for_employee,
    manager_has_subordinates,
)
from services.upload import save_upload

modulos_bp = Blueprint("modulos", __name__)

# -------------------------
# Helpers
# -------------------------
def _require_admin():
    roles = getattr(current_user, "roles", None) or []
    # soporta roles como set/list
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


def _user_can_request_wfh() -> bool:
    # Primero el flag inyectado en login; si no está, valida en DB.
    if bool(getattr(current_user, "puede_trabajo_casa", False)):
        return True
    try:
        return employee_can_work_from_home(getattr(current_user, "employee_id", None))
    except Exception:
        return False


def _can_approve_wfh(employee_id: int, ref_date: date) -> bool:
    """Reglas de aprobación Trabajo en Casa (WFH):

    ref_date = fecha de referencia para validar jerarquía (recomendado: work_date).

    - ADMIN/RRHH: ve y aprueba todo.
    - Jefe: solo aprueba solicitudes de su equipo (relación vigente en ref_date).
    - Solicitudes de *jefes* (personas que tienen subordinados): NO las aprueba otro jefe;
      esas quedan para RRHH/ADMIN.
    """
    if _is_admin_or_rrhh():
        return True

    # Si el solicitante es jefe (tiene subordinados) en ref_date, su aprobación debe ser RRHH/ADMIN.
    try:
        if manager_has_subordinates(employee_id, ref_date):
            return False
    except Exception:
        pass

    mgr_id = get_manager_for_employee(employee_id, ref_date)
    return bool(mgr_id and mgr_id == getattr(current_user, "employee_id", None))


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
    # solo dígitos
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
    # intenta yyyy-mm-dd o dd/mm/yyyy
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
    # soporta DB vieja (employee_code) o nueva (doc_number)
    col = fetch_one(
        "SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS "
        "WHERE TABLE_SCHEMA='rrhh' AND TABLE_NAME='att_import_row' AND COLUMN_NAME='doc_number'"
    )
    return "doc_number" if col else "employee_code"

# -------------------------
# Dashboard
# -------------------------
@modulos_bp.route("/dashboard")
@login_required
def dashboard():
    # Conteo rápido de aprobaciones pendientes para mostrar en el dashboard.
    roles = set((getattr(current_user, "roles", []) or []))
    is_admin = "ADMINISTRADOR" in roles
    is_rrhh = "RRHH" in roles
    is_backoffice = is_admin or is_rrhh
    es_jefe = bool(getattr(current_user, "es_jefe", False))
    my_emp_id = getattr(current_user, "employee_id", None)

    pending_aprobaciones_count = 0
    try:
        two_step = _wfh_two_step_enabled()

        if is_backoffice:
            if two_step:
                row = fetch_one(
                    "SELECT COUNT(1) AS c FROM rrhh.wfh_day "
                    "WHERE is_active=0 AND manager_approved=1 AND hr_approved=0"
                )
            else:
                row = fetch_one("SELECT COUNT(1) AS c FROM rrhh.wfh_day WHERE is_active=0")
            pending_aprobaciones_count = int(getattr(row, "c", 0) or 0) if row else 0

        elif es_jefe and my_emp_id:
            if two_step:
                row = fetch_one(
                    "SELECT COUNT(1) AS c "
                    "FROM rrhh.wfh_day w "
                    "JOIN rrhh.hr_employee_manager m ON m.employee_id=w.employee_id "
                    "  AND m.manager_employee_id=? "
                    "  AND m.is_primary=1 "
                    "  AND m.valid_from <= w.work_date "
                    "  AND (m.valid_to IS NULL OR m.valid_to >= w.work_date) "
                    "WHERE w.is_active=0 "
                    "  AND w.manager_approved=0 AND w.hr_approved=0 "
                    "  AND w.employee_id <> ? "
                    "  AND NOT EXISTS ("  # solicitudes de jefes se gestionan por RRHH
                    "    SELECT 1 FROM rrhh.hr_employee_manager mm "
                    "    WHERE mm.manager_employee_id = w.employee_id "
                    "      AND mm.is_primary=1 "
                    "      AND mm.valid_from <= w.work_date "
                    "      AND (mm.valid_to IS NULL OR mm.valid_to >= w.work_date)"
                    "  )",
                    (int(my_emp_id), int(my_emp_id)),
                )
            else:
                row = fetch_one(
                    "SELECT COUNT(1) AS c "
                    "FROM rrhh.wfh_day w "
                    "JOIN rrhh.hr_employee_manager m ON m.employee_id=w.employee_id "
                    "  AND m.manager_employee_id=? "
                    "  AND m.is_primary=1 "
                    "  AND m.valid_from <= w.work_date "
                    "  AND (m.valid_to IS NULL OR m.valid_to >= w.work_date) "
                    "WHERE w.is_active=0 "
                    "  AND w.employee_id <> ? "
                    "  AND NOT EXISTS ("
                    "    SELECT 1 FROM rrhh.hr_employee_manager mm "
                    "    WHERE mm.manager_employee_id = w.employee_id "
                    "      AND mm.is_primary=1 "
                    "      AND mm.valid_from <= w.work_date "
                    "      AND (mm.valid_to IS NULL OR mm.valid_to >= w.work_date)"
                    "  )",
                    (int(my_emp_id), int(my_emp_id)),
                )
            pending_aprobaciones_count = int(getattr(row, "c", 0) or 0) if row else 0
    except Exception:
        pending_aprobaciones_count = 0

    return render_template(
        "dashboard.html",
        pending_aprobaciones_count=pending_aprobaciones_count,
    )

# -------------------------
# Turnos (asignación de horario)
# -------------------------
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
        "SELECT shift_id, shift_code, start_time, end_time FROM rrhh.shift_definition WHERE is_active=1 ORDER BY shift_id"
    )
    return render_template("modulos/turnos.html", empleados=rows, shifts=shifts)

@modulos_bp.route("/turnos/asignar", methods=["GET", "POST"])
@login_required
def turnos_asignar():
    if not _require_admin():
        return redirect(url_for("modulos.dashboard"))

    empleados = fetch_all(
        "SELECT employee_id, doc_number, first_name, last_name FROM rrhh.hr_employee WHERE is_active=1 ORDER BY last_name, first_name"
    )
    shifts = fetch_all(
        "SELECT shift_id, shift_code, start_time, end_time FROM rrhh.shift_definition WHERE is_active=1 ORDER BY shift_id"
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

# -------------------------
# Asistencia (import + vista)
# -------------------------
def _parse_attendance_excel(file_path: str) -> List[Dict]:
    wb = load_workbook(file_path, data_only=True)
    ws = wb.active

    # detectar cabecera (primera fila)
    header = []
    for cell in ws[1]:
        header.append(str(cell.value).strip().lower() if cell.value is not None else "")
    header_map = {name: idx for idx, name in enumerate(header)}

    def find_col(patterns):
        for name, idx in header_map.items():
            for p in patterns:
                if p in name:
                    return idx
        return None

    col_doc = find_col(["cedula", "cédula", "documento", "doc", "identific", "cc", "c.c", "dni", "id"])
    col_date = find_col(["fecha", "date"])
    col_in = find_col(["entrada", "first", "ingreso", "in"])
    col_out = find_col(["salida", "last", "egreso", "out"])
    col_min = find_col(["min", "minutos", "total", "tiempo", "duracion", "duración"])

    if col_doc is None or col_date is None:
        raise ValueError("No se encontró columna de Cédula/Documento y/o Fecha en el Excel (revisa encabezados).")

    grouped: Dict[Tuple[str, date], Dict] = {}
    for r in ws.iter_rows(min_row=2, values_only=True):
        doc = _parse_doc_number(r[col_doc] if col_doc < len(r) else None)
        wdate = _parse_date(r[col_date] if col_date < len(r) else None)
        if not doc or not wdate:
            continue

        fin = _parse_time(r[col_in] if (col_in is not None and col_in < len(r)) else None)
        fout = _parse_time(r[col_out] if (col_out is not None and col_out < len(r)) else None)
        mins = None
        if col_min is not None and col_min < len(r):
            try:
                mins = int(float(r[col_min])) if r[col_min] is not None else None
            except Exception:
                mins = None

        key = (doc, wdate)
        entry = grouped.get(key)
        raw = {"row": [str(x) if x is not None else "" for x in r]}

        if entry is None:
            grouped[key] = {
                "doc_number": doc,
                "work_date": wdate,
                "first_in": fin,
                "last_out": fout,
                "total_minutes": mins,
                "raw": [raw],
            }
        else:
            if fin and (entry["first_in"] is None or fin < entry["first_in"]):
                entry["first_in"] = fin
            if fout and (entry["last_out"] is None or fout > entry["last_out"]):
                entry["last_out"] = fout
            # si viene minutes, dejamos el máximo
            if mins is not None:
                if entry["total_minutes"] is None or mins > entry["total_minutes"]:
                    entry["total_minutes"] = mins
            entry["raw"].append(raw)

    rows = []
    for v in grouped.values():
        if v["total_minutes"] is None and v["first_in"] and v["last_out"]:
            try:
                v["total_minutes"] = max(0, _diff_minutes(v["first_in"], v["last_out"]))
            except Exception:
                v["total_minutes"] = None
        v["raw_text"] = json.dumps(v["raw"], ensure_ascii=False)
        rows.append(v)

    return rows

@modulos_bp.route("/asistencia")
@login_required
def asistencia():
    if not _require_admin():
        return redirect(url_for("modulos.dashboard"))

    y = int(request.args.get("year", date.today().year))
    m = int(request.args.get("month", date.today().month))
    d_from, d_to = _month_range(y, m)

    # fechas del mes
    days = [d_from + timedelta(days=i) for i in range((d_to - d_from).days + 1)]

    empleados = fetch_all(
        "SELECT employee_id, doc_number, first_name, last_name FROM rrhh.hr_employee WHERE is_active=1 ORDER BY last_name, first_name"
    )

    att_rows = fetch_all(
        "SELECT employee_id, work_date, first_in, last_out, total_minutes, has_manual_override "
        "FROM rrhh.vw_attendance_effective "
        "WHERE work_date BETWEEN ? AND ?",
        (d_from, d_to),
    )
    att_map = {(r.employee_id, r.work_date): r for r in att_rows}

    # WFH (si existe)
    wfh_exists = fetch_one(
        "SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='rrhh' AND TABLE_NAME='wfh_day'"
    ) is not None
    wfh_map = set()
    if wfh_exists:
        wfh_rows = fetch_all(
            "SELECT employee_id, work_date FROM rrhh.wfh_day WHERE is_active=1 AND work_date BETWEEN ? AND ?",
            (d_from, d_to),
        )
        wfh_map = {(r.employee_id, r.work_date) for r in wfh_rows}

    # time reduction (si existe y está aprobado)
    tr_exists = fetch_one(
        "SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='rrhh' AND TABLE_NAME='time_reduction_request'"
    ) is not None
    reduction_map = {}
    if tr_exists:
        tr_rows = fetch_all(
            "SELECT r.employee_id, t.reduction_date, t.minutes "
            "FROM rrhh.wf_request r "
            "JOIN rrhh.time_reduction_request t ON t.request_id = r.request_id "
            "WHERE r.request_type = 'TIME_REDUCTION' AND r.status = 'APPROVED' "
            "AND t.reduction_date BETWEEN ? AND ?",
            (d_from, d_to),
        )
        for rr in tr_rows:
            reduction_map[(rr.employee_id, rr.reduction_date)] = int(rr.minutes or 60)

    # shift assignments (traer los que cruzan el mes)
    shifts = fetch_all(
        "SELECT sa.employee_id, sa.valid_from, sa.valid_to, sd.shift_code, sd.start_time, sd.end_time "
        "FROM rrhh.shift_assignment sa "
        "JOIN rrhh.shift_definition sd ON sd.shift_id = sa.shift_id "
        "WHERE sa.valid_from <= ? AND (sa.valid_to IS NULL OR sa.valid_to >= ?) "
        "ORDER BY sa.employee_id, sa.valid_from",
        (d_to, d_from),
    )
    shifts_by_emp: Dict[int, List] = {}
    for s in shifts:
        shifts_by_emp.setdefault(s.employee_id, []).append(s)

    def shift_for(emp_id: int, day: date):
        lst = shifts_by_emp.get(emp_id, [])
        best = None
        for s in lst:
            if s.valid_from <= day and (s.valid_to is None or s.valid_to >= day):
                best = s
        return best

    matrix = []
    for e in empleados:
        row_days = []
        summary = {"ok": 0, "faltante": 0, "incompleto": 0, "casa": 0, "sin_turno": 0}
        for d in days:
            if d.weekday() >= 5:  # sábado/domingo
                row_days.append({"status": "—", "title": "Fin de semana", "link": None})
                continue

            sh = shift_for(e.employee_id, d)
            if not sh:
                summary["sin_turno"] += 1
                row_days.append({"status": "ST", "title": "Sin turno asignado", "link": None})
                continue

            required = _diff_minutes(sh.start_time, sh.end_time)
            required -= reduction_map.get((e.employee_id, d), 0)
            required = max(0, required)

            att = att_map.get((e.employee_id, d))
            if att is None or att.total_minutes is None:
                if (e.employee_id, d) in wfh_map:
                    summary["casa"] += 1
                    row_days.append({"status": "CASA", "title": f"Trabajo en casa. Requerido {required} min", "link": url_for("modulos.asistencia_ajuste", employee_id=e.employee_id, work_date=d.isoformat())})
                else:
                    summary["faltante"] += 1
                    row_days.append({"status": "F", "title": f"Faltante. Requerido {required} min", "link": url_for("modulos.asistencia_ajuste", employee_id=e.employee_id, work_date=d.isoformat())})
                continue

            mins = int(att.total_minutes or 0)
            ok = mins >= required
            if ok:
                summary["ok"] += 1
                st = "OK*" if att.has_manual_override else "OK"
                row_days.append({"status": st, "title": f"{mins} min (req {required}). In {_time_to_str(att.first_in)} / Out {_time_to_str(att.last_out)}", "link": url_for("modulos.asistencia_ajuste", employee_id=e.employee_id, work_date=d.isoformat())})
            else:
                summary["incompleto"] += 1
                row_days.append({"status": "I", "title": f"Incompleto: {mins} min (req {required}). In {_time_to_str(att.first_in)} / Out {_time_to_str(att.last_out)}", "link": url_for("modulos.asistencia_ajuste", employee_id=e.employee_id, work_date=d.isoformat())})

        # turno actual para mostrar
        sh_today = shift_for(e.employee_id, date.today())
        shift_label = sh_today.shift_code if sh_today else "—"
        matrix.append({
            "employee_id": e.employee_id,
            "doc_number": e.doc_number,
            "name": f"{e.first_name} {e.last_name}",
            "shift": shift_label,
            "days": row_days,
            "summary": summary,
        })

    return render_template("modulos/asistencia.html", year=y, month=m, days=days, matrix=matrix)

@modulos_bp.route("/asistencia/cargar", methods=["GET", "POST"])
@login_required
def asistencia_cargar():
    if not _require_admin():
        return redirect(url_for("modulos.dashboard"))

    if request.method == "POST":
        year_no = int(request.form.get("year_no"))
        month_no = int(request.form.get("month_no"))
        f = request.files.get("file")

        if not f or not f.filename:
            flash("Debes seleccionar un archivo.", "warning")
            return render_template("modulos/asistencia_cargar.html", year_no=year_no, month_no=month_no)

        try:
            meta = save_upload(f, prefix="asistencia")
            checksum = _checksum_file(meta["storage_path"])

            # registrar archivo
            execute(
                "INSERT INTO rrhh.sys_attachment(file_name, mime_type, size_bytes, storage_path, uploaded_by_user_id) "
                "VALUES (?, ?, ?, ?, ?)",
                (meta["file_name"], f.mimetype or "application/octet-stream", meta["size_bytes"], meta["storage_path"], current_user.user_id),
            )
            file_id = execute_scalar("SELECT SCOPE_IDENTITY()")

            # batch
            execute(
                "INSERT INTO rrhh.att_import_batch(year_no, month_no, file_name, file_id, checksum, uploaded_by_user_id) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (year_no, month_no, meta["file_name"], file_id, checksum, current_user.user_id),
            )
            batch_id = int(execute_scalar("SELECT SCOPE_IDENTITY()"))

            # parse excel (todos los registros)
            parsed = _parse_attendance_excel(meta["storage_path"])
            doc_col = _att_import_doc_column()

            # insert rows (OK / ERROR si no existe empleado)
            for r in parsed:
                # validar empleado existe
                emp = fetch_one("SELECT employee_id FROM rrhh.hr_employee WHERE doc_number = ? AND is_active=1", (r["doc_number"],))
                if not emp:
                    execute(
                        f"INSERT INTO rrhh.att_import_row(batch_id, {doc_col}, work_date, first_in, last_out, total_minutes, raw_text, load_status, error_message) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, 'ERROR', ?)",
                        (batch_id, r["doc_number"], r["work_date"], r["first_in"], r["last_out"], r["total_minutes"], r["raw_text"], "Empleado no existe (doc_number no encontrado)"),
                    )
                else:
                    execute(
                        f"INSERT INTO rrhh.att_import_row(batch_id, {doc_col}, work_date, first_in, last_out, total_minutes, raw_text, load_status, error_message) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, 'OK', NULL)",
                        (batch_id, r["doc_number"], r["work_date"], r["first_in"], r["last_out"], r["total_minutes"], r["raw_text"]),
                    )

            # aplicar a att_day
            call_proc("rrhh.sp_apply_attendance_batch", [batch_id, current_user.user_id])

            flash(f"Asistencia cargada y aplicada. Batch #{batch_id}.", "success")
            return redirect(url_for("modulos.asistencia", year=year_no, month=month_no))

        except Exception as ex:
            flash(f"Error cargando asistencia: {ex}", "danger")

    today = date.today()
    return render_template("modulos/asistencia_cargar.html", year_no=today.year, month_no=today.month)

@modulos_bp.route("/asistencia/ajuste", methods=["GET", "POST"])
@login_required
def asistencia_ajuste():
    if not _require_admin():
        return redirect(url_for("modulos.dashboard"))

    employee_id = int(request.values.get("employee_id"))
    work_date_s = request.values.get("work_date")
    work_date = datetime.strptime(work_date_s, "%Y-%m-%d").date()

    emp = fetch_one("SELECT employee_id, doc_number, first_name, last_name FROM rrhh.hr_employee WHERE employee_id=?", (employee_id,))
    if not emp:
        flash("Empleado no encontrado.", "warning")
        return redirect(url_for("modulos.asistencia"))

    # asistencia efectiva
    att = fetch_one(
        "SELECT employee_id, work_date, first_in, last_out, total_minutes, has_manual_override "
        "FROM rrhh.vw_attendance_effective WHERE employee_id=? AND work_date=?",
        (employee_id, work_date),
    )

    # turno del día y requerido
    sh = fetch_one(
        "SELECT TOP (1) sd.shift_code, sd.start_time, sd.end_time "
        "FROM rrhh.shift_assignment sa "
        "JOIN rrhh.shift_definition sd ON sd.shift_id = sa.shift_id "
        "WHERE sa.employee_id=? AND sa.valid_from <= ? AND (sa.valid_to IS NULL OR sa.valid_to >= ?) "
        "ORDER BY sa.valid_from DESC",
        (employee_id, work_date, work_date),
    )
    required = _diff_minutes(sh.start_time, sh.end_time) if sh else None

    if request.method == "POST":
        reason = (request.form.get("reason") or "").strip()
        comment = (request.form.get("comment") or "").strip() or None
        first_in = _parse_time(request.form.get("first_in") or None)
        last_out = _parse_time(request.form.get("last_out") or None)
        total_minutes = request.form.get("total_minutes")
        total_minutes = int(total_minutes) if total_minutes not in (None, "",) else None

        # quick action: cumplir
        if request.form.get("action") == "cumplio":
            if required is None:
                flash("No hay turno asignado para calcular requerido.", "warning")
                return redirect(url_for("modulos.asistencia_ajuste", employee_id=employee_id, work_date=work_date.isoformat()))
            total_minutes = required
            if not reason:
                reason = "NOVEDAD: cumplimiento (salida temprana u otra)"

        if not reason:
            flash("Debes indicar un motivo (reason).", "warning")
            return redirect(url_for("modulos.asistencia_ajuste", employee_id=employee_id, work_date=work_date.isoformat()))

        try:
            # desactivar overrides anteriores
            execute(
                "UPDATE rrhh.att_manual_override SET is_active=0 "
                "WHERE employee_id=? AND work_date=? AND is_active=1",
                (employee_id, work_date),
            )
            execute(
                "INSERT INTO rrhh.att_manual_override(employee_id, work_date, first_in, last_out, total_minutes, reason, comment, created_by_user_id, is_active) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)",
                (employee_id, work_date, first_in, last_out, total_minutes, reason, comment, current_user.user_id),
            )
            flash("Ajuste/Novedad guardado.", "success")
            return redirect(url_for("modulos.asistencia", year=work_date.year, month=work_date.month))
        except Exception as ex:
            flash(f"No se pudo guardar el ajuste: {ex}", "danger")

    return render_template(
        "modulos/asistencia_ajuste.html",
        emp=emp,
        work_date=work_date,
        att=att,
        shift=sh,
        required_minutes=required,
    )

# -------------------------
# Trabajo en casa
#
# Reglas (doble aprobación):
# - Empleado solicita en /trabajo-casa/solicitar => queda PENDIENTE (is_active=0)
# - Paso 1 (JEFE): marca manager_approved=1
# - Paso 2 (RRHH/ADMIN): marca hr_approved=1 e is_active=1
#
# Excepciones:
# - Si el solicitante es jefe (tiene subordinados), se omite el paso 1 y queda directo para RRHH.
# - Si el empleado no tiene jefe asignado en rrhh.hr_employee_manager (para la fecha solicitada),
#   se omite el paso 1 y queda directo para RRHH (se recomienda corregir jerarquía).
#
# Compatibilidad:
# - Si la tabla rrhh.wfh_day no tiene columnas manager_approved/hr_approved, el flujo funciona
#   como aprobación única (is_active).
# -------------------------


def _wfh_two_step_enabled() -> bool:
    """True si rrhh.wfh_day tiene columnas para doble aprobación."""
    try:
        r = fetch_one(
            "SELECT CASE WHEN COL_LENGTH('rrhh.wfh_day','manager_approved') IS NULL "
            "OR COL_LENGTH('rrhh.wfh_day','hr_approved') IS NULL THEN 0 ELSE 1 END AS ok"
        )
        return bool(getattr(r, "ok", 0)) if r else False
    except Exception:
        return False


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
        "SELECT employee_id, doc_number, first_name, last_name FROM rrhh.hr_employee WHERE is_active=1 ORDER BY last_name, first_name"
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

    two_step = _wfh_two_step_enabled()

    try:
        if two_step:
            now = datetime.now()
            execute(
                "MERGE rrhh.wfh_day AS t "
                "USING (SELECT ? AS employee_id, ? AS work_date) AS s "
                "ON (t.employee_id = s.employee_id AND t.work_date = s.work_date) "
                "WHEN MATCHED THEN UPDATE SET "
                "  is_active=1, reason=?, "
                "  manager_approved=1, manager_approved_by_user_id=?, manager_approved_at=?, "
                "  hr_approved=1, hr_approved_by_user_id=?, hr_approved_at=? "
                "WHEN NOT MATCHED THEN INSERT (employee_id, work_date, reason, created_by_user_id, is_active, "
                "  manager_approved, manager_approved_by_user_id, manager_approved_at, "
                "  hr_approved, hr_approved_by_user_id, hr_approved_at) "
                "VALUES (?, ?, ?, ?, 1, 1, ?, ?, 1, ?, ?);",
                (
                    employee_id, work_date, reason,
                    current_user.user_id, now,
                    current_user.user_id, now,
                    employee_id, work_date, reason, current_user.user_id,
                    current_user.user_id, now,
                    current_user.user_id, now,
                ),
            )
        else:
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
    """Empleado solicita WFH. Queda pendiente hasta completar aprobaciones."""
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

    two_step = _wfh_two_step_enabled()

    if request.method == "POST":
        work_date = datetime.strptime(request.form.get("work_date"), "%Y-%m-%d").date()
        reason = (request.form.get("reason") or "Trabajo en casa").strip()

        # si ya está aprobado, no permitir re-solicitar
        row = fetch_one(
            "SELECT is_active FROM rrhh.wfh_day WHERE employee_id=? AND work_date=?",
            (int(employee_id), work_date),
        )
        if row and int(row.is_active) == 1:
            flash("Ese día ya está aprobado como Trabajo en casa.", "warning")
            return redirect(url_for("modulos.trabajo_casa_solicitar"))

        try:
            approval_ref_date = work_date

            requester_emp_id = getattr(current_user, "employee_id", None)
            requester_is_manager = False
            if requester_emp_id:
                try:
                    requester_is_manager = manager_has_subordinates(int(requester_emp_id), approval_ref_date)
                except Exception:
                    requester_is_manager = False

            mgr_id = None
            try:
                mgr_id = get_manager_for_employee(int(employee_id), approval_ref_date)
            except Exception:
                mgr_id = None

            # Si el solicitante es jefe, o no tiene jefe asignado: pasa directo a RRHH
            auto_skip_manager = bool(requester_is_manager or not mgr_id)

            if two_step:
                manager_approved = 1 if auto_skip_manager else 0
                hr_approved = 0
                now = datetime.now()

                mgr_approved_by = current_user.user_id if requester_is_manager else None
                mgr_approved_at = now if requester_is_manager else None

                if row:
                    execute(
                        "UPDATE rrhh.wfh_day SET "
                        "  is_active=0, reason=?, created_by_user_id=?, "
                        "  manager_approved=?, manager_approved_by_user_id=?, manager_approved_at=?, "
                        "  hr_approved=?, hr_approved_by_user_id=NULL, hr_approved_at=NULL "
                        "WHERE employee_id=? AND work_date=?",
                        (
                            reason, current_user.user_id,
                            manager_approved, mgr_approved_by, mgr_approved_at,
                            hr_approved,
                            int(employee_id), work_date,
                        ),
                    )
                else:
                    execute(
                        "INSERT INTO rrhh.wfh_day("
                        "  employee_id, work_date, reason, created_by_user_id, is_active, "
                        "  manager_approved, manager_approved_by_user_id, manager_approved_at, "
                        "  hr_approved, hr_approved_by_user_id, hr_approved_at"
                        ") VALUES (?, ?, ?, ?, 0, ?, ?, ?, ?, NULL, NULL)",
                        (
                            int(employee_id), work_date, reason, current_user.user_id,
                            manager_approved, mgr_approved_by, mgr_approved_at,
                            hr_approved,
                        ),
                    )
            else:
                # modo legacy (una sola aprobación)
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

            # Mensajes
            if requester_is_manager:
                flash("Solicitud enviada a RRHH para aprobación.", "success")
            else:
                if mgr_id:
                    flash("Solicitud enviada a tu jefe para aprobación.", "success")
                else:
                    flash(
                        "Solicitud enviada a RRHH porque no tienes jefe asignado en el sistema. RRHH debe asignarlo para el flujo normal.",
                        "warning",
                    )
        except Exception as ex:
            flash(f"No se pudo registrar la solicitud: {ex}", "danger")

        return redirect(url_for("modulos.trabajo_casa_solicitar"))

    return render_template("modulos/trabajo_casa_solicitar.html")


@modulos_bp.route("/trabajo-casa/aprobaciones")
@login_required
def trabajo_casa_aprobaciones():
    """Bandeja de aprobaciones de Trabajo en casa.

    Reglas:
    - RRHH / Admin: ve pendientes (paso RRHH) de todos.
    - Jefe: ve pendientes (paso Jefe) de sus subordinados.
      La relación jefe-subordinado se evalúa contra la fecha solicitada (work_date).

    Nota:
    - Doble paso si existen columnas manager_approved/hr_approved.
    - Legacy: pendiente = is_active=0.
    """

    is_backoffice = _is_admin_or_rrhh()
    my_emp_id = getattr(current_user, "employee_id", None)

    if not my_emp_id:
        flash("Tu usuario no está asociado a un empleado. Contacta a RRHH.", "error")
        return redirect(url_for("modulos.dashboard"))

    # Si no es backoffice, debe ser jefe con subordinados para entrar.
    if not is_backoffice:
        try:
            if not bool(getattr(current_user, "es_jefe", False)) and not manager_has_subordinates(int(my_emp_id)):
                flash("No tienes solicitudes de Trabajo en casa para aprobar.", "warning")
                return redirect(url_for("modulos.dashboard"))
        except Exception:
            flash("No tienes solicitudes de Trabajo en casa para aprobar.", "warning")
            return redirect(url_for("modulos.dashboard"))

    # Validar existencia tabla
    exists = fetch_one(
        "SELECT 1 AS ok FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='rrhh' AND TABLE_NAME='wfh_day'"
    )
    if not exists:
        flash("No existe la tabla rrhh.wfh_day. Revisa el script de base de datos.", "error")
        return redirect(url_for("modulos.dashboard"))

    two_step = _wfh_two_step_enabled()

    # Pendientes / aprobadas
    if is_backoffice:
        if two_step:
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
                  AND w.manager_approved = 1
                  AND w.hr_approved = 0
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
        if two_step:
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
                  AND w.manager_approved = 0
                  AND w.hr_approved = 0
                  AND w.employee_id <> ?
                ORDER BY w.created_at DESC
                """,
                (int(my_emp_id), int(my_emp_id)),
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
        two_step=two_step,
    )


@modulos_bp.route("/trabajo-casa/aprobar", methods=["POST"])
@login_required
def trabajo_casa_aprobar():
    employee_id = int(request.form.get("employee_id"))
    work_date = datetime.strptime(request.form.get("work_date"), "%Y-%m-%d").date()

    is_backoffice = _is_admin_or_rrhh()
    two_step = _wfh_two_step_enabled()

    # Legacy: aprobación única
    if not two_step:
        if not _can_approve_wfh(employee_id, work_date):
            flash("No tienes permiso para aprobar esta solicitud.", "warning")
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

    # Doble paso
    try:
        now = datetime.now()

        if not is_backoffice:
            # Paso JEFE
            if not _can_approve_wfh(employee_id, work_date):
                flash("No tienes permiso para aprobar esta solicitud.", "warning")
                return redirect(url_for("modulos.trabajo_casa_aprobaciones"))

            rc = execute(
                "UPDATE rrhh.wfh_day SET "
                "  manager_approved=1, manager_approved_by_user_id=?, manager_approved_at=? "
                "WHERE employee_id=? AND work_date=? "
                "  AND is_active=0 AND manager_approved=0 AND hr_approved=0",
                (current_user.user_id, now, employee_id, work_date),
            )
            if rc:
                flash("Aprobada por jefe. Queda pendiente de RRHH.", "success")
            else:
                flash("La solicitud no estaba pendiente (paso jefe) o ya fue gestionada.", "warning")
            return redirect(url_for("modulos.trabajo_casa_aprobaciones"))

        # Paso RRHH (normal)
        rc = execute(
            "UPDATE rrhh.wfh_day SET "
            "  hr_approved=1, hr_approved_by_user_id=?, hr_approved_at=?, "
            "  is_active=1 "
            "WHERE employee_id=? AND work_date=? "
            "  AND is_active=0 AND hr_approved=0 "
            "  AND manager_approved=1",
            (current_user.user_id, now, employee_id, work_date),
        )

        # Override RRHH: si aún no pasó por jefe, RRHH puede completar ambos pasos
        if rc == 0:
            rc = execute(
                "UPDATE rrhh.wfh_day SET "
                "  manager_approved=1, manager_approved_by_user_id=?, manager_approved_at=?, "
                "  hr_approved=1, hr_approved_by_user_id=?, hr_approved_at=?, "
                "  is_active=1 "
                "WHERE employee_id=? AND work_date=? "
                "  AND is_active=0 AND hr_approved=0",
                (
                    current_user.user_id, now,
                    current_user.user_id, now,
                    employee_id, work_date,
                ),
            )

        if rc:
            flash("Solicitud aprobada por RRHH.", "success")
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

    is_backoffice = _is_admin_or_rrhh()
    two_step = _wfh_two_step_enabled()

    if not two_step:
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

    # Doble paso
    if not is_backoffice:
        # Paso JEFE
        if not _can_approve_wfh(employee_id, work_date):
            flash("No tienes permiso para rechazar esta solicitud.", "warning")
            return redirect(url_for("modulos.trabajo_casa_aprobaciones"))

        try:
            rc = execute(
                "DELETE FROM rrhh.wfh_day "
                "WHERE employee_id=? AND work_date=? "
                "  AND is_active=0 AND manager_approved=0 AND hr_approved=0",
                (employee_id, work_date),
            )
            if rc:
                flash("Solicitud rechazada.", "success")
            else:
                flash("La solicitud no estaba pendiente (paso jefe) o ya fue gestionada.", "warning")
        except Exception as ex:
            flash(f"No se pudo rechazar: {ex}", "danger")

        return redirect(url_for("modulos.trabajo_casa_aprobaciones"))

    # RRHH: puede rechazar en cualquier estado pendiente
    try:
        rc = execute(
            "DELETE FROM rrhh.wfh_day "
            "WHERE employee_id=? AND work_date=? "
            "  AND is_active=0 AND hr_approved=0",
            (employee_id, work_date),
        )
        if rc:
            flash("Solicitud rechazada.", "success")
        else:
            flash("La solicitud no estaba pendiente o no existe.", "warning")
    except Exception as ex:
        flash(f"No se pudo rechazar: {ex}", "danger")

    return redirect(url_for("modulos.trabajo_casa_aprobaciones"))
