from __future__ import annotations

from datetime import date, datetime, timedelta
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Tuple

from flask import flash, redirect, render_template, request, url_for
from flask_login import current_user, login_required

from services.rrhh_db import execute, fetch_all, fetch_one
from services.turnos_roster_service import (
    ensure_week_row,
    fetch_roster_days,
    get_week_header,
    list_assignments_for_day,
    list_employees_for_group,
    normalize_group,
    parse_date,
    upsert_roster_day,
    validate_week,
    week_start,
)

from .modulos import modulos_bp
from .modulos_common import _month_range, _require_turnos_admin


# --------------------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------------------

DAY_LABELS = ["Lun", "Mar", "Mié", "Jue", "Vie", "Sáb", "Dom"]

SPECIAL_OPTIONS = [
    {"value": "DES", "label": "DES · Descanso"},
    {"value": "INC", "label": "INC · Incapacidad"},
    {"value": "VAC", "label": "VAC · Vacaciones"},
    {"value": "HB", "label": "HB · Día cumpleaños"},
    {"value": "1P", "label": "1P · Medio día chequera"},
    {"value": "2P", "label": "2P · Día completo chequera"},
]


def _actor_user_id() -> Optional[int]:
    v = int(getattr(current_user, "user_id", 0) or 0)
    if not v:
        v = int(getattr(current_user, "id", 0) or 0)
    return v or None


def _as_date_str(d: date) -> str:
    return d.strftime("%Y-%m-%d")


def _as_month_str(d: date) -> str:
    return d.strftime("%Y-%m")


def _parse_month_param(s: str) -> date:
    """month = 'YYYY-MM' -> retorna primer día del mes"""
    s = (s or "").strip()
    if not s:
        return date.today().replace(day=1)
    try:
        return datetime.strptime(s, "%Y-%m").date().replace(day=1)
    except Exception:
        return date.today().replace(day=1)


def _hour_end_hour(t) -> int:
    """Si end_time tiene minutos > 0, redondea hacia arriba."""
    if not t:
        return 0
    e = int(t.hour)
    if int(getattr(t, "minute", 0) or 0) > 0:
        e = (e + 1) % 24
    return e


def _covers_hour_on_target_date(shift_def, assigned_date: date, target_date: date, hour: int) -> bool:
    """Cobertura consistente con lógica de cruces de medianoche."""
    if not shift_def or not shift_def.start_time or not shift_def.end_time:
        return False

    s = int(shift_def.start_time.hour)
    e = _hour_end_hour(shift_def.end_time)
    crosses = shift_def.end_time <= shift_def.start_time

    h = int(hour)

    if not crosses:
        if target_date != assigned_date:
            return False
        return s <= h < e

    # Cruza medianoche
    if target_date == assigned_date:
        return h >= s
    if target_date == (assigned_date + timedelta(days=1)):
        return h < e
    return False


def _shift_label(s) -> str:
    st = s.start_time.strftime("%H:%M") if getattr(s, "start_time", None) else ""
    en = s.end_time.strftime("%H:%M") if getattr(s, "end_time", None) else ""
    if st and en:
        return f"{st}-{en}"
    return ""


def _build_shift_options(shifts) -> Tuple[List[dict], List[dict]]:
    """Opciones:
    - shift_options: value=<shift_code>
    - half_options: value=1P/<shift_code>
    """
    shift_options: List[dict] = []
    half_options: List[dict] = []
    for s in shifts or []:
        code = (s.shift_code or "").strip()
        if not code:
            continue
        label = f"{code} · {_shift_label(s)}" if _shift_label(s) else code
        shift_options.append({"value": code, "label": label})
        half_options.append({"value": f"1P/{code}", "label": f"1P/{code} · (medio permiso + {code})"})
    return shift_options, half_options


def _weekday_1_to_7(d: date) -> int:
    # Monday=1 ... Sunday=7
    return d.weekday() + 1


def _list_shifts_for_group(group: str):
    """Lista turnos SOLO del grupo, para que el selector sea claro."""
    rows = fetch_all(
        "SELECT shift_id, shift_code, start_time, end_time "
        "FROM rrhh.shift_definition "
        "WHERE is_active=1 AND shift_group=? "
        "ORDER BY shift_code",
        (group,),
    )
    out = []
    for r in rows or []:
        out.append(
            SimpleNamespace(
                shift_id=int(r.shift_id),
                shift_code=str(getattr(r, "shift_code", "") or "").strip(),
                start_time=getattr(r, "start_time", None),
                end_time=getattr(r, "end_time", None),
            )
        )
    return out


# --------------------------------------------------------------------------------------
# Planificación operativa (roster): Mes / Semana / Día / Horas / Cobertura
# --------------------------------------------------------------------------------------

@modulos_bp.route("/turnos/mes")
@login_required
def turnos_mes():
    if not _require_turnos_admin():
        return redirect(url_for("modulos.dashboard"))

    group = normalize_group(request.args.get("group") or "CABINA")

    month_s = (request.args.get("month") or "").strip()
    first = _parse_month_param(month_s)
    last = _month_range(first.year, first.month)[1]

    prev_first = (first.replace(day=1) - timedelta(days=1)).replace(day=1)
    next_first = (last + timedelta(days=1)).replace(day=1)

    ws0 = week_start(first)
    weeks = []
    cursor = ws0
    actor = _actor_user_id()

    while cursor <= last:
        we = cursor + timedelta(days=6)

        week_id = ensure_week_row(cursor, group, actor)
        hdr = get_week_header(week_id)

        v = validate_week(week_id)
        err_count = len(v.get("errors") or [])
        warn_count = len(v.get("warnings") or [])

        weeks.append(
            SimpleNamespace(
                week_id=int(week_id),
                week_start=cursor,
                week_end=we,
                status=(getattr(hdr, "status", None) if hdr else "DRAFT"),
                errors=err_count,
                warnings=warn_count,
            )
        )

        cursor = cursor + timedelta(days=7)

    return render_template(
        "modulos/turnos_mes.html",
        month=_as_month_str(first),
        prev_month=_as_month_str(prev_first),
        next_month=_as_month_str(next_first),
        group=group,
        weeks=weeks,
    )


@modulos_bp.route("/turnos/tablero")
@login_required
def turnos_tablero():
    if not _require_turnos_admin():
        return redirect(url_for("modulos.dashboard"))

    group = normalize_group(request.args.get("group") or "CABINA")
    week_s = (request.args.get("week") or "").strip()

    ref = parse_date(week_s) if week_s else date.today()
    ws = week_start(ref)
    we = ws + timedelta(days=6)

    week_id = ensure_week_row(ws, group, _actor_user_id())
    hdr = get_week_header(week_id)

    employees = list_employees_for_group(group)
    shifts = _list_shifts_for_group(group)

    days = fetch_roster_days(week_id)
    grid: Dict[Tuple[int, date], Any] = {}
    for r in days or []:
        grid[(int(r.employee_id), r.work_date)] = r

    validation = validate_week(week_id)

    week_days = [ws + timedelta(days=i) for i in range(7)]
    shift_options, half_options = _build_shift_options(shifts)

    return render_template(
        "modulos/turnos_tablero.html",
        week_id=int(week_id),
        hdr=hdr,
        ws=ws,
        we=we,
        group=group,
        week_days=week_days,
        day_labels=DAY_LABELS,
        employees=employees,
        shifts=shifts,
        grid=grid,
        validation=validation,
        special_options=SPECIAL_OPTIONS,
        shift_options=shift_options,
        half_options=half_options,
    )


@modulos_bp.route("/turnos/tablero/set", methods=["POST"])
@login_required
def turnos_tablero_set():
    if not _require_turnos_admin():
        return redirect(url_for("modulos.dashboard"))

    week_id = int(request.form.get("week_id") or 0)
    group = normalize_group(request.form.get("group") or "CABINA")
    employee_id = int(request.form.get("employee_id") or 0)
    work_date_s = (request.form.get("work_date") or "").strip()
    value = (request.form.get("value") or "").strip()

    if not week_id or not employee_id or not work_date_s:
        flash("Datos incompletos para guardar.", "warning")
        return redirect(url_for("modulos.turnos_tablero", group=group))

    try:
        work_date = parse_date(work_date_s)
        upsert_roster_day(week_id, employee_id, work_date, value, _actor_user_id())
        flash("Asignación guardada.", "success")
    except Exception as ex:
        flash(f"No se pudo guardar: {ex}", "warning")

    hdr = get_week_header(week_id)
    ws = hdr.week_start if hdr else week_start(date.today())

    return_to = (request.form.get("return_to") or "").strip().lower()
    if return_to == "horas":
        day_s = (request.form.get("day") or "").strip() or _as_date_str(work_date)
        return redirect(url_for("modulos.turnos_horas", week=_as_date_str(ws), group=group, day=day_s))

    return redirect(url_for("modulos.turnos_tablero", week=_as_date_str(ws), group=group))


@modulos_bp.route("/turnos/tablero/publicar", methods=["POST"])
@login_required
def turnos_tablero_publicar():
    """Publicación:
    - Bloquea por reglas duras (44h, descanso, domingos)
    - Cobertura 24/7:
        * CABINA: estricto (bloquea)
        * AJUSTADOR: permite huecos 23:00-05:00 como advertencia
    """
    if not _require_turnos_admin():
        return redirect(url_for("modulos.dashboard"))

    week_id = int(request.form.get("week_id") or 0)
    group = normalize_group(request.form.get("group") or "CABINA")

    if not week_id:
        flash("Semana inválida.", "warning")
        return redirect(url_for("modulos.turnos_tablero", group=group))

    v = validate_week(week_id)
    errors = v.get("errors") or []
    gaps = v.get("coverage_gaps") or []

    if errors:
        flash("No se puede publicar: hay reglas incumplidas (44h/descanso/domingos).", "warning")
        hdr = get_week_header(week_id)
        ws = hdr.week_start if hdr else date.today()
        return redirect(url_for("modulos.turnos_tablero", week=_as_date_str(ws), group=group))

    gaps_to_block = gaps
    allowed_window = None

    if group == "AJUSTADOR" and gaps:
        # Permitir huecos: 23:00-00:00, 00:00-01:00, 01:00-02:00, 02:00-03:00, 03:00-04:00, 04:00-05:00
        allowed_hours = {23, 0, 1, 2, 3, 4}
        allowed_window = "23:00–05:00"
        gaps_to_block = [g for g in gaps if int(g.get("hour", -1)) not in allowed_hours]

    if gaps_to_block and group != "AJUSTADOR":
        # CABINA (y cualquier otro grupo): estricto
        flash("No se puede publicar: hay horas sin cobertura (24/7).", "warning")
        hdr = get_week_header(week_id)
        ws = hdr.week_start if hdr else date.today()
        return redirect(url_for("modulos.turnos_tablero", week=_as_date_str(ws), group=group))

    if gaps_to_block and group == "AJUSTADOR":
        flash("No se puede publicar: hay horas sin cobertura fuera de la franja permitida.", "warning")
        hdr = get_week_header(week_id)
        ws = hdr.week_start if hdr else date.today()
        return redirect(url_for("modulos.turnos_tablero", week=_as_date_str(ws), group=group))

    execute(
        "UPDATE rrhh.shift_roster_week "
        "SET status='PUBLISHED', published_by_user_id=?, published_at=GETDATE() "
        "WHERE week_id=?",
        (_actor_user_id(), int(week_id)),
    )

    if gaps and group == "AJUSTADOR" and allowed_window:
        flash(f"Semana publicada. Advertencia: sin cobertura en franja permitida {allowed_window}.", "warning")
    else:
        flash("Semana publicada correctamente.", "success")

    hdr = get_week_header(week_id)
    ws = hdr.week_start if hdr else date.today()
    return redirect(url_for("modulos.turnos_tablero", week=_as_date_str(ws), group=group))


@modulos_bp.route("/turnos/sesion")
@login_required
def turnos_sesion():
    if not _require_turnos_admin():
        return redirect(url_for("modulos.dashboard"))

    group = normalize_group(request.args.get("group") or "CABINA")
    week_s = (request.args.get("week") or "").strip()
    day_s = (request.args.get("day") or "").strip()

    ref = parse_date(week_s) if week_s else (parse_date(day_s) if day_s else date.today())
    ws = week_start(ref)

    day = parse_date(day_s) if day_s else ws
    prev_day = day - timedelta(days=1)

    week_id = ensure_week_row(ws, group, _actor_user_id())

    shifts = _list_shifts_for_group(group)
    shift_by_id = {int(s.shift_id): s for s in shifts or []}

    employees = list_employees_for_group(group)
    emp_by_id = {int(e.employee_id): e for e in employees or []}

    day_rows = list_assignments_for_day(week_id, day)
    prev_rows = list_assignments_for_day(week_id, prev_day)

    hour_participants: List[List[Any]] = [[] for _ in range(24)]

    def add_participant(h: int, eid: int, shift_code: str):
        e = emp_by_id.get(eid)
        if not e:
            return
        hour_participants[h].append(
            SimpleNamespace(
                name=f"{e.first_name} {e.last_name}".strip(),
                shift_code=shift_code,
            )
        )

    for r in day_rows or []:
        if int(getattr(r, "is_day_off", 0) or 0) == 1:
            continue
        sid = getattr(r, "shift_id", None)
        if sid is None:
            continue
        sd = shift_by_id.get(int(sid))
        if not sd:
            continue
        for h in range(24):
            if _covers_hour_on_target_date(sd, day, day, h):
                add_participant(h, int(r.employee_id), sd.shift_code)

    for r in prev_rows or []:
        if int(getattr(r, "is_day_off", 0) or 0) == 1:
            continue
        sid = getattr(r, "shift_id", None)
        if sid is None:
            continue
        sd = shift_by_id.get(int(sid))
        if not sd:
            continue
        for h in range(24):
            if _covers_hour_on_target_date(sd, prev_day, day, h):
                add_participant(h, int(r.employee_id), sd.shift_code)

    gaps = [h for h in range(24) if len(hour_participants[h]) == 0]

    hour_suggestions: Dict[int, List[str]] = {}
    for h in range(24):
        codes = []
        for s in shifts or []:
            if _covers_hour_on_target_date(s, day, day, h):
                codes.append(f"{s.shift_code} · {_shift_label(s)}" if _shift_label(s) else s.shift_code)
        hour_suggestions[h] = codes[:12]

    shift_opts = []
    for s in shifts or []:
        shift_opts.append(
            SimpleNamespace(
                code=s.shift_code,
                label=f"{s.shift_code} · {_shift_label(s)}" if _shift_label(s) else s.shift_code,
            )
        )

    assigned = []
    for r in day_rows or []:
        eid = int(r.employee_id)
        e = emp_by_id.get(eid)
        if not e:
            continue

        code = (getattr(r, "code", None) or getattr(r, "notes", "") or "").strip()
        sid = getattr(r, "shift_id", None)
        is_off = int(getattr(r, "is_day_off", 0) or 0) == 1

        if is_off and code:
            label = code
        elif sid is not None:
            sd = shift_by_id.get(int(sid))
            label = f"{sd.shift_code} · {_shift_label(sd)}" if sd else "Turno"
        else:
            label = "—"

        assigned.append(
            SimpleNamespace(
                employee_id=eid,
                name=f"{e.first_name} {e.last_name}".strip(),
                label=label,
            )
        )

    return render_template(
        "modulos/turnos_sesion.html",
        week_id=int(week_id),
        ws=ws,
        day=day,
        group=group,
        gaps=gaps,
        hour_participants=hour_participants,
        shift_opts=shift_opts,
        hour_suggestions=hour_suggestions,
        employees=employees,
        assigned=assigned,
    )


@modulos_bp.route("/turnos/sesion/asignar", methods=["POST"])
@login_required
def turnos_sesion_asignar():
    if not _require_turnos_admin():
        return redirect(url_for("modulos.dashboard"))

    week_id = int(request.form.get("week_id") or 0)
    group = normalize_group(request.form.get("group") or "CABINA")
    day_s = (request.form.get("day") or "").strip()
    shift_code = (request.form.get("shift_code") or "").strip()
    employee_ids = request.form.getlist("employee_ids")

    if not week_id or not day_s or not shift_code or not employee_ids:
        flash("Completa turno, día y al menos una persona.", "warning")
        return redirect(url_for("modulos.turnos_tablero", group=group))

    day = parse_date(day_s)
    ok = 0
    err = 0
    for eid in employee_ids:
        try:
            upsert_roster_day(week_id, int(eid), day, shift_code, _actor_user_id())
            ok += 1
        except Exception:
            err += 1

    flash(f"Asignación aplicada. OK: {ok}. Errores: {err}.", "warning" if err else "success")

    hdr = get_week_header(week_id)
    ws = hdr.week_start if hdr else week_start(day)
    return redirect(url_for("modulos.turnos_sesion", week=_as_date_str(ws), group=group, day=_as_date_str(day)))


@modulos_bp.route("/turnos/sesion/quitar", methods=["POST"])
@login_required
def turnos_sesion_quitar():
    if not _require_turnos_admin():
        return redirect(url_for("modulos.dashboard"))

    week_id = int(request.form.get("week_id") or 0)
    group = normalize_group(request.form.get("group") or "CABINA")
    day_s = (request.form.get("day") or "").strip()
    employee_id = int(request.form.get("employee_id") or 0)

    if not week_id or not day_s or not employee_id:
        flash("Datos incompletos.", "warning")
        return redirect(url_for("modulos.turnos_tablero", group=group))

    day = parse_date(day_s)
    try:
        upsert_roster_day(week_id, employee_id, day, "", _actor_user_id())
        flash("Asignación removida.", "success")
    except Exception as ex:
        flash(f"No se pudo quitar: {ex}", "warning")

    hdr = get_week_header(week_id)
    ws = hdr.week_start if hdr else week_start(day)
    return redirect(url_for("modulos.turnos_sesion", week=_as_date_str(ws), group=group, day=_as_date_str(day)))


@modulos_bp.route("/turnos/horas")
@login_required
def turnos_horas():
    """Vista por horas para “leer” el día."""
    if not _require_turnos_admin():
        return redirect(url_for("modulos.dashboard"))

    group = normalize_group(request.args.get("group") or "CABINA")
    week_s = (request.args.get("week") or "").strip()
    day_s = (request.args.get("day") or "").strip()

    ref = parse_date(week_s) if week_s else (parse_date(day_s) if day_s else date.today())
    ws = week_start(ref)
    we = ws + timedelta(days=6)

    day_date = parse_date(day_s) if day_s else ws
    prev_day = day_date - timedelta(days=1)

    week_id = ensure_week_row(ws, group, _actor_user_id())

    shifts = _list_shifts_for_group(group)
    shift_by_id = {int(s.shift_id): s for s in shifts or []}

    employees = list_employees_for_group(group)

    day_rows = list_assignments_for_day(week_id, day_date)
    prev_rows = list_assignments_for_day(week_id, prev_day)
    day_row_by_emp = {int(r.employee_id): r for r in (day_rows or [])}
    prev_row_by_emp = {int(r.employee_id): r for r in (prev_rows or [])}

    wd_today = _weekday_1_to_7(day_date)
    wd_prev = _weekday_1_to_7(prev_day)

    # Requerimientos por shift+weekday (columna real: required_count)
    req_rows = fetch_all(
        "SELECT weekday, shift_id, required_count AS min_count "
        "FROM rrhh.shift_coverage_requirement "
        "WHERE shift_group=? AND weekday IN (?, ?)",
        (group, wd_today, wd_prev),
    )
    req_map: Dict[Tuple[int, int], int] = {}
    for r in req_rows or []:
        req_map[(int(r.weekday), int(r.shift_id))] = int(r.min_count or 0)

    coverage_required = [0] * 24
    for s in shifts or []:
        if not s.start_time or not s.end_time:
            continue
        min_today = req_map.get((wd_today, int(s.shift_id)), 0)
        min_prev = req_map.get((wd_prev, int(s.shift_id)), 0)
        crosses = s.end_time <= s.start_time

        start_h = int(s.start_time.hour)
        end_h = _hour_end_hour(s.end_time)

        if not crosses:
            for h in range(start_h, end_h):
                coverage_required[h] += min_today
        else:
            for h in range(start_h, 24):
                coverage_required[h] += min_today
            for h in range(0, end_h):
                coverage_required[h] += min_prev

    coverage_assigned = [0] * 24

    def count_assignment(r, assigned_date: date, target_date: date, h: int) -> bool:
        if not r:
            return False
        if int(getattr(r, "is_day_off", 0) or 0) == 1:
            return False
        sid = getattr(r, "shift_id", None)
        if sid is None:
            return False
        sd = shift_by_id.get(int(sid))
        if not sd:
            return False
        return _covers_hour_on_target_date(sd, assigned_date, target_date, h)

    for h in range(24):
        cnt = 0
        for e in employees or []:
            eid = int(e.employee_id)
            if count_assignment(day_row_by_emp.get(eid), day_date, day_date, h):
                cnt += 1
            elif count_assignment(prev_row_by_emp.get(eid), prev_day, day_date, h):
                cnt += 1
        coverage_assigned[h] = cnt

    out_employees = []
    hour_labels = [f"{i+1:02d}" for i in range(24)]  # 01–24

    for e in employees or []:
        eid = int(e.employee_id)
        name = f"{e.first_name} {e.last_name}".strip()
        doc = getattr(e, "doc_number", "") or ""

        slots = []
        r_today = day_row_by_emp.get(eid)
        r_prev = prev_row_by_emp.get(eid)

        for h in range(24):
            kind = "EMPTY"
            label = ""
            start = False

            if count_assignment(r_prev, prev_day, day_date, h):
                sid = int(getattr(r_prev, "shift_id"))
                sd = shift_by_id.get(sid)
                if sd:
                    kind = "SHIFT"
                    if h == 0:
                        label = f"{sd.shift_code}"
                        start = True

            if count_assignment(r_today, day_date, day_date, h):
                sid = int(getattr(r_today, "shift_id"))
                sd = shift_by_id.get(sid)
                if sd:
                    kind = "SHIFT"
                    if h == int(sd.start_time.hour):
                        label = f"{sd.shift_code}"
                        start = True

            if r_today and int(getattr(r_today, "is_day_off", 0) or 0) == 1:
                code = (getattr(r_today, "code", None) or getattr(r_today, "notes", "") or "").strip().upper()
                kind = "OFF"
                if h == 0:
                    label = code or "OFF"
                    start = True

            slots.append(SimpleNamespace(kind=kind, label=label, start=start))

        out_employees.append(SimpleNamespace(employee_id=eid, name=name, doc=doc, slots=slots))

    shift_options, half_options = _build_shift_options(shifts)
    week_days = [ws + timedelta(days=i) for i in range(7)]

    return render_template(
        "modulos/turnos_horas.html",
        week_id=int(week_id),
        ws=ws,
        we=we,
        group=group,
        week_days=week_days,
        day_labels=DAY_LABELS,
        day_date=day_date,
        hour_labels=hour_labels,
        employees=out_employees,
        shifts=shifts,
        coverage_assigned=coverage_assigned,
        coverage_required=coverage_required,
        special_options=SPECIAL_OPTIONS,
        shift_options=shift_options,
        half_options=half_options,
    )


@modulos_bp.route("/turnos/cobertura", methods=["GET", "POST"])
@login_required
def turnos_cobertura():
    if not _require_turnos_admin():
        return redirect(url_for("modulos.dashboard"))

    group = normalize_group((request.values.get("group") or "CABINA").strip())

    srows = fetch_all(
        "SELECT shift_id, shift_code, start_time, end_time "
        "FROM rrhh.shift_definition WHERE is_active=1 AND shift_group=? "
        "ORDER BY shift_code",
        (group,),
    )
    shifts = []
    for s in srows or []:
        shifts.append(
            SimpleNamespace(
                shift_id=int(s.shift_id),
                shift_code=(s.shift_code or "").strip(),
                start_time=getattr(s, "start_time", None),
                end_time=getattr(s, "end_time", None),
            )
        )

    req_rows = fetch_all(
        "SELECT weekday, shift_id, required_count AS min_count "
        "FROM rrhh.shift_coverage_requirement "
        "WHERE shift_group=?",
        (group,),
    )
    req_map: Dict[Tuple[int, int], int] = {}
    for r in req_rows or []:
        req_map[(int(r.weekday), int(r.shift_id))] = int(r.min_count or 0)

    if request.method == "POST":
        changed = 0
        for s in shifts:
            for wd in range(1, 8):
                key = f"req_{wd}_{int(s.shift_id)}"
                if key not in request.form:
                    continue
                try:
                    val = int((request.form.get(key) or "0").strip() or 0)
                except Exception:
                    val = 0

                exists = fetch_one(
                    "SELECT 1 FROM rrhh.shift_coverage_requirement WHERE shift_group=? AND weekday=? AND shift_id=?",
                    (group, int(wd), int(s.shift_id)),
                )
                if exists:
                    execute(
                        "UPDATE rrhh.shift_coverage_requirement SET required_count=? WHERE shift_group=? AND weekday=? AND shift_id=?",
                        (val, group, int(wd), int(s.shift_id)),
                    )
                else:
                    execute(
                        "INSERT INTO rrhh.shift_coverage_requirement(shift_group, weekday, shift_id, required_count) VALUES (?,?,?,?)",
                        (group, int(wd), int(s.shift_id), val),
                    )
                req_map[(int(wd), int(s.shift_id))] = val
                changed += 1

        flash(f"Cobertura guardada ({changed} celdas).", "success")
        return redirect(url_for("modulos.turnos_cobertura", group=group))

    return render_template("modulos/turnos_cobertura.html", group=group, shifts=shifts, req_map=req_map)
