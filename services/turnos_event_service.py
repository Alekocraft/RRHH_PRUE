from __future__ import annotations

"""Planner de turnos por eventos (drag/resize por horas, estilo Teams).

Este módulo agrega planificación operativa por intervalos start/end.

Tablas (ver sql/patch_turnos_planner_events.sql):
- rrhh.shift_roster_week (ya existe): agregamos coverage_policy_json.
- rrhh.shift_roster_event (nueva): eventos por empleado con start_dt/end_dt.

Reglas (generan alertas; publicar requiere confirmación si hay alertas):
- Max 44 horas semanales trabajadas.
- No permitir 7 días consecutivos trabajados.
- No trabajar 2 domingos seguidos.
- Respetar chequera (rrhh.timebook_request) y cumpleaños (hr_employee.birth_date): bloquean solapes.
- Cobertura es opcional: solo si la política semanal define un mínimo.

Notas:
- Guardar (DRAFT) siempre permitido.
- Publicar: si hay alertas devuelve needs_confirm; con force=1 publica.
"""

import json
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple

from services.rrhh_db import fetch_all, fetch_one, execute
from services import turnos_roster_service as roster
from services import schedule_service


@dataclass
class Issue:
    severity: str  # 'error'|'warning'
    rule: str
    message: str
    employee_id: Optional[int] = None
    employee_name: Optional[str] = None
    meta: Optional[Dict[str, Any]] = None


# -----------------------------------------------------------------------------
# DB helpers
# -----------------------------------------------------------------------------

def _has_col(schema: str, table: str, col: str) -> bool:
    return (
        fetch_one(
            "SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND COLUMN_NAME=?",
            (schema, table, col),
        )
        is not None
    )


def _has_table(schema: str, table: str) -> bool:
    return (
        fetch_one(
            "SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=?",
            (schema, table),
        )
        is not None
    )


def assert_schema() -> None:
    if not _has_table("rrhh", "shift_roster_week"):
        raise RuntimeError("No existe rrhh.shift_roster_week")
    if not _has_table("rrhh", "shift_roster_event"):
        raise RuntimeError(
            "No existe rrhh.shift_roster_event. Ejecuta sql/patch_turnos_planner_events.sql"
        )


# -----------------------------------------------------------------------------
# Week header + policy
# -----------------------------------------------------------------------------

def ensure_week(week_start: date, group: str, actor_user_id: Optional[int]) -> int:
    return roster.ensure_week_row(week_start, group, actor_user_id)


def get_week_header(week_id: int) -> Any:
    return roster.get_week_header(week_id)


def get_policy_json(week_id: int) -> Optional[dict]:
    if not _has_col("rrhh", "shift_roster_week", "coverage_policy_json"):
        return None
    row = fetch_one(
        "SELECT coverage_policy_json FROM rrhh.shift_roster_week WHERE week_id=?",
        (int(week_id),),
    )
    if not row:
        return None
    raw = getattr(row, "coverage_policy_json", None)
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return None


def set_policy_json(week_id: int, policy: Optional[dict]) -> None:
    if not _has_col("rrhh", "shift_roster_week", "coverage_policy_json"):
        return
    raw = json.dumps(policy, ensure_ascii=False) if policy else None
    execute(
        "UPDATE rrhh.shift_roster_week SET coverage_policy_json=? WHERE week_id=?",
        (raw, int(week_id)),
    )


def set_week_status_draft(week_id: int) -> None:
    """Si está publicada y cambias eventos, vuelve a DRAFT."""
    execute(
        "UPDATE rrhh.shift_roster_week "
        "SET status='DRAFT', published_at=NULL, published_by_user_id=NULL "
        "WHERE week_id=? AND status='PUBLISHED'",
        (int(week_id),),
    )


def publish_week(week_id: int, actor_user_id: Optional[int]) -> None:
    execute(
        "UPDATE rrhh.shift_roster_week "
        "SET status='PUBLISHED', published_at=GETDATE(), published_by_user_id=? "
        "WHERE week_id=?",
        (actor_user_id, int(week_id)),
    )


# -----------------------------------------------------------------------------
# Events CRUD
# -----------------------------------------------------------------------------

def _clip_interval(a_start: datetime, a_end: datetime, b_start: datetime, b_end: datetime) -> Optional[Tuple[datetime, datetime]]:
    s = max(a_start, b_start)
    e = min(a_end, b_end)
    if e <= s:
        return None
    return s, e


def list_events(week_id: int, employee_id: Optional[int] = None) -> List[dict]:
    assert_schema()
    if employee_id:
        rows = fetch_all(
            "SELECT event_id, week_id, employee_id, shift_group, start_dt, end_dt, event_type, leave_code, notes "
            "FROM rrhh.shift_roster_event WHERE week_id=? AND employee_id=? ORDER BY start_dt",
            (int(week_id), int(employee_id)),
        )
    else:
        rows = fetch_all(
            "SELECT event_id, week_id, employee_id, shift_group, start_dt, end_dt, event_type, leave_code, notes "
            "FROM rrhh.shift_roster_event WHERE week_id=? ORDER BY employee_id, start_dt",
            (int(week_id),),
        )

    out: List[dict] = []
    for r in rows or []:
        out.append(
            {
                "event_id": int(r.event_id),
                "week_id": int(r.week_id),
                "employee_id": int(r.employee_id),
                "group": str(r.shift_group),
                "start": r.start_dt.isoformat(sep=" ") if getattr(r, "start_dt", None) else None,
                "end": r.end_dt.isoformat(sep=" ") if getattr(r, "end_dt", None) else None,
                "event_type": str(r.event_type),
                "leave_code": getattr(r, "leave_code", None),
                "notes": getattr(r, "notes", None),
            }
        )
    return out


def _overlaps_other(employee_id: int, start_dt: datetime, end_dt: datetime, ignore_event_id: Optional[int]) -> bool:
    sql = (
        "SELECT TOP (1) 1 AS ok FROM rrhh.shift_roster_event "
        "WHERE employee_id=? AND start_dt < ? AND end_dt > ?"
    )
    params: List[Any] = [int(employee_id), end_dt, start_dt]
    if ignore_event_id:
        sql += " AND event_id <> ?"
        params.append(int(ignore_event_id))
    row = fetch_one(sql, tuple(params))
    return row is not None


# -----------------------------------------------------------------------------
# Blocking leave: chequera + cumpleaños
# -----------------------------------------------------------------------------

def _employee_birthday_date(employee_id: int, year: int) -> Optional[date]:
    """Devuelve la fecha de cumpleaños (mes/día) para un año.

    Tu esquema actual (rrhh.sql) NO trae columna de cumpleaños en rrhh.hr_employee.
    Antes esto rompía /turnos/planificacion con un 500 (Invalid column name 'birth_date').

    Comportamiento:
    - Si no existe una columna conocida de cumpleaños, retorna None (no bloquea por cumpleaños).
    - Si existe (birth_date / fecha_nacimiento / etc.), retorna la fecha para el año solicitado.
    """

    # Cacheo de nombre de columna para evitar buscar en INFORMATION_SCHEMA cada vez.
    global _EMP_BDAY_COL
    try:
        _EMP_BDAY_COL
    except Exception:
        _EMP_BDAY_COL = "__UNRESOLVED__"

    if _EMP_BDAY_COL == "__UNRESOLVED__":
        candidates = [
            "birth_date",
            "birthday",
            "date_of_birth",
            "dob",
            "fecha_nacimiento",
            "nacimiento",
        ]
        found: Optional[str] = None
        for c in candidates:
            ok = fetch_one(
                "SELECT 1 AS ok FROM INFORMATION_SCHEMA.COLUMNS "
                "WHERE TABLE_SCHEMA='rrhh' AND TABLE_NAME='hr_employee' AND COLUMN_NAME=?",
                (c,),
            )
            if ok:
                found = c
                break
        _EMP_BDAY_COL = found

    col = _EMP_BDAY_COL
    if not col:
        return None

    row = fetch_one(
        f"SELECT {col} AS bday FROM rrhh.hr_employee WHERE employee_id=?",
        (int(employee_id),),
    )
    if not row:
        return None

    bd = getattr(row, "bday", None)
    if not bd:
        return None

    try:
        bdd = bd if isinstance(bd, date) and not isinstance(bd, datetime) else bd.date()
        return date(int(year), int(bdd.month), int(bdd.day))
    except Exception:
        return None


def _chequera_blocks(employee_id: int, d_from: date, d_to: date) -> List[Tuple[datetime, datetime, str]]:
    if not _has_table("rrhh", "timebook_request"):
        return []

    rows = fetch_all(
        "SELECT request_date, slot FROM rrhh.timebook_request "
        "WHERE status='APPROVED' AND employee_id=? AND request_date BETWEEN ? AND ?",
        (int(employee_id), d_from, d_to),
    )

    out: List[Tuple[datetime, datetime, str]] = []
    for r in rows or []:
        d = r.request_date
        slot = (r.slot or "AM").upper()
        sh = schedule_service.get_current_shift_assignment(int(employee_id), d)
        if not sh or not getattr(sh, "start_time", None) or not getattr(sh, "end_time", None):
            out.append((datetime.combine(d, time(0, 0)), datetime.combine(d, time(23, 59)), f"CHEQ-{slot}"))
            continue

        st: time = sh.start_time
        et: time = sh.end_time
        st_dt = datetime.combine(d, st)
        et_dt = datetime.combine(d, et)
        if et_dt <= st_dt:
            et_dt += timedelta(days=1)

        dur = et_dt - st_dt
        half = dur / 2

        if slot == "AM":
            out.append((st_dt, st_dt + half, "CHEQ-AM"))
        else:
            out.append((et_dt - half, et_dt, "CHEQ-PM"))

    return out


def blocking_leave_intervals(employee_id: int, start_dt: datetime, end_dt: datetime) -> List[Tuple[datetime, datetime, str]]:
    d_from = start_dt.date() - timedelta(days=1)
    d_to = end_dt.date() + timedelta(days=1)

    blocks: List[Tuple[datetime, datetime, str]] = []
    blocks.extend(_chequera_blocks(employee_id, d_from, d_to))

    bday = _employee_birthday_date(employee_id, start_dt.year)
    if bday and d_from <= bday <= d_to:
        blocks.append((datetime.combine(bday, time(0, 0)), datetime.combine(bday, time(23, 59)), "CUMPLE"))

    return blocks


def assert_no_blocking_leave(employee_id: int, start_dt: datetime, end_dt: datetime) -> None:
    for bs, be, code in blocking_leave_intervals(employee_id, start_dt, end_dt):
        if _clip_interval(start_dt, end_dt, bs, be):
            raise ValueError(f"Se solapa con {code} (chequera/cumpleaños)")


def list_blocking_leave_for_week(employee_id: int, ws: date) -> List[dict]:
    start_dt = datetime.combine(ws, time(0, 0))
    end_dt = datetime.combine(ws + timedelta(days=7), time(0, 0))
    out: List[dict] = []
    for bs, be, code in blocking_leave_intervals(employee_id, start_dt, end_dt):
        if not _clip_interval(bs, be, start_dt, end_dt):
            continue
        out.append({"start": bs.isoformat(sep=" "), "end": be.isoformat(sep=" "), "code": code})
    return out


# -----------------------------------------------------------------------------
# CRUD using the blocking checks
# -----------------------------------------------------------------------------

def create_event(
    week_id: int,
    employee_id: int,
    group: str,
    start_dt: datetime,
    end_dt: datetime,
    event_type: str,
    leave_code: Optional[str],
    notes: Optional[str],
    actor_user_id: Optional[int],
) -> int:
    assert_schema()
    if end_dt <= start_dt:
        raise ValueError("end_dt debe ser mayor que start_dt")

    if _overlaps_other(employee_id, start_dt, end_dt, None):
        raise ValueError("El evento se solapa con otro bloque del empleado")

    assert_no_blocking_leave(employee_id, start_dt, end_dt)

    execute(
        "INSERT INTO rrhh.shift_roster_event(week_id, employee_id, shift_group, start_dt, end_dt, event_type, leave_code, notes, created_by_user_id, created_at) "
        "VALUES (?,?,?,?,?,?,?,?,?,GETDATE())",
        (
            int(week_id),
            int(employee_id),
            roster.normalize_group(group),
            start_dt,
            end_dt,
            (event_type or "WORK").upper(),
            (leave_code or None),
            (notes or None),
            actor_user_id,
        ),
    )

    row = fetch_one(
        "SELECT TOP (1) event_id FROM rrhh.shift_roster_event WHERE week_id=? AND employee_id=? ORDER BY event_id DESC",
        (int(week_id), int(employee_id)),
    )
    if not row:
        raise RuntimeError("No se pudo crear el evento")

    set_week_status_draft(week_id)
    return int(row.event_id)


def update_event(
    event_id: int,
    start_dt: datetime,
    end_dt: datetime,
    actor_user_id: Optional[int],
    notes: Optional[str] = None,
) -> None:
    assert_schema()
    row = fetch_one(
        "SELECT week_id, employee_id FROM rrhh.shift_roster_event WHERE event_id=?",
        (int(event_id),),
    )
    if not row:
        raise ValueError("Evento no existe")

    week_id = int(row.week_id)
    employee_id = int(row.employee_id)

    if end_dt <= start_dt:
        raise ValueError("end_dt debe ser mayor que start_dt")

    if _overlaps_other(employee_id, start_dt, end_dt, event_id):
        raise ValueError("El evento se solapa con otro bloque del empleado")

    assert_no_blocking_leave(employee_id, start_dt, end_dt)

    execute(
        "UPDATE rrhh.shift_roster_event SET start_dt=?, end_dt=?, notes=?, updated_by_user_id=?, updated_at=GETDATE() WHERE event_id=?",
        (start_dt, end_dt, notes, actor_user_id, int(event_id)),
    )

    set_week_status_draft(week_id)


def delete_event(event_id: int) -> None:
    assert_schema()
    row = fetch_one(
        "SELECT week_id FROM rrhh.shift_roster_event WHERE event_id=?",
        (int(event_id),),
    )
    if not row:
        return
    week_id = int(row.week_id)
    execute("DELETE FROM rrhh.shift_roster_event WHERE event_id=?", (int(event_id),))
    set_week_status_draft(week_id)


# -----------------------------------------------------------------------------
# Validation
# -----------------------------------------------------------------------------

def _minutes_between(a: datetime, b: datetime) -> int:
    return int((b - a).total_seconds() // 60)


def _date_range(d1: date, d2: date) -> List[date]:
    out: List[date] = []
    cur = d1
    while cur <= d2:
        out.append(cur)
        cur += timedelta(days=1)
    return out


def _employee_name_map(emp_rows: Iterable[Any]) -> Dict[int, str]:
    out: Dict[int, str] = {}
    for e in emp_rows or []:
        out[int(e.employee_id)] = f"{getattr(e, 'first_name', '')} {getattr(e, 'last_name', '')}".strip()
    return out


def validate_week(week_id: int) -> Dict[str, Any]:
    assert_schema()

    hdr = get_week_header(week_id)
    if not hdr:
        raise ValueError("Semana no existe")

    ws: date = hdr.week_start
    we: date = ws + timedelta(days=7)
    group = roster.normalize_group(hdr.shift_group)

    employees = roster.list_employees_for_group(group) or []
    emp_name = _employee_name_map(employees)
    emp_ids = [int(e.employee_id) for e in employees]

    week_start_dt = datetime.combine(ws, time(0, 0))
    week_end_dt = datetime.combine(we, time(0, 0))

    events = fetch_all(
        "SELECT employee_id, start_dt, end_dt, event_type FROM rrhh.shift_roster_event WHERE week_id=?",
        (int(week_id),),
    ) or []

    work_by_emp: Dict[int, List[Tuple[datetime, datetime]]] = {eid: [] for eid in emp_ids}
    for ev in events:
        if str(ev.event_type).upper() != "WORK":
            continue
        eid = int(ev.employee_id)
        if eid not in work_by_emp:
            continue
        clip = _clip_interval(ev.start_dt, ev.end_dt, week_start_dt, week_end_dt)
        if clip:
            work_by_emp[eid].append(clip)

    issues: List[Issue] = []

    # 44h rule
    for eid, intervals in work_by_emp.items():
        total_min = sum(_minutes_between(s, e) for s, e in intervals)
        total_h = total_min / 60.0
        if total_h > 44.0 + 1e-6:
            issues.append(
                Issue(
                    severity="error",
                    rule="MAX_44H_WEEK",
                    employee_id=eid,
                    employee_name=emp_name.get(eid),
                    message=f"Supera 44h semanales: {total_h:.1f}h (límite 44h)",
                    meta={"hours": round(total_h, 2)},
                )
            )

    # Worked days (look back 6 days)
    lookback_from = ws - timedelta(days=6)
    lookback_start_dt = datetime.combine(lookback_from, time(0, 0))

    worked_days: Dict[int, set] = {eid: set() for eid in emp_ids}

    # Load work events that overlap lookback..week_end
    if emp_ids:
        rows_prev = fetch_all(
            "SELECT employee_id, start_dt, end_dt, event_type FROM rrhh.shift_roster_event "
            "WHERE employee_id IN ({}) AND start_dt < ? AND end_dt > ?".format(
                ",".join(["?"] * len(emp_ids))
            ),
            tuple(emp_ids + [week_end_dt, lookback_start_dt]),
        )
    else:
        rows_prev = []

    def _mark_days(eid: int, sdt: datetime, edt: datetime):
        cur = sdt.date()
        last = (edt - timedelta(minutes=1)).date() if edt > sdt else sdt.date()
        for d in _date_range(cur, last):
            worked_days[eid].add(d)

    for r in rows_prev or []:
        if str(r.event_type).upper() != "WORK":
            continue
        eid = int(r.employee_id)
        if eid not in worked_days:
            continue
        clip = _clip_interval(r.start_dt, r.end_dt, lookback_start_dt, week_end_dt)
        if clip:
            _mark_days(eid, clip[0], clip[1])

    for eid in emp_ids:
        window_days = _date_range(lookback_from, we - timedelta(days=1))
        streak = 0
        max_streak = 0
        streak_end: Optional[date] = None
        for d in window_days:
            if d in worked_days[eid]:
                streak += 1
                if streak > max_streak:
                    max_streak = streak
                    streak_end = d
            else:
                streak = 0
        if max_streak >= 7:
            issues.append(
                Issue(
                    severity="error",
                    rule="MAX_6_CONSECUTIVE_DAYS",
                    employee_id=eid,
                    employee_name=emp_name.get(eid),
                    message=f"Trabaja {max_streak} días seguidos (debe descansar antes de 7 días).",
                    meta={"max_streak": max_streak, "streak_end": str(streak_end) if streak_end else None},
                )
            )

    # Sundays
    this_sun = ws + timedelta(days=(6 - ws.weekday()))
    prev_sun = this_sun - timedelta(days=7)
    for eid in emp_ids:
        if this_sun in worked_days[eid] and prev_sun in worked_days[eid]:
            issues.append(
                Issue(
                    severity="error",
                    rule="NO_TWO_SUNDAYS",
                    employee_id=eid,
                    employee_name=emp_name.get(eid),
                    message=f"Trabaja dos domingos seguidos ({prev_sun} y {this_sun}).",
                )
            )

    # Leave conflicts (chequera/cumple)
    for eid, intervals in work_by_emp.items():
        blocks = blocking_leave_intervals(eid, week_start_dt, week_end_dt)
        for bs, be, code in blocks:
            for s, e in intervals:
                if _clip_interval(s, e, bs, be):
                    issues.append(
                        Issue(
                            severity="error",
                            rule="LEAVE_CONFLICT",
                            employee_id=eid,
                            employee_name=emp_name.get(eid),
                            message=f"Se programó trabajo durante {code} (chequera/cumpleaños).",
                            meta={"leave": code},
                        )
                    )
                    break

    # Coverage (optional)
    policy = get_policy_json(week_id) or {}
    cov_rule = policy.get("rule") if isinstance(policy, dict) else None
    if cov_rule and isinstance(cov_rule, dict) and cov_rule.get("min"):
        try:
            min_req = int(cov_rule.get("min"))
        except Exception:
            min_req = 0
        if min_req > 0:
            step_min = int(cov_rule.get("step_min") or 30)
            if step_min not in (15, 30, 60):
                step_min = 30

            def _parse_hhmm(v: str, default: str) -> time:
                s = (v or default).strip()
                if s == "24:00":
                    return time(0, 0)
                hh, mm = s.split(":")
                return time(int(hh), int(mm))

            t_start = _parse_hhmm(cov_rule.get("start") or "00:00", "00:00")
            t_end = _parse_hhmm(cov_rule.get("end") or "24:00", "24:00")

            all_work: List[Tuple[datetime, datetime]] = []
            for intervals in work_by_emp.values():
                all_work.extend(intervals)

            cur_day = ws
            while cur_day < we:
                day_start_dt = datetime.combine(cur_day, t_start)
                # end: if 24:00 -> next day 00:00
                if t_end == time(0, 0):
                    day_end_dt = datetime.combine(cur_day + timedelta(days=1), time(0, 0))
                else:
                    day_end_dt = datetime.combine(cur_day, t_end)
                if day_end_dt <= day_start_dt:
                    day_end_dt += timedelta(days=1)

                cur = day_start_dt
                while cur + timedelta(minutes=step_min) <= day_end_dt:
                    nxt = cur + timedelta(minutes=step_min)
                    count = 0
                    for s, e in all_work:
                        if _clip_interval(s, e, cur, nxt):
                            count += 1
                    if count < min_req:
                        issues.append(
                            Issue(
                                severity="warning",
                                rule="COVERAGE",
                                message=f"Cobertura baja {cur.strftime('%a %H:%M')}–{nxt.strftime('%H:%M')}: {count}/{min_req}",
                                meta={"count": count, "min": min_req, "start": cur.isoformat(sep=' '), "end": nxt.isoformat(sep=' ')},
                            )
                        )
                    cur = nxt

                cur_day += timedelta(days=1)

    totals = {eid: round(sum(_minutes_between(s, e) for s, e in intervals) / 60.0, 2) for eid, intervals in work_by_emp.items()}

    return {
        "week_id": int(week_id),
        "week_start": str(ws),
        "group": group,
        "issues": [i.__dict__ for i in issues],
        "totals_hours": totals,
        "status": getattr(hdr, "status", "DRAFT"),
        "policy": policy,
    }
