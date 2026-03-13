from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, time
from typing import Any, Dict, List, Optional, Tuple

from services.rrhh_db import fetch_one, fetch_all, execute


# --------------------------------------------------------------------------------------
# Data
# --------------------------------------------------------------------------------------


@dataclass
class ShiftDef:
    shift_id: int
    shift_code: str
    start_time: Optional[time]
    end_time: Optional[time]


SPECIAL_CODES = ["DES", "INC", "VAC", "HB", "1P", "2P"]

# Solo se planifica operación 24/7 para estos tableros.
BOARD_GROUPS = ("CABINA", "AJUSTADOR")


def normalize_group(group: str) -> str:
    g = (group or "CABINA").strip().upper() or "CABINA"
    if g.startswith("AJUST"):
        return "AJUSTADOR"
    if g.startswith("CAB"):
        return "CABINA"
    # Cualquier otro valor se fuerza a CABINA para no exponer administrativos.
    return "CABINA"


# --------------------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------------------


def week_start(d: date) -> date:
    # Monday
    return d - timedelta(days=(d.weekday() % 7))


def parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _has_col(schema: str, table: str, column: str) -> bool:
    r = fetch_one(
        "SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND COLUMN_NAME=?",
        (schema, table, column),
    )
    return r is not None


def _shift_minutes(start_t: Optional[time], end_t: Optional[time]) -> int:
    if not start_t or not end_t:
        return 0
    dt1 = datetime.combine(date(2000, 1, 1), start_t)
    dt2 = datetime.combine(date(2000, 1, 1), end_t)
    if dt2 <= dt1:
        dt2 = dt2 + timedelta(days=1)
    return int((dt2 - dt1).total_seconds() // 60)


def _weekday_1_to_7(d: date) -> int:
    # Monday=1 ... Sunday=7
    return d.weekday() + 1


def _is_work_day(is_day_off: int, shift_id: Optional[int], code: str, shift_minutes: int) -> bool:
    if int(is_day_off or 0) == 1:
        return False
    if not shift_id:
        return False
    # 2P is full permission => no work
    if (code or "").strip().upper() == "2P":
        return False
    if shift_minutes <= 0:
        return False
    # 1P/shift still counts as work
    return True


# --------------------------------------------------------------------------------------
# Core queries
# --------------------------------------------------------------------------------------


def ensure_week_row(week_start_date: date, shift_group: str, created_by_user_id: Optional[int]) -> int:
    sg = normalize_group(shift_group)
    row = fetch_one(
        "SELECT week_id FROM rrhh.shift_roster_week WHERE week_start=? AND shift_group=?",
        (week_start_date, sg),
    )
    if row:
        return int(row.week_id)

    execute(
        "INSERT INTO rrhh.shift_roster_week(week_start, shift_group, status, created_by_user_id) VALUES (?,?, 'DRAFT', ?)",
        (week_start_date, sg, created_by_user_id),
    )
    row2 = fetch_one(
        "SELECT week_id FROM rrhh.shift_roster_week WHERE week_start=? AND shift_group=?",
        (week_start_date, sg),
    )
    if not row2:
        raise RuntimeError("No se pudo crear shift_roster_week")
    return int(row2.week_id)


def get_week_header(week_id: int):
    return fetch_one(
        "SELECT week_id, week_start, shift_group, status, created_at, published_at FROM rrhh.shift_roster_week WHERE week_id=?",
        (int(week_id),),
    )


def list_employees_for_group(shift_group: str) -> List[Any]:
    sg = normalize_group(shift_group)
    has_group = _has_col("rrhh", "hr_employee", "shift_group")
    if has_group:
        return fetch_all(
            "SELECT employee_id, doc_number, first_name, last_name, shift_group "
            "FROM rrhh.hr_employee WHERE is_active=1 AND shift_group=? "
            "ORDER BY last_name, first_name",
            (sg,),
        )
    # Fallback: no columna, devuelve todos
    return fetch_all(
        "SELECT employee_id, doc_number, first_name, last_name, NULL AS shift_group "
        "FROM rrhh.hr_employee WHERE is_active=1 ORDER BY last_name, first_name"
    )


def list_shift_defs(shift_group: str) -> List[ShiftDef]:
    """Lista turnos activos para un tablero (CABINA/AJUSTADOR).

    Importante:
    - rrhh.shift_definition.shift_code es UNIQUE global.
    - Para AJUSTADOR se recomienda usar códigos con prefijo (por ejemplo AJ-01, AJ-02...).
    """

    sg = normalize_group(shift_group)
    rows = fetch_all(
        "SELECT shift_id, shift_code, start_time, end_time "
        "FROM rrhh.shift_definition "
        "WHERE is_active=1 AND shift_group=? "
        "ORDER BY start_time, end_time, shift_code",
        (sg,),
    )
    out: List[ShiftDef] = []
    for r in rows or []:
        out.append(
            ShiftDef(
                shift_id=int(r.shift_id),
                shift_code=str(getattr(r, 'shift_code', '') or '').strip(),
                start_time=getattr(r, 'start_time', None),
                end_time=getattr(r, 'end_time', None),
            )
        )
    return out


def _shift_id_by_code(code: str) -> Optional[int]:
    c = (code or "").strip()
    if not c:
        return None
    row = fetch_one(
        "SELECT TOP 1 shift_id FROM rrhh.shift_definition WHERE is_active=1 AND LTRIM(RTRIM(shift_code))=?",
        (c,),
    )
    return int(row.shift_id) if row else None


def fetch_roster_days(week_id: int) -> List[Any]:
    """Lee el detalle del roster.

    Compatibilidad:
    - Esquema nuevo: columna status_code
    - Esquema antiguo: columna code

    Siempre exponemos el campo como atributo .code (vía alias) cuando exista.
    """

    has_status_code = _has_col('rrhh', 'shift_roster_day', 'status_code')
    has_code = _has_col('rrhh', 'shift_roster_day', 'code')

    cols = 'employee_id, work_date, shift_id, is_day_off, notes'
    if has_status_code:
        cols = 'employee_id, work_date, shift_id, is_day_off, status_code AS code, notes'
    elif has_code:
        cols = 'employee_id, work_date, shift_id, is_day_off, code, notes'

    return fetch_all(f"SELECT {cols} FROM rrhh.shift_roster_day WHERE week_id=?", (int(week_id),))


def upsert_roster_day(
    week_id: int,
    employee_id: int,
    work_date: date,
    value: str,
    actor_user_id: Optional[int],
) -> None:
    """value:
    - '' => delete
    - 'DES','INC','VAC','HB','1P','2P'
    - '1P/<code>'
    - '<code>'
    """
    v = (value or "").strip().upper()

    # Delete
    if not v:
        execute(
            "DELETE FROM rrhh.shift_roster_day WHERE employee_id=? AND work_date=?",
            (int(employee_id), work_date),
        )
        return

    is_day_off = 0
    shift_id: Optional[int] = None
    notes: Optional[str] = None
    code: Optional[str] = None

    if v in ("DES", "INC", "VAC", "HB", "1P", "2P"):
        is_day_off = 1
        shift_id = None
        code = v
        notes = None
    elif v.startswith("1P/"):
        # 1P/ 9 -> half day + shift
        code = v.split("/", 1)[1].strip()
        sid = _shift_id_by_code(code)
        if not sid:
            raise ValueError(f"Turno no encontrado: {code}")
        is_day_off = 0
        shift_id = sid
        code = f"1P/{code}"
        notes = None
    else:
        sid = _shift_id_by_code(v)
        if not sid:
            raise ValueError(f"Turno no encontrado: {v}")
        is_day_off = 0
        shift_id = sid
        code = None
        notes = None

    has_status_code_col = _has_col("rrhh", "shift_roster_day", "status_code")
    has_code_col = _has_col("rrhh", "shift_roster_day", "code")

    # Upsert (PK employee_id, work_date)
    row = fetch_one(
        "SELECT 1 AS ok FROM rrhh.shift_roster_day WHERE employee_id=? AND work_date=?",
        (int(employee_id), work_date),
    )


    if row:
        code_col = 'status_code' if has_status_code_col else ('code' if has_code_col else None)
        if code_col:
            execute(
                (
                    "UPDATE rrhh.shift_roster_day "
                    f"SET week_id=?, shift_id=?, is_day_off=?, {code_col}=?, notes=?, updated_by_user_id=?, updated_at=GETDATE() "
                    "WHERE employee_id=? AND work_date=?"
                ),
                (
                    int(week_id),
                    shift_id,
                    int(is_day_off),
                    code,
                    notes,
                    actor_user_id,
                    int(employee_id),
                    work_date,
                ),
            )
        else:
            # Fallback: guarda el código en notes
            execute(
                "UPDATE rrhh.shift_roster_day "
                "SET week_id=?, shift_id=?, is_day_off=?, notes=?, updated_by_user_id=?, updated_at=GETDATE() "
                "WHERE employee_id=? AND work_date=?",
                (
                    int(week_id),
                    shift_id,
                    int(is_day_off),
                    code or notes,
                    actor_user_id,
                    int(employee_id),
                    work_date,
                ),
            )
    else:
        code_col = 'status_code' if has_status_code_col else ('code' if has_code_col else None)
        if code_col:
            execute(
                (
                    "INSERT INTO rrhh.shift_roster_day(employee_id, work_date, week_id, shift_id, is_day_off, "
                    f"{code_col}, notes, created_by_user_id) "
                    "VALUES (?,?,?,?,?,?,?,?)"
                ),
                (
                    int(employee_id),
                    work_date,
                    int(week_id),
                    shift_id,
                    int(is_day_off),
                    code,
                    notes,
                    actor_user_id,
                ),
            )
        else:
            execute(
                "INSERT INTO rrhh.shift_roster_day(employee_id, work_date, week_id, shift_id, is_day_off, notes, created_by_user_id) "
                "VALUES (?,?,?,?,?,?,?)",
                (
                    int(employee_id),
                    work_date,
                    int(week_id),
                    shift_id,
                    int(is_day_off),
                    code or notes,
                    actor_user_id,
                ),
            )


# --------------------------------------------------------------------------------------
# Validation
# --------------------------------------------------------------------------------------


def validate_week(week_id: int) -> Dict[str, Any]:
    hdr = get_week_header(week_id)
    if not hdr:
        raise RuntimeError("Semana no encontrada")

    ws: date = hdr.week_start
    sg = str(hdr.shift_group)

    employees = list_employees_for_group(sg)
    days = fetch_roster_days(week_id)

    shift_map: Dict[int, ShiftDef] = {s.shift_id: s for s in list_shift_defs(sg)}

    # Build dict employee_id -> date -> record
    per_emp: Dict[int, Dict[date, Any]] = {}
    for drow in days or []:
        eid = int(drow.employee_id)
        per_emp.setdefault(eid, {})[drow.work_date] = drow

    # Validation results
    errors: List[Dict[str, Any]] = []
    warnings: List[Dict[str, Any]] = []

    # Cobertura 24/7 unificada (sin configuración externa):
    # Se calcula hora a hora con base en los turnos asignados en el roster.
    # El objetivo es que el usuario lo gestione visualmente en el tablero (estilo "sesión").
    coverage_by_hour: Dict[date, List[int]] = {ws + timedelta(days=i): [0] * 24 for i in range(7)}

    # Per-employee compute
    for e in employees or []:
        eid = int(e.employee_id)

        # Weekly minutes
        total_min = 0

        # Work flags for consecutive days check (14 days window)
        start_14 = ws - timedelta(days=7)
        end_14 = ws + timedelta(days=6)

        # Fetch prior+current days (only from roster table). Missing days => no assignment.
        has_status_code = _has_col('rrhh', 'shift_roster_day', 'status_code')
        has_code = _has_col('rrhh', 'shift_roster_day', 'code')

        cols = 'work_date, shift_id, is_day_off, notes'
        if has_status_code:
            cols = 'work_date, shift_id, is_day_off, status_code AS code, notes'
        elif has_code:
            cols = 'work_date, shift_id, is_day_off, code, notes'

        prior_rows = fetch_all(
            f"SELECT {cols} FROM rrhh.shift_roster_day WHERE employee_id=? AND work_date BETWEEN ? AND ?",
            (eid, start_14, end_14),
        )
        # Build a set for quick lookup
        prior_map: Dict[date, Any] = {r.work_date: r for r in (prior_rows or [])}

        # Daily minutes function
        def day_minutes(d0: date) -> int:
            rr = prior_map.get(d0)
            if not rr:
                return 0
            code = (getattr(rr, "code", None) or getattr(rr, "notes", "") or "").strip().upper()
            if int(getattr(rr, "is_day_off", 0) or 0) == 1:
                return 0
            sid = getattr(rr, "shift_id", None)
            if sid is None:
                return 0
            sd = shift_map.get(int(sid))
            base = _shift_minutes(sd.start_time, sd.end_time) if sd else 0
            if code.startswith("1P/"):
                return base // 2
            return base

        # Weekly range (7 days)
        for i in range(7):
            d0 = ws + timedelta(days=i)
            rr = prior_map.get(d0)
            if rr:
                code = (getattr(rr, "code", None) or getattr(rr, "notes", "") or "").strip().upper()
                sid = getattr(rr, "shift_id", None)
                mins = day_minutes(d0)

                total_min += mins

                # Cobertura por hora (aprox): cuenta como disponible durante todo el turno.
                if mins > 0 and sid is not None:
                    sd = shift_map.get(int(sid))
                    if sd and sd.start_time and sd.end_time:
                        _add_hours_coverage(coverage_by_hour, d0, sd.start_time, sd.end_time)

        # Rule: <= 44h weekly
        if total_min > 44 * 60:
            errors.append(
                {
                    "employee_id": eid,
                    "rule": "44H",
                    "message": f"Supera 44 horas semanales: {total_min/60:.1f}h",
                }
            )

        # Rule: rest within 7 days (no >7 consecutive work days)
        consec = 0
        max_consec = 0
        for j in range((end_14 - start_14).days + 1):
            d0 = start_14 + timedelta(days=j)
            mins = day_minutes(d0)
            if mins > 0:
                consec += 1
                max_consec = max(max_consec, consec)
            else:
                consec = 0

        if max_consec > 7:
            errors.append(
                {
                    "employee_id": eid,
                    "rule": "DESCANSO",
                    "message": f"Más de 7 días seguidos trabajando (máx: {max_consec}).",
                }
            )

        # Rule: no two consecutive Sundays
        # Current week Sunday:
        sunday = ws + timedelta(days=6 - ws.weekday() + 6)  # incorrect; use ws+6? ws is Monday so Sunday=ws+6
        sunday = ws + timedelta(days=6)
        prev_sunday = sunday - timedelta(days=7)
        if day_minutes(sunday) > 0 and day_minutes(prev_sunday) > 0:
            errors.append(
                {
                    "employee_id": eid,
                    "rule": "DOMINGOS",
                    "message": "No puede trabajar dos domingos seguidos.",
                }
            )

    # Cobertura 24/7 (hora a hora): warning si hay huecos (0 personas) en cualquier hora.
    hourly_gaps: List[Dict[str, Any]] = []
    for i in range(7):
        d0 = ws + timedelta(days=i)
        hours = coverage_by_hour.get(d0) or [0] * 24
        for h, cnt in enumerate(hours):
            if cnt <= 0:
                hourly_gaps.append({"date": d0, "hour": h, "count": cnt})

    if hourly_gaps:
        warnings.append(
            {
                "rule": "COBERTURA24H",
                "message": "Hay horas sin personal asignado (cobertura 24/7).",
                "details": hourly_gaps,
            }
        )

    return {
        "errors": errors,
        "warnings": warnings,
        "coverage_gaps": hourly_gaps,
    }


def _has_table(schema: str, table: str) -> bool:
    r = fetch_one(
        "SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=?",
        (schema, table),
    )
    return r is not None


def publish_week(week_id: int, actor_user_id: Optional[int]) -> Dict[str, Any]:
    v = validate_week(week_id)
    # Publishing is strict SOLO para reglas (44h, descanso, domingos, etc.).
    # Cobertura 24/7 se gestiona visualmente en el tablero (warning), no bloquea.
    if v.get("errors"):
        return {"ok": False, "reason": "VALIDATION", **v}

    execute(
        "UPDATE rrhh.shift_roster_week SET status='PUBLISHED', published_by_user_id=?, published_at=GETDATE() WHERE week_id=?",
        (actor_user_id, int(week_id)),
    )
    return {"ok": True, **v}


def _add_hours_coverage(coverage_by_hour: Dict[date, List[int]], d0: date, start_t: time, end_t: time) -> None:
    """Incrementa conteo por hora para el día y, si cruza medianoche, para el día siguiente."""
    s = start_t.hour
    e = end_t.hour
    if end_t.minute > 0:
        e = (e + 1) % 24

    if end_t <= start_t:
        # Cruza medianoche
        for h in range(s, 24):
            coverage_by_hour.setdefault(d0, [0] * 24)[h] += 1
        d1 = d0 + timedelta(days=1)
        for h in range(0, e):
            coverage_by_hour.setdefault(d1, [0] * 24)[h] += 1
    else:
        for h in range(s, e):
            coverage_by_hour.setdefault(d0, [0] * 24)[h] += 1


def list_assignments_for_day(week_id: int, work_date: date) -> List[Any]:
    """Lista asignaciones del roster para un día específico."""

    has_status_code = _has_col('rrhh', 'shift_roster_day', 'status_code')
    has_code = _has_col('rrhh', 'shift_roster_day', 'code')

    cols = 'employee_id, work_date, shift_id, is_day_off, notes'
    if has_status_code:
        cols = 'employee_id, work_date, shift_id, is_day_off, status_code AS code, notes'
    elif has_code:
        cols = 'employee_id, work_date, shift_id, is_day_off, code, notes'

    return fetch_all(
        f"SELECT {cols} FROM rrhh.shift_roster_day WHERE week_id=? AND work_date=?",
        (int(week_id), work_date),
    )


def shifts_covering_hour(shifts: List[ShiftDef], hour: int) -> List[ShiftDef]:
    """Turnos que cubren una hora (0-23), considerando cruces de medianoche."""
    out: List[ShiftDef] = []
    h = int(hour)
    for s in shifts or []:
        if not s.start_time or not s.end_time:
            continue
        sh = s.start_time.hour
        eh = s.end_time.hour
        crosses = s.end_time <= s.start_time
        if crosses:
            if h >= sh or h < eh:
                out.append(s)
        else:
            if sh <= h < eh:
                out.append(s)
    return out
