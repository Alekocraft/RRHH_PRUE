from __future__ import annotations

"""Servicios de horario (turno base) y cambio de horario (hora flexible).

Objetivo
---------
Centralizar:
  - Lectura/asignación de turno base (rrhh.shift_assignment)
  - Lectura/asignación de Hora flexible (rrhh.time_flexible_rule)

El módulo Turnos ya usa roster semanal (shift_roster_*), pero el "horario" del
colaborador (turno base) se mantiene en shift_assignment para reportes y cruces
en Asistencia.

No rompe compatibilidad:
  - Si no existe el SP rrhh.sp_set_shift_assignment, hace fallback a DML.
  - Si no existen tablas de hora flexible, se comporta como no disponible.
"""

from datetime import date, datetime, timedelta
from typing import Any, Optional

from services.rrhh_db import fetch_all, fetch_one, execute, call_proc


def _has_table(schema: str, table: str) -> bool:
    return (
        fetch_one(
            "SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=?",
            (schema, table),
        )
        is not None
    )


def _has_col(schema: str, table: str, col: str) -> bool:
    return (
        fetch_one(
            "SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND COLUMN_NAME=?",
            (schema, table, col),
        )
        is not None
    )


def list_shift_definitions() -> list[Any]:
    if not _has_table("rrhh", "shift_definition"):
        return []
    return fetch_all(
        "SELECT shift_id, shift_code, start_time, end_time "
        "FROM rrhh.shift_definition WHERE is_active=1 ORDER BY shift_code"
    )


def get_current_shift_assignment(employee_id: int, on_date: Optional[date] = None):
    if not employee_id:
        return None
    if not _has_table("rrhh", "shift_assignment"):
        return None

    d = on_date or date.today()
    return fetch_one(
        "SELECT TOP (1) sa.shift_id, sa.valid_from, sa.valid_to, sd.shift_code, sd.start_time, sd.end_time "
        "FROM rrhh.shift_assignment sa "
        "LEFT JOIN rrhh.shift_definition sd ON sd.shift_id = sa.shift_id "
        "WHERE sa.employee_id=? AND sa.valid_from <= ? AND (sa.valid_to IS NULL OR sa.valid_to >= ?) "
        "ORDER BY sa.valid_from DESC",
        (int(employee_id), d, d),
    )


def set_shift_assignment(
    employee_id: int,
    shift_id: int,
    valid_from: date,
    valid_to: Optional[date],
    actor_user_id: Optional[int],
    reason: str = "Asignación desde creación/edición de usuario",
) -> None:
    """Asigna el turno base del colaborador.

    Preferimos el SP `rrhh.sp_set_shift_assignment` si existe.
    Si no existe, cerramos la asignación vigente y creamos una nueva.
    """
    if not employee_id or not shift_id:
        raise ValueError("employee_id y shift_id son obligatorios")

    # 1) SP (si existe)
    try:
        # Si el SP no existe, call_proc lanzará error y caeremos a fallback.
        call_proc(
            "rrhh.sp_set_shift_assignment",
            [int(employee_id), int(shift_id), valid_from, valid_to, actor_user_id, reason],
        )
        return
    except Exception:
        pass

    # 2) Fallback DML
    if not _has_table("rrhh", "shift_assignment"):
        raise RuntimeError("No existe rrhh.shift_assignment y no se pudo usar sp_set_shift_assignment")

    # Cerrar asignaciones que se traslapen con valid_from
    execute(
        "UPDATE rrhh.shift_assignment "
        "SET valid_to=DATEADD(day,-1,?) "
        "WHERE employee_id=? AND (valid_to IS NULL OR valid_to >= ?) AND valid_from < ?",
        (valid_from, int(employee_id), valid_from, valid_from),
    )

    cols = ["employee_id", "shift_id", "valid_from", "valid_to"]
    vals = [int(employee_id), int(shift_id), valid_from, valid_to]
    if _has_col("rrhh", "shift_assignment", "created_by_user_id"):
        cols.append("created_by_user_id")
        vals.append(actor_user_id)
    if _has_col("rrhh", "shift_assignment", "reason"):
        cols.append("reason")
        vals.append(reason)
    if _has_col("rrhh", "shift_assignment", "is_active"):
        cols.append("is_active")
        vals.append(1)

    ph = ", ".join(["?"] * len(cols))
    execute(
        f"INSERT INTO rrhh.shift_assignment({', '.join(cols)}) VALUES ({ph})",
        tuple(vals),
    )


# -----------------------------------------------------------------------------
# Hora flexible (cambio de horario)
# -----------------------------------------------------------------------------


def flex_tables_exist() -> bool:
    return _has_table("rrhh", "time_flexible_rule")


def get_active_flex_rule(employee_id: int):
    if not employee_id or not flex_tables_exist():
        return None
    return fetch_one(
        "SELECT TOP (1) rule_id, weekday, slot, minutes, valid_from, valid_to, is_active "
        "FROM rrhh.time_flexible_rule "
        "WHERE employee_id=? AND is_active=1 AND valid_to IS NULL "
        "ORDER BY rule_id DESC",
        (int(employee_id),),
    )


def set_active_flex_rule(
    employee_id: int,
    weekday: int,
    slot: str,
    valid_from: date,
    actor_user_id: Optional[int],
) -> None:
    """Crea/actualiza la regla activa de hora flexible para el colaborador.

    - Cierra regla vigente (valid_to = valid_from-1)
    - Inserta nueva regla con minutes=60 (estándar del proyecto)
    """
    if not flex_tables_exist():
        raise RuntimeError("No existen tablas de Hora flexible (rrhh.time_flexible_rule)")

    weekday = int(weekday)
    slot_n = (slot or "").strip().upper()
    if weekday not in (1, 2, 3, 4, 5, 6, 7):
        raise ValueError("weekday inválido")
    if slot_n not in ("AM", "PM"):
        raise ValueError("slot inválido (AM/PM)")

    old = fetch_one(
        "SELECT TOP (1) rule_id, valid_from "
        "FROM rrhh.time_flexible_rule "
        "WHERE employee_id=? AND is_active=1 AND valid_to IS NULL "
        "ORDER BY rule_id DESC",
        (int(employee_id),),
    )
    if old:
        close_to = valid_from - timedelta(days=1)
        try:
            if close_to < old.valid_from:
                close_to = old.valid_from
        except Exception:
            pass

        sets = ["valid_to=?", "is_active=0"]
        params = [close_to]
        if _has_col("rrhh", "time_flexible_rule", "closed_by_user_id"):
            sets.append("closed_by_user_id=?")
            params.append(actor_user_id)
        if _has_col("rrhh", "time_flexible_rule", "closed_at"):
            sets.append("closed_at=GETDATE()")
        params.append(int(old.rule_id))
        execute(
            f"UPDATE rrhh.time_flexible_rule SET {', '.join(sets)} WHERE rule_id=?",
            tuple(params),
        )

    cols = [
        "employee_id",
        "weekday",
        "slot",
        "minutes",
        "valid_from",
        "valid_to",
        "is_active",
    ]
    vals = [int(employee_id), weekday, slot_n, 60, valid_from, None, 1]
    if _has_col("rrhh", "time_flexible_rule", "created_by_user_id"):
        cols.append("created_by_user_id")
        vals.append(actor_user_id)
    if _has_col("rrhh", "time_flexible_rule", "created_at"):
        # default en DB; omitimos
        pass

    ph = ", ".join(["?"] * len(cols))
    execute(
        f"INSERT INTO rrhh.time_flexible_rule({', '.join(cols)}) VALUES ({ph})",
        tuple(vals),
    )
