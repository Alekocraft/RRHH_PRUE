from __future__ import annotations

"""Servicios de workflow (rrhh.wf_*).

El proyecto ya usa tablas `rrhh.wf_request`, `rrhh.wf_request_step`, `rrhh.wf_action`.
En algunos entornos existe el SP `rrhh.sp_submit_request`. En otros no, o no crea
los pasos deseados.

Este módulo agrega un mecanismo *tolerante* para:
  - Marcar la solicitud como SUBMITTED
  - Crear una secuencia de pasos PENDING (step_no 1..N)

Se usa para asegurar flujos de 2 aprobaciones: Jefe -> RRHH.
"""

from datetime import date
from typing import Optional, Sequence

from services.rrhh_db import fetch_one, execute


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


def wf_tables_exist() -> bool:
    return _has_table("rrhh", "wf_request") and _has_table("rrhh", "wf_request_step")


def wf_submit_request(request_id: int, actor_user_id: Optional[int]) -> None:
    """Marca rrhh.wf_request como SUBMITTED (si existen columnas)."""
    if not wf_tables_exist():
        raise RuntimeError("No existen tablas rrhh.wf_request / rrhh.wf_request_step")

    sets = []
    params = []

    # status
    if _has_col("rrhh", "wf_request", "status"):
        sets.append("status='SUBMITTED'")

    # submitted_at
    if _has_col("rrhh", "wf_request", "submitted_at"):
        sets.append("submitted_at=GETDATE()")

    # updated_by / submitted_by
    if actor_user_id is not None:
        if _has_col("rrhh", "wf_request", "submitted_by_user_id"):
            sets.append("submitted_by_user_id=?")
            params.append(int(actor_user_id))
        elif _has_col("rrhh", "wf_request", "updated_by_user_id"):
            sets.append("updated_by_user_id=?")
            params.append(int(actor_user_id))

    if not sets:
        return

    params.append(int(request_id))
    execute(
        f"UPDATE rrhh.wf_request SET {', '.join(sets)} WHERE request_id=?",
        tuple(params),
    )


def wf_clear_steps(request_id: int) -> None:
    """Elimina pasos existentes (para reconstrucción controlada)."""
    if not _has_table("rrhh", "wf_request_step"):
        return
    execute("DELETE FROM rrhh.wf_request_step WHERE request_id=?", (int(request_id),))


def wf_create_steps(
    request_id: int,
    step_assignees: Sequence[Optional[int]],
) -> None:
    """Crea pasos secuenciales (step_no=1..N).

    step_assignees: lista de user_id o None.
      - None significa: cola de backoffice (RRHH/ADMIN), no restringido.
    """
    if not _has_table("rrhh", "wf_request_step"):
        raise RuntimeError("No existe rrhh.wf_request_step")

    has_assigned = _has_col("rrhh", "wf_request_step", "assigned_to_user_id")
    has_status = _has_col("rrhh", "wf_request_step", "status")

    for idx, uid in enumerate(step_assignees, start=1):
        cols = ["request_id", "step_no"]
        vals = [int(request_id), int(idx)]

        if has_assigned:
            cols.append("assigned_to_user_id")
            vals.append(int(uid) if uid is not None else None)

        if has_status:
            cols.append("status")
            vals.append("PENDING")

        # created_at defaults in DB if exists
        ph = ", ".join(["?"] * len(cols))
        execute(
            f"INSERT INTO rrhh.wf_request_step({', '.join(cols)}) VALUES ({ph})",
            tuple(vals),
        )


def resolve_manager_user_id(employee_id: int, on_date: Optional[date] = None) -> Optional[int]:
    """Devuelve user_id del jefe inmediato del employee_id (si existe relación y usuario)."""
    if not employee_id:
        return None
    if not _has_table("rrhh", "hr_employee_manager"):
        return None

    d = on_date or date.today()
    mgr = fetch_one(
        "SELECT TOP 1 manager_employee_id "
        "FROM rrhh.hr_employee_manager "
        "WHERE employee_id=? AND is_primary=1 AND valid_from<=? AND (valid_to IS NULL OR valid_to>=?) "
        "ORDER BY valid_from DESC",
        (int(employee_id), d, d),
    )
    if not mgr or mgr.manager_employee_id is None:
        return None

    if not _has_table("rrhh", "auth_user"):
        return None
    u = fetch_one(
        "SELECT TOP 1 user_id FROM rrhh.auth_user WHERE employee_id=? AND is_active=1 ORDER BY user_id DESC",
        (int(mgr.manager_employee_id),),
    )
    return int(u.user_id) if u else None
