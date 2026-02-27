from __future__ import annotations

from datetime import date

from flask import render_template, url_for
from flask_login import login_required, current_user

from services.rrhh_db import fetch_one, fetch_all
from services.rrhh_security import ROLE_ADMIN, ROLE_RRHH

from .modulos import modulos_bp


def _table_exists(schema: str, table: str) -> bool:
    row = fetch_one(
        "SELECT 1 AS ok FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=?",
        (schema, table),
    )
    return row is not None


def _roles_set() -> set[str]:
    try:
        return set((getattr(current_user, "roles", []) or []))
    except Exception:
        return set()


@modulos_bp.route("/dashboard")
@login_required
def dashboard():
    """Dashboard principal.

    - Respeta permisos (UI en templates).
    - Incluye conteos rápidos de pendientes:
        * Trabajo en casa (wfh_day)
        * Hora flexible (wf_request_step)
        * Chequera (timebook_request)
    - Incluye un resumen de "Mis solicitudes recientes" (incluye chequera).
    """

    roles = _roles_set()
    is_admin = (ROLE_ADMIN in roles) or ("ADMINISTRADOR" in roles)
    is_rrhh = (ROLE_RRHH in roles) or ("RRHH" in roles)
    is_backoffice = is_admin or is_rrhh
    es_jefe = bool(getattr(current_user, "es_jefe", False))
    my_emp_id = getattr(current_user, "employee_id", None)

    # ------------------------------------------------------------------
    # Pendientes (Trabajo en casa)
    # ------------------------------------------------------------------
    pending_aprobaciones_count = 0
    try:
        if _table_exists("rrhh", "wfh_day"):
            if is_backoffice:
                row = fetch_one("SELECT COUNT(1) AS c FROM rrhh.wfh_day WHERE is_active=0")
                pending_aprobaciones_count = int(getattr(row, "c", 0) or 0) if row else 0
            elif es_jefe and my_emp_id:
                # Pendientes del equipo (excluye propio)
                row = fetch_one(
                    "SELECT COUNT(1) AS c "
                    "FROM rrhh.wfh_day w "
                    "JOIN rrhh.hr_employee_manager m ON m.employee_id=w.employee_id "
                    "  AND m.manager_employee_id=? "
                    "  AND m.is_primary=1 "
                    "  AND m.valid_from <= CAST(GETDATE() AS DATE) "
                    "  AND (m.valid_to IS NULL OR m.valid_to >= CAST(GETDATE() AS DATE)) "
                    "WHERE w.is_active=0 "
                    "AND w.employee_id <> ? ",
                    (int(my_emp_id), int(my_emp_id)),
                )
                pending_aprobaciones_count = int(getattr(row, "c", 0) or 0) if row else 0
    except Exception:
        pending_aprobaciones_count = 0

    # ------------------------------------------------------------------
    # Pendientes (Hora flexible - workflow)
    # ------------------------------------------------------------------
    pending_workflow_count = 0
    try:
        # Solo si existen tablas de workflow
        if _table_exists("rrhh", "wf_request") and _table_exists("rrhh", "wf_request_step"):
            # request_type del módulo Hora flexible (mantenemos la constante "HORA_FLEXIBLE")
            if is_backoffice:
                row = fetch_one(
                    "SELECT COUNT(1) AS c "
                    "FROM rrhh.wf_request_step s "
                    "JOIN rrhh.wf_request r ON r.request_id=s.request_id "
                    "WHERE r.request_type='HORA_FLEXIBLE' "
                    "  AND r.status='SUBMITTED' "
                    "  AND s.status='PENDING'"
                )
                pending_workflow_count = int(getattr(row, "c", 0) or 0) if row else 0
            else:
                # Jefe: solo asignadas a su usuario
                row = fetch_one(
                    "SELECT COUNT(1) AS c "
                    "FROM rrhh.wf_request_step s "
                    "JOIN rrhh.wf_request r ON r.request_id=s.request_id "
                    "WHERE r.request_type='HORA_FLEXIBLE' "
                    "  AND r.status='SUBMITTED' "
                    "  AND s.status='PENDING' "
                    "  AND s.assigned_to_user_id=?",
                    (int(current_user.user_id),),
                )
                pending_workflow_count = int(getattr(row, "c", 0) or 0) if row else 0
    except Exception:
        pending_workflow_count = 0

    # ------------------------------------------------------------------
    # Pendientes (Chequera)
    # ------------------------------------------------------------------
    pending_chequera_count = 0
    try:
        if _table_exists("rrhh", "timebook_request"):
            if is_backoffice:
                row = fetch_one("SELECT COUNT(1) AS c FROM rrhh.timebook_request WHERE status='PENDING'")
                pending_chequera_count = int(getattr(row, "c", 0) or 0) if row else 0
            elif es_jefe and my_emp_id:
                row = fetch_one(
                    "SELECT COUNT(1) AS c "
                    "FROM rrhh.timebook_request r "
                    "JOIN rrhh.hr_employee_manager m "
                    "  ON m.employee_id=r.employee_id "
                    " AND m.manager_employee_id=? "
                    " AND m.is_primary=1 "
                    " AND m.valid_from <= r.request_date "
                    " AND (m.valid_to IS NULL OR m.valid_to >= r.request_date) "
                    "WHERE r.status='PENDING' "
                    "  AND r.employee_id <> ?",
                    (int(my_emp_id), int(my_emp_id)),
                )
                pending_chequera_count = int(getattr(row, "c", 0) or 0) if row else 0
    except Exception:
        pending_chequera_count = 0

    # ------------------------------------------------------------------
    # Mis solicitudes recientes
    # ------------------------------------------------------------------
    my_recent_requests: list[dict] = []

    # 1) Workflow genérico (si existe)
    try:
        if my_emp_id and _table_exists("rrhh", "wf_request"):
            tipos = (
                "HORA_FLEXIBLE",
                "INCAPACIDAD",
                "TRABAJO_CASA",
                "TRABAJO_EN_CASA",
                "WFH",
                "WORK_FROM_HOME",
                "CHEQUERA_TIEMPO",
            )
            placeholders = ",".join(["?"] * len(tipos))
            rows = fetch_all(
                "SELECT TOP 15 request_id, request_type, status, created_at, submitted_at, closed_at "
                "FROM rrhh.wf_request "
                f"WHERE employee_id=? AND request_type IN ({placeholders}) "
                "ORDER BY created_at DESC",
                (int(my_emp_id), *tipos),
            )

            for r in rows or []:
                rt = (getattr(r, "request_type", "") or "").upper()
                if rt == "HORA_FLEXIBLE":
                    modulo = "Hora flexible"
                    url = url_for("modulos.hora_flexible")
                elif rt == "INCAPACIDAD":
                    modulo = "Incapacidad médica"
                    url = url_for("modulos.incapacidad")
                elif rt in ("TRABAJO_CASA", "TRABAJO_EN_CASA", "WFH", "WORK_FROM_HOME"):
                    modulo = "Trabajo en casa"
                    url = url_for("modulos.trabajo_casa_solicitar")
                elif rt == "CHEQUERA_TIEMPO":
                    modulo = "Chequera"
                    url = url_for("modulos.chequera")
                else:
                    modulo = rt or "Solicitud"
                    url = url_for("modulos.dashboard")

                my_recent_requests.append(
                    {
                        "module": modulo,
                        "request_id": getattr(r, "request_id", None),
                        "status": getattr(r, "status", None),
                        "created_at": getattr(r, "created_at", None),
                        "submitted_at": getattr(r, "submitted_at", None),
                        "closed_at": getattr(r, "closed_at", None),
                        "url": url,
                    }
                )
    except Exception:
        pass

    # 2) Fallback WFH (si no usa wf_request)
    try:
        if my_emp_id and _table_exists("rrhh", "wfh_day"):
            rows = fetch_all(
                "SELECT TOP 10 work_date, is_active, created_at "
                "FROM rrhh.wfh_day "
                "WHERE employee_id=? "
                "ORDER BY created_at DESC",
                (int(my_emp_id),),
            )
            for r in rows or []:
                st = "PENDING" if int(getattr(r, "is_active", 0) or 0) == 0 else "APPROVED"
                my_recent_requests.append(
                    {
                        "module": "Trabajo en casa",
                        "request_id": None,
                        "work_date": getattr(r, "work_date", None),
                        "status": st,
                        "created_at": getattr(r, "created_at", None),
                        "submitted_at": None,
                        "closed_at": None,
                        "url": url_for("modulos.trabajo_casa_solicitar"),
                    }
                )
    except Exception:
        pass

    # 3) Chequera (timebook_request)
    try:
        if my_emp_id and _table_exists("rrhh", "timebook_request"):
            rows = fetch_all(
                "SELECT TOP 10 request_id, request_date, slot, status, created_at, decided_at "
                "FROM rrhh.timebook_request "
                "WHERE employee_id=? "
                "ORDER BY created_at DESC",
                (int(my_emp_id),),
            )
            for r in rows or []:
                my_recent_requests.append(
                    {
                        "module": "Chequera",
                        "request_id": getattr(r, "request_id", None),
                        "status": getattr(r, "status", None),
                        "created_at": getattr(r, "created_at", None),
                        "submitted_at": None,
                        "closed_at": getattr(r, "decided_at", None),
                        "url": url_for("modulos.chequera"),
                    }
                )
    except Exception:
        pass

    # Ordenar por created_at desc (si viene None, va al final)
    try:
        my_recent_requests.sort(
            key=lambda x: (x.get("created_at") is None, x.get("created_at")),
            reverse=True,
        )
        my_recent_requests = my_recent_requests[:15]
    except Exception:
        pass

    return render_template(
        "dashboard.html",
        pending_aprobaciones_count=pending_aprobaciones_count,
        pending_workflow_count=pending_workflow_count,
        pending_chequera_count=pending_chequera_count,
        my_recent_requests=my_recent_requests,
    )
