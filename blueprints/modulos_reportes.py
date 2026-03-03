from __future__ import annotations

import re
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Sequence, Tuple

from flask import Response, flash, redirect, render_template, request, url_for
from flask_login import current_user, login_required

from services.rrhh_db import fetch_all, fetch_one
from services.report_export import build_excel, build_pdf

from .modulos import modulos_bp
from .modulos_common import _is_admin_or_rrhh, _parse_date, _parse_doc_number


# --------------------------------------------------------------------------------------
# Config
# --------------------------------------------------------------------------------------

REPORTS = {
    "hora-flexible": {
        "title": "Reporte · Hora flexible",
        "default_status": "ALL",
    },
    "incapacidad": {
        "title": "Reporte · Incapacidad médica",
        "default_status": "ALL",
    },
    "chequera": {
        "title": "Reporte · Chequera de tiempo",
        "default_status": "ALL",
    },
    "trabajo-casa": {
        "title": "Reporte · Trabajo en casa",
        "default_status": "ALL",
    },
}

MAX_EXPORT_ROWS = 5000


def _roles() -> List[str]:
    return list(getattr(current_user, "roles", None) or [])


def _is_admin() -> bool:
    r = _roles()
    return bool(getattr(current_user, "is_admin", False) or ("ADMINISTRADOR" in r))


def _is_rrhh() -> bool:
    r = _roles()
    return bool("RRHH" in r)


def _is_manager() -> bool:
    return bool(getattr(current_user, "es_jefe", False))


def _allowed_scopes() -> List[str]:
    # mine: solo lo mío
    # team: lo mío + equipo
    # all: todo
    if _is_admin() or _is_rrhh():
        return ["mine", "team", "all"]
    if _is_manager():
        return ["mine", "team"]
    return ["mine"]


def _resolve_scope(scope: str | None) -> str:
    s = (scope or "").strip().lower() or "mine"
    if s not in ("mine", "team", "all"):
        s = "mine"
    allowed = _allowed_scopes()
    if s not in allowed:
        # default seguro
        return "team" if ("team" in allowed) else "mine"
    return s


def _parse_status(s: str | None) -> str:
    v = (s or "").strip().upper() or "ALL"
    # dejamos ALL o cualquier estado explícito (validación por módulo en query)
    return v


def _default_range() -> Tuple[date, date]:
    # último mes
    today = date.today()
    return today - timedelta(days=30), today


def _has_table(schema: str, name: str) -> bool:
    try:
        return bool(
            fetch_one(
                "SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=?",
                (schema, name),
            )
        )
    except Exception:
        return False


def _scope_where_sql(
    scope: str,
    my_emp_id: int,
    employee_col_sql: str,
    ref_date_sql: str,
) -> Tuple[str, List[Any]]:
    """Construye WHERE seguro por alcance.

    - employee_col_sql: expresión SQL que representa el employee_id del registro
    - ref_date_sql: expresión SQL date usada para vigencia de jefe (hr_employee_manager)
    """

    if scope == "all" and (_is_admin() or _is_rrhh()):
        return "", []

    if scope == "mine":
        return f" AND {employee_col_sql} = ? ", [int(my_emp_id)]

    # team
    return (
        " AND ( "
        f"   {employee_col_sql} = ? "
        "   OR EXISTS ("
        "       SELECT 1 FROM rrhh.hr_employee_manager mm "
        f"       WHERE mm.employee_id = {employee_col_sql} "
        "         AND mm.manager_employee_id = ? "
        "         AND mm.is_primary = 1 "
        f"         AND mm.valid_from <= {ref_date_sql} "
        f"         AND (mm.valid_to IS NULL OR mm.valid_to >= {ref_date_sql}) "
        "   )"
        " ) ",
        [int(my_emp_id), int(my_emp_id)],
    )


def _normalize_filename(s: str) -> str:
    s = re.sub(r"[^a-zA-Z0-9._-]+", "_", s).strip("_")
    return s or "reporte"


# --------------------------------------------------------------------------------------
# Query builders
# --------------------------------------------------------------------------------------


def _query_hora_flexible(desde: date, hasta: date, status: str, scope: str, doc: str | None):
    if not _has_table("rrhh", "wf_request"):
        raise RuntimeError("No existe rrhh.wf_request")

    # detalle opcional
    has_det = _has_table("rrhh", "time_flexible_rule_request")

    sql = (
        "SELECT TOP (5000) "
        "  r.request_id, r.status, r.created_at, r.submitted_at, r.closed_at, "
        "  e.doc_number, (e.first_name + ' ' + e.last_name) AS employee_name, "
        + (
            "  d.weekday, d.slot, d.valid_from "
            "FROM rrhh.wf_request r "
            "LEFT JOIN rrhh.time_flexible_rule_request d ON d.request_id=r.request_id "
            if has_det
            else "  NULL AS weekday, NULL AS slot, NULL AS valid_from FROM rrhh.wf_request r "
        )
        + "JOIN rrhh.hr_employee e ON e.employee_id=r.employee_id "
        "WHERE r.request_type='HORA_FLEXIBLE' "
        "  AND CAST(r.created_at AS date) BETWEEN ? AND ? "
    )

    params: List[Any] = [desde, hasta]

    # status
    if status != "ALL":
        sql += " AND r.status = ? "
        params.append(status)

    my_emp_id = int(getattr(current_user, "employee_id", 0) or 0)
    w, p = _scope_where_sql(scope, my_emp_id, "r.employee_id", "CAST(r.created_at AS date)")
    sql += w
    params.extend(p)

    if doc:
        sql += " AND e.doc_number = ? "
        params.append(doc)

    sql += " ORDER BY r.created_at DESC "

    rows = fetch_all(sql, tuple(params)) or []

    headers = [
        "Request",
        "Estado",
        "Empleado",
        "Cédula",
        "Creada",
        "Vigencia desde",
        "Día",
        "Franja",
    ]

    data = []
    for r in rows:
        data.append(
            (
                getattr(r, "request_id", ""),
                getattr(r, "status", ""),
                getattr(r, "employee_name", ""),
                getattr(r, "doc_number", ""),
                getattr(r, "created_at", ""),
                getattr(r, "valid_from", ""),
                getattr(r, "weekday", ""),
                getattr(r, "slot", ""),
            )
        )

    return headers, data


def _query_incapacidad(desde: date, hasta: date, status: str, scope: str, doc: str | None):
    if not _has_table("rrhh", "medical_leave_request"):
        raise RuntimeError("No existe rrhh.medical_leave_request")

    sql = (
        "SELECT TOP (5000) "
        "  r.request_id, r.status, r.created_at, r.submitted_at, r.closed_at, "
        "  d.start_date, d.end_date, d.notes, "
        "  e.doc_number, (e.first_name + ' ' + e.last_name) AS employee_name "
        "FROM rrhh.wf_request r "
        "JOIN rrhh.medical_leave_request d ON d.request_id=r.request_id "
        "JOIN rrhh.hr_employee e ON e.employee_id=r.employee_id "
        "WHERE r.request_type='INCAPACIDAD' "
        "  AND d.start_date BETWEEN ? AND ? "
    )
    params: List[Any] = [desde, hasta]

    if status != "ALL":
        sql += " AND r.status = ? "
        params.append(status)

    my_emp_id = int(getattr(current_user, "employee_id", 0) or 0)
    w, p = _scope_where_sql(scope, my_emp_id, "r.employee_id", "d.start_date")
    sql += w
    params.extend(p)

    if doc:
        sql += " AND e.doc_number = ? "
        params.append(doc)

    sql += " ORDER BY d.start_date DESC, r.created_at DESC "

    rows = fetch_all(sql, tuple(params)) or []

    headers = [
        "Request",
        "Estado",
        "Empleado",
        "Cédula",
        "Inicio",
        "Fin",
        "Creada",
        "Notas",
    ]

    data = []
    for r in rows:
        data.append(
            (
                getattr(r, "request_id", ""),
                getattr(r, "status", ""),
                getattr(r, "employee_name", ""),
                getattr(r, "doc_number", ""),
                getattr(r, "start_date", ""),
                getattr(r, "end_date", ""),
                getattr(r, "created_at", ""),
                getattr(r, "notes", ""),
            )
        )

    return headers, data


def _query_chequera(desde: date, hasta: date, status: str, scope: str, doc: str | None):
    if not _has_table("rrhh", "timebook_request"):
        raise RuntimeError("No existe rrhh.timebook_request")

    sql = (
        "SELECT TOP (5000) "
        "  t.request_id, t.status, t.created_at, t.request_date, t.slot, t.reason, "
        "  t.decided_at, u2.ad_username AS decided_by, "
        "  e.doc_number, (e.first_name + ' ' + e.last_name) AS employee_name "
        "FROM rrhh.timebook_request t "
        "JOIN rrhh.hr_employee e ON e.employee_id=t.employee_id "
        "LEFT JOIN rrhh.auth_user u2 ON u2.user_id=t.decided_by_user_id "
        "WHERE t.request_date BETWEEN ? AND ? "
    )
    params: List[Any] = [desde, hasta]

    if status != "ALL":
        sql += " AND t.status = ? "
        params.append(status)

    my_emp_id = int(getattr(current_user, "employee_id", 0) or 0)
    w, p = _scope_where_sql(scope, my_emp_id, "t.employee_id", "t.request_date")
    sql += w
    params.extend(p)

    if doc:
        sql += " AND e.doc_number = ? "
        params.append(doc)

    sql += " ORDER BY t.request_date DESC, t.created_at DESC "

    rows = fetch_all(sql, tuple(params)) or []

    headers = [
        "ID",
        "Estado",
        "Empleado",
        "Cédula",
        "Fecha",
        "Franja",
        "Motivo",
        "Creada",
        "Decidida",
        "Decidida por",
    ]

    data = []
    for r in rows:
        data.append(
            (
                getattr(r, "request_id", ""),
                getattr(r, "status", ""),
                getattr(r, "employee_name", ""),
                getattr(r, "doc_number", ""),
                getattr(r, "request_date", ""),
                getattr(r, "slot", ""),
                getattr(r, "reason", ""),
                getattr(r, "created_at", ""),
                getattr(r, "decided_at", ""),
                getattr(r, "decided_by", ""),
            )
        )

    return headers, data


def _query_trabajo_casa(desde: date, hasta: date, status: str, scope: str, doc: str | None):
    if not _has_table("rrhh", "wfh_day"):
        raise RuntimeError("No existe rrhh.wfh_day")

    # status para WFH: APPROVED/PENDING
    sql = (
        "SELECT TOP (5000) "
        "  e.doc_number, (e.first_name + ' ' + e.last_name) AS employee_name, "
        "  w.work_date, w.reason, w.is_active, w.created_at "
        "FROM rrhh.wfh_day w "
        "JOIN rrhh.hr_employee e ON e.employee_id=w.employee_id "
        "WHERE w.work_date BETWEEN ? AND ? "
    )
    params: List[Any] = [desde, hasta]

    if status in ("APPROVED", "PENDING"):
        sql += " AND w.is_active = ? "
        params.append(1 if status == "APPROVED" else 0)

    my_emp_id = int(getattr(current_user, "employee_id", 0) or 0)
    wsql, p = _scope_where_sql(scope, my_emp_id, "w.employee_id", "w.work_date")
    sql += wsql
    params.extend(p)

    if doc:
        sql += " AND e.doc_number = ? "
        params.append(doc)

    sql += " ORDER BY w.work_date DESC, w.created_at DESC "

    rows = fetch_all(sql, tuple(params)) or []

    headers = [
        "Empleado",
        "Cédula",
        "Día",
        "Estado",
        "Motivo",
        "Creada",
    ]

    data = []
    for r in rows:
        st = "APPROVED" if int(getattr(r, "is_active", 0) or 0) == 1 else "PENDING"
        data.append(
            (
                getattr(r, "employee_name", ""),
                getattr(r, "doc_number", ""),
                getattr(r, "work_date", ""),
                st,
                getattr(r, "reason", ""),
                getattr(r, "created_at", ""),
            )
        )

    return headers, data


def _get_report_data(slug: str, desde: date, hasta: date, status: str, scope: str, doc: str | None):
    if slug == "hora-flexible":
        return _query_hora_flexible(desde, hasta, status, scope, doc)
    if slug == "incapacidad":
        return _query_incapacidad(desde, hasta, status, scope, doc)
    if slug == "chequera":
        return _query_chequera(desde, hasta, status, scope, doc)
    if slug == "trabajo-casa":
        return _query_trabajo_casa(desde, hasta, status, scope, doc)
    raise KeyError(slug)


# --------------------------------------------------------------------------------------
# Routes
# --------------------------------------------------------------------------------------


@modulos_bp.route("/reportes")
@login_required
def reportes_index():
    return render_template(
        "modulos/reportes_index.html",
        reports=[{"slug": k, **v} for k, v in REPORTS.items()],
        allowed_scopes=_allowed_scopes(),
        is_admin=_is_admin(),
        is_rrhh=_is_rrhh(),
        es_jefe=_is_manager(),
    )


@modulos_bp.route("/reportes/<slug>")
@login_required
def reportes_detalle(slug: str):
    if slug not in REPORTS:
        flash("Reporte no existe.", "warning")
        return redirect(url_for("modulos.reportes_index"))

    d0, d1 = _default_range()
    desde = _parse_date(request.args.get("desde")) or d0
    hasta = _parse_date(request.args.get("hasta")) or d1
    if hasta < desde:
        desde, hasta = hasta, desde

    scope = _resolve_scope(request.args.get("scope"))
    status = _parse_status(request.args.get("status") or REPORTS[slug].get("default_status"))

    doc = None
    if _is_admin() or _is_rrhh():
        doc = _parse_doc_number(request.args.get("doc"))

    headers: Sequence[str] = []
    rows: List[Sequence[Any]] = []

    try:
        headers, rows = _get_report_data(slug, desde, hasta, status, scope, doc)
    except RuntimeError as rt:
        flash("No fue posible generar el reporte. Verifica los filtros e inténtalo de nuevo.", "warning")
        headers, rows = [], []
    except Exception:
        flash("No se pudo construir el reporte. Revisa configuración de BD.", "error")
        headers, rows = [], []

    # limit vista previa
    preview = rows[:500]

    # build query string for export links
    qs = {
        "desde": desde.isoformat(),
        "hasta": hasta.isoformat(),
        "scope": scope,
        "status": status,
    }
    if doc:
        qs["doc"] = doc

    return render_template(
        "modulos/reportes_detalle.html",
        slug=slug,
        title=REPORTS[slug]["title"],
        desde=desde,
        hasta=hasta,
        scope=scope,
        status=status,
        doc=doc or "",
        allowed_scopes=_allowed_scopes(),
        is_backoffice=_is_admin_or_rrhh(),
        is_admin=_is_admin(),
        is_rrhh=_is_rrhh(),
        es_jefe=_is_manager(),
        headers=headers,
        rows=preview,
        row_count=len(rows),
        export_qs=qs,
    )


@modulos_bp.route("/reportes/<slug>/export/<fmt>")
@login_required
def reportes_export(slug: str, fmt: str):
    if slug not in REPORTS:
        flash("Reporte no existe.", "warning")
        return redirect(url_for("modulos.reportes_index"))

    fmt = (fmt or "").lower().strip()
    if fmt not in ("pdf", "excel"):
        flash("Formato inválido.", "warning")
        return redirect(url_for("modulos.reportes_detalle", slug=slug))

    d0, d1 = _default_range()
    desde = _parse_date(request.args.get("desde")) or d0
    hasta = _parse_date(request.args.get("hasta")) or d1
    if hasta < desde:
        desde, hasta = hasta, desde

    scope = _resolve_scope(request.args.get("scope"))
    status = _parse_status(request.args.get("status") or REPORTS[slug].get("default_status"))

    doc = None
    if _is_admin() or _is_rrhh():
        doc = _parse_doc_number(request.args.get("doc"))

    try:
        headers, rows = _get_report_data(slug, desde, hasta, status, scope, doc)
    except RuntimeError as rt:
        flash("No fue posible generar el reporte. Verifica los filtros e inténtalo de nuevo.", "warning")
        return redirect(url_for("modulos.reportes_detalle", slug=slug))
    except Exception:
        flash("No se pudo exportar el reporte.", "error")
        return redirect(url_for("modulos.reportes_detalle", slug=slug))

    if len(rows) > MAX_EXPORT_ROWS:
        flash(
            f"Demasiadas filas para exportar ({len(rows)}). Ajusta filtros (fecha/estado) para bajar de {MAX_EXPORT_ROWS}.",
            "warning",
        )
        return redirect(url_for("modulos.reportes_detalle", slug=slug, **request.args))

    # Build response
    scope_label = {"mine": "mio", "team": "equipo", "all": "todo"}.get(scope, scope)
    base = _normalize_filename(f"{slug}_{desde.isoformat()}_{hasta.isoformat()}_{scope_label}")

    title = REPORTS[slug]["title"]
    subtitle = [
        f"Rango: {desde.isoformat()} a {hasta.isoformat()}",
        f"Alcance: {scope_label} · Estado: {status}",
        f"Generado por: {getattr(current_user, 'ad_username', '')}",
    ]

    if fmt == "excel":
        payload = build_excel(title, headers, rows)
        fn = f"{base}.xlsx"
        return Response(
            payload,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename=\"{fn}\""},
        )

    payload = build_pdf(title, subtitle, headers, rows)
    fn = f"{base}.pdf"
    return Response(
        payload,
        mimetype="application/pdf",
        headers={"Content-Disposition": f"attachment; filename=\"{fn}\""},
    )
