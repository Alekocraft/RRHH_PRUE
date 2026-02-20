from __future__ import annotations

from datetime import date
from typing import Optional, List

from services.rrhh_db import fetch_all, fetch_one, execute


# -----------------------------------------------------------------------------
# Helpers de esquema
# -----------------------------------------------------------------------------

def _has_col(schema: str, table: str, column: str) -> bool:
    """True si existe la columna en la tabla indicada (SQL Server)."""
    row = fetch_one(
        "SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS "
        "WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND COLUMN_NAME=?",
        (schema, table, column),
    )
    return row is not None


def _hr_employee_has_col(col_name: str) -> bool:
    return _has_col("rrhh", "hr_employee", col_name)


# -----------------------------------------------------------------------------
# Flags / compatibilidad
# -----------------------------------------------------------------------------

def ensure_can_work_from_home_column() -> None:
    """Best-effort: asegura rrhh.hr_employee.can_work_from_home (BIT).

    Si el usuario de BD no tiene permisos DDL, la creación fallará y se ignora.
    """
    try:
        execute(
            "IF COL_LENGTH('rrhh.hr_employee','can_work_from_home') IS NULL "
            "BEGIN "
            "ALTER TABLE rrhh.hr_employee "
            "ADD can_work_from_home BIT NOT NULL "
            "CONSTRAINT DF_hr_employee_can_wfh DEFAULT(0); "
            "END"
        )
    except Exception:
        # No bloquear el login/flujo si no hay permisos DDL
        pass


def employee_can_work_from_home(employee_id: Optional[int]) -> bool:
    """Retorna True si el empleado tiene habilitado trabajo en casa."""
    if not employee_id:
        return False

    if not _hr_employee_has_col("can_work_from_home"):
        return False

    r = fetch_one(
        "SELECT can_work_from_home FROM rrhh.hr_employee WHERE employee_id=?",
        (int(employee_id),),
    )
    return bool(getattr(r, "can_work_from_home", 0)) if r else False


# -----------------------------------------------------------------------------
# CRUD Empleados
# -----------------------------------------------------------------------------

def get_all_employees(active_only: bool = True):
    sql = (
        "SELECT employee_id, doc_number, first_name, last_name, email, department, position_name, is_active "
        "FROM rrhh.hr_employee "
    )
    if active_only:
        sql += "WHERE is_active = 1 "
    sql += "ORDER BY last_name, first_name"
    return fetch_all(sql)


def get_employee(employee_id: int):
    cols = (
        "employee_id, doc_number, first_name, last_name, email, department, position_name, "
        "hire_date, cost_center, area_name, ad_username, is_exec_approval_by_hr, is_active"
    )
    if _hr_employee_has_col("can_work_from_home"):
        cols += ", can_work_from_home"

    return fetch_one(
        f"SELECT {cols} FROM rrhh.hr_employee WHERE employee_id = ?",
        (int(employee_id),),
    )


def find_employee_by_doc_number(doc_number: str):
    return fetch_one(
        "SELECT employee_id FROM rrhh.hr_employee WHERE doc_number = ?",
        (doc_number,),
    )


def upsert_employee_by_ad(
    sam: str,
    doc_number: Optional[str],
    first_name: str,
    last_name: str,
    email: Optional[str],
    department: Optional[str],
    position_name: Optional[str],
    is_exec_approval_by_hr: bool,
    can_work_from_home: bool = False,
) -> int:
    """Crea o actualiza un empleado usando ad_username (samAccountName).

    Compatible con llamadas previas que no enviaban can_work_from_home.
    Retorna employee_id.
    """

    sam = (sam or "").strip()
    if not sam:
        raise ValueError("sam (ad_username) es obligatorio")

    has_wfh = _hr_employee_has_col("can_work_from_home")

    # 1) Si ya existe por ad_username, actualizar
    e = fetch_one("SELECT employee_id FROM rrhh.hr_employee WHERE ad_username = ?", (sam,))
    if e:
        if has_wfh:
            execute(
                "UPDATE rrhh.hr_employee SET doc_number=?, first_name=?, last_name=?, email=?, department=?, position_name=?, "
                "is_exec_approval_by_hr=?, can_work_from_home=?, is_active=1 "
                "WHERE employee_id=?",
                (
                    doc_number,
                    first_name,
                    last_name,
                    email,
                    department,
                    position_name,
                    1 if is_exec_approval_by_hr else 0,
                    1 if can_work_from_home else 0,
                    int(e.employee_id),
                ),
            )
        else:
            execute(
                "UPDATE rrhh.hr_employee SET doc_number=?, first_name=?, last_name=?, email=?, department=?, position_name=?, "
                "is_exec_approval_by_hr=?, is_active=1 "
                "WHERE employee_id=?",
                (
                    doc_number,
                    first_name,
                    last_name,
                    email,
                    department,
                    position_name,
                    1 if is_exec_approval_by_hr else 0,
                    int(e.employee_id),
                ),
            )
        return int(e.employee_id)

    # 2) Insertar
    if has_wfh:
        execute(
            "INSERT INTO rrhh.hr_employee(doc_number, first_name, last_name, email, department, position_name, "
            "is_exec_approval_by_hr, can_work_from_home, is_active, ad_username) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?)",
            (
                doc_number,
                first_name,
                last_name,
                email,
                department,
                position_name,
                1 if is_exec_approval_by_hr else 0,
                1 if can_work_from_home else 0,
                sam,
            ),
        )
    else:
        execute(
            "INSERT INTO rrhh.hr_employee(doc_number, first_name, last_name, email, department, position_name, "
            "is_exec_approval_by_hr, is_active, ad_username) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?)",
            (
                doc_number,
                first_name,
                last_name,
                email,
                department,
                position_name,
                1 if is_exec_approval_by_hr else 0,
                sam,
            ),
        )

    new_row = fetch_one(
        "SELECT TOP 1 employee_id FROM rrhh.hr_employee WHERE ad_username = ? ORDER BY employee_id DESC",
        (sam,),
    )
    if not new_row:
        raise RuntimeError("No se pudo obtener employee_id luego de insertar")

    return int(new_row.employee_id)


# -----------------------------------------------------------------------------
# Jerarquía empleado-jefe
# -----------------------------------------------------------------------------

def get_manager_for_employee(employee_id: int, on_date: Optional[date] = None) -> Optional[int]:
    """Retorna el employee_id del jefe vigente para el empleado en la fecha on_date."""
    if not employee_id:
        return None

    d = on_date or date.today()
    row = fetch_one(
        "SELECT TOP 1 manager_employee_id "
        "FROM rrhh.hr_employee_manager "
        "WHERE employee_id=? AND valid_from <= ? AND (valid_to IS NULL OR valid_to >= ?) "
        "ORDER BY valid_from DESC",
        (int(employee_id), d, d),
    )
    return int(row.manager_employee_id) if row and row.manager_employee_id is not None else None


def manager_has_subordinates(manager_employee_id: Optional[int], on_date: Optional[date] = None) -> bool:
    """True si el empleado tiene al menos 1 subordinado vigente en on_date."""
    if not manager_employee_id:
        return False

    d = on_date or date.today()
    row = fetch_one(
        "SELECT TOP 1 1 "
        "FROM rrhh.hr_employee_manager "
        "WHERE manager_employee_id=? AND valid_from <= ? AND (valid_to IS NULL OR valid_to >= ?)",
        (int(manager_employee_id), d, d),
    )
    return row is not None


def get_subordinates(manager_employee_id: int, on_date: Optional[date] = None) -> List[int]:
    """Lista de employee_id de subordinados vigentes para un jefe."""
    if not manager_employee_id:
        return []

    d = on_date or date.today()
    rows = fetch_all(
        "SELECT employee_id "
        "FROM rrhh.hr_employee_manager "
        "WHERE manager_employee_id=? AND valid_from <= ? AND (valid_to IS NULL OR valid_to >= ?)",
        (int(manager_employee_id), d, d),
    )
    return [int(r.employee_id) for r in rows]
