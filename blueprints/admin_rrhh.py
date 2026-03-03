from datetime import date, datetime
import logging

from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from flask_login import login_required, current_user

from services.rrhh_db import fetch_all, fetch_one, execute
from services.rrhh_user import (
    list_users_for_admin,
    link_user_to_employee,
    set_user_active,
    set_user_is_admin,
    get_or_create_auth_user,
)
from services.hr_employee_service import get_employee, upsert_employee_by_ad
from services.ldap_directory import search_users, get_user_by_sam
from services.rrhh_security import ROLE_ADMIN, ROLE_RRHH

admin_rrhh_bp = Blueprint("admin_rrhh", __name__, url_prefix="/admin")

logger = logging.getLogger(__name__)


def _require_admin() -> bool:
    """Permite acceso a ADMINISTRADOR o RRHH.

    Nota: el modelo User del proyecto expone es_administrador/es_rrhh.
    Se deja también soporte por roles crudos por compatibilidad.
    """
    if getattr(current_user, "es_administrador", False) or getattr(current_user, "es_rrhh", False):
        return True

    roles = getattr(current_user, "roles", []) or []
    if (ROLE_ADMIN in roles) or (ROLE_RRHH in roles) or ("ADMINISTRADOR" in roles) or ("RRHH" in roles):
        return True

    flash("No tienes permisos para acceder a esta sección.", "warning")
    return False


@admin_rrhh_bp.route("/usuarios")
@login_required
def usuarios_listar():
    if not _require_admin():
        return redirect(url_for("modulos.dashboard"))

    usuarios = list_users_for_admin()
    empleados = fetch_all(
        "SELECT employee_id, doc_number, first_name, last_name "
        "FROM rrhh.hr_employee WHERE is_active=1 "
        "ORDER BY last_name, first_name"
    )
    roles_catalog = [{"role_code": ROLE_ADMIN, "role_name": "Administrador"}]
    return render_template(
        "admin/usuarios.html",
        usuarios=usuarios,
        empleados=empleados,
        roles_catalog=roles_catalog,
    )


@admin_rrhh_bp.route("/usuarios/actualizar", methods=["POST"])
@login_required
def usuarios_actualizar():
    if not _require_admin():
        return redirect(url_for("modulos.dashboard"))

    user_id = int(request.form.get("user_id"))
    employee_id_raw = request.form.get("employee_id") or ""
    employee_id = int(employee_id_raw) if employee_id_raw.strip() else None
    is_active = True if request.form.get("is_active") == "on" else False
    is_admin = True if request.form.get("role_ADMINISTRADOR") == "on" else False

    try:
        link_user_to_employee(user_id, employee_id)
        set_user_active(user_id, is_active)
        set_user_is_admin(user_id, is_admin)
        flash("Usuario actualizado.", "success")
    except Exception as ex:
        flash(f"No se pudo actualizar: {ex}", "danger")

    return redirect(url_for("admin_rrhh.usuarios_listar"))


@admin_rrhh_bp.route("/empleados/nuevo", methods=["GET", "POST"])
@login_required
def empleado_nuevo():
    if not _require_admin():
        return redirect(url_for("modulos.dashboard"))

    managers = fetch_all(
        "SELECT employee_id, doc_number, first_name, last_name, ad_username "
        "FROM rrhh.hr_employee WHERE is_active=1 "
        "ORDER BY last_name, first_name"
    )

    form_data = {}
    cur_mgr_id = None
    if request.method == "POST":
        sam = (request.form.get("ad_username") or "").strip()
        doc_number = (request.form.get("doc_number") or "").strip()
        first_name = (request.form.get("first_name") or "").strip()
        last_name = (request.form.get("last_name") or "").strip()
        email = (request.form.get("email") or "").strip() or None
        department = (request.form.get("department") or "").strip() or None
        position_name = (request.form.get("position_name") or "").strip() or None
        is_exec_approval_by_hr = True if request.form.get("is_exec_approval_by_hr") == "on" else False
        can_work_from_home = True if request.form.get("can_work_from_home") == "on" else False
        manager_employee_id = request.form.get("manager_employee_id") or None
        manager_employee_id = int(manager_employee_id) if manager_employee_id else None

        form_data = dict(request.form)
        cur_mgr_id = manager_employee_id

        birth_date_raw = (request.form.get("birth_date") or "").strip()
        birth_date = None
        if birth_date_raw:
            try:
                birth_date = datetime.strptime(birth_date_raw, "%Y-%m-%d").date()
            except Exception:
                flash("Fecha de cumpleaños inválida. Usa el selector de fecha.", "warning")
                return render_template("admin/empleado_nuevo.html", managers=managers, form=form_data, cur_mgr_id=cur_mgr_id)
        else:
            flash("Debes ingresar la fecha de cumpleaños.", "warning")
            return render_template("admin/empleado_nuevo.html", managers=managers, form=form_data, cur_mgr_id=cur_mgr_id)


        if not sam:
            flash("Debes seleccionar un usuario de AD (samAccountName).", "warning")
            return render_template("admin/empleado_nuevo.html", managers=managers, form=form_data, cur_mgr_id=cur_mgr_id)

        if not doc_number:
            flash("Debes ingresar la cédula (doc_number).", "warning")
            return render_template("admin/empleado_nuevo.html", managers=managers, form=form_data, cur_mgr_id=cur_mgr_id)

        try:
            employee_id = upsert_employee_by_ad(
                sam=sam,
                doc_number=doc_number,
                first_name=first_name or sam,
                last_name=last_name or "",
                email=email,
                department=department,
                position_name=position_name,
                is_exec_approval_by_hr=is_exec_approval_by_hr,
                can_work_from_home=can_work_from_home,
                birth_date=birth_date,
            )

            # manager relationship
            if manager_employee_id:
                _set_manager(employee_id, manager_employee_id)

            # crear/auth_user y asociar
            u = get_or_create_auth_user(sam)
            link_user_to_employee(u["user_id"], employee_id)

            flash("Empleado creado/actualizado y asociado al usuario.", "success")
            return redirect(url_for("admin_rrhh.usuarios_listar"))
        except Exception as ex:
            flash(f"No se pudo crear el empleado: {ex}", "danger")

    return render_template("admin/empleado_nuevo.html", managers=managers, form=form_data, cur_mgr_id=cur_mgr_id)


def _set_manager(employee_id: int, manager_employee_id: int):
    """Registra jefe inmediato.

    La tabla rrhh.hr_employee_manager suele tener PK (employee_id, valid_from) (DATE).
    Por eso, si se intenta asignar jefe más de una vez el mismo día, NO se puede insertar
    otra fila con el mismo valid_from. En ese caso se actualiza la fila de hoy.
    """

    def _has_mgr_col(col: str) -> bool:
        return (
            fetch_one(
                "SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS "
                "WHERE TABLE_SCHEMA='rrhh' AND TABLE_NAME='hr_employee_manager' AND COLUMN_NAME=?",
                (col,),
            )
            is not None
        )

    today = date.today()
    created_by = getattr(current_user, "user_db_id", None)
    created_by = int(created_by) if created_by is not None else None
    has_created_by = _has_mgr_col("created_by_user_id")

    # Si ya existe fila con valid_from = hoy (abierta o cerrada), se actualiza y se deja vigente.
    today_row = fetch_one(
        "SELECT manager_employee_id, valid_to FROM rrhh.hr_employee_manager WHERE employee_id=? AND valid_from=?",
        (employee_id, today),
    )
    if today_row:
        # Si ya está exactamente igual y vigente, no hacer nada
        if (today_row.manager_employee_id == manager_employee_id) and (today_row.valid_to is None):
            return

        if has_created_by:
            execute(
                "UPDATE rrhh.hr_employee_manager "
                "SET manager_employee_id=?, valid_to=NULL, created_by_user_id=? "
                "WHERE employee_id=? AND valid_from=?",
                (manager_employee_id, created_by, employee_id, today),
            )
        else:
            execute(
                "UPDATE rrhh.hr_employee_manager "
                "SET manager_employee_id=?, valid_to=NULL "
                "WHERE employee_id=? AND valid_from=?",
                (manager_employee_id, employee_id, today),
            )
        return

    # Buscar relación vigente (valid_to IS NULL)
    cur = fetch_one(
        "SELECT TOP 1 manager_employee_id, valid_from "
        "FROM rrhh.hr_employee_manager "
        "WHERE employee_id=? AND valid_to IS NULL "
        "ORDER BY valid_from DESC",
        (employee_id,),
    )

    # Si la relación vigente ya es la misma, no hacemos nada.
    if cur and (cur.manager_employee_id == manager_employee_id):
        return

    # Cerrar relación vigente (si existe) – como no existe fila de hoy, el valid_from no choca.
    if cur:
        execute(
            "UPDATE rrhh.hr_employee_manager SET valid_to=? WHERE employee_id=? AND valid_to IS NULL",
            (today, employee_id),
        )

    # Insertar nueva relación vigente con valid_from = hoy
    if has_created_by:
        execute(
            "INSERT INTO rrhh.hr_employee_manager(employee_id, manager_employee_id, valid_from, valid_to, created_by_user_id) "
            "VALUES (?, ?, ?, NULL, ?)",
            (employee_id, manager_employee_id, today, created_by),
        )
    else:
        execute(
            "INSERT INTO rrhh.hr_employee_manager(employee_id, manager_employee_id, valid_from, valid_to) "
            "VALUES (?, ?, ?, NULL)",
            (employee_id, manager_employee_id, today),
        )


@admin_rrhh_bp.route("/empleados/<int:employee_id>/editar", methods=["GET", "POST"])
@login_required
def empleado_editar(employee_id: int):
    if not _require_admin():
        return redirect(url_for("modulos.dashboard"))

    emp = get_employee(employee_id)
    if not emp:
        flash("Empleado no encontrado.", "warning")
        return redirect(url_for("admin_rrhh.usuarios_listar"))

    managers = fetch_all(
        "SELECT employee_id, doc_number, first_name, last_name, ad_username "
        "FROM rrhh.hr_employee WHERE is_active=1 AND employee_id <> ? "
        "ORDER BY last_name, first_name",
        (employee_id,),
    )
    cur_mgr = fetch_one(
        "SELECT TOP 1 manager_employee_id FROM rrhh.hr_employee_manager "
        "WHERE employee_id=? AND (valid_to IS NULL OR valid_to >= ?) "
        "ORDER BY valid_from DESC",
        (employee_id, date.today()),
    )
    cur_mgr_id = cur_mgr.manager_employee_id if cur_mgr else None

    if request.method == "POST":
        doc_number = (request.form.get("doc_number") or "").strip()
        first_name = (request.form.get("first_name") or "").strip()
        last_name = (request.form.get("last_name") or "").strip()
        email = (request.form.get("email") or "").strip() or None
        department = (request.form.get("department") or "").strip() or None
        position_name = (request.form.get("position_name") or "").strip() or None
        is_active = True if request.form.get("is_active") == "on" else False
        is_exec_approval_by_hr = True if request.form.get("is_exec_approval_by_hr") == "on" else False
        can_work_from_home = True if request.form.get("can_work_from_home") == "on" else False
        manager_employee_id = request.form.get("manager_employee_id") or None
        manager_employee_id = int(manager_employee_id) if manager_employee_id else None

        if not doc_number:
            flash("La cédula (doc_number) es obligatoria.", "warning")
            return render_template(
                "admin/empleado_editar.html",
                emp=emp,
                managers=managers,
                cur_mgr_id=cur_mgr_id,
            )

        try:
            has_wfh = (
                fetch_one(
                    "SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS "
                    "WHERE TABLE_SCHEMA='rrhh' AND TABLE_NAME='hr_employee' AND COLUMN_NAME='can_work_from_home'"
                )
                is not None
            )

            if has_wfh:
                execute(
                    "UPDATE rrhh.hr_employee SET doc_number=?, first_name=?, last_name=?, email=?, department=?, position_name=?, "
                    "is_active=?, is_exec_approval_by_hr=?, can_work_from_home=? "
                    "WHERE employee_id=?",
                    (
                        doc_number,
                        first_name,
                        last_name,
                        email,
                        department,
                        position_name,
                        1 if is_active else 0,
                        1 if is_exec_approval_by_hr else 0,
                        1 if can_work_from_home else 0,
                        employee_id,
                    ),
                )
            else:
                execute(
                    "UPDATE rrhh.hr_employee SET doc_number=?, first_name=?, last_name=?, email=?, department=?, position_name=?, "
                    "is_active=?, is_exec_approval_by_hr=? "
                    "WHERE employee_id=?",
                    (
                        doc_number,
                        first_name,
                        last_name,
                        email,
                        department,
                        position_name,
                        1 if is_active else 0,
                        1 if is_exec_approval_by_hr else 0,
                        employee_id,
                    ),
                )
            if manager_employee_id:
                _set_manager(employee_id, manager_employee_id)

            flash("Empleado actualizado.", "success")
            return redirect(url_for("admin_rrhh.usuarios_listar"))
        except Exception as ex:
            flash(f"No se pudo actualizar: {ex}", "danger")

    return render_template(
        "admin/empleado_editar.html",
        emp=emp,
        managers=managers,
        cur_mgr_id=cur_mgr_id,
    )


@admin_rrhh_bp.route("/ldap/search")
@login_required
def ldap_search():
    if not _require_admin():
        return jsonify({"items": []})

    q = (request.args.get("q") or request.args.get("term") or "").strip()
    if len(q) < 3:
        return jsonify({"items": []})

    try:
        items = search_users(q, limit=15)
        return jsonify({"items": items})
    except Exception as ex:
        logger.warning("Búsqueda en directorio falló")
        return jsonify({"items": [], "error": "INTERNAL_ERROR"})


@admin_rrhh_bp.route("/ldap/user")
@login_required
def ldap_user():
    if not _require_admin():
        return jsonify(None)

    sam = (request.args.get("sam") or request.args.get("id") or "").strip()
    item = get_user_by_sam(sam)
    return jsonify(item)