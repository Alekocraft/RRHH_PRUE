from datetime import date, datetime
import logging
from typing import Optional

from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from flask_login import login_required, current_user

from services.rrhh_db import fetch_all, fetch_one, execute
from services.rrhh_user import (
    list_users_for_admin,
    get_user_by_id,
    link_user_to_employee,
    set_user_active,
    get_or_create_auth_user,
)
from services.hr_employee_service import get_employee, upsert_employee_by_ad
from services.ldap_directory import search_users, get_user_by_sam
from services.rrhh_security import (
    ROLE_ADMIN,
    ROLE_RRHH,
    ROLE_EMPLEADO,
    ROLE_COORD_INDEM,
    ROLE_GERENTE_INDEM,
    set_user_roles,
)
from services.schedule_service import (
    list_shift_definitions,
    get_current_shift_assignment,
    set_shift_assignment,
    flex_tables_exist,
    get_active_flex_rule,
    set_active_flex_rule,
)

admin_rrhh_bp = Blueprint("admin_rrhh", __name__, url_prefix="/admin")
logger = logging.getLogger(__name__)


def _require_admin() -> bool:
    """Permite acceso a ADMINISTRADOR o RRHH."""
    if getattr(current_user, "es_administrador", False) or getattr(current_user, "es_rrhh", False):
        return True

    roles = getattr(current_user, "roles", []) or []
    if (ROLE_ADMIN in roles) or (ROLE_RRHH in roles) or ("ADMINISTRADOR" in roles) or ("RRHH" in roles):
        return True

    flash("No tienes permisos para acceder a esta sección.", "warning")
    return False


def _roles_catalog():
    return [
        {"role_code": ROLE_ADMIN, "role_name": "Administrador"},
        {"role_code": ROLE_RRHH, "role_name": "RRHH"},
        {"role_code": ROLE_COORD_INDEM, "role_name": "Coordinador de indemnizaciones"},
        {"role_code": ROLE_GERENTE_INDEM, "role_name": "Gerente de indemnizaciones"},
    ]


def _employees_for_select(include_inactive: bool = False):
    sql = (
        "SELECT employee_id, doc_number, first_name, last_name, ad_username "
        "FROM rrhh.hr_employee "
    )
    if not include_inactive:
        sql += "WHERE is_active=1 "
    sql += "ORDER BY last_name, first_name"
    return fetch_all(sql)


@admin_rrhh_bp.route("/usuarios")
@login_required
def usuarios_listar():
    if not _require_admin():
        return redirect(url_for("modulos.dashboard"))

    usuarios = list_users_for_admin()
    return render_template("admin/usuarios.html", usuarios=usuarios)


@admin_rrhh_bp.route("/usuarios/nuevo", methods=["GET", "POST"])
@login_required
def usuario_nuevo():
    """Unificado: Crear usuario + colaborador + roles.

    - Crea (si no existe) rrhh.auth_user
    - Opcionalmente crea/actualiza rrhh.hr_employee
    - Asocia user->employee
    - Asigna roles (EMPLEADO siempre)
    """
    if not _require_admin():
        return redirect(url_for("modulos.dashboard"))

    managers = _employees_for_select(include_inactive=False)
    empleados = _employees_for_select(include_inactive=False)
    roles_catalog = _roles_catalog()
    shifts_catalog = list_shift_definitions()
    flex_enabled = flex_tables_exist()

    form = {}
    if request.method == "POST":
        form = dict(request.form)
        sam = (request.form.get("ad_username") or "").strip()
        if not sam:
            flash("Debes seleccionar un usuario de AD (samAccountName).", "warning")
            return render_template(
                "admin/usuario_form.html",
                mode="create",
                form=form,
                user=None,
                empleados=empleados,
                managers=managers,
                roles_catalog=roles_catalog,
                shifts_catalog=shifts_catalog,
                flex_enabled=flex_enabled,
                current_shift=None,
                current_flex=None,
                lock_admin=False,
                lock_rrhh=False,
            )

        employee_mode = (request.form.get("employee_mode") or "NEW").strip().upper()
        employee_id: Optional[int] = None

        # Usuario
        is_active = True
        role_codes = [ROLE_EMPLEADO]
        for rc in (ROLE_ADMIN, ROLE_RRHH, ROLE_COORD_INDEM, ROLE_GERENTE_INDEM):
            if request.form.get(f"role_{rc}") == "on":
                role_codes.append(rc)

        try:
            # Colaborador
            if employee_mode == "":
                employee_id = None
            elif employee_mode == "NEW":
                doc_number = (request.form.get("doc_number") or "").strip()
                first_name = (request.form.get("first_name") or "").strip()
                last_name = (request.form.get("last_name") or "").strip()
                email = (request.form.get("email") or "").strip() or None
                department = (request.form.get("department") or "").strip() or None
                position_name = (request.form.get("position_name") or "").strip() or None
                shift_group = (request.form.get("shift_group") or "").strip().upper()
                birth_date_raw = (request.form.get("birth_date") or "").strip()
                can_work_from_home = True if request.form.get("can_work_from_home") == "on" else False
                is_exec_approval_by_hr = True if request.form.get("is_exec_approval_by_hr") == "on" else False
                manager_employee_id = request.form.get("manager_employee_id") or None
                manager_employee_id = int(manager_employee_id) if manager_employee_id else None

                # Validaciones mínimas
                if not doc_number:
                    raise ValueError("Debes ingresar la cédula (doc_number).")
                if not first_name or not last_name:
                    raise ValueError("Debes ingresar nombres y apellidos.")
                if not birth_date_raw:
                    raise ValueError("Debes ingresar la fecha de cumpleaños.")
                try:
                    birth_date = datetime.strptime(birth_date_raw, "%Y-%m-%d").date()
                except Exception:
                    raise ValueError("Fecha de cumpleaños inválida. Usa el selector de fecha.")

                if shift_group not in ("CABINA", "AJUSTADOR", "ADMIN"):
                    raise ValueError("Debes seleccionar el equipo operativo (Cabina o Ajustadores).")

                employee_id = upsert_employee_by_ad(
                    sam=sam,
                    doc_number=doc_number,
                    first_name=first_name,
                    last_name=last_name,
                    email=email,
                    department=department,
                    position_name=position_name,
                    is_exec_approval_by_hr=is_exec_approval_by_hr,
                    can_work_from_home=can_work_from_home,
                    birth_date=birth_date,
                    shift_group=shift_group,
                )

                if manager_employee_id:
                    _set_manager(int(employee_id), int(manager_employee_id))
            else:
                # Asociar a empleado existente
                employee_id = int(employee_mode)

            # -------------------------------------------------------------
            # Horario (turno base) y Cambio de horario (Hora flexible)
            # -------------------------------------------------------------
            if employee_id:
                # Determinar grupo (para obligar turno base en operativos)
                sg = (request.form.get("shift_group") or "").strip().upper()
                if not sg:
                    try:
                        er = get_employee(int(employee_id))
                        sg = (getattr(er, "shift_group", "") or "").strip().upper()
                    except Exception:
                        sg = ""

                base_shift_id = (request.form.get("base_shift_id") or "").strip()
                base_from_s = (request.form.get("base_shift_valid_from") or "").strip()
                base_to_s = (request.form.get("base_shift_valid_to") or "").strip()

                if sg in ("CABINA", "AJUSTADOR") and not base_shift_id:
                    raise ValueError("Debes asignar el horario (turno base) para Cabina/Ajustadores.")

                if base_shift_id:
                    try:
                        base_from = datetime.strptime(base_from_s or date.today().strftime("%Y-%m-%d"), "%Y-%m-%d").date()
                    except Exception:
                        raise ValueError("Fecha 'Desde' del horario inválida.")
                    base_to = None
                    if base_to_s:
                        try:
                            base_to = datetime.strptime(base_to_s, "%Y-%m-%d").date()
                        except Exception:
                            raise ValueError("Fecha 'Hasta' del horario inválida.")

                    set_shift_assignment(
                        int(employee_id),
                        int(base_shift_id),
                        base_from,
                        base_to,
                        getattr(current_user, "user_id", None),
                    )

                if flex_enabled and request.form.get("flex_apply") == "on":
                    wd_raw = (request.form.get("flex_weekday") or "").strip()
                    slot = (request.form.get("flex_slot") or "").strip()
                    vf_s = (request.form.get("flex_valid_from") or "").strip()
                    if not (wd_raw and slot and vf_s):
                        raise ValueError("Para aplicar cambio de horario debes completar día, AM/PM y fecha.")
                    try:
                        vf = datetime.strptime(vf_s, "%Y-%m-%d").date()
                    except Exception:
                        raise ValueError("Fecha 'Desde' del cambio de horario inválida.")
                    set_active_flex_rule(
                        int(employee_id),
                        int(wd_raw),
                        slot,
                        vf,
                        getattr(current_user, "user_id", None),
                    )

            # Crear / upsert usuario y asociar
            u = get_or_create_auth_user(sam)
            uid = int(u["user_id"]) if isinstance(u, dict) else int(getattr(u, "user_id"))
            link_user_to_employee(uid, employee_id)
            set_user_active(uid, is_active)
            set_user_roles(uid, role_codes)

            flash("Usuario/colaborador guardado.", "success")
            return redirect(url_for("admin_rrhh.usuario_editar", user_id=uid))

        except Exception as ex:
            flash(str(ex), "warning")

    return render_template(
        "admin/usuario_form.html",
        mode="create",
        form=form,
        user=None,
        empleados=empleados,
        managers=managers,
        roles_catalog=roles_catalog,
        shifts_catalog=shifts_catalog,
        flex_enabled=flex_enabled,
        current_shift=None,
        current_flex=None,
        lock_admin=False,
        lock_rrhh=False,
    )


@admin_rrhh_bp.route("/usuarios/<int:user_id>/editar", methods=["GET", "POST"])
@login_required
def usuario_editar(user_id: int):
    if not _require_admin():
        return redirect(url_for("modulos.dashboard"))

    user = get_user_by_id(int(user_id))
    if not user:
        flash("Usuario no encontrado.", "warning")
        return redirect(url_for("admin_rrhh.usuarios_listar"))

    managers = _employees_for_select(include_inactive=False)
    empleados = _employees_for_select(include_inactive=False)
    roles_catalog = _roles_catalog()
    shifts_catalog = list_shift_definitions()
    flex_enabled = flex_tables_exist()

    # Si el usuario ya tiene ADMIN o RRHH, se bloquea desmarcado
    lock_admin = ROLE_ADMIN in (user.get("roles") or [])
    lock_rrhh = ROLE_RRHH in (user.get("roles") or [])

    # Prefill de empleado si existe
    emp = None
    if user.get("employee_id"):
        emp = get_employee(int(user.get("employee_id")))

    current_shift = None
    current_flex = None
    if emp:
        try:
            current_shift = get_current_shift_assignment(int(emp.employee_id))
        except Exception:
            current_shift = None
        try:
            current_flex = get_active_flex_rule(int(emp.employee_id)) if flex_enabled else None
        except Exception:
            current_flex = None

    # Jefe actual
    cur_mgr_id = None
    if emp:
        cur_mgr = fetch_one(
            "SELECT TOP 1 manager_employee_id FROM rrhh.hr_employee_manager "
            "WHERE employee_id=? AND (valid_to IS NULL OR valid_to >= ?) "
            "ORDER BY valid_from DESC",
            (int(emp.employee_id), date.today()),
        )
        cur_mgr_id = cur_mgr.manager_employee_id if cur_mgr else None

    form = {}
    if request.method == "POST":
        form = dict(request.form)
        sam = (request.form.get("ad_username") or user.get("ad_username") or "").strip()
        if not sam:
            flash("Usuario AD es obligatorio.", "warning")
            return redirect(url_for("admin_rrhh.usuario_editar", user_id=user_id))

        employee_mode = (request.form.get("employee_mode") or "NEW").strip().upper()
        employee_id: Optional[int] = None

        is_active = True
        role_codes = [ROLE_EMPLEADO]
        for rc in (ROLE_ADMIN, ROLE_RRHH, ROLE_COORD_INDEM, ROLE_GERENTE_INDEM):
            if request.form.get(f"role_{rc}") == "on":
                role_codes.append(rc)

        # Política: si ya tenía ADMIN/RRHH, quedan fijos
        if lock_admin and ROLE_ADMIN not in role_codes:
            role_codes.append(ROLE_ADMIN)
        if lock_rrhh and ROLE_RRHH not in role_codes:
            role_codes.append(ROLE_RRHH)

        try:
            if employee_mode == "":
                employee_id = None
            elif employee_mode == "NEW":
                doc_number = (request.form.get("doc_number") or "").strip()
                first_name = (request.form.get("first_name") or "").strip()
                last_name = (request.form.get("last_name") or "").strip()
                email = (request.form.get("email") or "").strip() or None
                department = (request.form.get("department") or "").strip() or None
                position_name = (request.form.get("position_name") or "").strip() or None
                shift_group = (request.form.get("shift_group") or "").strip().upper()
                birth_date_raw = (request.form.get("birth_date") or "").strip()
                can_work_from_home = True if request.form.get("can_work_from_home") == "on" else False
                is_exec_approval_by_hr = True if request.form.get("is_exec_approval_by_hr") == "on" else False
                manager_employee_id = request.form.get("manager_employee_id") or None
                manager_employee_id = int(manager_employee_id) if manager_employee_id else None

                if not doc_number:
                    raise ValueError("La cédula (doc_number) es obligatoria.")
                if not first_name or not last_name:
                    raise ValueError("Debes ingresar nombres y apellidos.")
                if not birth_date_raw:
                    raise ValueError("Debes ingresar la fecha de cumpleaños.")
                try:
                    birth_date = datetime.strptime(birth_date_raw, "%Y-%m-%d").date()
                except Exception:
                    raise ValueError("Fecha de cumpleaños inválida.")

                if shift_group not in ("CABINA", "AJUSTADOR", "ADMIN"):
                    raise ValueError("Debes seleccionar el equipo operativo (Cabina o Ajustadores).")

                employee_id = upsert_employee_by_ad(
                    sam=sam,
                    doc_number=doc_number,
                    first_name=first_name,
                    last_name=last_name,
                    email=email,
                    department=department,
                    position_name=position_name,
                    is_exec_approval_by_hr=is_exec_approval_by_hr,
                    can_work_from_home=can_work_from_home,
                    birth_date=birth_date,
                    shift_group=shift_group,
                )
                if manager_employee_id:
                    _set_manager(int(employee_id), int(manager_employee_id))
            else:
                employee_id = int(employee_mode)

            # Horario (turno base) y Cambio de horario (Hora flexible)
            if employee_id:
                sg = (request.form.get("shift_group") or "").strip().upper()
                if not sg:
                    try:
                        er = get_employee(int(employee_id))
                        sg = (getattr(er, "shift_group", "") or "").strip().upper()
                    except Exception:
                        sg = ""

                base_shift_id = (request.form.get("base_shift_id") or "").strip()
                base_from_s = (request.form.get("base_shift_valid_from") or "").strip()
                base_to_s = (request.form.get("base_shift_valid_to") or "").strip()

                if sg in ("CABINA", "AJUSTADOR") and not base_shift_id:
                    raise ValueError("Debes asignar el horario (turno base) para Cabina/Ajustadores.")

                if base_shift_id:
                    try:
                        base_from = datetime.strptime(base_from_s or date.today().strftime("%Y-%m-%d"), "%Y-%m-%d").date()
                    except Exception:
                        raise ValueError("Fecha 'Desde' del horario inválida.")
                    base_to = None
                    if base_to_s:
                        try:
                            base_to = datetime.strptime(base_to_s, "%Y-%m-%d").date()
                        except Exception:
                            raise ValueError("Fecha 'Hasta' del horario inválida.")

                    set_shift_assignment(
                        int(employee_id),
                        int(base_shift_id),
                        base_from,
                        base_to,
                        getattr(current_user, "user_id", None),
                        reason="Asignación/actualización desde edición de usuario",
                    )

                if flex_enabled and request.form.get("flex_apply") == "on":
                    wd_raw = (request.form.get("flex_weekday") or "").strip()
                    slot = (request.form.get("flex_slot") or "").strip()
                    vf_s = (request.form.get("flex_valid_from") or "").strip()
                    if not (wd_raw and slot and vf_s):
                        raise ValueError("Para aplicar cambio de horario debes completar día, AM/PM y fecha.")
                    try:
                        vf = datetime.strptime(vf_s, "%Y-%m-%d").date()
                    except Exception:
                        raise ValueError("Fecha 'Desde' del cambio de horario inválida.")
                    set_active_flex_rule(
                        int(employee_id),
                        int(wd_raw),
                        slot,
                        vf,
                        getattr(current_user, "user_id", None),
                    )

            link_user_to_employee(int(user_id), employee_id)
            set_user_active(int(user_id), is_active)
            set_user_roles(int(user_id), role_codes)

            flash("Usuario actualizado.", "success")
            return redirect(url_for("admin_rrhh.usuario_editar", user_id=int(user_id)))

        except Exception as ex:
            flash(str(ex), "warning")

    # Prefill en GET
    if not form:
        form = {
            "ad_username": user.get("ad_username") or "",
            "is_active": "on" if user.get("is_active") else "",
        }
        for rc in (ROLE_ADMIN, ROLE_RRHH, ROLE_COORD_INDEM, ROLE_GERENTE_INDEM):
            if rc in (user.get("roles") or []):
                form[f"role_{rc}"] = "on"

        if emp:
            form.update(
                {
                    "employee_mode": "NEW",
                    "doc_number": getattr(emp, "doc_number", "") or "",
                    "first_name": getattr(emp, "first_name", "") or "",
                    "last_name": getattr(emp, "last_name", "") or "",
                    "email": getattr(emp, "email", "") or "",
                    "department": getattr(emp, "department", "") or "",
                    "position_name": getattr(emp, "position_name", "") or "",
                    "shift_group": (getattr(emp, "shift_group", "ADMIN") or "ADMIN"),
                    "birth_date": (
                        getattr(emp, "birth_date", None).strftime("%Y-%m-%d")
                        if getattr(emp, "birth_date", None)
                        else ""
                    ),
                    "can_work_from_home": "on" if bool(getattr(emp, "can_work_from_home", 0)) else "",
                    "is_exec_approval_by_hr": "on" if bool(getattr(emp, "is_exec_approval_by_hr", 0)) else "",
                    "manager_employee_id": str(cur_mgr_id) if cur_mgr_id else "",
                }
            )

            if current_shift:
                form.update(
                    {
                        "base_shift_id": str(getattr(current_shift, "shift_id", "") or ""),
                        "base_shift_valid_from": (
                            getattr(current_shift, "valid_from", None).strftime("%Y-%m-%d")
                            if getattr(current_shift, "valid_from", None)
                            else ""
                        ),
                        "base_shift_valid_to": (
                            getattr(current_shift, "valid_to", None).strftime("%Y-%m-%d")
                            if getattr(current_shift, "valid_to", None)
                            else ""
                        ),
                    }
                )

            if current_flex:
                form.update(
                    {
                        "flex_apply": "on",
                        "flex_weekday": str(getattr(current_flex, "weekday", "") or ""),
                        "flex_slot": str(getattr(current_flex, "slot", "") or ""),
                        "flex_valid_from": (
                            getattr(current_flex, "valid_from", None).strftime("%Y-%m-%d")
                            if getattr(current_flex, "valid_from", None)
                            else ""
                        ),
                    }
                )
        else:
            form["employee_mode"] = ""

    return render_template(
        "admin/usuario_form.html",
        mode="edit",
        form=form,
        user=user,
        empleados=empleados,
        managers=managers,
        roles_catalog=roles_catalog,
        shifts_catalog=shifts_catalog,
        flex_enabled=flex_enabled,
        current_shift=current_shift,
        current_flex=current_flex,
        lock_admin=lock_admin,
        lock_rrhh=lock_rrhh,
    )


@admin_rrhh_bp.route("/empleados/nuevo", methods=["GET", "POST"])
@login_required
def empleado_nuevo():
    # UX: unificado
    if not _require_admin():
        return redirect(url_for("modulos.dashboard"))
    return redirect(url_for("admin_rrhh.usuario_nuevo"))


@admin_rrhh_bp.route("/empleados/<int:employee_id>/editar", methods=["GET", "POST"])
@login_required
def empleado_editar(employee_id: int):
    # UX: si el empleado tiene usuario asociado, ir al formulario unificado.
    if not _require_admin():
        return redirect(url_for("modulos.dashboard"))

    u = fetch_one("SELECT TOP 1 user_id FROM rrhh.auth_user WHERE employee_id=?", (int(employee_id),))
    if u:
        return redirect(url_for("admin_rrhh.usuario_editar", user_id=int(u.user_id)))

    flash("Este colaborador no tiene usuario asociado. Crea/edita desde 'Usuarios'.", "warning")
    return redirect(url_for("admin_rrhh.usuarios_listar"))


def _set_manager(employee_id: int, manager_employee_id: int):
    """Registra jefe inmediato (tabla rrhh.hr_employee_manager)."""

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

    today_row = fetch_one(
        "SELECT manager_employee_id, valid_to FROM rrhh.hr_employee_manager WHERE employee_id=? AND valid_from=?",
        (employee_id, today),
    )
    if today_row:
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

    cur = fetch_one(
        "SELECT TOP 1 manager_employee_id, valid_from "
        "FROM rrhh.hr_employee_manager "
        "WHERE employee_id=? AND valid_to IS NULL "
        "ORDER BY valid_from DESC",
        (employee_id,),
    )

    if cur and (cur.manager_employee_id == manager_employee_id):
        return

    if cur:
        execute(
            "UPDATE rrhh.hr_employee_manager SET valid_to=? WHERE employee_id=? AND valid_to IS NULL",
            (today, employee_id),
        )

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
        # Importante: no ocultar el error; si LDAP no está configurado el UI queda "silencioso".
        logger.exception("Búsqueda en directorio falló")
        msg = str(ex)
        # Mensaje más amigable para el usuario
        if not msg or msg == 'None':
            msg = 'LDAP/AD no está configurado o no está disponible.'
        return jsonify({"items": [], "error": msg})


@admin_rrhh_bp.route("/ldap/user")
@login_required
def ldap_user():
    if not _require_admin():
        return jsonify(None)

    sam = (request.args.get("sam") or request.args.get("id") or "").strip()
    item = get_user_by_sam(sam)
    return jsonify(item)
