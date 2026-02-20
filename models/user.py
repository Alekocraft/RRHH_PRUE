"""Modelo de usuario para flask-login.

Objetivo: evitar roturas por cambios de firma/atributos.
Este User acepta tanto un dict (de services.rrhh_user) como kwargs.

Expone alias usados en templates:
- user_id (y get_id())
- ad_username / username
- roles
- puede_trabajo_casa (alias de can_work_from_home)
- es_jefe (alias de is_manager)
- cargo (position_name)

NOTA importante:
Flask-Login (UserMixin) expone `is_active` como propiedad de solo lectura.
Por eso almacenamos el estado en `_is_active` y sobre-escribimos la propiedad.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, Optional

from flask_login import UserMixin


class User(UserMixin):
    def __init__(
        self,
        user_id: Optional[int] = None,
        username: Optional[str] = None,
        ad_username: Optional[str] = None,
        employee_id: Optional[int] = None,
        roles: Optional[Iterable[str]] = None,
        is_active: bool = True,
        cargo: Optional[str] = None,
        puede_trabajo_casa: bool = False,
        es_jefe: bool = False,
        **kwargs,
    ):
        # Soporta construir desde dict: User(user_dict)
        if isinstance(user_id, dict) and username is None and ad_username is None:
            d: Dict[str, Any] = user_id
            user_id = d.get("user_id", d.get("id"))
            ad_username = d.get("ad_username")
            username = d.get("username") or ad_username
            employee_id = d.get("employee_id")
            roles = d.get("roles")
            is_active = bool(d.get("is_active", True))

            cargo = d.get("cargo") or d.get("position_name") or d.get("title")

            puede_trabajo_casa = bool(
                d.get("puede_trabajo_casa", d.get("can_work_from_home", False))
            )
            es_jefe = bool(d.get("es_jefe", d.get("is_manager", False)))

        self.user_id = int(user_id) if user_id is not None else None
        # Flask-Login usa "id" para get_id() (vía UserMixin)
        self.id = str(self.user_id) if self.user_id is not None else ""

        self.ad_username = ad_username or username or ""
        self.username = username or self.ad_username
        self.employee_id = employee_id

        # Guardar estado activo sin asignar a la propiedad `is_active`.
        self._is_active = bool(is_active)

        self.roles = set(roles or [])

        self.cargo = cargo
        self.position_name = cargo  # alias

        self.puede_trabajo_casa = bool(puede_trabajo_casa)
        self.can_work_from_home = bool(puede_trabajo_casa)  # alias

        self.es_jefe = bool(es_jefe)
        self.is_manager = bool(es_jefe)  # alias

        # Mantener cualquier dato adicional sin romper
        for k, v in kwargs.items():
            if not hasattr(self, k):
                setattr(self, k, v)

    @property
    def is_active(self) -> bool:  # type: ignore[override]
        return bool(getattr(self, "_is_active", True))

    def __repr__(self) -> str:
        return f"<User user_id={self.user_id} ad_username={self.ad_username}>"


def load_user(user_id: str):
    """user_loader para flask-login.

    Se mantiene aquí para compatibilidad con 'from models.user import load_user'.
    """

    try:
        uid = int(user_id)
    except Exception:
        return None

    from services.rrhh_user import get_user_by_id

    data = get_user_by_id(uid)
    if not data:
        return None
    return User(data)
