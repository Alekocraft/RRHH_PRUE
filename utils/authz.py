from functools import wraps
from flask import abort
from flask_login import current_user


def roles_required(*roles):
    """
    Decorador para validar que el usuario tenga al menos uno de los roles requeridos.
    Uso:
        @roles_required("ADMINISTRADOR", "RRHH")
    """
    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kwargs):

            # Usuario no autenticado
            if not current_user.is_authenticated:
                abort(401)

            # Si es administrador, siempre pasa
            if hasattr(current_user, "es_administrador") and current_user.es_administrador:
                return f(*args, **kwargs)

            # Validar roles
            user_roles = getattr(current_user, "roles", [])

            if not any(r in user_roles for r in roles):
                abort(403)

            return f(*args, **kwargs)

        return wrapper
    return decorator
