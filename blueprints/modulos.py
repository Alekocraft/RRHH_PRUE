"""Blueprint principal de módulos.

Este archivo se dejó intencionalmente pequeño.
Los endpoints siguen siendo los mismos; solo se separó el código en archivos
por dominio para facilitar mantenimiento.
"""

from __future__ import annotations

from flask import Blueprint


modulos_bp = Blueprint("modulos", __name__)


# Importar rutas (registra decorators sobre modulos_bp)
# Nota: imports al final para evitar ciclos.
from . import modulos_dashboard as _modulos_dashboard  # noqa: F401,E402
from . import modulos_turnos as _modulos_turnos  # noqa: F401,E402
from . import modulos_asistencia as _modulos_asistencia  # noqa: F401,E402
from . import modulos_trabajo_casa as _modulos_trabajo_casa  # noqa: F401,E402
from . import modulos_hora_flexible as _modulos_hora_flexible  # noqa: F401,E402
from . import modulos_incapacidad as _modulos_incapacidad  # noqa: F401,E402

from . import modulos_chequera as _modulos_chequera  # noqa: F401,E402
