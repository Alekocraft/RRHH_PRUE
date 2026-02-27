import ssl
import logging
import re
from ldap3 import Server, Connection, ALL, SUBTREE, Tls
from ldap3.utils.conv import escape_filter_chars

from config.ldap_config import (
    LDAP_ENABLED, LDAP_SERVER, LDAP_PORT, LDAP_USE_SSL,
    LDAP_DOMAIN, LDAP_SEARCH_BASE,
    LDAP_SERVICE_USER, LDAP_SERVICE_PASSWORD,
    LDAP_CONNECTION_TIMEOUT
)

logger = logging.getLogger(__name__)


# Sub-códigos típicos devueltos por Active Directory en el mensaje de error ("data 52e", etc.)
_AD_DATA_MAP = {
    "525": "Usuario no encontrado",
    "52e": "Usuario o contraseña incorrecta",
    "530": "Logon no permitido en este momento",
    "531": "Logon no permitido desde este equipo",
    "532": "Contraseña expirada",
    "533": "Cuenta deshabilitada",
    "701": "Cuenta expirada",
    "773": "Usuario debe cambiar la contraseña",
    "775": "Cuenta bloqueada",
}


def _as_upn(user: str) -> str:
    """Convierte 'usuario' -> 'usuario@dominio' si no trae @ o DOMAIN\\."""
    if not user:
        return ""
    user = user.strip()
    if "@" in user or "\\" in user:
        return user
    return f"{user}@{LDAP_DOMAIN}" if LDAP_DOMAIN else user


def _sam_only(user: str) -> str:
    """Extrae sAMAccountName desde 'DOMINIO\\user' o 'user@dominio'."""
    if not user:
        return ""
    u = user.strip()
    if "\\" in u:
        u = u.split("\\", 1)[1]
    if "@" in u:
        u = u.split("@", 1)[0]
    return u.strip()


def _extract_ad_data(msg: str) -> str:
    """Extrae el subcódigo AD 'data XXX' desde el mensaje devuelto por el servidor."""
    if not msg:
        return ""
    m = re.search(r"data\s+([0-9a-fA-F]{3,4})", msg)
    return (m.group(1).lower() if m else "")


def _server() -> Server:
    tls = None
    # En entornos internos suele bastar CERT_NONE; en producción ideal validar certificado.
    if LDAP_USE_SSL:
        tls = Tls(validate=ssl.CERT_NONE)

    return Server(
        LDAP_SERVER,
        port=LDAP_PORT,
        use_ssl=LDAP_USE_SSL,
        tls=tls,
        get_info=ALL,
        connect_timeout=LDAP_CONNECTION_TIMEOUT
    )


def test_connection():
    """Prueba bind con la cuenta de servicio (bind user)."""
    if not LDAP_ENABLED:
        return False, "LDAP_DISABLED"
    if not LDAP_SERVICE_USER or not LDAP_SERVICE_PASSWORD:
        return False, "LDAP_SERVICE_ACCOUNT_EMPTY (defina LDAP_SERVICE_USER y LDAP_SERVICE_PASSWORD en .env)"
    try:
        server = _server()
        bind_user = _as_upn(LDAP_SERVICE_USER)
        conn = Connection(server, user=bind_user, password=LDAP_SERVICE_PASSWORD, auto_bind=True)
        conn.unbind()
        return True, "OK"
    except Exception as e:
        logger.exception("LDAP test_connection falló")
        return False, str(e)


def authenticate(username: str, password: str):
    """
    Autentica contra AD/LDAP:
    1) Bind con cuenta de servicio
    2) Busca DN del usuario
    3) Bind con DN + contraseña del usuario
    """
    if not LDAP_ENABLED:
        return False, {"error": "LDAP_DISABLED"}

    username = (username or "").strip()
    password = password or ""

    if not username or not password:
        return False, {"error": "EMPTY_CREDENTIALS"}

    if not LDAP_SEARCH_BASE:
        return False, {"error": "LDAP_SEARCH_BASE_EMPTY"}

    if not LDAP_SERVICE_USER or not LDAP_SERVICE_PASSWORD:
        return False, {"error": "LDAP_SERVICE_ACCOUNT_EMPTY"}

    try:
        server = _server()

        # 1) Bind cuenta servicio
        svc_user = _as_upn(LDAP_SERVICE_USER)
        svc = Connection(server, user=svc_user, password=LDAP_SERVICE_PASSWORD, auto_bind=True)

        # 2) Buscar DN del usuario
        # Normalizamos para evitar que 'DOMINIO\\user' rompa el filtro de sAMAccountName.
        sam_in = _sam_only(username)

        # Escapar para evitar errores por caracteres especiales
        raw_esc = escape_filter_chars(username)
        sam_esc = escape_filter_chars(sam_in)
        upn_esc = escape_filter_chars(_as_upn(sam_in or username))

        # Cubrimos:
        # - login con sAMAccountName (user)
        # - login con UPN (user@dominio)
        # - login con DOMAIN\\user (se busca por sAMAccountName=user y/o UPN)
        search_filter = (
            f"(|"
            f"(sAMAccountName={sam_esc})"
            f"(sAMAccountName={raw_esc})"
            f"(userPrincipalName={raw_esc})"
            f"(userPrincipalName={upn_esc})"
            f")"
        )
        attrs = ["displayName", "mail", "userPrincipalName", "sAMAccountName"]

        ok = svc.search(
            search_base=LDAP_SEARCH_BASE,
            search_filter=search_filter,
            search_scope=SUBTREE,
            attributes=attrs,
            size_limit=1
        )

        if not ok or not svc.entries:
            svc.unbind()
            return False, {"error": "USER_NOT_FOUND"}

        entry = svc.entries[0]
        user_dn = entry.entry_dn

        display_name = entry.displayName.value if hasattr(entry, "displayName") else ""
        mail = entry.mail.value if hasattr(entry, "mail") else ""
        sam = entry.sAMAccountName.value if hasattr(entry, "sAMAccountName") else sam_in or username

        svc.unbind()

        # 3) Bind usuario (validación real de password)
        # IMPORTANTE: auto_bind=True lanza excepción y oculta subcódigos útiles (52e/775/etc.).
        user_conn = Connection(server, user=user_dn, password=password, auto_bind=False)
        if not user_conn.bind():
            result = user_conn.result or {}
            desc = result.get("description", "")
            msg = result.get("message", "") or ""

            ad_data = _extract_ad_data(msg)
            ad_hint = _AD_DATA_MAP.get(ad_data, "")

            user_conn.unbind()
            logger.warning("LDAP bind usuario falló: %s | %s", desc, msg)
            return False, {
                "error": "INVALID_CREDENTIALS",
                "detail": f"{desc} | {msg}",
                "ad_data": ad_data,
                "ad_hint": ad_hint,
            }

        user_conn.unbind()

        return True, {
            "username": sam,  # sAMAccountName
            "display_name": display_name or sam,
            "email": mail or "",
            "dn": user_dn
        }

    except Exception as e:
        logger.exception("LDAP authenticate falló")
        return False, {"error": "LDAP_ERROR", "detail": str(e)}