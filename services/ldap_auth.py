import ssl
import logging

from ldap3 import Server, Connection, ALL, SUBTREE, Tls
from ldap3.core.exceptions import LDAPBindError
from ldap3.utils.conv import escape_filter_chars

from config.ldap_config import (
    LDAP_ENABLED,
    LDAP_SERVER,
    LDAP_PORT,
    LDAP_USE_SSL,
    LDAP_DOMAIN,
    LDAP_SEARCH_BASE,
    LDAP_SERVICE_USER,
    LDAP_SERVICE_PASSWORD,
    LDAP_CONNECTION_TIMEOUT,
)

logger = logging.getLogger(__name__)


def _as_upn(user: str) -> str:
    """Convierte 'usuario' -> 'usuario@dominio' si no trae @ o DOMAIN\\."""
    if not user:
        return ""
    user = user.strip()
    if "@" in user or "\\" in user:
        return user
    return f"{user}@{LDAP_DOMAIN}" if LDAP_DOMAIN else user


def _server() -> Server:
    tls = None
    # En redes internas suele bastar CERT_NONE; en producción ideal validar certificado.
    if LDAP_USE_SSL:
        tls = Tls(validate=ssl.CERT_NONE)

    return Server(
        LDAP_SERVER,
        port=LDAP_PORT,
        use_ssl=LDAP_USE_SSL,
        tls=tls,
        get_info=ALL,
        connect_timeout=LDAP_CONNECTION_TIMEOUT,
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
    """Autentica contra AD/LDAP usando:

    1) Bind con cuenta de servicio
    2) Búsqueda del usuario
    3) Validación de password intentando bind del usuario con varios identificadores (DN/UPN)

    Esto evita problemas de formato cuando el usuario escribe "DOMINIO\\usuario".
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

    # Si viene como DOMINIO\usuario, usar solo el sam
    sam_input = username.split("\\")[-1].strip()

    try:
        server = _server()

        # 1) Bind cuenta servicio
        svc_user = _as_upn(LDAP_SERVICE_USER)
        svc = Connection(server, user=svc_user, password=LDAP_SERVICE_PASSWORD, auto_bind=True)

        # 2) Buscar DN del usuario
        u_sam = escape_filter_chars(sam_input)
        u_raw = escape_filter_chars(username)
        upn_guess = escape_filter_chars(_as_upn(sam_input))

        # En AD, con esto suele bastar (samAccountName / userPrincipalName)
        search_filter = (
            f"(|(sAMAccountName={u_sam})"
            f"(userPrincipalName={u_sam})"
            f"(userPrincipalName={u_raw})"
            f"(userPrincipalName={upn_guess}))"
        )

        attrs = ["displayName", "mail", "userPrincipalName", "sAMAccountName"]

        ok = svc.search(
            search_base=LDAP_SEARCH_BASE,
            search_filter=search_filter,
            search_scope=SUBTREE,
            attributes=attrs,
            size_limit=1,
        )

        if not ok or not svc.entries:
            svc.unbind()
            return False, {"error": "USER_NOT_FOUND"}

        entry = svc.entries[0]
        user_dn = entry.entry_dn
        display_name = entry.displayName.value if hasattr(entry, "displayName") else ""
        mail = entry.mail.value if hasattr(entry, "mail") else ""
        upn_value = entry.userPrincipalName.value if hasattr(entry, "userPrincipalName") else ""
        sam_value = entry.sAMAccountName.value if hasattr(entry, "sAMAccountName") else sam_input

        svc.unbind()

        # 3) Validación de password: intenta bind con DN y UPN (algunos AD prefieren UPN)
        candidates = []
        for v in (user_dn, upn_value, _as_upn(sam_value), _as_upn(sam_input)):
            v = (v or "").strip()
            if v and v not in candidates:
                candidates.append(v)

        last_bind_err = None
        for bind_id in candidates:
            try:
                user_conn = Connection(server, user=bind_id, password=password, auto_bind=True)
                user_conn.unbind()

                return True, {
                    "username": sam_value,  # sAMAccountName
                    "display_name": display_name or sam_value,
                    "email": mail or "",
                    "dn": user_dn,
                    "upn": upn_value or "",
                }

            except LDAPBindError as e:
                # No loguear stack completo en credenciales inválidas (ruido en consola)
                last_bind_err = str(e)
                if "invalidCredentials" in last_bind_err:
                    continue
                # Otros errores de bind: reportar
                logger.warning("LDAP bind falló para %s: %s", bind_id, last_bind_err)
                return False, {"error": "LDAP_ERROR", "detail": last_bind_err}

        # Si llegamos aquí: ninguno funcionó -> credenciales inválidas / cuenta bloqueada
        return False, {"error": "INVALID_CREDENTIALS", "detail": last_bind_err or "invalidCredentials"}

    except Exception as e:
        logger.exception("LDAP authenticate falló")
        return False, {"error": "LDAP_ERROR", "detail": str(e)}
