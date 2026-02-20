import os

def _bool(v, default=False) -> bool:
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "y", "on")

def _norm_server(raw: str):
    raw = (raw or "").strip()
    use_ssl_from_url = None
    host = raw

    if raw.lower().startswith("ldaps://"):
        use_ssl_from_url = True
        host = raw[8:]
    elif raw.lower().startswith("ldap://"):
        use_ssl_from_url = False
        host = raw[7:]

    return host.strip(), use_ssl_from_url

# --- Flags ---
LDAP_ENABLED = _bool(os.getenv("LDAP_ENABLED"), True)

# --- Server / Connection ---
_raw_server = os.getenv("LDAP_SERVER", "localhost")
LDAP_SERVER, _ssl_from_url = _norm_server(_raw_server)

LDAP_USE_SSL = _bool(os.getenv("LDAP_USE_SSL"), _ssl_from_url if _ssl_from_url is not None else False)
LDAP_PORT = int(os.getenv("LDAP_PORT", "636" if LDAP_USE_SSL else "389"))
LDAP_CONNECTION_TIMEOUT = int(os.getenv("LDAP_CONNECTION_TIMEOUT", "8"))

# --- Domain / Base DN ---
LDAP_DOMAIN = os.getenv("LDAP_DOMAIN", "").strip()

LDAP_SEARCH_BASE = (
    os.getenv("LDAP_SEARCH_BASE", "").strip()
    or os.getenv("LDAP_BASE_DN", "").strip()
    or os.getenv("LDAP_BASE", "").strip()
)

# --- Service account (bind) ---
LDAP_SERVICE_USER = (
    os.getenv("LDAP_SERVICE_USER", "").strip()
    or os.getenv("LDAP_USER", "").strip()
    or os.getenv("LDAP_BIND_USER", "").strip()
)

LDAP_SERVICE_PASSWORD = (
    os.getenv("LDAP_SERVICE_PASSWORD", "").strip()
    or os.getenv("LDAP_PASS", "").strip()
    or os.getenv("LDAP_BIND_PASS", "").strip()
)

# --- Aliases de compatibilidad (para código antiguo) ---
LDAP_BASE_DN = LDAP_SEARCH_BASE
LDAP_USER = LDAP_SERVICE_USER
LDAP_PASS = LDAP_SERVICE_PASSWORD
LDAP_BIND_USER = LDAP_SERVICE_USER
LDAP_BIND_PASS = LDAP_SERVICE_PASSWORD
