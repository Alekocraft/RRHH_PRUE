import os

def _bool(env_value, default=False) -> bool:
    if env_value is None:
        return default
    return (f"{env_value}").strip().lower() in ("1", "true", "yes", "y", "on")

RRHH_DB_DSN = os.getenv("RRHH_DB_DSN", "").strip()  # opcional

RRHH_DB_DRIVER = os.getenv("RRHH_DB_DRIVER", "ODBC Driver 17 for SQL Server")
RRHH_DB_SERVER = os.getenv("RRHH_DB_SERVER", r"localhost\SQLEXPRESS")
RRHH_DB_NAME = os.getenv("RRHH_DB_NAME", "RRHH")

RRHH_DB_USER = os.getenv("RRHH_DB_USER", "").strip()
RRHH_DB_PASSWORD = os.getenv("RRHH_DB_PASSWORD", "").strip()

RRHH_DB_TRUSTED = _bool(os.getenv("RRHH_DB_TRUSTED"), True)
RRHH_DB_ENCRYPT = _bool(os.getenv("RRHH_DB_ENCRYPT"), False)
RRHH_DB_TRUST_CERT = _bool(os.getenv("RRHH_DB_TRUST_CERT"), True)

RRHH_DB_TIMEOUT = int(os.getenv("RRHH_DB_TIMEOUT", "5"))

def _build_conn_str() -> str:
    # Si usan DSN, se prioriza
    if RRHH_DB_DSN:
        return f"DSN={RRHH_DB_DSN};"

    base = (
        f"DRIVER={{{RRHH_DB_DRIVER}}};"
        f"SERVER={RRHH_DB_SERVER};"
        f"DATABASE={RRHH_DB_NAME};"
        f"Connection Timeout={RRHH_DB_TIMEOUT};"
        f"Encrypt={'yes' if RRHH_DB_ENCRYPT else 'no'};"
        f"TrustServerCertificate={'yes' if RRHH_DB_TRUST_CERT else 'no'};"
    )

    # Autenticación Windows (Trusted)
    if RRHH_DB_TRUSTED or (not RRHH_DB_USER):
        return base + "Trusted_Connection=yes;"

    # Usuario/clave SQL
    return base + f"UID={RRHH_DB_USER};PWD={RRHH_DB_PASSWORD};"

# Compatibilidad: services/rrhh_db.py espera esto
DB_CONNECTION_STRING = _build_conn_str()
