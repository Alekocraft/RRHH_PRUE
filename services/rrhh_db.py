import os
import pyodbc


def get_conn():
    """Obtiene conexión a SQL Server usando variables de entorno RRHH_DB_*.
    Soporta autenticación integrada (Trusted_Connection) o usuario/contraseña.
    """
    driver = os.getenv("RRHH_DB_DRIVER", "ODBC Driver 17 for SQL Server")
    server = os.getenv("RRHH_DB_SERVER", r"localhost\SQLEXPRESS")
    dbname = os.getenv("RRHH_DB_NAME", "RRHH")

    trusted = os.getenv("RRHH_DB_TRUSTED", "true").lower() in ("1", "true", "yes", "y")
    encrypt = os.getenv("RRHH_DB_ENCRYPT", "false").lower() in ("1", "true", "yes", "y")
    trust_cert = os.getenv("RRHH_DB_TRUST_CERT", "true").lower() in ("1", "true", "yes", "y")

    uid = os.getenv("RRHH_DB_USER", "").strip()
    pwd = os.getenv("RRHH_DB_PASSWORD", "").strip()
    timeout = int(os.getenv("RRHH_DB_TIMEOUT", "5"))

    cs = f"DRIVER={{{driver}}};SERVER={server};DATABASE={dbname};"
    if trusted and not uid:
        cs += "Trusted_Connection=yes;"
    else:
        cs += f"UID={uid};PWD={pwd};"

    if encrypt:
        cs += "Encrypt=yes;"
    if trust_cert:
        cs += "TrustServerCertificate=yes;"

    # autocommit False: commit explícito en execute/call_proc
    return pyodbc.connect(cs, timeout=timeout, autocommit=False)


def fetch_one(sql: str, params=()):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, params)
        return cur.fetchone()


def fetch_all(sql: str, params=()):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, params)
        return cur.fetchall()


def execute(sql: str, params=()):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, params)
        conn.commit()
        return cur.rowcount


def execute_scalar(sql: str, params=()):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, params)
        row = cur.fetchone()
        conn.commit()
        return row[0] if row else None


def call_proc(proc_sql: str, params=()):
    """Ejecuta un stored procedure.

    Soporta 2 estilos:
      1) Explícito (recomendado):
         call_proc('EXEC rrhh.sp_apply_attendance_batch ?, ?', (batch_id, actor_user_id))
      2) Solo nombre del SP:
         call_proc('rrhh.sp_apply_attendance_batch', (batch_id, actor_user_id))
         -> se convierte internamente a: EXEC rrhh.sp_apply_attendance_batch ?, ?
    """

    sql = (proc_sql or "").strip()

    # Normaliza params a tuple (pyodbc requiere secuencia)
    if params is None:
        params_t = tuple()
    elif isinstance(params, tuple):
        params_t = params
    elif isinstance(params, list):
        params_t = tuple(params)
    else:
        try:
            params_t = tuple(params)
        except Exception:
            params_t = (params,)

    # Si llega solo el nombre del SP (sin EXEC/CALL y sin marcadores), construye EXEC con marcadores
    low = sql.lower()
    if sql and ("?" not in sql) and (not low.startswith("exec")) and (not low.startswith("{call")):
        if " " not in sql:  # típico: rrhh.sp_xxx
            if len(params_t) > 0:
                marks = ", ".join(["?"] * len(params_t))
                sql = f"EXEC {sql} {marks}"
            else:
                sql = f"EXEC {sql}"

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, params_t)
        conn.commit()
