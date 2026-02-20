from __future__ import annotations

from typing import Any, Dict, List, Optional

import re

from ldap3 import ALL, SIMPLE, Connection, Server

from config.ldap_config import (
    LDAP_CONNECTION_TIMEOUT,
    LDAP_DOMAIN,
    LDAP_ENABLED,
    LDAP_PORT,
    LDAP_SEARCH_BASE,
    LDAP_SERVER,
    LDAP_SERVICE_PASSWORD,
    LDAP_SERVICE_USER,
    LDAP_USE_SSL,
)


def _bind_candidates(raw_user: str) -> List[str]:
    """Candidatos de usuario para bind SIMPLE.

    AD suele aceptar DN o UPN. En algunos entornos también funciona DOMAIN\\user.
    Probamos varios formatos para máxima compatibilidad.
    """

    raw_user = (raw_user or "").strip()
    if not raw_user:
        return []

    # DN (CN=...,OU=...,DC=...)
    if "=" in raw_user and "," in raw_user:
        return [raw_user]

    # UPN
    if "@" in raw_user:
        return [raw_user]

    out = [raw_user]
    if LDAP_DOMAIN:
        out.append(f"{LDAP_DOMAIN}\\{raw_user}")
        out.append(f"{raw_user}@{LDAP_DOMAIN}")

    # Deduplicar preservando orden
    seen = set()
    uniq: List[str] = []
    for u in out:
        if u and u not in seen:
            seen.add(u)
            uniq.append(u)
    return uniq


def _conn() -> Connection:
    if not LDAP_ENABLED:
        raise RuntimeError("LDAP está deshabilitado")

    server = Server(
        LDAP_SERVER,
        port=LDAP_PORT,
        use_ssl=LDAP_USE_SSL,
        connect_timeout=LDAP_CONNECTION_TIMEOUT,
        get_info=ALL,
    )

    last_err: Optional[Exception] = None
    for bind_user in _bind_candidates(LDAP_SERVICE_USER):
        try:
            return Connection(
                server,
                user=bind_user,
                password=LDAP_SERVICE_PASSWORD,
                authentication=SIMPLE,
                auto_bind=True,
                receive_timeout=LDAP_CONNECTION_TIMEOUT,
            )
        except Exception as ex:
            last_err = ex

    raise RuntimeError(f"No se pudo hacer bind LDAP con el usuario de servicio: {last_err}")


def _pick_first_str(entry: Any, attr_names: List[str]) -> Optional[str]:
    """Obtiene el primer atributo no vacío del entry (ldap3 Entry)."""
    for a in attr_names:
        try:
            if a in entry and getattr(entry, a):
                v = str(getattr(entry, a))
                v = (v or "").strip()
                if v:
                    return v
        except Exception:
            continue
    return None



def _sanitize_doc_number(raw: Optional[str]) -> str:
    """Normaliza la cédula/documento.

    Regla: SOLO aceptamos valores numéricos. Si el atributo trae prefijos como 'CL0600222'
    (códigos internos), se ignora para no llenar mal la cédula.
    """
    raw = (raw or "").strip()
    if not raw:
        return ""
    # Si contiene letras, no lo tomamos como cédula
    if re.search(r"[A-Za-z]", raw):
        return ""
    digits = re.sub(r"\D", "", raw)
    if not digits:
        return ""
    if len(digits) < 5 or len(digits) > 15:
        return ""
    return digits


def _pick_doc_number(entry: Any) -> str:
    for a in [
        "employeeNumber",
        "employeeID",
        "extensionAttribute1",
        "extensionAttribute2",
        "extensionAttribute3",
        "description",
    ]:
        v = _pick_first_str(entry, [a])
        v2 = _sanitize_doc_number(v)
        if v2:
            return v2
    return ""


def _escape_ldap(term: str) -> str:
    # Escape mínimo para filtros LDAP
    return (
        (term or "")
        .replace("\\", "\\5c")
        .replace("*", "\\2a")
        .replace("(", "\\28")
        .replace(")", "\\29")
    )


def _build_search_filter(term: str) -> str:
    """Filtro AD: tokens AND, cada token OR entre campos."""
    tokens = [t for t in (term or "").strip().split() if t]

    # Excluir cuentas deshabilitadas: userAccountControl bit 2
    enabled_filter = "(!(userAccountControl:1.2.840.113556.1.4.803:=2))"

    if not tokens:
        return f"(&(objectCategory=person)(objectClass=user){enabled_filter})"

    per_token = []
    for t in tokens:
        t = _escape_ldap(t)
        per_token.append(
            "(|"
            f"(sAMAccountName=*{t}*)"
            f"(displayName=*{t}*)"
            f"(givenName=*{t}*)"
            f"(sn=*{t}*)"
            f"(mail=*{t}*)"
            ")"
        )

    return f"(&(objectCategory=person)(objectClass=user){enabled_filter}{''.join(per_token)})"


def _map_entry_to_portal_dict(e: Any) -> Dict[str, Any]:
    sam = _pick_first_str(e, ["sAMAccountName"]) or ""
    display_name = _pick_first_str(e, ["displayName"]) or sam
    first_name = _pick_first_str(e, ["givenName"]) or ""
    last_name = _pick_first_str(e, ["sn"]) or ""
    mail = _pick_first_str(e, ["mail"]) or ""
    department = _pick_first_str(e, ["department"]) or ""
    position = _pick_first_str(e, ["title"]) or ""

    # Documento / cédula: depende de cómo lo tengan en AD.
    # Solo se llena si es estrictamente numérico.
    doc_number = _pick_doc_number(e)

    return {
        "ad_username": sam,
        "sam": sam,  # compat
        "display_name": display_name,
        "first_name": first_name,
        "last_name": last_name,
        "mail": mail,
        "email": mail,  # compat
        "doc_number": doc_number,
        "position": position,
        "title": position,  # compat
        "department": department,
    }


def search_users(term: str, limit: int = 15) -> List[Dict[str, Any]]:
    term = (term or "").strip()
    if len(term) < 3:
        return []

    conn = _conn()
    try:
        conn.search(
            search_base=LDAP_SEARCH_BASE,
            search_filter=_build_search_filter(term),
            attributes=[
                "sAMAccountName",
                "displayName",
                "givenName",
                "sn",
                "mail",
                "department",
                "title",
                "employeeID",
                "employeeNumber",
                "extensionAttribute1",
                "extensionAttribute2",
                "extensionAttribute3",
                "description",
            ],
            size_limit=limit,
        )

        return [_map_entry_to_portal_dict(e) for e in conn.entries]
    finally:
        try:
            conn.unbind()
        except Exception:
            pass


def get_user_by_sam(sam: str) -> Optional[Dict[str, Any]]:
    sam = (sam or "").strip()
    if not sam:
        return None

    conn = _conn()
    try:
        sam_esc = _escape_ldap(sam)
        conn.search(
            search_base=LDAP_SEARCH_BASE,
            search_filter=f"(&(objectCategory=person)(objectClass=user)(sAMAccountName={sam_esc}))",
            attributes=[
                "sAMAccountName",
                "displayName",
                "mail",
                "department",
                "title",
                "employeeID",
                "employeeNumber",
                "extensionAttribute1",
                "extensionAttribute2",
                "extensionAttribute3",
                "description",
            ],
            size_limit=1,
        )
        if not conn.entries:
            return None
        return _map_entry_to_portal_dict(conn.entries[0])
    finally:
        try:
            conn.unbind()
        except Exception:
            pass
