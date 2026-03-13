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


def _entry_attr_value(entry: Any, attr_name: str) -> Optional[Any]:
    """Obtiene valor 'real' de un atributo ldap3 Entry.

    ldap3 expone EntryAttribute con .value o .values. En algunos casos str(attr)
    devuelve un objeto/representación; aquí normalizamos para extraer el dato.
    """
    try:
        if attr_name not in entry:
            return None
        attr = getattr(entry, attr_name, None)
        if attr is None:
            # Fallback: entry['attr']
            try:
                attr = entry[attr_name]
            except Exception:
                return None
        # Preferir .value
        if hasattr(attr, "value"):
            return attr.value
        if hasattr(attr, "values"):
            vals = getattr(attr, "values")
            if isinstance(vals, list) and vals:
                return vals[0]
            return vals
        return attr
    except Exception:
        return None


def _pick_first_str(entry: Any, attr_names: List[str]) -> Optional[str]:
    """Obtiene el primer atributo no vacío del entry."""
    for a in attr_names:
        v = _entry_attr_value(entry, a)
        if v is None:
            continue
        # Listas (multi-valued)
        if isinstance(v, (list, tuple)):
            v = v[0] if v else None
        if v is None:
            continue
        s = str(v).strip()
        if s:
            return s
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
    # AD suele tener cédula en employeeNumber/ID o en extensionAttributeX.
    # Lo intentamos de manera amplia.
    candidates = [
        "employeeNumber",
        "employeeID",
        "employeeId",
        "extensionAttribute1",
        "extensionAttribute2",
        "extensionAttribute3",
        "extensionAttribute4",
        "extensionAttribute5",
        "description",
    ]
    for a in candidates:
        v = _pick_first_str(entry, [a])
        v2 = _sanitize_doc_number(v)
        if v2:
            return v2
    return ""


def _pick_email(entry: Any) -> str:
    # Preferimos mail. Si no, userPrincipalName. Si no, proxyAddresses (SMTP:).
    mail = _pick_first_str(entry, ["mail"]) or ""
    if mail:
        return mail

    upn = _pick_first_str(entry, ["userPrincipalName"]) or ""
    if upn and "@" in upn:
        return upn

    proxies = _entry_attr_value(entry, "proxyAddresses")
    if isinstance(proxies, (list, tuple)):
        # buscamos SMTP: (principal)
        for p in proxies:
            ps = str(p)
            if ps.startswith("SMTP:"):
                return ps.replace("SMTP:", "").strip()
        for p in proxies:
            ps = str(p)
            if ps.lower().startswith("smtp:"):
                return ps.split(":", 1)[1].strip()
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
            f"(userPrincipalName=*{t}*)"
            ")"
        )

    return f"(&(objectCategory=person)(objectClass=user){enabled_filter}{''.join(per_token)})"


def _split_display_name(display_name: str) -> Dict[str, str]:
    dn = (display_name or "").strip()
    if not dn:
        return {"first": "", "last": ""}
    # heurística simple: dos o más tokens, último token(s) apellidos.
    parts = [p for p in dn.split() if p]
    if len(parts) == 1:
        return {"first": parts[0], "last": ""}
    if len(parts) == 2:
        return {"first": parts[0], "last": parts[1]}
    return {"first": " ".join(parts[:-2]), "last": " ".join(parts[-2:])}


def _map_entry_to_portal_dict(e: Any) -> Dict[str, Any]:
    sam = _pick_first_str(e, ["sAMAccountName"]) or ""
    display_name = _pick_first_str(e, ["displayName", "cn", "name"]) or sam

    first_name = _pick_first_str(e, ["givenName"]) or ""
    last_name = _pick_first_str(e, ["sn"]) or ""
    if not first_name and not last_name and display_name:
        sp = _split_display_name(display_name)
        first_name = sp["first"]
        last_name = sp["last"]

    mail = _pick_email(e)

    # Área / departamento: según AD puede estar en department, division, company o physicalDeliveryOfficeName.
    department = _pick_first_str(
        e,
        [
            "department",
            "division",
            "company",
            "physicalDeliveryOfficeName",
            "extensionAttribute10",
            "extensionAttribute11",
            "extensionAttribute12",
        ],
    ) or ""

    # Cargo: title suele ser el estándar, pero a veces está en description.
    position = _pick_first_str(
        e,
        [
            "title",
            "description",
            "extensionAttribute13",
            "extensionAttribute14",
        ],
    ) or ""

    # Documento / cédula: depende de cómo lo tengan en AD. Solo se llena si es estrictamente numérico.
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


# Atributos solicitados a AD. Los listamos explícitamente para performance.
_ATTRS = [
    "sAMAccountName",
    "displayName",
    "givenName",
    "sn",
    "mail",
    "userPrincipalName",
    "proxyAddresses",
    "department",
    "division",
    "company",
    "physicalDeliveryOfficeName",
    "title",
    "employeeID",
    "employeeNumber",
    "extensionAttribute1",
    "extensionAttribute2",
    "extensionAttribute3",
    "extensionAttribute4",
    "extensionAttribute5",
    "extensionAttribute10",
    "extensionAttribute11",
    "extensionAttribute12",
    "extensionAttribute13",
    "extensionAttribute14",
    "description",
]


def search_users(term: str, limit: int = 15) -> List[Dict[str, Any]]:
    term = (term or "").strip()
    if len(term) < 3:
        return []

    conn = _conn()
    try:
        conn.search(
            search_base=LDAP_SEARCH_BASE,
            search_filter=_build_search_filter(term),
            attributes=_ATTRS,
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
            attributes=_ATTRS,
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
