from ldap3 import Server, Connection, ALL
from ldap3.utils.conv import escape_filter_chars
from flask import current_app


def _as_upn(user: str) -> str:
    if not user:
        return ""
    user = user.strip()
    if "@" in user or "\\" in user:
        return user
    return f"{user}@{current_app.config['LDAP_DOMAIN']}"


def _get_connection():
    server = Server(
        current_app.config["LDAP_SERVER"],
        port=current_app.config["LDAP_PORT"],
        use_ssl=current_app.config["LDAP_USE_SSL"],
        get_info=ALL
    )

    service_user = _as_upn(current_app.config["LDAP_SERVICE_USER"])

    return Connection(
        server,
        user=service_user,
        password=current_app.config["LDAP_SERVICE_PASSWORD"],
        auto_bind=True
    )


def buscar_usuario_ldap(sam):

    if not current_app.config.get("LDAP_ENABLED"):
        return None

    try:
        conn = _get_connection()

        conn.search(
            search_base=current_app.config["LDAP_SEARCH_BASE"],
            search_filter=f"(sAMAccountName={escape_filter_chars(sam)})",
            attributes=[
                "displayName",
                "mail",
                "givenName",
                "sn",
                "department",
                "title"
            ]
        )

        if not conn.entries:
            conn.unbind()
            return None

        entry = conn.entries[0]

        data = {
            "display_name": (f"{entry.displayName}" if entry.displayName else ""),
            "email": (f"{entry.mail}" if entry.mail else ""),
            "first_name": (f"{entry.givenName}" if entry.givenName else ""),
            "last_name": (f"{entry.sn}" if entry.sn else ""),
            "department": (f"{entry.department}" if entry.department else ""),
            "title": (f"{entry.title}" if entry.title else ""),
        }

        conn.unbind()
        return data

    except Exception:
        return None


def buscar_usuarios_ldap_parcial(texto):

    if not texto:
        return []

    try:
        conn = _get_connection()

        search_filter = (
            f"(&(objectClass=user)"
            f"(|(sAMAccountName=*{escape_filter_chars(texto)}*)"
            f"(displayName=*{escape_filter_chars(texto)}*)"
            f"(mail=*{escape_filter_chars(texto)}*)))"
        )

        conn.search(
            search_base=current_app.config["LDAP_SEARCH_BASE"],
            search_filter=search_filter,
            attributes=[
                "sAMAccountName",
                "givenName",
                "sn",
                "mail",
                "department",
                "title"
            ],
            size_limit=10
        )

        results = []

        for entry in conn.entries:
            data = entry.entry_attributes_as_dict
            results.append({
                "sam": data.get("sAMAccountName", [""])[0],
                "first_name": data.get("givenName", [""])[0],
                "last_name": data.get("sn", [""])[0],
                "email": data.get("mail", [""])[0],
                "department": data.get("department", [""])[0],
                "title": data.get("title", [""])[0],
            })

        conn.unbind()
        return results

    except Exception:
        return []
