#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Validador estricto de ajustes de seguridad (Kiuwan / CWE) según PDFs adjuntos.

Ejecución (desde la raíz del proyecto):
  python validate_security_adjustments.py

Códigos de salida:
  - 0  OK (sin hallazgos)
  - 2  FAIL (hay hallazgos)
  - 3  ERROR (no se pudo leer/parsear algo crítico)

Salidas:
  - Consola (resumen + lista corta)
  - TXT (por defecto: security_validation_report.txt)
  - JSON opcional (--json-out)

Opciones útiles:
  --txt-out archivo.txt     Cambia ruta del TXT (default: security_validation_report.txt)
  --json-out reporte.json   Exporta hallazgos en JSON
  --paths src templates     Limita el análisis a rutas concretas
  --context 0|1|2           Muestra líneas de contexto (default: 0)
  --relaxed                Reduce falsos positivos (NO recomendado)
"""

from __future__ import annotations

import argparse
import ast
import json
import re
import sys
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple


# -----------------------------
# Configuración
# -----------------------------
DEFAULT_EXCLUDE_DIRS = {
    ".git", ".hg", ".svn",
    "__pycache__", ".pytest_cache", ".mypy_cache",
    ".venv", "venv", "env",
    "envt2",
    "node_modules", "dist", "build",
    ".idea", ".vscode",
}

TEXT_EXTENSIONS = {".py", ".html", ".htm", ".js", ".jsx", ".ts", ".tsx", ".jinja", ".j2"}

# Archivos a omitir (por nombre)
DEFAULT_EXCLUDE_FILES = {".env", "seguridadtest.py"}


# -----------------------------
# Modelos
# -----------------------------
@dataclass(frozen=True)
class Finding:
    rule: str
    severity: str  # "ERROR" | "WARN"
    path: str
    line: int
    message: str
    snippet: str = ""


# -----------------------------
# Utilidades
# -----------------------------
def eprint(*args: object) -> None:
    print(*args, file=sys.stderr)


def is_binary_file(path: Path) -> bool:
    try:
        with path.open("rb") as f:
            chunk = f.read(2048)
        return b"\0" in chunk
    except Exception:
        return True


def read_text(path: Path) -> Optional[str]:
    """Lee texto en UTF-8 con tolerancia; retorna None si no se puede leer."""
    if is_binary_file(path):
        return None
    try:
        return path.read_text(encoding="utf-8", errors="replace")
    except Exception:
        try:
            return path.read_text(encoding="latin-1", errors="replace")
        except Exception:
            return None


def iter_project_files(root: Path, only_paths: Optional[List[str]] = None) -> Iterable[Path]:
    """Itera por archivos relevantes del proyecto."""
    if only_paths:
        bases = [root / p for p in only_paths]
    else:
        bases = [root]

    seen: set[Path] = set()
    for base in bases:
        if not base.exists():
            continue
        if base.is_file():
            # Omitir archivos excluidos explícitamente (p.ej. .env)
            if base.name in DEFAULT_EXCLUDE_FILES:
                continue
            if base.suffix.lower() in TEXT_EXTENSIONS:
                yield base
            continue

        for p in base.rglob("*"):
            if p in seen:
                continue
            seen.add(p)

            if not p.is_file():
                continue
            if any(part in DEFAULT_EXCLUDE_DIRS for part in p.parts):
                continue
            if p.name in DEFAULT_EXCLUDE_FILES:
                continue
            if p.suffix.lower() not in TEXT_EXTENSIONS:
                continue
            yield p


def line_for_offset(text: str, offset: int) -> int:
    return text.count("\n", 0, max(0, offset)) + 1


def get_line(text: str, lineno: int) -> str:
    lines = text.splitlines()
    if 1 <= lineno <= len(lines):
        return lines[lineno - 1]
    return ""


def find_line_snippet(text: str, lineno: int, context: int = 0) -> str:
    """Devuelve snippet compacto. Con context=0 solo la línea objetivo."""
    lines = text.splitlines()
    if not lines:
        return ""
    start = max(0, lineno - 1 - context)
    end = min(len(lines), lineno + context)
    block = []
    for i in range(start, end):
        prefix = ">" if (i + 1) == lineno else " "
        # truncado suave para que no "rompa" logs
        ln = lines[i]
        if len(ln) > 220:
            ln = ln[:220] + "…"
        block.append(f"{prefix}{i+1:04d}: {ln}")
    return "\n".join(block)


def safe_ast_parse(path: Path, text: str) -> Optional[ast.AST]:
    try:
        return ast.parse(text, filename=str(path))
    except SyntaxError:
        return None


def normalize_path(p: str) -> str:
    # Normaliza separadores para comparar/deduplicar
    return p.replace("\\", "/")


# -----------------------------
# Reglas (según PDFs)
# -----------------------------
# CWE-200: IP hardcodeada
IPV4_RE = re.compile(r"(?<![\w.])(?:\d{1,3}\.){3}\d{1,3}(?![\w.])")

ALLOWED_IPS = {"0.0.0.0", "127.0.0.1", "255.255.255.255"}

# CWE-20: Form validation disabled
NOVALIDATE_RE = re.compile(r"\bnovalidate\b", re.IGNORECASE)

# CWE-1022: target="_blank" sin rel="noopener noreferrer"
TARGET_BLANK_RE = re.compile(r'target\s*=\s*["\']_blank["\']', re.IGNORECASE)

# CWE-117: untrusted input in log (heurística estricta)
LOGGER_METHODS = {"debug", "info", "warning", "error", "critical", "exception"}
SANITIZER_PREFIXES = ("sanitizar_", "sanitize_", "sanitise_")

SENSITIVE_KEYWORDS = {
    "password", "passwd", "contraseña", "contrasena",
    "token", "secret", "apikey", "api_key", "key",
    "authorization", "cookie", "session", "jwt",
    "ldap", "dsn", "connection string", "cadena de conexion",
}

# CWE-209: exposición de info sensible en errores
# Nota: evitamos patrones demasiado genéricos ([ERROR]/[error](...) ) para no duplicar.
CWE209_PATTERNS: List[Tuple[str, re.Pattern]] = [
    ("type(e).__name__", re.compile(r"type\s*\(\s*\w+\s*\)\s*\.\s*__name__", re.IGNORECASE)),
    ("traceback", re.compile(r"\btraceback\b", re.IGNORECASE)),
    ("format_exc", re.compile(r"\bformat_exc\b", re.IGNORECASE)),
    ("str(e)", re.compile(r"\bstr\s*\(\s*\w+\s*\)", re.IGNORECASE)),
    ("repr(e)", re.compile(r"\brepr\s*\(\s*\w+\s*\)", re.IGNORECASE)),
]

# CWE-311: Insecure transport in HTTP servers
APP_RUN_FILE_CANDIDATES = ["app.py", "wsgi.py", "main.py", "run.py"]


# -----------------------------
# Helpers AST
# -----------------------------
def _call_func_name(call: ast.Call) -> str:
    if isinstance(call.func, ast.Name):
        return call.func.id
    if isinstance(call.func, ast.Attribute):
        return call.func.attr
    return ""


def _is_exception_typename(expr: ast.AST) -> bool:
    """
    Detecta expresiones tipo: type(e).__name__ o type(err).__name__
    para tratarlas como "no untrusted input" (evita ruido de CWE-117),
    pero estas igual serán marcadas por CWE-209.
    """
    # type(e).__name__ -> Attribute(attr="__name__", value=Call(type, [Name]))
    if isinstance(expr, ast.Attribute) and expr.attr == "__name__":
        v = expr.value
        if isinstance(v, ast.Call) and _call_func_name(v) == "type" and v.args:
            return True
    return False


def _is_sanitized_expr(expr: ast.AST) -> bool:
    # Aceptamos sanitizadores y casts seguros (int/float/bool)
    if _is_exception_typename(expr):
        return True
    if isinstance(expr, ast.Call):
        fname = _call_func_name(expr)
        if fname.startswith(SANITIZER_PREFIXES):
            return True
        if fname in {"int", "float", "bool"}:
            return True
        if fname in {"uuid4", "UUID"}:
            return True
    if isinstance(expr, ast.Constant):
        return True
    if isinstance(expr, (ast.Num, ast.BinOp, ast.UnaryOp)):
        return True
    return False


def _contains_sensitive_keyword(s: str) -> bool:
    lower = s.lower()
    return any(k in lower for k in SENSITIVE_KEYWORDS)
# -----------------------------
# Heurística de "untrusted input" (para CWE-134 y otros)
# -----------------------------
UNTRUSTED_REQUEST_ATTRS = {"args", "form", "values", "json", "headers", "cookies", "files"}

def _root_name(node: ast.AST) -> str:
    """Devuelve el nombre raíz de una cadena de atributos/subscripts (p.ej. request.form.get -> request)."""
    cur = node
    while True:
        if isinstance(cur, ast.Attribute):
            cur = cur.value
            continue
        if isinstance(cur, ast.Subscript):
            cur = cur.value
            continue
        if isinstance(cur, ast.Call):
            cur = cur.func
            continue
        break
    if isinstance(cur, ast.Name):
        return cur.id
    return ""

def _attr_chain(node: ast.AST) -> List[str]:
    """Extrae una cadena de atributos: request.form.get -> ['request','form','get'] (aprox)."""
    out: List[str] = []
    cur = node
    while True:
        if isinstance(cur, ast.Attribute):
            out.append(cur.attr)
            cur = cur.value
            continue
        if isinstance(cur, ast.Name):
            out.append(cur.id)
            break
        if isinstance(cur, ast.Call):
            cur = cur.func
            continue
        if isinstance(cur, ast.Subscript):
            cur = cur.value
            continue
        break
    return list(reversed(out))

def _is_untrusted_expr(expr: ast.AST) -> bool:
    """
    Heurística: marca como "untrusted" expresiones típicas de entrada externa.
    - request.form.get(...), request.args.get(...), request.values.get(...), request.get_json(), request.json, request.headers.get(...)
    - request.form[...], request.args[...], etc.
    - input(...)
    """
    # input(...)
    if isinstance(expr, ast.Call) and _call_func_name(expr) == "input":
        return True

    # request.get_json()
    if isinstance(expr, ast.Call) and isinstance(expr.func, ast.Attribute):
        chain = _attr_chain(expr.func)
        if chain and chain[0] == "request" and chain[-1] in {"get_json", "get_data"}:
            return True

    # request.<attr>  (p.ej. request.json)
    if isinstance(expr, ast.Attribute):
        chain = _attr_chain(expr)
        if len(chain) >= 2 and chain[0] == "request" and chain[1] in UNTRUSTED_REQUEST_ATTRS:
            return True

    # request.<attr>[...]
    if isinstance(expr, ast.Subscript):
        chain = _attr_chain(expr.value)
        if len(chain) >= 2 and chain[0] == "request" and chain[1] in UNTRUSTED_REQUEST_ATTRS:
            return True

    # request.<attr>.get(...)
    if isinstance(expr, ast.Call) and isinstance(expr.func, ast.Attribute) and expr.func.attr == "get":
        chain = _attr_chain(expr.func.value)
        # request.form.get / request.args.get / request.headers.get ...
        if len(chain) >= 2 and chain[0] == "request" and chain[1] in UNTRUSTED_REQUEST_ATTRS:
            return True

    return False

def _iter_format_values(expr: ast.AST) -> Iterable[ast.AST]:
    """Extrae los valores interpolados en formatos clásicos (% / .format / f-string)."""
    # "..." % (a,b) o "..." % a
    if isinstance(expr, ast.BinOp) and isinstance(expr.op, ast.Mod):
        right = expr.right
        if isinstance(right, (ast.Tuple, ast.List)):
            for elt in right.elts:
                yield elt
        else:
            yield right
        return

    # "...".format(a, b, x=c)
    if isinstance(expr, ast.Call) and isinstance(expr.func, ast.Attribute) and expr.func.attr == "format":
        for a in expr.args:
            yield a
        for kw in expr.keywords:
            if kw.value is not None:
                yield kw.value
        return

    # f"...{x}..."
    if isinstance(expr, ast.JoinedStr):
        for v in expr.values:
            if isinstance(v, ast.FormattedValue):
                yield v.value
        return

    return


# -----------------------------
# Checks
# -----------------------------
def check_hardcoded_ips(root: Path, files: Iterable[Path], relaxed: bool, context: int) -> List[Finding]:
    findings: List[Finding] = []
    for path in files:
        text = read_text(path)
        if text is None:
            continue

        for m in IPV4_RE.finditer(text):
            ip = m.group(0)
            if ip in ALLOWED_IPS:
                continue
            if relaxed:
                lineno = line_for_offset(text, m.start())
                line = get_line(text, lineno)
                if line.lstrip().startswith("#") or "<!--" in line:
                    continue

            lineno = line_for_offset(text, m.start())
            findings.append(Finding(
                rule="CWE-200: Do not write IP address in source code",
                severity="ERROR",
                path=normalize_path(str(path.relative_to(root))),
                line=lineno,
                message=f"IP hardcodeada detectada: {ip}",
                snippet=find_line_snippet(text, lineno, context=context),
            ))

        if path.suffix.lower() == ".py":
            for m in re.finditer(
                r"os\.getenv\(\s*['\"][^'\"]+['\"]\s*,\s*['\"]((?:\d{1,3}\.){3}\d{1,3})['\"]\s*\)",
                text
            ):
                ip = m.group(1)
                if ip in ALLOWED_IPS:
                    continue
                lineno = line_for_offset(text, m.start())
                findings.append(Finding(
                    rule="CWE-200: Do not write IP address in source code",
                    severity="ERROR",
                    path=normalize_path(str(path.relative_to(root))),
                    line=lineno,
                    message=f"os.getenv(...) con IP por defecto ({ip}). En producción, el default no debe ser una IP hardcodeada.",
                    snippet=find_line_snippet(text, lineno, context=context),
                ))

    return findings


def check_logging_untrusted_input(root: Path, files: Iterable[Path], relaxed: bool, context: int) -> List[Finding]:
    """
    CWE-117 + (parte) CWE-532.
    Estricto:
      - Prohíbe f-strings en logger.* si interpolan expresiones no sanitizadas
      - Prohíbe loggear keywords sensibles (password/token/authorization/etc.)
    """
    findings: List[Finding] = []

    for path in files:
        if path.suffix.lower() != ".py":
            continue
        text = read_text(path)
        if text is None:
            continue
        tree = safe_ast_parse(path, text)
        if tree is None:
            findings.append(Finding(
                rule="Parser",
                severity="ERROR",
                path=normalize_path(str(path.relative_to(root))),
                line=1,
                message="No se pudo parsear el archivo Python (SyntaxError).",
                snippet="",
            ))
            continue

        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            if not isinstance(node.func, ast.Attribute):
                continue
            method = node.func.attr
            if method not in LOGGER_METHODS:
                continue

            if not node.args:
                continue
            msg_arg = node.args[0]

            # 1) f-string
            if isinstance(msg_arg, ast.JoinedStr):
                for fv in [x for x in msg_arg.values if isinstance(x, ast.FormattedValue)]:
                    if not _is_sanitized_expr(fv.value):
                        lineno = getattr(node, "lineno", 1)
                        findings.append(Finding(
                            rule="CWE-117: Unvalidated untrusted input in log",
                            severity="ERROR",
                            path=normalize_path(str(path.relative_to(root))),
                            line=lineno,
                            message=("logger.* usa f-string con interpolación NO sanitizada. "
                                     "Usa sanitizar_log_text(...) / sanitizar_username(...) o evita interpolar input."),
                            snippet=find_line_snippet(text, lineno, context=context),
                        ))
                        break

                const_parts = "".join(
                    [x.value for x in msg_arg.values if isinstance(x, ast.Constant) and isinstance(x.value, str)]
                )
                if _contains_sensitive_keyword(const_parts):
                    lineno = getattr(node, "lineno", 1)
                    findings.append(Finding(
                        rule="CWE-532: Avoid exposing sensitive information through log",
                        severity="ERROR",
                        path=normalize_path(str(path.relative_to(root))),
                        line=lineno,
                        message="Mensaje de log contiene keywords sensibles (password/token/authorization/cookie/etc.).",
                        snippet=find_line_snippet(text, lineno, context=context),
                    ))

            # 2) formato logger.info("..%s..", var)
            if isinstance(msg_arg, ast.Constant) and isinstance(msg_arg.value, str):
                msg_text = msg_arg.value
                if _contains_sensitive_keyword(msg_text):
                    lineno = getattr(node, "lineno", 1)
                    findings.append(Finding(
                        rule="CWE-532: Avoid exposing sensitive information through log",
                        severity="ERROR",
                        path=normalize_path(str(path.relative_to(root))),
                        line=lineno,
                        message="Mensaje de log contiene keywords sensibles (password/token/authorization/cookie/etc.).",
                        snippet=find_line_snippet(text, lineno, context=context),
                    ))

                if ("%s" in msg_text or "{}" in msg_text) and len(node.args) > 1:
                    for extra in node.args[1:]:
                        if not _is_sanitized_expr(extra):
                            if relaxed and isinstance(extra, ast.Name) and extra.id.endswith("_id"):
                                continue
                            lineno = getattr(node, "lineno", 1)
                            findings.append(Finding(
                                rule="CWE-117: Unvalidated untrusted input in log",
                                severity="ERROR",
                                path=normalize_path(str(path.relative_to(root))),
                                line=lineno,
                                message=("logger.* pasa argumento no sanitizado a formato %s/{}. "
                                         "Envolvelo con sanitizar_log_text(...) o sanitizador específico."),
                                snippet=find_line_snippet(text, lineno, context=context),
                            ))
                            break

            # 3) sensibles por nombre de variables
            for arg in node.args[1:]:
                if isinstance(arg, ast.Name) and _contains_sensitive_keyword(arg.id):
                    lineno = getattr(node, "lineno", 1)
                    findings.append(Finding(
                        rule="CWE-532: Avoid exposing sensitive information through log",
                        severity="ERROR",
                        path=normalize_path(str(path.relative_to(root))),
                        line=lineno,
                        message=f"Posible dato sensible loggeado por nombre de variable: {arg.id}",
                        snippet=find_line_snippet(text, lineno, context=context),
                    ))

    return findings


def check_sensitive_error_messages(root: Path, files: Iterable[Path], context: int) -> List[Finding]:
    """
    CWE-209: Evitar que errores expongan detalles técnicos.
    Genera UN hallazgo por línea, combinando los patrones detectados para evitar duplicados.
    """
    findings: List[Finding] = []

    for path in files:
        if path.suffix.lower() != ".py":
            continue
        text = read_text(path)
        if text is None:
            continue

        per_line: Dict[int, Set[str]] = {}

        # Escaneo por regex (offset -> línea)
        for label, pat in CWE209_PATTERNS:
            for m in pat.finditer(text):
                lineno = line_for_offset(text, m.start())
                per_line.setdefault(lineno, set()).add(label)

        # Reglas adicionales: si aparece "Exception as e" y en esa misma línea se hace logger.* con type(e).__name__,
        # ya está cubierto por regex. No agregamos más.

        for lineno in sorted(per_line.keys()):
            labels = sorted(per_line[lineno])
            findings.append(Finding(
                rule="CWE-209: Avoid sensitive information exposure through error messages",
                severity="ERROR",
                path=normalize_path(str(path.relative_to(root))),
                line=lineno,
                message="Fuga de detalle técnico detectada en error/log: " + ", ".join(labels),
                snippet=find_line_snippet(text, lineno, context=context),
            ))

    return findings


def check_insecure_transport(root: Path, context: int) -> List[Finding]:
    """
    CWE-311: Insecure transport in HTTP servers
    Revisa entrypoints conocidos buscando app.run(... ssl_context=...)
    """
    findings: List[Finding] = []
    candidates = [root / p for p in APP_RUN_FILE_CANDIDATES]
    target: Optional[Path] = next((p for p in candidates if p.exists() and p.is_file()), None)

    if target is None:
        findings.append(Finding(
            rule="CWE-311: Insecure transport in HTTP servers",
            severity="ERROR",
            path="(no encontrado)",
            line=1,
            message=("No se encontró un entrypoint típico (app.py/wsgi.py/main.py/run.py). "
                     "No se puede validar ssl_context en app.run."),
            snippet="",
        ))
        return findings

    text = read_text(target)
    if text is None:
        findings.append(Finding(
            rule="CWE-311: Insecure transport in HTTP servers",
            severity="ERROR",
            path=normalize_path(str(target.relative_to(root))),
            line=1,
            message="No se pudo leer el archivo de arranque para validar TLS.",
            snippet="",
        ))
        return findings

    tree = safe_ast_parse(target, text)
    if tree is None:
        findings.append(Finding(
            rule="CWE-311: Insecure transport in HTTP servers",
            severity="ERROR",
            path=normalize_path(str(target.relative_to(root))),
            line=1,
            message="No se pudo parsear el entrypoint para validar TLS (SyntaxError).",
            snippet="",
        ))
        return findings

    found_run = False
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if not isinstance(node.func, ast.Attribute):
            continue
        if node.func.attr != "run":
            continue

        found_run = True
        kw = {k.arg: k.value for k in node.keywords if k.arg}

        ssl = kw.get("ssl_context")
        lineno = getattr(node, "lineno", 1)

        if ssl is None:
            findings.append(Finding(
                rule="CWE-311: Insecure transport in HTTP servers",
                severity="ERROR",
                path=normalize_path(str(target.relative_to(root))),
                line=lineno,
                message="app.run(...) sin ssl_context=... (TLS requerido).",
                snippet=find_line_snippet(text, lineno, context=context),
            ))
        elif isinstance(ssl, ast.Constant) and ssl.value is None:
            findings.append(Finding(
                rule="CWE-311: Insecure transport in HTTP servers",
                severity="ERROR",
                path=normalize_path(str(target.relative_to(root))),
                line=lineno,
                message="ssl_context=None en app.run(...). Debe estar configurado para TLS.",
                snippet=find_line_snippet(text, lineno, context=context),
            ))

        dbg = kw.get("debug")
        if isinstance(dbg, ast.Constant) and dbg.value is True:
            findings.append(Finding(
                rule="Hardening: debug=True",
                severity="ERROR",
                path=normalize_path(str(target.relative_to(root))),
                line=lineno,
                message="debug=True en app.run(...). En entornos no locales debe ser False.",
                snippet=find_line_snippet(text, lineno, context=context),
            ))

    if not found_run:
        findings.append(Finding(
            rule="CWE-311: Insecure transport in HTTP servers",
            severity="ERROR",
            path=normalize_path(str(target.relative_to(root))),
            line=1,
            message=("No se encontró ninguna llamada a *.run(...) en el entrypoint. "
                     "No se puede validar ssl_context."),
            snippet="",
        ))

    return findings


def check_form_validation_disabled(root: Path, files: Iterable[Path], context: int) -> List[Finding]:
    findings: List[Finding] = []
    for path in files:
        if path.suffix.lower() not in {".html", ".htm", ".jinja", ".j2"}:
            continue
        text = read_text(path)
        if text is None:
            continue
        for m in NOVALIDATE_RE.finditer(text):
            lineno = line_for_offset(text, m.start())
            findings.append(Finding(
                rule="CWE-20: Form validation disabled (novalidate)",
                severity="ERROR",
                path=normalize_path(str(path.relative_to(root))),
                line=lineno,
                message="Encontrado atributo 'novalidate' (deshabilita validación del navegador).",
                snippet=find_line_snippet(text, lineno, context=context),
            ))
    return findings


def check_target_blank_rel(root: Path, files: Iterable[Path], context: int) -> List[Finding]:
    """CWE-1022: target=_blank sin rel=noopener noreferrer."""
    findings: List[Finding] = []
    rel_needed = re.compile(r"\brel\s*=\s*['\"][^'\"]*['\"]", re.IGNORECASE)
    has_noopener = re.compile(r"\bnoopener\b", re.IGNORECASE)
    has_noreferrer = re.compile(r"\bnoreferrer\b", re.IGNORECASE)

    for path in files:
        if path.suffix.lower() not in {".html", ".htm", ".jinja", ".j2"}:
            continue
        text = read_text(path)
        if text is None:
            continue

        for m in TARGET_BLANK_RE.finditer(text):
            lineno = line_for_offset(text, m.start())
            line = get_line(text, lineno)

            block = line
            if ">" not in block:
                lines = text.splitlines()
                i = lineno - 1
                for _ in range(10):
                    if i + 1 >= len(lines):
                        break
                    i += 1
                    block += "\n" + lines[i]
                    if ">" in lines[i]:
                        break

            rel_match = rel_needed.search(block)
            if not rel_match or (not has_noopener.search(rel_match.group(0)) or not has_noreferrer.search(rel_match.group(0))):
                findings.append(Finding(
                    rule="CWE-1022: External link target=_blank without rel=noopener noreferrer",
                    severity="ERROR",
                    path=normalize_path(str(path.relative_to(root))),
                    line=lineno,
                    message='Encontrado target="_blank" sin rel="noopener noreferrer".',
                    snippet=find_line_snippet(text, lineno, context=context),
                ))

    return findings


def check_execution_after_redirect(root: Path, files: Iterable[Path], context: int) -> List[Finding]:
    """
    CWE-698: Execution After Redirect (EAR)
    Heurística estricta: cualquier redirect(...) que NO esté dentro de "return redirect(...)" falla.
    """
    findings: List[Finding] = []
    for path in files:
        if path.suffix.lower() != ".py":
            continue
        text = read_text(path)
        if text is None:
            continue
        tree = safe_ast_parse(path, text)
        if tree is None:
            continue

        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            func_name = _call_func_name(node)
            if func_name != "redirect":
                continue

            lineno = getattr(node, "lineno", 1)
            line = get_line(text, lineno)
            if "return" in line and "redirect" in line:
                continue

            findings.append(Finding(
                rule="CWE-698: Execution After Redirect (EAR)",
                severity="ERROR",
                path=normalize_path(str(path.relative_to(root))),
                line=lineno,
                message="Llamada redirect(...) sin 'return redirect(...)' (posible EAR).",
                snippet=find_line_snippet(text, lineno, context=context),
            ))

    return findings


def check_dom_xss_patterns(root: Path, files: Iterable[Path], context: int) -> List[Finding]:
    """
    CWE-79: XSS (heurística)
      - innerHTML/outerHTML/insertAdjacentHTML/document.write sin evidencia de escape/sanitize
      - Caso reportado: setAttribute('type', type) exige whitelist explícita (password/text)
    """
    findings: List[Finding] = []
    dangerous_sinks = [
        re.compile(r"\binnerHTML\s*=\s*[^;]+", re.IGNORECASE),
        re.compile(r"\bouterHTML\s*=\s*[^;]+", re.IGNORECASE),
        re.compile(r"\binsertAdjacentHTML\s*\(", re.IGNORECASE),
        re.compile(r"\bdocument\.write\s*\(", re.IGNORECASE),
    ]

    for path in files:
        if path.suffix.lower() not in {".html", ".htm", ".js", ".jsx", ".ts", ".tsx", ".jinja", ".j2"}:
            continue
        text = read_text(path)
        if text is None:
            continue

        # 1) Caso específico setAttribute('type', type)
        for m in re.finditer(r"setAttribute\s*\(\s*['\"]type['\"]\s*,\s*type\s*\)", text, flags=re.IGNORECASE):
            lineno = line_for_offset(text, m.start())
            lines = text.splitlines()
            window = "\n".join(lines[max(0, lineno - 15): min(len(lines), lineno + 15)])
            has_whitelist = (
                (re.search(r"type\s*===\s*['\"]password['\"]", window) and re.search(r"type\s*===\s*['\"]text['\"]", window))
                or re.search(r"\[\s*['\"]password['\"]\s*,\s*['\"]text['\"]\s*\]\s*\.includes\s*\(\s*type\s*\)", window)
            )
            if not has_whitelist:
                findings.append(Finding(
                    rule="CWE-79: XSS (DOM-based) - setAttribute type without whitelist",
                    severity="ERROR",
                    path=normalize_path(str(path.relative_to(root))),
                    line=lineno,
                    message="setAttribute('type', type) sin whitelist (permitir solo 'password' o 'text').",
                    snippet=find_line_snippet(text, lineno, context=context),
                ))

        # 2) sinks genéricos
        for sink in dangerous_sinks:
            for m in sink.finditer(text):
                lineno = line_for_offset(text, m.start())
                line = get_line(text, lineno)
                if re.search(r"\bescape\b|\bsanitize\b|\bsanitizar\b", line, flags=re.IGNORECASE):
                    continue
                findings.append(Finding(
                    rule="CWE-79: XSS (DOM sink usage)",
                    severity="ERROR",
                    path=normalize_path(str(path.relative_to(root))),
                    line=lineno,
                    message=f"Uso de sink DOM potencialmente peligroso ({sink.pattern}).",
                    snippet=find_line_snippet(text, lineno, context=context),
                ))

    return findings



def check_format_string_untrusted_input(root: Path, files: Iterable[Path], relaxed: bool, context: int) -> List[Finding]:
    """
    CWE-134: Exclude unsanitized user input from format strings
    Según reportes Kiuwan: evitar incorporar input no sanitizado en interpolaciones (% / .format / f-strings)
    cuando el valor proviene de fuentes típicas (request.form/args/values/json/headers/cookies/files, input()).

    NOTA:
      - Es una heurística "strict-by-default": si detecta entrada no sanitizada en un formato, marca ERROR.
      - Si --relaxed: permite algunos IDs (_id) en interpolaciones simples.
    """
    findings: List[Finding] = []

    for path in files:
        if path.suffix.lower() != ".py":
            continue
        text = read_text(path)
        if text is None:
            continue
        tree = safe_ast_parse(path, text)
        if tree is None:
            continue

        for node in ast.walk(tree):
            candidate: Optional[ast.AST] = None

            # 1) "..." % (...)
            if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Mod) and isinstance(node.left, ast.Constant) and isinstance(node.left.value, str):
                candidate = node

            # 2) "...".format(...)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "format":
                # value debe ser string literal para reducir FP
                if isinstance(node.func.value, ast.Constant) and isinstance(node.func.value.value, str):
                    candidate = node

            # 3) f-string en retornos / argumentos de llamadas (reduce ruido vs. cualquier f-string suelto)
            if isinstance(node, ast.JoinedStr):
                candidate = node

            if candidate is None:
                continue

            for v in _iter_format_values(candidate):
                if _is_sanitized_expr(v):
                    continue
                if relaxed and isinstance(v, ast.Name) and v.id.endswith("_id"):
                    continue
                if _is_untrusted_expr(v):
                    lineno = getattr(candidate, "lineno", getattr(node, "lineno", 1))
                    findings.append(Finding(
                        rule="CWE-134: Exclude unsanitized user input from format strings",
                        severity="ERROR",
                        path=normalize_path(str(path.relative_to(root))),
                        line=lineno,
                        message=("Interpolación de input no sanitizado en formato (% / .format / f-string). "
                                 "Sanitiza/castea el valor (p.ej. sanitizar_log_text/sanitizar_username/int/escape) "
                                 "o evita construir el mensaje con input directo."),
                        snippet=find_line_snippet(text, lineno, context=context),
                    ))
                    break

    return findings


def check_known_unused_locals(root: Path, files: Iterable[Path], context: int) -> List[Finding]:
    """
    Kiuwan: Avoid unused local variable (pattern-based)
    Basado en patrones concretos observados en reportes.
    """
    findings: List[Finding] = []
    patterns = [
        r"\btooltipList\s*=\s*tooltipTriggerList\.map\(",
        r"\bnewState\s*=\s*isHidden\s*\?\s*['\"]visible['\"]\s*:\s*['\"]oculta['\"]",
        r"\busuario\s*=\s*response\.usuario\b",
        r"\binfo\s*=\s*tabla\.page\.info\(\)",
        r"\bfechaArchivo\s*=\s*new Date\(\)\.toISOString\(\)\.split\(",
    ]
    combined = re.compile("|".join(f"(?:{p})" for p in patterns), re.IGNORECASE)

    for path in files:
        if path.suffix.lower() not in {".html", ".htm", ".js", ".jsx", ".ts", ".tsx", ".jinja", ".j2"}:
            continue
        text = read_text(path)
        if text is None:
            continue
        for m in combined.finditer(text):
            lineno = line_for_offset(text, m.start())
            findings.append(Finding(
                rule="Kiuwan: Avoid unused local variable (pattern-based)",
                severity="ERROR",
                path=normalize_path(str(path.relative_to(root))),
                line=lineno,
                message="Patrón conocido de variable local no usada (según reportes).",
                snippet=find_line_snippet(text, lineno, context=context),
            ))
    return findings


# -----------------------------
# Runner
# -----------------------------
def run_all(root: Path, only_paths: Optional[List[str]], relaxed: bool, context: int) -> List[Finding]:
    files = list(iter_project_files(root, only_paths=only_paths))

    findings: List[Finding] = []
    findings += check_hardcoded_ips(root, files, relaxed=relaxed, context=context)
    findings += check_insecure_transport(root, context=context)
    findings += check_form_validation_disabled(root, files, context=context)
    findings += check_target_blank_rel(root, files, context=context)
    findings += check_execution_after_redirect(root, files, context=context)
    findings += check_dom_xss_patterns(root, files, context=context)
    findings += check_format_string_untrusted_input(root, files, relaxed=relaxed, context=context)
    findings += check_logging_untrusted_input(root, files, relaxed=relaxed, context=context)
    findings += check_sensitive_error_messages(root, files, context=context)
    findings += check_known_unused_locals(root, files, context=context)

    # Deduplicación suave (mismo rule+path+line+message)
    uniq: Dict[Tuple[str, str, int, str], Finding] = {}
    for f in findings:
        key = (f.rule, f.path, f.line, f.message)
        if key not in uniq:
            uniq[key] = f

    # Orden consistente: por severidad, rule, path, line
    ordered = sorted(
        uniq.values(),
        key=lambda x: (0 if x.severity == "ERROR" else 1, x.rule, x.path, x.line, x.message)
    )
    return ordered


def summarize(findings: List[Finding]) -> Tuple[Dict[str, int], Dict[str, int]]:
    by_rule: Dict[str, int] = {}
    by_file: Dict[str, int] = {}
    for f in findings:
        by_rule[f.rule] = by_rule.get(f.rule, 0) + 1
        by_file[f.path] = by_file.get(f.path, 0) + 1
    return by_rule, by_file


def render_txt_report(root: Path, findings: List[Finding]) -> str:
    by_rule, by_file = summarize(findings)

    lines: List[str] = []
    lines.append("SECURITY VALIDATION REPORT")
    lines.append("=" * 80)
    lines.append(f"Root: {root}")
    lines.append(f"Total findings: {len(findings)}")
    lines.append("")

    if findings:
        lines.append("Resumen por regla:")
        for rule, cnt in sorted(by_rule.items(), key=lambda x: (-x[1], x[0])):
            lines.append(f"  - {cnt:>3}  {rule}")
        lines.append("")

        lines.append("Resumen por archivo (top 20):")
        for path, cnt in sorted(by_file.items(), key=lambda x: (-x[1], x[0]))[:20]:
            lines.append(f"  - {cnt:>3}  {path}")
        lines.append("")

        lines.append("Detalle:")
        lines.append("-" * 80)
        for f in findings:
            lines.append(f"[{f.severity}] {f.rule}")
            lines.append(f"  {f.path}:{f.line}")
            lines.append(f"  {f.message}")
            if f.snippet:
                # indent para que sea fácil de leer
                for ln in f.snippet.splitlines():
                    lines.append("  " + ln)
            lines.append("")
    else:
        lines.append("OK: No se encontraron hallazgos.")
        lines.append("")

    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Validador estricto de ajustes de vulnerabilidades (según PDFs Kiuwan).")
    parser.add_argument("--json-out", default=None, help="Ruta para exportar el reporte JSON.")
    parser.add_argument("--txt-out", default="security_validation_report.txt", help="Ruta para exportar el reporte TXT.")
    parser.add_argument("--paths", nargs="*", default=None, help="Rutas a analizar (por defecto: todo el repo).")
    parser.add_argument("--relaxed", action="store_true", help="Reduce falsos positivos (NO recomendado).")
    parser.add_argument("--context", type=int, default=0, choices=[0, 1, 2], help="Líneas de contexto a imprimir.")
    args = parser.parse_args()

    root = Path.cwd()
    findings = run_all(root=root, only_paths=args.paths, relaxed=args.relaxed, context=args.context)

    # Consola: resumen corto + lista en 1-2 líneas por hallazgo (sin spam)
    if findings:
        by_rule, _ = summarize(findings)
        eprint(f"[FAIL] {len(findings)} hallazgos.\n")
        eprint("Resumen por regla:")
        for rule, cnt in sorted(by_rule.items(), key=lambda x: (-x[1], x[0])):
            eprint(f"  - {cnt:>3}  {rule}")
        eprint("\nPrimeros 30 hallazgos:")
        for f in findings[:30]:
            eprint(f"  - [{f.severity}] {f.rule} | {f.path}:{f.line} | {f.message}")
        if len(findings) > 30:
            eprint(f"  ... ({len(findings) - 30} más; ver TXT para el detalle completo)")
        eprint("")
    else:
        print("[OK] No se encontraron hallazgos según las reglas configuradas.")

    # TXT siempre (más fácil para CI / adjuntar)
    try:
        txt_path = Path(args.txt_out)
        txt_path.write_text(render_txt_report(root, findings), encoding="utf-8")
        print(f"[INFO] Reporte TXT: {txt_path}")
    except Exception as ex:
        eprint(f"[ERROR] No se pudo escribir el TXT: {ex}")
        return 3

    # JSON opcional
    if args.json_out:
        try:
            out_path = Path(args.json_out)
            payload = {"root": str(root), "findings": [asdict(x) for x in findings]}
            out_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
            print(f"[INFO] Reporte JSON: {out_path}")
        except Exception as ex:
            eprint(f"[ERROR] No se pudo escribir el JSON: {ex}")
            return 3

    return 2 if findings else 0


if __name__ == "__main__":
    raise SystemExit(main())