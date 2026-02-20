# RRHH - Login LDAP (Flask MVC)

Starter mínimo para RRHH con:
- Login por **LDAP/Active Directory** (ldap3)
- Sesión con **Flask-Login**
- Redirección a **/dashboard** (dashboard.html en blanco)

## 1) Instalación
```bash
python -m venv .venv
# Windows:
.venv\Scripts\activate
# Linux/Mac:
source .venv/bin/activate

pip install -r requirements.txt
```

## 2) Configuración
Copia `.env.example` a `.env` y completa los valores:

```bash
cp .env.example .env
```

> No subas `.env` al repositorio (contiene credenciales).

## 3) Ejecutar
```bash
python app.py
```

Abre:
- http://127.0.0.1:5000/login

## Estructura MVC
- `blueprints/` -> controladores/rutas
- `services/` -> lógica LDAP
- `models/` -> modelos (User para Flask-Login)
- `templates/` -> vistas
- `static/` -> css/js/img
