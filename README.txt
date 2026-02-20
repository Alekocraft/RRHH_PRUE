RRHH - Paquete Roles en Español + Panel Admin Usuarios

Qué incluye:
- Login LDAP con provisioning en BD RRHH (rrhh.auth_user + rol EMPLEADO por defecto)
- Roles en español: EMPLEADO / RRHH / ADMINISTRADOR
- Dashboard con acceso a /admin/usuarios (solo RRHH/ADMIN)
- Pantalla /perfil-pendiente para usuarios sin employee_id

Variables .env mínimas:
RRHH_DB_SERVER=localhost\SQLEXPRESS
RRHH_DB_NAME=RRHH
RRHH_DB_TRUSTED=true
RRHH_DB_DRIVER=ODBC Driver 17 for SQL Server

Asegúrate de tener creados los roles en rrhh.auth_role (role_code en español):
EMPLEADO, RRHH, ADMINISTRADOR

Ruta admin:
http://localhost:5011/admin/usuarios


[Update v2] Se agregaron rutas y pantallas placeholder para: /asistencia, /turnos, /hora-flexible.
