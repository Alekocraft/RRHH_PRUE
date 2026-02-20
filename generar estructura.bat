@echo off
setlocal enabledelayedexpansion

set "output_file=estructura_proyecto.txt"

echo ESTRUCTURA DEL PROYECTO FLASK > "%output_file%"
echo ============================= >> "%output_file%"
echo Fecha: %date% %time% >> "%output_file%"
echo Directorio actual: %cd% >> "%output_file%"
echo. >> "%output_file%"

echo [ARCHIVOS PRINCIPALES .PY] >> "%output_file%"
echo -------------------------- >> "%output_file%"
for /f "tokens=*" %%f in ('dir /b /s /a-d *.py ^| findstr /v /i "\\.venv\\ \venv\\ \env\\ \virtualenv\\ \\envirt\\"') do (
    set "file=%%f"
    set "file=!file:%cd%\=!"
    echo !file! >> "%output_file%"
)
echo. >> "%output_file%"

echo [ARCHIVOS HTML en /templates/] >> "%output_file%"
echo ------------------------------ >> "%output_file%"
if exist "templates" (
    for /f "tokens=*" %%f in ('dir /b /s /a-d templates\*.html 2^>nul ^| findstr /v /i "\\envirt\\"') do (
        set "file=%%f"
        set "file=!file:%cd%\=!"
        echo !file! >> "%output_file%"
    )
) else (
    echo No existe carpeta templates >> "%output_file%"
)
echo. >> "%output_file%"

echo [ARCHIVOS CSS en /static/] >> "%output_file%"
echo ------------------------- >> "%output_file%"
if exist "static" (
    for /f "tokens=*" %%f in ('dir /b /s /a-d static\*.css 2^>nul ^| findstr /v /i "\\envirt\\"') do (
        set "file=%%f"
        set "file=!file:%cd%\=!"
        echo !file! >> "%output_file%"
    )
) else (
    echo No existe carpeta static >> "%output_file%"
)
echo. >> "%output_file%"

echo [ARCHIVOS JS en /static/] >> "%output_file%"
echo ------------------------- >> "%output_file%"
if exist "static" (
    for /f "tokens=*" %%f in ('dir /b /s /a-d static\*.js 2^>nul ^| findstr /v /i "\\envirt\\"') do (
        set "file=%%f"
        set "file=!file:%cd%\=!"
        echo !file! >> "%output_file%"
    )
)
echo. >> "%output_file%"

echo [ARCHIVOS DE CONFIGURACION] >> "%output_file%"
echo --------------------------- >> "%output_file%"
for %%f in (requirements.txt Dockerfile docker-compose.yml .env .flaskenv config.py config.ini setup.py pyproject.toml) do (
    if exist "%%f" echo %%f >> "%output_file%"
)
echo. >> "%output_file%"

echo [ARCHIVOS DE ENTORNO] >> "%output_file%"
echo -------------------- >> "%output_file%"
for /f "tokens=*" %%f in ('dir /b /a-d .env* .flaskenv* env* environment* .environment* config.env 2^>nul ^| findstr /v /i "\\envirt\\"') do (
    echo %%f >> "%output_file%"
)
echo. >> "%output_file%"

echo [ARCHIVOS DE LOGS] >> "%output_file%"
echo ------------------ >> "%output_file%"
if exist "logs" (
    for /f "tokens=*" %%f in ('dir /b /s /a-d logs\*.log logs\*.txt logs\*.log.* 2^>nul ^| findstr /v /i "\\envirt\\"') do (
        set "file=%%f"
        set "file=!file:%cd%\=!"
        echo !file! >> "%output_file%"
    )
    echo. >> "%output_file%"
    echo [ARCHIVOS LOG EN RAIZ] >> "%output_file%"
    echo ---------------------- >> "%output_file%"
)
for /f "tokens=*" %%f in ('dir /b /a-d *.log log.txt debug.log error.log app.log flask.log *.log.* 2^>nul ^| findstr /v /i "\\envirt\\"') do (
    echo %%f >> "%output_file%"
)
echo. >> "%output_file%"

echo [CARPETAS PRINCIPALES (excluyendo entornos virtuales)] >> "%output_file%"
echo ------------------------------------------------------ >> "%output_file%"
for /f "tokens=*" %%d in ('dir /b /ad ^| findstr /v /i "^\.venv$ ^venv$ ^env$ ^virtualenv$ ^envirt$"') do (
    echo [%%d] >> "%output_file%"
)
echo. >> "%output_file%"

echo Resumen: >> "%output_file%"
echo --------- >> "%output_file%"

REM Contar archivos .py excluyendo carpetas no deseadas
set py_count=0
for /f "tokens=*" %%f in ('dir /b /s /a-d *.py ^| find /c /v ""') do set /a py_count=%%f
for /f "tokens=*" %%f in ('dir /b /s /a-d *.py ^| findstr /i "\\.venv\\ \venv\\ \env\\ \virtualenv\\ \\envirt\\" ^| find /c /v ""') do set /a py_count-=%%f
echo Archivos .py: !py_count! >> "%output_file%"

REM Contar HTML, CSS y JS usando dir con find /c
set html_count=0
set css_count=0
set js_count=0

if exist "templates" (
    for /f "tokens=*" %%f in ('dir /b /s /a-d templates\*.html 2^>nul ^| findstr /v /i "\\envirt\\" ^| find /c /v ""') do set html_count=%%f
)
echo Archivos HTML: !html_count! >> "%output_file%"

if exist "static" (
    for /f "tokens=*" %%f in ('dir /b /s /a-d static\*.css 2^>nul ^| findstr /v /i "\\envirt\\" ^| find /c /v ""') do set css_count=%%f
    for /f "tokens=*" %%f in ('dir /b /s /a-d static\*.js 2^>nul ^| findstr /v /i "\\envirt\\" ^| find /c /v ""') do set js_count=%%f
)
echo Archivos CSS: !css_count! >> "%output_file%"
echo Archivos JS: !js_count! >> "%output_file%"

REM Contar archivos de entorno
set env_count=0
for /f "tokens=*" %%f in ('dir /b /a-d .env* .flaskenv* env* environment* .environment* config.env 2^>nul ^| findstr /v /i "\\envirt\\" ^| find /c /v ""') do set env_count=%%f
echo Archivos de entorno: !env_count! >> "%output_file%"

REM Contar archivos de logs
set log_count=0
if exist "logs" (
    for /f "tokens=*" %%f in ('dir /b /s /a-d logs\*.log logs\*.txt logs\*.log.* 2^>nul ^| findstr /v /i "\\envirt\\" ^| find /c /v ""') do (
        set /a log_count+=%%f
    )
)
for /f "tokens=*" %%f in ('dir /b /a-d *.log log.txt debug.log error.log app.log flask.log *.log.* 2^>nul ^| findstr /v /i "\\envirt\\" ^| find /c /v ""') do (
    set /a log_count+=%%f
)
echo Archivos de logs: !log_count! >> "%output_file%"

REM Contar carpetas excluyendo entornos virtuales y envirt
set folder_count=0
for /f "tokens=*" %%f in ('dir /b /ad ^| findstr /v /i "^\.venv$ ^venv$ ^env$ ^virtualenv$ ^envirt$" ^| find /c /v ""') do set folder_count=%%f
echo Carpetas (sin entornos virtuales): !folder_count! >> "%output_file%"

REM Mostrar información sobre entornos virtuales excluidos
echo. >> "%output_file%"
echo [ENTORNOS VIRTUALES EXCLUIDOS] >> "%output_file%"
echo ------------------------------ >> "%output_file%"
if exist ".venv" echo .venv >> "%output_file%"
if exist "venv" echo venv >> "%output_file%"
if exist "env" echo env >> "%output_file%"
if exist "virtualenv" echo virtualenv >> "%output_file%"
if exist "envirt" echo envirt >> "%output_file%"

echo. >> "%output_file%"
echo Proceso completado. Estructura guardada en %output_file%