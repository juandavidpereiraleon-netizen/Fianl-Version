@echo off
echo ========================================
echo   Metafiance - Instalacion y Ejecucion
echo ========================================
echo.

REM Verificar si Python esta instalado
python --version >nul 2>&1
if %errorlevel% equ 0 (
    echo [OK] Python encontrado
    python --version
    echo.
    goto instalar_dependencias
)

py --version >nul 2>&1
if %errorlevel% equ 0 (
    echo [OK] Python encontrado (usando py)
    py --version
    echo.
    set PYTHON_CMD=py
    goto instalar_dependencias
)

echo [ERROR] Python no encontrado
echo.
echo Por favor instala Python desde: https://www.python.org/downloads/
echo.
echo IMPORTANTE: Durante la instalacion, marca la opcion "Add Python to PATH"
echo.
echo Despues de instalar:
echo 1. Cierra esta ventana
echo 2. Abre una nueva terminal
echo 3. Ejecuta este script de nuevo
echo.
pause
exit /b 1

:instalar_dependencias
echo Instalando dependencias...
echo.

if defined PYTHON_CMD (
    %PYTHON_CMD% -m pip install -r requirements.txt
) else (
    python -m pip install -r requirements.txt
)

if %errorlevel% neq 0 (
    echo.
    echo [ERROR] No se pudieron instalar las dependencias
    echo.
    pause
    exit /b 1
)

echo.
echo [OK] Dependencias instaladas correctamente
echo.
echo ========================================
echo   Iniciando aplicacion...
echo ========================================
echo.
echo La aplicacion estara disponible en: http://localhost:5000
echo Presiona Ctrl+C para detener la aplicacion
echo.

if defined PYTHON_CMD (
    %PYTHON_CMD% app.py
) else (
    python app.py
)

pause

