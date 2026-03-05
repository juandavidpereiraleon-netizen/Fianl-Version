@echo off
echo ========================================
echo   Metafiance - Iniciando Aplicacion
echo ========================================
echo.

REM Intentar con diferentes comandos de Python
python app.py 2>nul
if %errorlevel% equ 0 goto end

py app.py 2>nul
if %errorlevel% equ 0 goto end

python3 app.py 2>nul
if %errorlevel% equ 0 goto end

echo ERROR: Python no encontrado
echo.
echo Por favor instala Python desde: https://www.python.org/downloads/
echo O asegurate de que Python este en el PATH del sistema
echo.
pause
goto end

:end

