Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  Metafiance - Iniciando Aplicacion" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Intentar encontrar Python
$pythonCommands = @("python", "py", "python3")

foreach ($cmd in $pythonCommands) {
    try {
        $pythonPath = Get-Command $cmd -ErrorAction Stop
        Write-Host "Python encontrado: $($pythonPath.Source)" -ForegroundColor Green
        Write-Host "Instalando dependencias..." -ForegroundColor Yellow
        
        & $cmd -m pip install -r requirements.txt
        
        Write-Host "Iniciando aplicacion..." -ForegroundColor Yellow
        Write-Host "La aplicacion estara disponible en: http://localhost:5000" -ForegroundColor Green
        Write-Host "Presiona Ctrl+C para detener la aplicacion" -ForegroundColor Yellow
        Write-Host ""
        
        & $cmd app.py
        break
    } catch {
        continue
    }
}

if (-not $pythonPath) {
    Write-Host "ERROR: Python no encontrado" -ForegroundColor Red
    Write-Host ""
    Write-Host "Por favor instala Python desde: https://www.python.org/downloads/" -ForegroundColor Yellow
    Write-Host "O asegurate de que Python este en el PATH del sistema" -ForegroundColor Yellow
    Write-Host ""
    Read-Host "Presiona Enter para salir"
}

