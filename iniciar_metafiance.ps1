Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  METAFIANCE - Iniciando Aplicacion" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Verificar Python
$pythonVersion = python --version 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] Python no encontrado" -ForegroundColor Red
    exit 1
}
Write-Host "[OK] $pythonVersion" -ForegroundColor Green

# Verificar dependencias
Write-Host ""
Write-Host "Verificando dependencias..." -ForegroundColor Yellow
try {
    python -c "import flask" 2>&1 | Out-Null
    Write-Host "[OK] Flask instalado" -ForegroundColor Green
} catch {
    Write-Host "[INFO] Instalando dependencias..." -ForegroundColor Yellow
    python -m pip install -r requirements.txt --quiet
}

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  Iniciando servidor Flask..." -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "La aplicacion estara disponible en:" -ForegroundColor Green
Write-Host "  http://localhost:5000" -ForegroundColor Cyan
Write-Host ""
Write-Host "Credenciales de administrador:" -ForegroundColor Yellow
Write-Host "  Email:    admin@metafiance.com" -ForegroundColor White
Write-Host "  Password: admin123" -ForegroundColor White
Write-Host ""
Write-Host "Presiona Ctrl+C para detener la aplicacion" -ForegroundColor Yellow
Write-Host ""

# Abrir navegador después de 2 segundos
Start-Job -ScriptBlock {
    Start-Sleep -Seconds 2
    Start-Process "http://localhost:5000"
} | Out-Null

# Ejecutar la aplicación
python app.py

