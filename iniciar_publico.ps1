Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  METAFIANCE - Modo Publico" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Verificar Python
$pythonVersion = python --version 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] Python no encontrado" -ForegroundColor Red
    exit 1
}
Write-Host "[OK] $pythonVersion" -ForegroundColor Green

# Verificar ngrok
$ngrokInstalled = Get-Command ngrok -ErrorAction SilentlyContinue
if (-not $ngrokInstalled) {
    Write-Host ""
    Write-Host "[INFO] ngrok no encontrado. Instalando..." -ForegroundColor Yellow
    try {
        winget install ngrok --silent --accept-package-agreements --accept-source-agreements
        Write-Host "[OK] ngrok instalado" -ForegroundColor Green
        Start-Sleep -Seconds 2
    } catch {
        Write-Host "[ADVERTENCIA] No se pudo instalar ngrok automáticamente" -ForegroundColor Yellow
        Write-Host "Instala manualmente desde: https://ngrok.com/download" -ForegroundColor Yellow
        Write-Host ""
        Write-Host "Iniciando aplicación solo en red local..." -ForegroundColor Yellow
        Write-Host "Accesible en: http://localhost:5000" -ForegroundColor Cyan
        Write-Host "Y en tu red local: http://[TU_IP]:5000" -ForegroundColor Cyan
        Write-Host ""
        python app.py
        exit
    }
}

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  Iniciando aplicacion..." -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Iniciar Flask en segundo plano
$flaskJob = Start-Job -ScriptBlock {
    Set-Location $using:PWD
    python app.py
}

# Esperar a que Flask inicie
Start-Sleep -Seconds 3

# Iniciar ngrok
Write-Host "Iniciando ngrok..." -ForegroundColor Yellow
Write-Host ""

$ngrokProcess = Start-Process -FilePath "ngrok" -ArgumentList "http", "5000" -NoNewWindow -PassThru

Start-Sleep -Seconds 2

# Obtener URL de ngrok
try {
    $ngrokApi = Invoke-RestMethod -Uri "http://localhost:4040/api/tunnels" -ErrorAction SilentlyContinue
    if ($ngrokApi.tunnels) {
        $publicUrl = $ngrokApi.tunnels[0].public_url
        Write-Host "========================================" -ForegroundColor Green
        Write-Host "  APLICACION PUBLICA DISPONIBLE" -ForegroundColor Green
        Write-Host "========================================" -ForegroundColor Green
        Write-Host ""
        Write-Host "URL Publica:" -ForegroundColor Yellow
        Write-Host "  $publicUrl" -ForegroundColor Cyan
        Write-Host ""
        Write-Host "URL Local:" -ForegroundColor Yellow
        Write-Host "  http://localhost:5000" -ForegroundColor Cyan
        Write-Host ""
        Write-Host "Rutas publicas disponibles:" -ForegroundColor Yellow
        Write-Host "  $publicUrl/" -ForegroundColor White
        Write-Host "  $publicUrl/publico" -ForegroundColor White
        Write-Host "  $publicUrl/api/public/info" -ForegroundColor White
        Write-Host ""
        Write-Host "Credenciales de administrador:" -ForegroundColor Yellow
        Write-Host "  Email: admin@metafiance.com" -ForegroundColor White
        Write-Host "  Password: admin123" -ForegroundColor White
        Write-Host ""
        Write-Host "Presiona Ctrl+C para detener" -ForegroundColor Yellow
        Write-Host ""
        
        # Abrir navegador
        Start-Process $publicUrl
    }
} catch {
    Write-Host "[INFO] ngrok iniciado. Revisa http://localhost:4040 para ver la URL" -ForegroundColor Yellow
}

# Mantener el script corriendo
try {
    Wait-Job $flaskJob
} catch {
    Write-Host ""
    Write-Host "Deteniendo aplicacion..." -ForegroundColor Yellow
    Stop-Process -Id $ngrokProcess.Id -Force -ErrorAction SilentlyContinue
    Stop-Job $flaskJob -ErrorAction SilentlyContinue
    Remove-Job $flaskJob -ErrorAction SilentlyContinue
}

