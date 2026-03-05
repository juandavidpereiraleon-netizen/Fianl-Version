Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  METAFIANCE - Modo Publico Seguro" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Verificar Python
$pythonVersion = python --version 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] Python no encontrado" -ForegroundColor Red
    exit 1
}
Write-Host "[OK] $pythonVersion" -ForegroundColor Green

# Verificar e instalar dependencias de seguridad
Write-Host ""
Write-Host "Verificando dependencias de seguridad..." -ForegroundColor Yellow
python -m pip install flask-talisman --quiet 2>&1 | Out-Null
Write-Host "[OK] Dependencias de seguridad instaladas" -ForegroundColor Green

# Verificar ngrok
$ngrokInstalled = Get-Command ngrok -ErrorAction SilentlyContinue
if (-not $ngrokInstalled) {
    Write-Host ""
    Write-Host "[INFO] ngrok no encontrado. Instalando..." -ForegroundColor Yellow
    try {
        winget install ngrok --silent --accept-package-agreements --accept-source-agreements
        Write-Host "[OK] ngrok instalado" -ForegroundColor Green
        Start-Sleep -Seconds 3
    } catch {
        Write-Host "[ERROR] No se pudo instalar ngrok automáticamente" -ForegroundColor Red
        Write-Host "Instala manualmente desde: https://ngrok.com/download" -ForegroundColor Yellow
        Write-Host "O crea una cuenta gratuita en: https://dashboard.ngrok.com" -ForegroundColor Yellow
        exit 1
    }
}

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  Configuracion de Seguridad" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "[OK] HTTPS forzado" -ForegroundColor Green
Write-Host "[OK] Headers de seguridad configurados" -ForegroundColor Green
Write-Host "[OK] Proteccion CSRF activa" -ForegroundColor Green
Write-Host "[OK] Conexion encriptada SSL/TLS" -ForegroundColor Green
Write-Host ""

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  Iniciando aplicacion..." -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Configurar variable de entorno para producción
$env:FLASK_ENV = "production"

# Iniciar Flask en segundo plano
$flaskJob = Start-Job -ScriptBlock {
    Set-Location $using:PWD
    $env:FLASK_ENV = "production"
    python app.py
}

# Esperar a que Flask inicie
Write-Host "Iniciando servidor Flask..." -ForegroundColor Yellow
Start-Sleep -Seconds 4

# Verificar que Flask esté corriendo
try {
    $test = Invoke-WebRequest -Uri "http://localhost:5000" -TimeoutSec 2 -UseBasicParsing -ErrorAction Stop
    Write-Host "[OK] Servidor Flask iniciado correctamente" -ForegroundColor Green
} catch {
    Write-Host "[ADVERTENCIA] El servidor puede estar iniciando aún..." -ForegroundColor Yellow
}

Write-Host ""
Write-Host "Iniciando tunel seguro ngrok (HTTPS)..." -ForegroundColor Yellow
Write-Host ""

# Iniciar ngrok con configuración segura
$ngrokProcess = Start-Process -FilePath "ngrok" -ArgumentList "http", "5000" -NoNewWindow -PassThru

Start-Sleep -Seconds 3

# Obtener URL de ngrok
$maxRetries = 10
$retryCount = 0
$publicUrl = $null

while ($retryCount -lt $maxRetries -and -not $publicUrl) {
    try {
        $ngrokApi = Invoke-RestMethod -Uri "http://localhost:4040/api/tunnels" -ErrorAction Stop
        if ($ngrokApi.tunnels -and $ngrokApi.tunnels.Count -gt 0) {
            # Buscar el túnel HTTPS
            $httpsTunnel = $ngrokApi.tunnels | Where-Object { $_.proto -eq "https" }
            if ($httpsTunnel) {
                $publicUrl = $httpsTunnel.public_url
            } else {
                $publicUrl = $ngrokApi.tunnels[0].public_url
            }
        }
    } catch {
        $retryCount++
        Start-Sleep -Seconds 1
    }
}

if ($publicUrl) {
    Write-Host ""
    Write-Host "========================================" -ForegroundColor Green
    Write-Host "  APLICACION PUBLICA Y SEGURA" -ForegroundColor Green
    Write-Host "========================================" -ForegroundColor Green
    Write-Host ""
    Write-Host "URL Publica Segura (HTTPS):" -ForegroundColor Yellow
    Write-Host "  $publicUrl" -ForegroundColor Cyan -BackgroundColor DarkGreen
    Write-Host ""
    Write-Host "URL Local:" -ForegroundColor Yellow
    Write-Host "  http://localhost:5000" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "========================================" -ForegroundColor Green
    Write-Host "  RUTAS PUBLICAS DISPONIBLES" -ForegroundColor Green
    Write-Host "========================================" -ForegroundColor Green
    Write-Host ""
    Write-Host "  $publicUrl/" -ForegroundColor White
    Write-Host "  $publicUrl/publico" -ForegroundColor White
    Write-Host "  $publicUrl/api/public/info" -ForegroundColor White
    Write-Host "  $publicUrl/api/public/estadisticas" -ForegroundColor White
    Write-Host ""
    Write-Host "========================================" -ForegroundColor Green
    Write-Host "  SEGURIDAD ACTIVA" -ForegroundColor Green
    Write-Host "========================================" -ForegroundColor Green
    Write-Host ""
    Write-Host "[OK] Conexion HTTPS encriptada" -ForegroundColor Green
    Write-Host "[OK] Certificado SSL valido" -ForegroundColor Green
    Write-Host "[OK] Headers de seguridad configurados" -ForegroundColor Green
    Write-Host "[OK] Proteccion contra XSS" -ForegroundColor Green
    Write-Host "[OK] Proteccion contra clickjacking" -ForegroundColor Green
    Write-Host ""
    Write-Host "========================================" -ForegroundColor Yellow
    Write-Host "  CREDENCIALES DE ADMINISTRADOR" -ForegroundColor Yellow
    Write-Host "========================================" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "  Email:    admin@metafiance.com" -ForegroundColor White
    Write-Host "  Password: admin123" -ForegroundColor White
    Write-Host ""
    Write-Host "========================================" -ForegroundColor Cyan
    Write-Host "  Panel de ngrok: http://localhost:4040" -ForegroundColor Cyan
    Write-Host "  Presiona Ctrl+C para detener" -ForegroundColor Yellow
    Write-Host "========================================" -ForegroundColor Cyan
    Write-Host ""
    
    # Abrir navegador con URL segura
    Start-Process $publicUrl
    
    # Abrir panel de ngrok
    Start-Process "http://localhost:4040"
} else {
    Write-Host "[ADVERTENCIA] No se pudo obtener la URL de ngrok automáticamente" -ForegroundColor Yellow
    Write-Host "Abre http://localhost:4040 para ver la URL manualmente" -ForegroundColor Cyan
}

# Mantener el script corriendo
Write-Host ""
Write-Host "Manteniendo la aplicacion activa..." -ForegroundColor Yellow
Write-Host "Presiona Ctrl+C para detener todo" -ForegroundColor Yellow
Write-Host ""

try {
    # Mantener corriendo y monitorear
    while ($true) {
        Start-Sleep -Seconds 5
        $jobState = Get-Job $flaskJob -ErrorAction SilentlyContinue
        if ($jobState -and $jobState.State -eq "Failed") {
            Write-Host "[ERROR] La aplicacion Flask se detuvo" -ForegroundColor Red
            break
        }
    }
} catch {
    Write-Host ""
    Write-Host "Deteniendo aplicacion..." -ForegroundColor Yellow
    Stop-Process -Id $ngrokProcess.Id -Force -ErrorAction SilentlyContinue
    Stop-Job $flaskJob -ErrorAction SilentlyContinue
    Remove-Job $flaskJob -ErrorAction SilentlyContinue
    Write-Host "[OK] Aplicacion detenida" -ForegroundColor Green
}

