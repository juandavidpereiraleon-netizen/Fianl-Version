# Script para ejecutar Metafiance
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  Metafiance - Iniciando Aplicacion" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Buscar Python en ubicaciones comunes
$pythonPaths = @(
    "python",
    "py",
    "python3",
    "$env:LOCALAPPDATA\Programs\Python\Python312\python.exe",
    "$env:LOCALAPPDATA\Programs\Python\Python311\python.exe",
    "$env:LOCALAPPDATA\Programs\Python\Python310\python.exe",
    "C:\Python312\python.exe",
    "C:\Python311\python.exe",
    "C:\Python310\python.exe"
)

$pythonFound = $false
$pythonCmd = $null

foreach ($path in $pythonPaths) {
    try {
        if (Test-Path $path) {
            $result = & $path --version 2>&1
            if ($LASTEXITCODE -eq 0 -or $result -match "Python") {
                Write-Host "[OK] Python encontrado: $path" -ForegroundColor Green
                Write-Host "Version: $result" -ForegroundColor Green
                $pythonCmd = $path
                $pythonFound = $true
                break
            }
        } elseif ($path -match "^python|^py|^python3$") {
            $result = Get-Command $path -ErrorAction SilentlyContinue
            if ($result) {
                $testVersion = & $path --version 2>&1
                if ($testVersion -match "Python" -or $LASTEXITCODE -eq 0) {
                    Write-Host "[OK] Python encontrado: $($result.Source)" -ForegroundColor Green
                    Write-Host "Version: $testVersion" -ForegroundColor Green
                    $pythonCmd = $path
                    $pythonFound = $true
                    break
                }
            }
        }
    } catch {
        continue
    }
}

if (-not $pythonFound) {
    Write-Host "[ERROR] Python no encontrado" -ForegroundColor Red
    Write-Host ""
    Write-Host "Por favor instala Python desde: https://www.python.org/downloads/" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "IMPORTANTE: Durante la instalacion marca 'Add Python to PATH'" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "Despues de instalar:" -ForegroundColor Yellow
    Write-Host "1. Cierra esta ventana" -ForegroundColor Yellow
    Write-Host "2. Abre una nueva terminal" -ForegroundColor Yellow
    Write-Host "3. Ejecuta este script de nuevo" -ForegroundColor Yellow
    Write-Host ""
    Read-Host "Presiona Enter para salir"
    exit 1
}

# Verificar e instalar dependencias
Write-Host ""
Write-Host "Verificando dependencias..." -ForegroundColor Yellow

$requirementsFile = "requirements.txt"
if (Test-Path $requirementsFile) {
    Write-Host "Instalando dependencias desde requirements.txt..." -ForegroundColor Yellow
    & $pythonCmd -m pip install -r $requirementsFile --quiet
    if ($LASTEXITCODE -ne 0) {
        Write-Host "[ADVERTENCIA] Algunas dependencias pueden no haberse instalado correctamente" -ForegroundColor Yellow
    } else {
        Write-Host "[OK] Dependencias instaladas" -ForegroundColor Green
    }
} else {
    Write-Host "[INFO] Instalando Flask directamente..." -ForegroundColor Yellow
    & $pythonCmd -m pip install Flask --quiet
}

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  Iniciando aplicacion..." -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "La aplicacion estara disponible en:" -ForegroundColor Green
Write-Host "  http://localhost:5000" -ForegroundColor Cyan
Write-Host ""
Write-Host "Credenciales de administrador:" -ForegroundColor Yellow
Write-Host "  Email: admin@metafiance.com" -ForegroundColor White
Write-Host "  Password: admin123" -ForegroundColor White
Write-Host ""
Write-Host "Presiona Ctrl+C para detener la aplicacion" -ForegroundColor Yellow
Write-Host ""

# Ejecutar la aplicación
& $pythonCmd app.py

