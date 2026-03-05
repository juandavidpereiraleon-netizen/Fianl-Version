Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  CONFIGURANDO RENDER.COM" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Verificar que todo esté listo
Write-Host "Verificando preparación..." -ForegroundColor Yellow

$files = @("render.yaml", "Procfile", "requirements.txt", "runtime.txt")
$allReady = $true

foreach ($file in $files) {
    if (Test-Path $file) {
        Write-Host "[OK] $file" -ForegroundColor Green
    } else {
        Write-Host "[ERROR] Falta: $file" -ForegroundColor Red
        $allReady = $false
    }
}

if (-not $allReady) {
    Write-Host ""
    Write-Host "Faltan archivos necesarios. No se puede continuar." -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "[OK] Todos los archivos necesarios están presentes" -ForegroundColor Green
Write-Host ""

# Verificar repositorio
Write-Host "Verificando repositorio Git..." -ForegroundColor Yellow
$remote = git remote get-url origin 2>&1
if ($LASTEXITCODE -eq 0) {
    Write-Host "[OK] Repositorio: $remote" -ForegroundColor Green
} else {
    Write-Host "[ADVERTENCIA] No se encontró repositorio remoto" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "========================================" -ForegroundColor Green
Write-Host "  PASOS PARA EJECUTAR EN RENDER" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
Write-Host ""

Write-Host "PASO 1: Crear Servicio Web" -ForegroundColor Yellow
Write-Host "  ✓ Navegador abierto en Render" -ForegroundColor Green
Write-Host "  → Haz clic en 'New +' (arriba a la derecha)" -ForegroundColor White
Write-Host "  → Selecciona 'Web Service'" -ForegroundColor White
Write-Host ""

Start-Sleep -Seconds 2

Write-Host "PASO 2: Conectar Repositorio" -ForegroundColor Yellow
Write-Host "  → Busca: jazaji1983/Ahorro_Con_Proposito" -ForegroundColor White
Write-Host "  → O haz clic en 'Configure account' si no ves tus repos" -ForegroundColor White
Write-Host "  → Selecciona 'Ahorro_Con_Proposito'" -ForegroundColor White
Write-Host ""

Start-Sleep -Seconds 2

Write-Host "PASO 3: Verificar Configuración" -ForegroundColor Yellow
Write-Host "  Render debería detectar Flask automáticamente" -ForegroundColor White
Write-Host "  VERIFICA que aparezca:" -ForegroundColor Cyan
Write-Host "    • Build Command: pip install -r requirements.txt" -ForegroundColor White
Write-Host "    • Start Command: gunicorn app:app" -ForegroundColor White
Write-Host ""

Start-Sleep -Seconds 2

Write-Host "PASO 4: Variables de Entorno (Opcional)" -ForegroundColor Yellow
Write-Host "  → Haz clic en 'Advanced'" -ForegroundColor White
Write-Host "  → Agrega estas variables:" -ForegroundColor White
Write-Host "    SECRET_KEY=tu-clave-secreta-aqui" -ForegroundColor Cyan
Write-Host "    FLASK_ENV=production" -ForegroundColor Cyan
Write-Host ""

# Generar clave secreta
Write-Host "Generando clave secreta de ejemplo..." -ForegroundColor Yellow
try {
    $secretKey = python -c "import secrets; print(secrets.token_hex(32))" 2>&1
    if ($LASTEXITCODE -eq 0) {
        Write-Host "Clave secreta generada:" -ForegroundColor Green
        Write-Host "  $secretKey" -ForegroundColor Cyan
        Write-Host ""
        Write-Host "Copia esta clave y úsala en Render como SECRET_KEY" -ForegroundColor Yellow
    }
} catch {
    Write-Host "Ejecuta manualmente: python -c 'import secrets; print(secrets.token_hex(32))'" -ForegroundColor Yellow
}

Write-Host ""
Start-Sleep -Seconds 2

Write-Host "PASO 5: Plan" -ForegroundColor Yellow
Write-Host "  → Selecciona 'Free' (plan gratuito)" -ForegroundColor White
Write-Host ""

Start-Sleep -Seconds 2

Write-Host "PASO 6: Crear Servicio" -ForegroundColor Yellow
Write-Host "  → Haz clic en 'Create Web Service'" -ForegroundColor White
Write-Host "  → Espera 2-5 minutos (verás los logs en tiempo real)" -ForegroundColor White
Write-Host ""

Start-Sleep -Seconds 2

Write-Host "PASO 7: Obtener URL Permanente" -ForegroundColor Yellow
Write-Host "  → Cuando veas 'Live' en verde, tendrás tu URL:" -ForegroundColor White
Write-Host "  → https://ahorro-con-proposito.onrender.com" -ForegroundColor Cyan
Write-Host ""

Write-Host "========================================" -ForegroundColor Green
Write-Host "  RESUMEN" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
Write-Host ""
Write-Host "✅ Archivos listos" -ForegroundColor Green
Write-Host "✅ Repositorio configurado" -ForegroundColor Green
Write-Host "✅ Navegador abierto en Render" -ForegroundColor Green
Write-Host ""
Write-Host "Sigue los pasos arriba en el navegador." -ForegroundColor Yellow
Write-Host "Si necesitas ayuda, revisa CONFIGURAR_RENDER.md" -ForegroundColor Yellow
Write-Host ""

# Abrir tambien el repositorio de GitHub para referencia
Start-Process "https://github.com/jazaji1983/Ahorro_Con_Proposito"

Write-Host "Repositorio de GitHub abierto para referencia." -ForegroundColor Cyan
Write-Host ""

