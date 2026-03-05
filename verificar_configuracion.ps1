Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  VERIFICACION FINAL - METAFIANCE" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

$errors = 0
$warnings = 0

# Verificar archivos necesarios
Write-Host "1. Verificando archivos de configuracion..." -ForegroundColor Yellow

$requiredFiles = @{
    "app.py" = "Aplicacion Flask principal"
    "database.py" = "Modulo de base de datos"
    "requirements.txt" = "Dependencias"
    "Procfile" = "Comando de inicio para Render"
    "render.yaml" = "Configuracion de Render"
    "runtime.txt" = "Version de Python"
}

foreach ($file in $requiredFiles.Keys) {
    if (Test-Path $file) {
        Write-Host "  [OK] $file - $($requiredFiles[$file])" -ForegroundColor Green
    } else {
        Write-Host "  [ERROR] Falta: $file" -ForegroundColor Red
        $errors++
    }
}

Write-Host ""

# Verificar contenido de requirements.txt
Write-Host "2. Verificando dependencias..." -ForegroundColor Yellow
$requirements = Get-Content requirements.txt
$requiredDeps = @("Flask", "Werkzeug", "gunicorn", "flask-talisman")

foreach ($dep in $requiredDeps) {
    $found = $requirements | Where-Object { $_ -match $dep }
    if ($found) {
        Write-Host "  [OK] $dep encontrado" -ForegroundColor Green
    } else {
        Write-Host "  [ERROR] Falta: $dep" -ForegroundColor Red
        $errors++
    }
}

Write-Host ""

# Verificar Procfile
Write-Host "3. Verificando Procfile..." -ForegroundColor Yellow
$procfile = Get-Content Procfile
if ($procfile -match "gunicorn") {
    Write-Host "  [OK] Procfile contiene gunicorn" -ForegroundColor Green
} else {
    Write-Host "  [ERROR] Procfile no contiene gunicorn" -ForegroundColor Red
    $errors++
}

if ($procfile -match '\$PORT') {
    Write-Host "  [OK] Procfile usa variable PORT" -ForegroundColor Green
} else {
    Write-Host "  [ADVERTENCIA] Procfile no usa variable PORT" -ForegroundColor Yellow
    $warnings++
}

Write-Host ""

# Verificar app.py
Write-Host "4. Verificando app.py..." -ForegroundColor Yellow
$appContent = Get-Content app.py -Raw

if ($appContent -match "from flask import") {
    Write-Host "  [OK] Flask importado correctamente" -ForegroundColor Green
} else {
    Write-Host "  [ERROR] Flask no importado" -ForegroundColor Red
    $errors++
}

if ($appContent -match "app = Flask") {
    Write-Host "  [OK] Aplicacion Flask creada" -ForegroundColor Green
} else {
    Write-Host "  [ERROR] Aplicacion Flask no creada" -ForegroundColor Red
    $errors++
}

if ($appContent -match "os.environ.get\('PORT'") {
    Write-Host "  [OK] Usa variable de entorno PORT" -ForegroundColor Green
} else {
    Write-Host "  [ADVERTENCIA] No usa variable PORT (puede funcionar igual)" -ForegroundColor Yellow
    $warnings++
}

Write-Host ""

# Verificar templates
Write-Host "5. Verificando templates..." -ForegroundColor Yellow
$templatesDir = "templates"
if (Test-Path $templatesDir) {
    $templates = Get-ChildItem $templatesDir -Filter "*.html"
    Write-Host "  [OK] $($templates.Count) templates encontrados" -ForegroundColor Green
} else {
    Write-Host "  [ERROR] Directorio templates no existe" -ForegroundColor Red
    $errors++
}

Write-Host ""

# Verificar static
Write-Host "6. Verificando archivos estaticos..." -ForegroundColor Yellow
$staticDir = "static"
if (Test-Path $staticDir) {
    Write-Host "  [OK] Directorio static existe" -ForegroundColor Green
} else {
    Write-Host "  [ADVERTENCIA] Directorio static no existe" -ForegroundColor Yellow
    $warnings++
}

Write-Host ""

# Verificar Git
Write-Host "7. Verificando repositorio Git..." -ForegroundColor Yellow
$gitRemote = git remote get-url origin 2>&1
if ($LASTEXITCODE -eq 0) {
    Write-Host "  [OK] Repositorio remoto: $gitRemote" -ForegroundColor Green
    
    $gitStatus = git status --porcelain 2>&1
    if ($gitStatus) {
        Write-Host "  [ADVERTENCIA] Hay cambios sin commitear" -ForegroundColor Yellow
        $warnings++
    } else {
        Write-Host "  [OK] Todo commiteado" -ForegroundColor Green
    }
} else {
    Write-Host "  [ADVERTENCIA] No se encontro repositorio remoto" -ForegroundColor Yellow
    $warnings++
}

Write-Host ""

# Resumen
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  RESUMEN DE VERIFICACION" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

if ($errors -eq 0 -and $warnings -eq 0) {
    Write-Host "  [PERFECTO] Todo esta listo para desplegar!" -ForegroundColor Green
    Write-Host ""
    Write-Host "  Siguiente paso: Configurar en Render.com" -ForegroundColor Yellow
    Write-Host "  1. Ve a: https://dashboard.render.com" -ForegroundColor White
    Write-Host "  2. New + -> Web Service" -ForegroundColor White
    Write-Host "  3. Conecta: Ahorro_Con_Proposito" -ForegroundColor White
    Write-Host "  4. Create Web Service" -ForegroundColor White
} elseif ($errors -eq 0) {
    Write-Host "  [LISTO] Configuracion lista (con $warnings advertencias)" -ForegroundColor Green
    Write-Host "  Puedes proceder con el despliegue" -ForegroundColor Yellow
} else {
    Write-Host "  [ERROR] Se encontraron $errors errores" -ForegroundColor Red
    Write-Host "  Corrige los errores antes de desplegar" -ForegroundColor Red
}

Write-Host ""

