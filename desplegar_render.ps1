Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  METAFIANCE - Preparacion para Render" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Verificar que Git esté instalado
$gitInstalled = Get-Command git -ErrorAction SilentlyContinue
if (-not $gitInstalled) {
    Write-Host "[ERROR] Git no está instalado" -ForegroundColor Red
    Write-Host "Instala Git desde: https://git-scm.com/download/win" -ForegroundColor Yellow
    exit 1
}

Write-Host "[OK] Git instalado" -ForegroundColor Green
Write-Host ""

# Verificar archivos necesarios
$requiredFiles = @("app.py", "requirements.txt", "Procfile", "render.yaml", "database.py")
$missingFiles = @()

foreach ($file in $requiredFiles) {
    if (-not (Test-Path $file)) {
        $missingFiles += $file
    }
}

if ($missingFiles.Count -gt 0) {
    Write-Host "[ERROR] Faltan archivos necesarios:" -ForegroundColor Red
    foreach ($file in $missingFiles) {
        Write-Host "  - $file" -ForegroundColor Red
    }
    exit 1
}

Write-Host "[OK] Todos los archivos necesarios están presentes" -ForegroundColor Green
Write-Host ""

# Verificar si es un repositorio Git
if (-not (Test-Path ".git")) {
    Write-Host "[INFO] Inicializando repositorio Git..." -ForegroundColor Yellow
    git init
    Write-Host "[OK] Repositorio Git inicializado" -ForegroundColor Green
    Write-Host ""
}

# Verificar estado de Git
Write-Host "Estado del repositorio Git:" -ForegroundColor Yellow
git status --short
Write-Host ""

# Mostrar instrucciones
Write-Host "========================================" -ForegroundColor Green
Write-Host "  INSTRUCCIONES PARA DESPLEGAR" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
Write-Host ""
Write-Host "1. SUBIR A GITHUB:" -ForegroundColor Yellow
Write-Host ""
Write-Host "   Si aún no tienes un repositorio en GitHub:" -ForegroundColor White
Write-Host "   a) Ve a: https://github.com/new" -ForegroundColor Cyan
Write-Host "   b) Crea un nuevo repositorio" -ForegroundColor Cyan
Write-Host "   c) Luego ejecuta estos comandos:" -ForegroundColor Cyan
Write-Host ""
Write-Host "   git add ." -ForegroundColor White
Write-Host "   git commit -m 'Initial commit - Metafiance'" -ForegroundColor White
Write-Host "   git branch -M main" -ForegroundColor White
Write-Host "   git remote add origin https://github.com/TU_USUARIO/metafiance.git" -ForegroundColor White
Write-Host "   git push -u origin main" -ForegroundColor White
Write-Host ""
Write-Host "2. DESPLEGAR EN RENDER:" -ForegroundColor Yellow
Write-Host ""
Write-Host "   a) Ve a: https://render.com" -ForegroundColor Cyan
Write-Host "   b) Regístrate o inicia sesión" -ForegroundColor Cyan
Write-Host "   c) Haz clic en 'New +' -> 'Web Service'" -ForegroundColor Cyan
Write-Host "   d) Conecta tu repositorio de GitHub" -ForegroundColor Cyan
Write-Host "   e) Selecciona el repositorio 'metafiance'" -ForegroundColor Cyan
Write-Host "   f) Render detectará Flask automáticamente" -ForegroundColor Cyan
Write-Host "   g) Haz clic en 'Create Web Service'" -ForegroundColor Cyan
Write-Host ""
Write-Host "3. OBTENER TU URL PERMANENTE:" -ForegroundColor Yellow
Write-Host ""
Write-Host "   Una vez desplegado, obtendrás:" -ForegroundColor White
Write-Host "   https://metafiance.onrender.com" -ForegroundColor Cyan
Write-Host ""
Write-Host "   ¡Esta URL es permanente y gratuita!" -ForegroundColor Green
Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  ARCHIVOS LISTOS PARA DESPLEGAR" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "[OK] render.yaml - Configuración de Render" -ForegroundColor Green
Write-Host "[OK] Procfile - Comando de inicio" -ForegroundColor Green
Write-Host "[OK] requirements.txt - Dependencias" -ForegroundColor Green
Write-Host "[OK] runtime.txt - Versión de Python" -ForegroundColor Green
Write-Host "[OK] .gitignore - Archivos a ignorar" -ForegroundColor Green
Write-Host ""
Write-Host "¿Quieres que abra el navegador en Render.com?" -ForegroundColor Yellow
$response = Read-Host "Presiona Enter para abrir, o 'n' para cancelar"
if ($response -ne 'n') {
    Start-Process "https://render.com"
    Write-Host ""
    Write-Host "Navegador abierto. Sigue las instrucciones arriba." -ForegroundColor Green
}

