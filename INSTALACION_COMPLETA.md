# ✅ Instalación Completa - Metafiance

## Lo que se ha instalado:

### 1. ✅ Python 3.12.10
   - Instalado usando Windows Package Manager (winget)
   - Ubicación: `C:\Users\Familia\AppData\Local\Programs\Python\Python312\`

### 2. ✅ Dependencias de Python
   - Flask 3.1.2
   - Werkzeug 3.1.3
   - Y todas las dependencias necesarias

### 3. ✅ Base de Datos
   - SQLite configurada y lista para usar
   - Se creará automáticamente al ejecutar la aplicación

## 🚀 Cómo Ejecutar la Aplicación

### Opción 1: Script Automático (Recomendado)
```powershell
.\iniciar_metafiance.ps1
```

### Opción 2: Manual
```powershell
python app.py
```

## 🌐 Acceder a la Aplicación

Una vez que la aplicación esté ejecutándose, abre tu navegador en:

```
http://localhost:5000
```

## 👤 Credenciales de Administrador

- **Email**: `admin@metafiance.com`
- **Contraseña**: `admin123`

## 📋 Funcionalidades Disponibles

### Para Administradores:
- ✅ Gestión de metas (crear, editar, eliminar)
- ✅ Ver todas las metas del sistema
- ✅ Panel de administración

### Para Usuarios:
- ✅ Ver metas asignadas a su curso
- ✅ Registrar ahorros
- ✅ Registrar salidas
- ✅ Ver historial de movimientos
- ✅ Ver detalle del plan de ahorro
- ✅ Visualización de progreso

## 🔧 Comandos Útiles

### Verificar que Python está instalado:
```powershell
python --version
```

### Verificar que Flask está instalado:
```powershell
python -c "import flask; print('Flask OK')"
```

### Instalar/Actualizar dependencias:
```powershell
python -m pip install -r requirements.txt
```

### Detener la aplicación:
Presiona `Ctrl + C` en la terminal donde está ejecutándose

## 📁 Archivos Importantes

- `app.py` - Aplicación Flask principal
- `database.py` - Módulo de base de datos
- `requirements.txt` - Dependencias del proyecto
- `iniciar_metafiance.ps1` - Script para iniciar la aplicación
- `metafiance.db` - Base de datos (se crea automáticamente)

## 🎨 Características de la Interfaz

- Colores: Azul (#3366CC), Verde Militar (#4d7a2e), Blanco
- Diseño responsive con Bootstrap 5
- Interfaz moderna y fácil de usar

## ⚠️ Notas Importantes

1. La aplicación se ejecuta en el puerto 5000 por defecto
2. Si el puerto está ocupado, edita `app.py` y cambia el puerto
3. La base de datos se crea automáticamente en la primera ejecución
4. Los usuarios se asignan automáticamente a las metas de su curso

## 🆘 Solución de Problemas

### Si la aplicación no inicia:
1. Verifica que Python esté instalado: `python --version`
2. Verifica que las dependencias estén instaladas: `python -m pip list`
3. Revisa los mensajes de error en la terminal

### Si el puerto 5000 está ocupado:
Edita `app.py` (última línea) y cambia:
```python
app.run(debug=True, host='0.0.0.0', port=8080)
```

### Si hay errores de base de datos:
Elimina el archivo `metafiance.db` y la aplicación lo recreará automáticamente.

## ✅ Estado de la Instalación

- ✅ Python 3.12.10 instalado
- ✅ Dependencias instaladas
- ✅ Base de datos configurada
- ✅ Aplicación lista para ejecutar

¡Todo está listo para usar! 🎉

