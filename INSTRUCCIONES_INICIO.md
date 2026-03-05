# Instrucciones para Iniciar la Aplicación Metafiance

## Opción 1: Usando el Script Automático (Recomendado)

### En Windows PowerShell:
```powershell
powershell -ExecutionPolicy Bypass -File iniciar_app.ps1
```

### En CMD (Símbolo del sistema):
```cmd
iniciar_app.bat
```

## Opción 2: Manual

### Paso 1: Verificar que Python esté instalado
Abre una terminal y ejecuta:
```bash
python --version
```

Si no funciona, intenta:
```bash
py --version
```

### Paso 2: Instalar dependencias
```bash
python -m pip install -r requirements.txt
```

O si usas `py`:
```bash
py -m pip install -r requirements.txt
```

### Paso 3: Ejecutar la aplicación
```bash
python app.py
```

O:
```bash
py app.py
```

## Opción 3: Si Python no está instalado

1. Descarga Python desde: https://www.python.org/downloads/
2. Durante la instalación, **asegúrate de marcar la opción "Add Python to PATH"**
3. Reinicia tu terminal
4. Sigue los pasos de la Opción 2

## Acceder a la Aplicación

Una vez que la aplicación esté ejecutándose, verás un mensaje como:
```
 * Running on http://127.0.0.1:5000
```

Abre tu navegador y visita:
```
http://localhost:5000
```

## Credenciales de Administrador

- **Email**: `admin@metafiance.com`
- **Contraseña**: `admin123`

## Detener la Aplicación

Presiona `Ctrl + C` en la terminal donde está ejecutándose la aplicación.

## Solución de Problemas

### Error: "Python no encontrado"
- Instala Python desde python.org
- Asegúrate de marcar "Add Python to PATH" durante la instalación
- Reinicia tu terminal después de instalar

### Error: "pip no encontrado"
- Ejecuta: `python -m ensurepip --upgrade`
- O reinstala Python con la opción "pip" marcada

### Error: "Flask no encontrado"
- Ejecuta: `python -m pip install Flask`

### Puerto 5000 ya en uso
- Cambia el puerto en `app.py` (última línea):
  ```python
  app.run(debug=True, host='0.0.0.0', port=8080)
  ```
- Luego accede a: `http://localhost:8080`

