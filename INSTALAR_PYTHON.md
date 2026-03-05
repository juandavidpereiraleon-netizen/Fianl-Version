# Guía de Instalación de Python para Metafiance

## Paso 1: Descargar Python

1. Ve a la página oficial de Python: **https://www.python.org/downloads/**
2. Haz clic en el botón **"Download Python 3.12.x"** (o la versión más reciente)
3. Se descargará el instalador `python-3.12.x-amd64.exe`

## Paso 2: Instalar Python

1. **Ejecuta el instalador** que acabas de descargar
2. **IMPORTANTE**: En la primera pantalla, marca la casilla:
   ```
   ☑ Add Python to PATH
   ```
   ⚠️ **ESTO ES CRÍTICO** - Si no marcas esta opción, tendrás problemas después.

3. Haz clic en **"Install Now"**
4. Espera a que termine la instalación (puede tardar unos minutos)
5. Cuando termine, verás un mensaje de "Setup was successful"
6. Haz clic en **"Close"**

## Paso 3: Verificar la Instalación

1. Abre una **nueva** ventana de PowerShell o CMD
   - ⚠️ **Importante**: Debe ser una ventana NUEVA, no la que ya tenías abierta
   
2. Ejecuta:
   ```powershell
   python --version
   ```
   
3. Deberías ver algo como:
   ```
   Python 3.12.x
   ```

4. También verifica pip:
   ```powershell
   python -m pip --version
   ```

## Paso 4: Instalar Dependencias de la Aplicación

1. Navega a la carpeta del proyecto:
   ```powershell
   cd "C:\Users\Familia\Desktop\Familia\JAIRO\NGC"
   ```

2. Instala las dependencias:
   ```powershell
   python -m pip install -r requirements.txt
   ```

## Paso 5: Ejecutar la Aplicación

1. Ejecuta la aplicación:
   ```powershell
   python app.py
   ```

2. Verás un mensaje como:
   ```
   * Running on http://127.0.0.1:5000
   ```

3. Abre tu navegador y visita:
   ```
   http://localhost:5000
   ```

## Solución de Problemas

### Si "python" no funciona después de instalar:

1. Cierra TODAS las ventanas de terminal
2. Abre una NUEVA ventana de PowerShell o CMD
3. Intenta de nuevo con `python --version`

### Si sigue sin funcionar:

1. Busca Python en el menú de inicio de Windows
2. Abre "Python 3.12" (o la versión que instalaste)
3. Esto abrirá una terminal de Python
4. Cierra esa ventana
5. Abre PowerShell normalmente
6. Intenta de nuevo

### Verificar la ruta de instalación:

Python normalmente se instala en:
```
C:\Users\Familia\AppData\Local\Programs\Python\Python312\
```

Si necesitas agregar Python al PATH manualmente:
1. Busca "Variables de entorno" en el menú de inicio
2. Haz clic en "Editar las variables de entorno del sistema"
3. Haz clic en "Variables de entorno"
4. En "Variables del sistema", busca "Path" y haz clic en "Editar"
5. Haz clic en "Nuevo" y agrega:
   ```
   C:\Users\Familia\AppData\Local\Programs\Python\Python312
   C:\Users\Familia\AppData\Local\Programs\Python\Python312\Scripts
   ```
6. Haz clic en "Aceptar" en todas las ventanas
7. Reinicia tu terminal

## ¿Necesitas Ayuda?

Si tienes problemas durante la instalación:
1. Asegúrate de marcar "Add Python to PATH"
2. Reinicia tu computadora después de instalar
3. Abre una nueva terminal después de reiniciar

