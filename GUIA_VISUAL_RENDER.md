# 🎯 Guía Visual - Configurar Render.com

## ✅ Pre-verificación Completada

Tu aplicación está lista. Todos los archivos necesarios están presentes y configurados correctamente.

## 📸 Pasos Visuales en Render

### PASO 1: Acceder a Render
1. Ve a: **https://dashboard.render.com**
2. Si no estás registrado, haz clic en **"Get Started for Free"**
3. Regístrate con **GitHub** (recomendado)

### PASO 2: Crear Nuevo Servicio
1. Haz clic en el botón **"New +"** (arriba a la derecha, color azul)
2. En el menú desplegable, selecciona **"Web Service"**

### PASO 3: Conectar Repositorio
1. En la sección **"Connect a repository"**:
   - Si ves tus repositorios: Busca y selecciona **"Ahorro_Con_Proposito"**
   - Si no ves tus repositorios: Haz clic en **"Configure account"** y autoriza a Render

### PASO 4: Configuración Automática
Render detectará automáticamente que es Flask. Deberías ver:

```
Name: ahorro-con-proposito
Environment: Python 3
Region: [Elige el más cercano]
Branch: main
Root Directory: (vacío)
Runtime: Python 3
Build Command: pip install -r requirements.txt
Start Command: gunicorn app:app
```

**✅ Verifica que estos comandos sean correctos**

### PASO 5: Variables de Entorno (Opcional pero Recomendado)
1. Haz clic en **"Advanced"** (abajo)
2. En **"Environment Variables"**, haz clic en **"Add Environment Variable"**
3. Agrega estas dos variables:

   **Variable 1:**
   - Key: `SECRET_KEY`
   - Value: (Genera una clave secreta - ver abajo)

   **Variable 2:**
   - Key: `FLASK_ENV`
   - Value: `production`

**Generar clave secreta:**
Ejecuta en PowerShell:
```powershell
python -c "import secrets; print(secrets.token_hex(32))"
```
Copia el resultado y úsalo como valor de `SECRET_KEY`

### PASO 6: Seleccionar Plan
- Selecciona **"Free"** (plan gratuito)
- Tienes 750 horas gratis al mes (suficiente para 24/7)

### PASO 7: Crear Servicio
1. Haz clic en el botón verde **"Create Web Service"** (abajo a la derecha)
2. Render comenzará a construir tu aplicación
3. Verás una pantalla con logs en tiempo real

### PASO 8: Esperar el Despliegue
- El proceso tomará **2-5 minutos**
- Verás mensajes como:
  - "Building..."
  - "Installing dependencies..."
  - "Starting service..."
- Al final verás: **"Your service is live"** en verde

### PASO 9: Obtener tu URL
Una vez que veas **"Live"** en verde, tendrás tu URL permanente:

```
https://ahorro-con-proposito.onrender.com
```

(O el nombre que hayas elegido)

## 🔍 Verificación Post-Despliegue

Una vez desplegado, verifica:

1. **Abre tu URL** en el navegador
2. **Deberías ver** la página principal de Metafiance
3. **Prueba las rutas públicas:**
   - `https://tu-app.onrender.com/`
   - `https://tu-app.onrender.com/publico`
   - `https://tu-app.onrender.com/api/public/info`

## 🆘 Solución de Problemas Comunes

### Error: "Build failed"
**Causa:** Dependencias faltantes o error en requirements.txt
**Solución:**
1. Revisa los logs en Render
2. Verifica que `requirements.txt` tenga todas las dependencias
3. Asegúrate de que `gunicorn` esté incluido

### Error: "Application failed to respond"
**Causa:** Error en el código o configuración incorrecta
**Solución:**
1. Revisa los logs en Render (pestaña "Logs")
2. Verifica que el `Procfile` tenga: `web: gunicorn app:app --bind 0.0.0.0:$PORT`
3. Verifica que `app.py` exista y tenga `app = Flask(__name__)`

### La app se "duerme"
**Causa:** Normal en plan gratuito después de 15 minutos de inactividad
**Solución:**
- Es normal, se despierta automáticamente en 30 segundos
- Para evitar esto, considera el plan de pago ($7/mes)

### Error 502 Bad Gateway
**Causa:** La aplicación no está respondiendo
**Solución:**
1. Revisa los logs en Render
2. Verifica que el puerto sea `$PORT` (no 5000 fijo)
3. Asegúrate de que gunicorn esté instalado

## 📊 Monitoreo

En el dashboard de Render puedes:
- **Ver logs en tiempo real** - Pestaña "Logs"
- **Ver métricas** - Pestaña "Metrics"
- **Configurar alertas** - Settings → Alerts
- **Ver el estado** - Indicador verde "Live"

## 🔄 Actualizar la Aplicación

Cada vez que hagas cambios:

```powershell
git add .
git commit -m "Descripción de los cambios"
git push
```

Render detectará automáticamente y redesplegará en 2-5 minutos.

## ✅ Checklist Final

Antes de considerar que está todo listo:

- [ ] Servicio creado en Render
- [ ] Estado "Live" (verde)
- [ ] URL permanente funcionando
- [ ] Página principal carga correctamente
- [ ] Rutas públicas accesibles
- [ ] HTTPS funcionando (candado verde en navegador)
- [ ] Login funciona correctamente
- [ ] Base de datos funciona

## 🎉 ¡Listo!

Una vez completado, tendrás:
- ✅ URL permanente: `https://tu-app.onrender.com`
- ✅ HTTPS seguro automático
- ✅ Despliegue automático desde GitHub
- ✅ Aplicación funcionando 24/7
- ✅ Logs y monitoreo disponibles

---

**¿Necesitas ayuda?** Revisa los logs en Render o consulta la documentación: https://render.com/docs

