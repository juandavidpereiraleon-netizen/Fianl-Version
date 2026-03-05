# 🚀 Configurar Render.com - Paso a Paso

## ✅ Código Subido a GitHub

Tu código ya está en: **https://github.com/jazaji1983/Ahorro_Con_Proposito**

## 📋 Pasos para Configurar Render

### Paso 1: En la página de Render que tienes abierta

1. **Si no estás registrado:**
   - Haz clic en **"Get Started for Free"**
   - Elige **"Sign up with GitHub"**
   - Autoriza a Render a acceder a tus repositorios

2. **Si ya estás registrado:**
   - Haz clic en **"New +"** (arriba a la derecha)
   - Selecciona **"Web Service"**

### Paso 2: Conectar Repositorio

1. En la sección **"Connect a repository"**:
   - Busca: `jazaji1983/Ahorro_Con_Proposito`
   - O haz clic en **"Configure account"** si no ves tus repositorios
   - Selecciona el repositorio **"Ahorro_Con_Proposito"**

### Paso 3: Configurar el Servicio

Render debería detectar automáticamente que es Flask, pero verifica:

**Configuración automática (debería aparecer así):**
- **Name:** `ahorro-con-proposito` (o el nombre que prefieras)
- **Environment:** `Python 3`
- **Region:** Elige el más cercano a ti
- **Branch:** `main`
- **Root Directory:** (dejar vacío)
- **Runtime:** `Python 3`
- **Build Command:** `pip install -r requirements.txt`
- **Start Command:** `gunicorn app:app`

**Si no aparece automáticamente, configura manualmente:**
- **Build Command:** `pip install -r requirements.txt`
- **Start Command:** `gunicorn app:app --bind 0.0.0.0:$PORT`

### Paso 4: Variables de Entorno (Opcional pero Recomendado)

Haz clic en **"Advanced"** y agrega estas variables:

```
SECRET_KEY=genera-una-clave-secreta-muy-larga-aqui
FLASK_ENV=production
```

**Para generar una clave secreta, ejecuta en PowerShell:**
```powershell
python -c "import secrets; print(secrets.token_hex(32))"
```

### Paso 5: Plan

- Selecciona **"Free"** (plan gratuito)
- Tienes 750 horas gratis al mes (suficiente para 24/7)

### Paso 6: Crear el Servicio

1. Haz clic en **"Create Web Service"**
2. Render comenzará a construir tu aplicación
3. Verás los logs en tiempo real
4. Esto tomará **2-5 minutos**

### Paso 7: Obtener tu URL Permanente

Una vez que el despliegue termine (verás "Live" en verde), tendrás:

```
https://ahorro-con-proposito.onrender.com
```

**O el nombre que hayas elegido:**
```
https://TU-NOMBRE.onrender.com
```

## ✅ Lo que ya está configurado

- ✅ `render.yaml` - Configuración de Render
- ✅ `Procfile` - Comando de inicio correcto
- ✅ `requirements.txt` - Todas las dependencias incluyendo gunicorn
- ✅ `runtime.txt` - Versión de Python
- ✅ Seguridad HTTPS activa
- ✅ Headers de seguridad configurados

## 🔒 Seguridad Incluida

- ✅ HTTPS automático (Render lo proporciona)
- ✅ Certificado SSL válido
- ✅ Flask-Talisman configurado
- ✅ Headers de seguridad activos
- ✅ Protección XSS y clickjacking

## 📱 URLs Públicas Disponibles

Una vez desplegado, estas rutas serán públicas:

- `https://tu-app.onrender.com/` - Página principal
- `https://tu-app.onrender.com/publico` - Información pública
- `https://tu-app.onrender.com/api/public/info` - API de información
- `https://tu-app.onrender.com/api/public/estadisticas` - API de estadísticas

## 🔄 Actualizaciones Automáticas

Cada vez que hagas cambios y hagas `git push`:

```powershell
git add .
git commit -m "Descripción de los cambios"
git push
```

Render detectará automáticamente los cambios y redesplegará tu aplicación.

## 🆘 Solución de Problemas

### Error: "Build failed"
- Revisa los logs en Render
- Verifica que `requirements.txt` tenga todas las dependencias
- Asegúrate de que `gunicorn` esté en requirements.txt

### Error: "Application failed to respond"
- Verifica que el `Procfile` tenga: `web: gunicorn app:app --bind 0.0.0.0:$PORT`
- Revisa los logs para ver errores específicos

### La app se "duerme" después de 15 minutos
- Es normal en el plan gratuito
- Se despierta automáticamente en 30 segundos cuando alguien la visita
- Para evitar esto, considera el plan de pago ($7/mes)

## 📊 Monitoreo

En el dashboard de Render puedes:
- Ver logs en tiempo real
- Ver métricas de uso
- Configurar alertas
- Ver el estado del servicio

## 🎉 ¡Listo!

Una vez completado, tendrás:
- ✅ URL permanente: `https://tu-app.onrender.com`
- ✅ HTTPS seguro
- ✅ Despliegue automático desde GitHub
- ✅ Aplicación funcionando 24/7

---

**Estado actual:**
- ✅ Código subido a GitHub
- ⏳ Configurando Render (sigue los pasos arriba)
- ⏳ Esperando despliegue

