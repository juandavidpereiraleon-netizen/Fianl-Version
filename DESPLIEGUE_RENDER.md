# 🚀 Despliegue en Render.com - URL Permanente Gratuita

## ✅ Configuración Completa para Render.com

Tu aplicación está lista para desplegarse en Render.com y obtener una URL permanente gratuita.

## 📋 Pasos para Desplegar (5 minutos)

### Paso 1: Crear Cuenta en Render

1. Ve a: **https://render.com**
2. Haz clic en **"Get Started for Free"**
3. Regístrate con tu cuenta de **GitHub** (recomendado) o email

### Paso 2: Preparar tu Código en GitHub

Si aún no tienes tu código en GitHub:

1. **Crea un repositorio en GitHub:**
   - Ve a: https://github.com/new
   - Nombre: `metafiance` (o el que prefieras)
   - Marca como **Público** o **Privado**
   - Haz clic en **"Create repository"**

2. **Sube tu código:**
   ```powershell
   git init
   git add .
   git commit -m "Initial commit - Metafiance app"
   git branch -M main
   git remote add origin https://github.com/TU_USUARIO/metafiance.git
   git push -u origin main
   ```

   O usa GitHub Desktop para subir los archivos.

### Paso 3: Conectar con Render

1. En Render, haz clic en **"New +"** → **"Web Service"**
2. Conecta tu repositorio de GitHub
3. Selecciona el repositorio `metafiance`
4. Render detectará automáticamente que es una aplicación Flask

### Paso 4: Configurar el Servicio

Render debería detectar automáticamente:
- **Build Command:** `pip install -r requirements.txt`
- **Start Command:** `gunicorn app:app`

Si no, configura manualmente:
- **Name:** `metafiance` (o el que prefieras)
- **Environment:** `Python 3`
- **Build Command:** `pip install -r requirements.txt`
- **Start Command:** `gunicorn app:app --bind 0.0.0.0:$PORT`

### Paso 5: Variables de Entorno (Opcional pero Recomendado)

En la sección **"Environment"**, agrega:

```
SECRET_KEY=tu-clave-secreta-muy-larga-y-aleatoria-aqui
FLASK_ENV=production
```

**Generar una clave secreta:**
```python
import secrets
print(secrets.token_hex(32))
```

### Paso 6: Desplegar

1. Haz clic en **"Create Web Service"**
2. Render comenzará a construir y desplegar tu aplicación
3. Esto tomará 2-5 minutos
4. Verás los logs en tiempo real

### Paso 7: Obtener tu URL Permanente

Una vez desplegado, obtendrás una URL como:
```
https://metafiance.onrender.com
```

**¡Esta URL es permanente y gratuita!**

## 🔒 Configuración de Seguridad

Tu aplicación ya tiene:
- ✅ HTTPS automático (Render lo proporciona)
- ✅ Headers de seguridad configurados
- ✅ Flask-Talisman activo
- ✅ Certificado SSL válido

## 📊 Plan Gratuito de Render

- ✅ **750 horas gratis al mes** (suficiente para 24/7)
- ✅ **URL permanente** con HTTPS
- ✅ **SSL automático**
- ✅ **Despliegue automático** desde GitHub
- ✅ **Logs en tiempo real**

**Nota:** Si no hay actividad por 15 minutos, la app se "duerme" pero se despierta automáticamente cuando alguien la visita (puede tomar 30 segundos).

## 🔄 Actualizaciones Automáticas

Cada vez que hagas `git push` a tu repositorio, Render:
1. Detecta el cambio automáticamente
2. Reconstruye la aplicación
3. La redespliega con los nuevos cambios

## 📝 Archivos de Configuración Incluidos

Ya están creados y listos:
- ✅ `render.yaml` - Configuración de Render
- ✅ `Procfile` - Comando de inicio
- ✅ `requirements.txt` - Dependencias
- ✅ `runtime.txt` - Versión de Python
- ✅ `.gitignore` - Archivos a ignorar

## 🎯 URLs Públicas Disponibles

Una vez desplegado, estas rutas serán públicas:

- `https://tu-app.onrender.com/` - Página principal
- `https://tu-app.onrender.com/publico` - Información pública
- `https://tu-app.onrender.com/api/public/info` - API de información
- `https://tu-app.onrender.com/api/public/estadisticas` - API de estadísticas

## 🆘 Solución de Problemas

### Error: "Build failed"
- Verifica que `requirements.txt` tenga todas las dependencias
- Revisa los logs de build en Render

### Error: "Application failed to respond"
- Verifica que el `Procfile` esté correcto
- Asegúrate de que `gunicorn` esté en `requirements.txt`

### La app se "duerme"
- Es normal en el plan gratuito
- Se despierta automáticamente en 30 segundos
- Para evitar esto, considera el plan de pago ($7/mes)

## ✅ Checklist de Despliegue

- [ ] Cuenta de Render creada
- [ ] Código subido a GitHub
- [ ] Repositorio conectado en Render
- [ ] Variables de entorno configuradas (opcional)
- [ ] Servicio creado y desplegado
- [ ] URL permanente obtenida
- [ ] Aplicación funcionando correctamente

## 🎉 ¡Listo!

Una vez completado, tendrás:
- ✅ URL permanente: `https://tu-app.onrender.com`
- ✅ HTTPS seguro
- [ ] Despliegue automático desde GitHub
- ✅ Aplicación funcionando 24/7

---

**¿Necesitas ayuda?** Revisa los logs en el dashboard de Render o consulta la documentación: https://render.com/docs

