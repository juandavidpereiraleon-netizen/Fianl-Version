# 🚀 Guía Rápida - URL Permanente en Render.com

## ⚡ Pasos Rápidos (5 minutos)

### 1️⃣ Crear Repositorio en GitHub

1. Ve a: **https://github.com/new**
2. Nombre: `metafiance`
3. Marca **Público** o **Privado**
4. **NO** marques "Initialize with README"
5. Haz clic en **"Create repository"**

### 2️⃣ Subir Código a GitHub

Abre PowerShell en esta carpeta y ejecuta:

```powershell
git init
git add .
git commit -m "Initial commit - Metafiance app"
git branch -M main
git remote add origin https://github.com/TU_USUARIO/metafiance.git
git push -u origin main
```

**Reemplaza `TU_USUARIO` con tu nombre de usuario de GitHub**

### 3️⃣ Desplegar en Render

1. Ve a: **https://render.com**
2. Haz clic en **"Get Started for Free"**
3. Regístrate con **GitHub** (recomendado)
4. Haz clic en **"New +"** → **"Web Service"**
5. Conecta tu repositorio `metafiance`
6. Render detectará Flask automáticamente
7. Haz clic en **"Create Web Service"**

### 4️⃣ ¡Listo!

En 2-5 minutos tendrás tu URL permanente:
```
https://metafiance.onrender.com
```

## ✅ Lo que ya está configurado

- ✅ `render.yaml` - Configuración de Render
- ✅ `Procfile` - Comando de inicio
- ✅ `requirements.txt` - Todas las dependencias
- ✅ `runtime.txt` - Versión de Python
- ✅ Seguridad HTTPS activa
- ✅ Headers de seguridad configurados

## 🔒 Seguridad Incluida

- ✅ HTTPS automático
- ✅ Certificado SSL válido
- ✅ Protección XSS
- ✅ Protección clickjacking
- ✅ HSTS activo

## 📱 URLs Públicas

Una vez desplegado:
- `https://tu-app.onrender.com/`
- `https://tu-app.onrender.com/publico`
- `https://tu-app.onrender.com/api/public/info`
- `https://tu-app.onrender.com/api/public/estadisticas`

## 💡 Tips

- **Plan Gratuito:** 750 horas/mes (suficiente para 24/7)
- **Auto-deploy:** Cada `git push` actualiza automáticamente
- **Logs:** Disponibles en tiempo real en Render
- **Sleep:** Si no hay actividad 15 min, se "duerme" pero se despierta automáticamente

## 🆘 Problemas Comunes

**Error al hacer push:**
```powershell
git config --global user.name "Tu Nombre"
git config --global user.email "tu@email.com"
```

**Render no detecta Flask:**
- Verifica que `Procfile` esté en la raíz
- Verifica que `requirements.txt` tenga `gunicorn`

**App no inicia:**
- Revisa los logs en Render
- Verifica que el puerto sea `$PORT` en el Procfile

---

**¿Listo?** Sigue los pasos arriba y tendrás tu URL permanente en minutos! 🎉

