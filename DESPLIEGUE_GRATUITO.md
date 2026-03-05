# 🚀 Guía de Despliegue Gratuito - Metafiance

Esta guía te ayudará a exponer tu aplicación Metafiance públicamente de forma gratuita.

## 📋 Opciones de Despliegue Gratuito

### 1. 🟢 Render.com (Recomendado - Más fácil)

**Ventajas:**
- ✅ Gratis para aplicaciones web
- ✅ SSL automático (HTTPS)
- ✅ Despliegue automático desde GitHub
- ✅ Base de datos PostgreSQL gratuita (opcional)

**Pasos:**

1. **Crear cuenta en Render:**
   - Ve a: https://render.com
   - Regístrate con GitHub

2. **Preparar el proyecto:**
   - Crea un archivo `render.yaml` (ya incluido)
   - Asegúrate de tener `requirements.txt`

3. **Desplegar:**
   - Conecta tu repositorio de GitHub
   - Render detectará automáticamente Flask
   - Tu app estará disponible en: `https://tu-app.onrender.com`

---

### 2. 🔵 Railway.app

**Ventajas:**
- ✅ $5 de crédito gratis al mes
- ✅ SSL automático
- ✅ Muy fácil de usar

**Pasos:**

1. Ve a: https://railway.app
2. Conecta tu repositorio de GitHub
3. Railway detectará Flask automáticamente
4. Tu app estará en: `https://tu-app.railway.app`

---

### 3. 🟡 PythonAnywhere

**Ventajas:**
- ✅ Gratis para aplicaciones básicas
- ✅ Dominio personalizado gratuito
- ✅ Base de datos MySQL incluida

**Pasos:**

1. Ve a: https://www.pythonanywhere.com
2. Crea una cuenta gratuita
3. Sube tus archivos
4. Configura el servidor web
5. Tu app estará en: `https://tu-usuario.pythonanywhere.com`

---

### 4. 🟣 ngrok (Para pruebas rápidas)

**Ventajas:**
- ✅ Instantáneo
- ✅ Perfecto para pruebas
- ✅ URL pública temporal

**Pasos:**

1. **Instalar ngrok:**
   ```powershell
   winget install ngrok
   ```

2. **Iniciar tu aplicación:**
   ```powershell
   python app.py
   ```

3. **En otra terminal, ejecutar ngrok:**
   ```powershell
   ngrok http 5000
   ```

4. **Obtendrás una URL pública como:**
   ```
   https://abc123.ngrok.io
   ```

---

### 5. 🟠 Fly.io

**Ventajas:**
- ✅ Generoso plan gratuito
- ✅ Global edge network
- ✅ SSL automático

**Pasos:**

1. Instala Fly CLI: `winget install flyctl`
2. Ejecuta: `fly launch`
3. Sigue las instrucciones
4. Tu app estará en: `https://tu-app.fly.dev`

---

## 📝 Configuración para Producción

### 1. Actualizar `app.py` para producción:

```python
if __name__ == '__main__':
    # Para desarrollo local
    if os.environ.get('FLASK_ENV') == 'development':
        app.run(debug=True, host='0.0.0.0', port=5000)
    else:
        # Para producción (usar Gunicorn)
        app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
```

### 2. Crear `Procfile` (para Render/Railway):

```
web: gunicorn app:app
```

### 3. Actualizar `requirements.txt`:

```
Flask>=2.3.0
Werkzeug>=2.3.0
gunicorn>=21.2.0
```

---

## 🔒 Seguridad para Producción

### 1. Cambiar la clave secreta:

```python
app.secret_key = os.environ.get('SECRET_KEY', os.urandom(24))
```

### 2. Variables de entorno:

Crea un archivo `.env` (no lo subas a GitHub):
```
SECRET_KEY=tu-clave-secreta-muy-larga-y-aleatoria
FLASK_ENV=production
```

### 3. Agregar `.gitignore`:

```
.env
*.db
__pycache__/
*.pyc
```

---

## 🌐 Configuración Rápida con ngrok (Prueba Inmediata)

### Script automático:

He creado `iniciar_publico.ps1` que:
1. Inicia la aplicación Flask
2. Inicia ngrok automáticamente
3. Te muestra la URL pública

**Ejecutar:**
```powershell
.\iniciar_publico.ps1
```

---

## 📊 Comparación de Opciones

| Servicio | Gratis | SSL | Base de Datos | Dificultad |
|----------|--------|-----|---------------|------------|
| Render | ✅ | ✅ | ✅ (PostgreSQL) | ⭐ Fácil |
| Railway | ✅ ($5/mes) | ✅ | ✅ | ⭐ Fácil |
| PythonAnywhere | ✅ | ✅ | ✅ (MySQL) | ⭐⭐ Media |
| ngrok | ✅ | ✅ | ❌ | ⭐ Muy Fácil |
| Fly.io | ✅ | ✅ | ✅ | ⭐⭐ Media |

---

## 🎯 Recomendación

**Para empezar rápido:** Usa **ngrok** para pruebas
**Para producción:** Usa **Render.com** o **Railway.app**

---

## 📞 URLs Públicas Creadas

Tu aplicación ahora tiene estas rutas públicas:

- `/` - Página principal
- `/publico` o `/public` - Información pública
- `/api/public/info` - API de información
- `/api/public/estadisticas` - API de estadísticas

Todas estas rutas son accesibles sin autenticación.

---

## ✅ Checklist de Despliegue

- [ ] Aplicación funciona localmente
- [ ] `requirements.txt` actualizado
- [ ] `Procfile` creado (si usas Render/Railway)
- [ ] Variables de entorno configuradas
- [ ] `.gitignore` configurado
- [ ] Código subido a GitHub
- [ ] Servicio de despliegue configurado
- [ ] URL pública funcionando
- [ ] SSL/HTTPS activo

---

## 🆘 Solución de Problemas

### Error: "Application failed to respond"
- Verifica que el puerto sea el correcto
- Asegúrate de que `host='0.0.0.0'` esté configurado

### Error: "Module not found"
- Verifica que todas las dependencias estén en `requirements.txt`
- Ejecuta: `pip install -r requirements.txt`

### Base de datos no funciona
- Algunos servicios requieren PostgreSQL en lugar de SQLite
- Considera usar una base de datos en la nube

---

¡Tu aplicación está lista para ser pública! 🎉

