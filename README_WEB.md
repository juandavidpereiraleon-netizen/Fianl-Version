# Metafiance - Aplicación Web

Sistema de gestión de metas financieras para estudiantes - Versión Web

## 🚀 Inicio Rápido

### Instalación

1. Instala las dependencias:
```bash
pip install -r requirements.txt
```

2. Ejecuta la aplicación:
```bash
python app.py
```

3. Abre tu navegador y visita:
```
http://localhost:5000
```

## 📋 Características

- ✅ Autenticación de usuarios (registro e inicio de sesión)
- ✅ Panel de administrador para gestionar metas
- ✅ Panel de usuario para ver y gestionar metas personales
- ✅ Registro de ahorros y salidas
- ✅ Historial de movimientos
- ✅ Visualización de progreso con barras de progreso
- ✅ Asignación automática de metas por curso
- ✅ Interfaz moderna con colores azul, verde militar y blanco

## 🎨 Colores de la Interfaz

- **Azul**: `#3366CC` - Botones principales y acciones importantes
- **Verde Militar**: `#4d7a2e` - Botones secundarios y acciones de confirmación
- **Blanco**: `#FFFFFF` - Fondos y texto

## 👤 Usuario Administrador

**Credenciales por defecto:**
- Email: `admin@metafiance.com`
- Contraseña: `admin123`

## 📁 Estructura del Proyecto

```
.
├── app.py                 # Aplicación Flask principal
├── database.py            # Módulo de base de datos
├── requirements.txt        # Dependencias
├── templates/             # Templates HTML
│   ├── base.html
│   ├── index.html
│   ├── login.html
│   ├── register.html
│   ├── admin_panel.html
│   ├── admin_metas.html
│   ├── admin_agregar_meta.html
│   ├── admin_editar_meta.html
│   ├── usuario_metas.html
│   ├── usuario_meta_detalle.html
│   ├── registrar_ahorro.html
│   ├── registrar_salida.html
│   ├── historial.html
│   └── detalle_plan.html
└── static/                # Archivos estáticos
    ├── css/
    │   └── style.css
    └── js/
```

## 🔧 Configuración

### Cambiar el puerto

Edita `app.py` y modifica la última línea:
```python
app.run(debug=True, host='0.0.0.0', port=5000)  # Cambia 5000 por el puerto deseado
```

### Modo de producción

Para producción, desactiva el modo debug:
```python
app.run(debug=False, host='0.0.0.0', port=5000)
```

## 🌐 Despliegue

### Opción 1: Heroku

1. Crea un archivo `Procfile`:
```
web: python app.py
```

2. Despliega en Heroku:
```bash
heroku create metafiance
git push heroku main
```

### Opción 2: Servidor VPS

1. Usa Gunicorn:
```bash
pip install gunicorn
gunicorn -w 4 -b 0.0.0.0:5000 app:app
```

2. Configura Nginx como proxy reverso

### Opción 3: Docker

Crea un `Dockerfile`:
```dockerfile
FROM python:3.9-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
CMD ["python", "app.py"]
```

## 📝 Notas

- La base de datos SQLite se crea automáticamente al ejecutar la aplicación
- Las contraseñas se almacenan con hash SHA256
- Los usuarios se asignan automáticamente a las metas de su curso al iniciar sesión
- La aplicación usa sesiones de Flask para mantener el estado del usuario

## 🔒 Seguridad

- Las contraseñas están hasheadas
- Las sesiones están protegidas
- Validación de entrada en todos los formularios
- Protección contra CSRF (considera agregar Flask-WTF para producción)

## 📞 Soporte

Para problemas o preguntas, revisa la documentación de Flask o contacta al administrador del sistema.

