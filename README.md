# Metafiance - Sistema de Gestión de Metas Financieras

Sistema de gestión de metas financieras para estudiantes desarrollado con KivyMD y Python.

## Características

- **Autenticación de usuarios**: Registro e inicio de sesión seguro
- **Panel de administrador**: Gestión completa de metas
- **Panel de usuario**: Visualización y seguimiento de metas personales
- **Base de datos SQLite**: Almacenamiento persistente de toda la información
- **Interfaz moderna**: Diseño con colores azul, verde militar y blanco

## Requisitos

- Python 3.7 o superior
- Kivy 2.1.0 o superior
- KivyMD 1.1.1 o superior

## Instalación

1. Instala las dependencias:
```bash
pip install -r requirements.txt
```

## Uso

1. Ejecuta la aplicación:
```bash
python main.py
```

2. **Usuario administrador por defecto:**
   - Email: `admin@metafiance.com`
   - Contraseña: `admin123`

3. **Registro de usuarios:**
   - Los usuarios pueden registrarse desde la pantalla principal
   - Al iniciar sesión, se les asignan automáticamente las metas de su curso

## Estructura del Proyecto

- `main.py`: Aplicación principal con todas las pantallas
- `database.py`: Módulo de base de datos SQLite
- `app.kv`: Archivo de diseño de la interfaz (Kivy Language)
- `requirements.txt`: Dependencias del proyecto

## Base de Datos

La aplicación utiliza SQLite con las siguientes tablas:
- `usuarios`: Información de usuarios
- `metas`: Metas financieras creadas por administradores
- `usuario_metas`: Relación entre usuarios y metas
- `movimientos`: Registro de ahorros y salidas

## Colores de la Interfaz

- **Azul**: `(0.2, 0.4, 0.8, 1)` - Botones principales y acciones importantes
- **Verde Militar**: `(0.3, 0.5, 0.2, 1)` - Botones secundarios y acciones de confirmación
- **Blanco**: `(1, 1, 1, 1)` - Fondo y texto

## Funcionalidades

### Administrador
- Crear, editar y eliminar metas
- Ver todas las metas del sistema
- Gestionar usuarios (pendiente)

### Usuario
- Ver metas asignadas a su curso
- Registrar ahorros
- Registrar salidas
- Ver historial de movimientos
- Ver detalle del plan de ahorro

## Notas

- La base de datos se crea automáticamente al ejecutar la aplicación
- Las contraseñas se almacenan con hash SHA256
- Los usuarios se asignan automáticamente a las metas de su curso al iniciar sesión

