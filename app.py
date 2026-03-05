"""
Aplicación web Metafiance
Sistema de gestión de metas financieras para estudiantes
"""
from flask import Flask, render_template, request, redirect, url_for, session, flash, jsonify
from flask_talisman import Talisman
from functools import wraps
from database import Database
from datetime import datetime, timedelta
import os
 

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'dev-secret-key')

debug_env = os.environ.get('FLASK_ENV') != 'production'
if not debug_env:
    try:
        Talisman(app, force_https=True, strict_transport_security=True, 
                 strict_transport_security_max_age=31536000,
                 content_security_policy={
                     'default-src': "'self'",
                     'style-src': "'self' 'unsafe-inline' https://cdn.jsdelivr.net",
                     'script-src': "'self' 'unsafe-inline' https://cdn.jsdelivr.net",
                     'img-src': "'self' data: https:",
                     'font-src': "'self' https://cdn.jsdelivr.net"
                 })
    except ImportError:
        pass
else:
    app.config['SESSION_COOKIE_SECURE'] = False
    app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'
    app.permanent_session_lifetime = timedelta(days=7)

# Inicializar base de datos
db = Database()

@app.template_filter('money')
def money(value, decimals=0):
    try:
        value = float(value)
        if decimals <= 0:
            return f"{value:,.0f}".replace(",", ".")
        return f"{value:,.{decimals}f}".replace(",", ".")
    except Exception:
        return str(value)


def login_required(f):
    """Decorador para requerir autenticación"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            flash('Por favor inicia sesión para acceder a esta página', 'warning')
            return redirect(url_for('login'))
        usuario = db.obtener_usuario(session['user_id'])
        if not usuario:
            session.clear()
            flash('Tu sesión ha expirado. Por favor inicia sesión nuevamente.', 'warning')
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function


def admin_required(f):
    """Decorador para requerir permisos de administrador"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            flash('Por favor inicia sesión', 'warning')
            return redirect(url_for('login'))
        usuario = db.obtener_usuario(session['user_id'])
        if not usuario or not usuario['es_admin']:
            flash('No tienes permisos para acceder a esta página', 'danger')
            return redirect(url_for('usuario_metas'))
        return f(*args, **kwargs)
    return decorated_function


@app.route('/')
def index():
    """Página principal pública"""
    if 'user_id' in session:
        usuario = db.obtener_usuario(session['user_id'])
        if not usuario:
            session.clear()
            return render_template('index.html')
        if usuario['es_admin']:
            return redirect(url_for('admin_panel'))
        else:
            return redirect(url_for('usuario_metas'))
    return render_template('index.html')

 

@app.route('/publico')
@app.route('/public')
def publico():
    """Página pública - Información general sin autenticación"""
    # Estadísticas públicas (sin datos sensibles)
    total_metas = len(db.obtener_metas())
    return render_template('publico.html', total_metas=total_metas)


@app.route('/api/public/info')
def api_public_info():
    """API pública - Información general de la aplicación"""
    return jsonify({
        'nombre': 'Metafiance',
        'version': '1.0.0',
        'descripcion': 'Sistema de gestión de metas financieras para estudiantes',
        'total_metas': len(db.obtener_metas()),
        'estado': 'activo'
    })


@app.route('/api/public/estadisticas')
def api_public_estadisticas():
    """API pública - Estadísticas generales (sin datos sensibles)"""
    metas = db.obtener_metas()
    cursos_disponibles = list(set([meta['curso'] for meta in metas]))
    
    return jsonify({
        'total_metas': len(metas),
        'cursos_disponibles': cursos_disponibles,
        'total_cursos': len(cursos_disponibles)
    })


@app.route('/login', methods=['GET', 'POST'])
def login():
    """Página de inicio de sesión"""
    if request.method == 'POST':
        email = request.form.get('email', '').strip()
        password = request.form.get('password', '').strip()
        
        if not email or not password:
            flash('Por favor complete todos los campos', 'danger')
            return render_template('login.html')
        
        usuario = db.autenticar_usuario(email, password)
        
        if usuario:
            session['user_id'] = usuario['id']
            session['user_name'] = usuario['nombre_estudiante']
            session['user_parent_name'] = usuario['nombre_padre']
            session['is_admin'] = usuario['es_admin']
            session.permanent = True
            
            # Asignar metas del curso si no las tiene
            asignar_metas_curso(usuario)
            
            if usuario['es_admin']:
                return redirect(url_for('admin_panel'))
            else:
                return redirect(url_for('usuario_metas'))
        else:
            flash('Credenciales incorrectas', 'danger')
    
    return render_template('login.html')


@app.route('/register', methods=['GET', 'POST'])
def register():
    """Página de registro"""
    if request.method == 'POST':
        nombre_padre = request.form.get('nombre_padre', '').strip()
        nombre_estudiante = request.form.get('nombre_estudiante', '').strip()
        email = request.form.get('email', '').strip()
        password = request.form.get('password', '').strip()
        curso = request.form.get('curso', '').strip()
        promocion = request.form.get('promocion', '').strip()
        
        if not all([nombre_padre, nombre_estudiante, email, password, curso, promocion]):
            flash('Por favor complete todos los campos', 'danger')
            return render_template('register.html')
        
        if db.registrar_usuario(nombre_padre, nombre_estudiante, email, 
                                password, curso, promocion):
            flash('Usuario registrado correctamente. Por favor inicia sesión', 'success')
            return redirect(url_for('login'))
        else:
            flash('El correo electrónico ya está registrado', 'danger')
    
    cursos = ["9", "10", "11"]
    promociones = ["2025/2026", "2026/2027", "2027/2028"]
    return render_template('register.html', cursos=cursos, promociones=promociones)


@app.route('/logout')
def logout():
    """Cerrar sesión"""
    session.clear()
    flash('Sesión cerrada correctamente', 'info')
    return redirect(url_for('index'))


def asignar_metas_curso(usuario):
    """Asigna automáticamente las metas del curso al usuario"""
    metas_usuario = db.obtener_metas_usuario(usuario['id'])
    if not metas_usuario:
        todas_metas = db.obtener_metas()
        for meta in todas_metas:
            if meta['curso'] == usuario['curso']:
                db.asignar_meta_usuario(usuario['id'], meta['id'])


# ========== RUTAS DE ADMINISTRADOR ==========

@app.route('/admin')
@admin_required
def admin_panel():
    """Panel de administrador"""
    return render_template('admin_panel.html')

@app.route('/admin/users')
@admin_required
def admin_users():
    """Listado de usuarios (admin)"""
    usuarios = db.listar_usuarios()
    return render_template('admin_users.html', usuarios=usuarios)

@app.route('/admin/logs')
@admin_required
def admin_logs():
    """Registros globales (admin)"""
    registros = db.listar_registros()
    return render_template('admin_logs.html', registros=registros)

@app.route('/admin/migrate/wipe', methods=['POST'])
@admin_required
def admin_wipe():
    """Wipe de datos excepto admin"""
    db.wipe_except_admin()
    flash('Datos borrados excepto el usuario administrador.', 'success')
    return redirect(url_for('admin_panel'))


@app.route('/admin/metas')
@admin_required
def admin_metas():
    """Gestión de metas (admin)"""
    metas = db.obtener_metas()
    return render_template('admin_metas.html', metas=metas)


@app.route('/admin/metas/agregar', methods=['GET', 'POST'])
@admin_required
def admin_agregar_meta():
    """Agregar nueva meta"""
    if request.method == 'POST':
        nombre = request.form.get('nombre', '').strip()
        curso_raw = request.form.get('curso', '').strip()
        curso = ''.join(filter(str.isdigit, curso_raw)) or curso_raw
        fecha_limite = request.form.get('fecha_limite', '').strip()
        costo_text = request.form.get('costo_estimado', '').strip()
        
        if not all([nombre, curso, fecha_limite, costo_text]):
            flash('Por favor complete todos los campos', 'danger')
            return render_template('admin_agregar_meta.html')
        
        try:
            costo = float(costo_text.replace('.', '').replace(',', '.'))
            if costo <= 0:
                raise ValueError
        except ValueError:
            flash('El costo debe ser un número positivo', 'danger')
            return render_template('admin_agregar_meta.html')
        
        try:
            meta_id = db.crear_meta(nombre, curso, fecha_limite, costo)
            if meta_id:
                usuarios = db.listar_usuarios()
                for u in usuarios:
                    if str(u['curso']) == str(curso):
                        db.asignar_meta_usuario(u['id'], meta_id)
                flash('Meta creada correctamente', 'success')
                return redirect(url_for('admin_metas'))
            else:
                flash('No se pudo crear la meta (ID vacío)', 'danger')
        except Exception as e:
            flash(f'Error al crear meta: {str(e)}', 'danger')
            return render_template('admin_agregar_meta.html')
    
    cursos = ["9", "10", "11"]
    return render_template('admin_agregar_meta.html', cursos=cursos)


@app.route('/admin/metas/editar/<int:meta_id>', methods=['GET', 'POST'])
@admin_required
def admin_editar_meta(meta_id):
    """Editar meta existente"""
    meta = db.obtener_meta(meta_id)
    if not meta:
        flash('Meta no encontrada', 'danger')
        return redirect(url_for('admin_metas'))
    
    if request.method == 'POST':
        nombre = request.form.get('nombre', '').strip()
        curso_raw = request.form.get('curso', '').strip()
        curso = ''.join(filter(str.isdigit, curso_raw)) or curso_raw
        fecha_limite = request.form.get('fecha_limite', '').strip()
        costo_text = request.form.get('costo_estimado', '').strip()
        
        if not all([nombre, curso, fecha_limite, costo_text]):
            flash('Por favor complete todos los campos', 'danger')
            return render_template('admin_editar_meta.html', meta=meta)
        
        try:
            costo = float(costo_text.replace('.', '').replace(',', '.'))
            if costo <= 0:
                raise ValueError
        except ValueError:
            flash('El costo debe ser un número positivo', 'danger')
            return render_template('admin_editar_meta.html', meta=meta)
        
        if db.actualizar_meta(meta_id, nombre, curso, fecha_limite, costo):
            flash('Meta actualizada correctamente', 'success')
            return redirect(url_for('admin_metas'))
        else:
            flash('No se pudo actualizar la meta', 'danger')
    
    cursos = ["9", "10", "11"]
    return render_template('admin_editar_meta.html', meta=meta, cursos=cursos)


@app.route('/admin/metas/eliminar/<int:meta_id>', methods=['POST'])
@admin_required
def admin_eliminar_meta(meta_id):
    """Eliminar meta"""
    if db.eliminar_meta(meta_id):
        flash('Meta eliminada correctamente', 'success')
    else:
        flash('No se pudo eliminar la meta', 'danger')
    return redirect(url_for('admin_metas'))


# ========== RUTAS DE USUARIO ==========

@app.route('/usuario/metas')
@login_required
def usuario_metas():
    """Pantalla de metas del usuario"""
    usuario = db.obtener_usuario(session['user_id'])
    
    # Asegurar que el usuario tenga metas asignadas
    metas = db.obtener_metas_usuario(usuario['id'])
    if not metas:
        todas_metas = db.obtener_metas()
        for meta in todas_metas:
            if meta['curso'] == usuario['curso']:
                db.asignar_meta_usuario(usuario['id'], meta['id'])
        metas = db.obtener_metas_usuario(usuario['id'])
    
    # Calcular progreso total
    total_ahorrado = 0
    total_costo = 0
    metas_con_progreso = []
    
    for meta in metas:
        balance = db.calcular_balance_meta(usuario['id'], meta['id'])
        total_ahorrado += balance['balance']
        total_costo += balance['costo_estimado']
        meta['balance'] = balance
        meta['progreso'] = (balance['balance'] / balance['costo_estimado'] * 100) if balance['costo_estimado'] > 0 else 0
        metas_con_progreso.append(meta)
    
    progreso_total = (total_ahorrado / total_costo * 100) if total_costo > 0 else 0
    
    return render_template('usuario_metas.html', 
                         usuario=usuario, 
                         metas=metas_con_progreso,
                         total_ahorrado=total_ahorrado,
                         total_costo=total_costo,
                         progreso_total=progreso_total)


@app.route('/usuario/meta/<int:meta_id>')
@login_required
def usuario_meta_detalle(meta_id):
    """Detalle de una meta específica"""
    usuario = db.obtener_usuario(session['user_id'])
    meta = db.obtener_meta(meta_id)
    
    if not meta:
        flash('Meta no encontrada', 'danger')
        return redirect(url_for('usuario_metas'))
    
    balance = db.calcular_balance_meta(usuario['id'], meta_id)
    progreso = (balance['balance'] / balance['costo_estimado'] * 100) if balance['costo_estimado'] > 0 else 0
    
    return render_template('usuario_meta_detalle.html', 
                         usuario=usuario, 
                         meta=meta, 
                         balance=balance,
                         progreso=progreso)


@app.route('/usuario/meta/<int:meta_id>/ahorro', methods=['GET', 'POST'])
@login_required
def usuario_registrar_ahorro(meta_id):
    """Registrar un ahorro"""
    usuario = db.obtener_usuario(session['user_id'])
    meta = db.obtener_meta(meta_id)
    
    if not meta:
        flash('Meta no encontrada', 'danger')
        return redirect(url_for('usuario_metas'))
    
    if request.method == 'POST':
        monto_text = request.form.get('monto', '').strip().replace('.', '').replace(',', '.')
        
        if not monto_text:
            flash('Por favor ingrese un monto', 'danger')
            return render_template('registrar_ahorro.html', meta=meta)
        
        try:
            monto = float(monto_text)
            if monto <= 0:
                raise ValueError
        except ValueError:
            flash('El monto debe ser un número positivo', 'danger')
            return render_template('registrar_ahorro.html', meta=meta)
        
        if db.registrar_movimiento(usuario['id'], meta_id, 'ahorro', monto, "Ahorro registrado"):
            flash('Ahorro registrado correctamente', 'success')
            return redirect(url_for('usuario_meta_detalle', meta_id=meta_id))
        else:
            flash('No se pudo registrar el ahorro', 'danger')
    
    return render_template('registrar_ahorro.html', meta=meta)


@app.route('/usuario/meta/<int:meta_id>/salida', methods=['GET', 'POST'])
@login_required
def usuario_registrar_salida(meta_id):
    """Registrar una salida"""
    usuario = db.obtener_usuario(session['user_id'])
    meta = db.obtener_meta(meta_id)
    
    if not meta:
        flash('Meta no encontrada', 'danger')
        return redirect(url_for('usuario_metas'))
    
    if request.method == 'POST':
        monto_text = request.form.get('monto', '').strip().replace('.', '').replace(',', '.')
        
        if not monto_text:
            flash('Por favor ingrese un monto', 'danger')
            return render_template('registrar_salida.html', meta=meta)
        
        try:
            monto = float(monto_text)
            if monto <= 0:
                raise ValueError
        except ValueError:
            flash('El monto debe ser un número positivo', 'danger')
            return render_template('registrar_salida.html', meta=meta)
        
        if db.registrar_movimiento(usuario['id'], meta_id, 'salida', monto, "Salida registrada"):
            flash('Salida registrada correctamente', 'success')
            return redirect(url_for('usuario_meta_detalle', meta_id=meta_id))
        else:
            flash('No se pudo registrar la salida', 'danger')
    
    return render_template('registrar_salida.html', meta=meta)


@app.route('/usuario/meta/<int:meta_id>/historial')
@login_required
def usuario_historial(meta_id):
    """Historial de movimientos de una meta"""
    usuario = db.obtener_usuario(session['user_id'])
    meta = db.obtener_meta(meta_id)
    
    if not meta:
        flash('Meta no encontrada', 'danger')
        return redirect(url_for('usuario_metas'))
    
    movimientos = db.obtener_movimientos_meta(usuario['id'], meta_id)
    
    return render_template('historial.html', 
                         usuario=usuario, 
                         meta=meta, 
                         movimientos=movimientos)


@app.route('/usuario/meta/<int:meta_id>/plan')
@login_required
def usuario_detalle_plan(meta_id):
    """Detalle del plan de ahorro"""
    usuario = db.obtener_usuario(session['user_id'])
    meta = db.obtener_meta(meta_id)
    
    if not meta:
        flash('Meta no encontrada', 'danger')
        return redirect(url_for('usuario_metas'))
    
    balance = db.calcular_balance_meta(usuario['id'], meta_id)
    
    return render_template('detalle_plan.html', 
                         usuario=usuario, 
                         meta=meta, 
                         balance=balance)



@app.route('/api/usuario/resumen')
@login_required
def api_usuario_resumen():
    usuario = db.obtener_usuario(session['user_id'])
    metas = db.obtener_metas_usuario(usuario['id'])
    total_ahorrado = 0
    total_costo = 0
    for meta in metas:
        balance = db.calcular_balance_meta(usuario['id'], meta['id'])
        total_ahorrado += balance['balance']
        total_costo += balance['costo_estimado']
    progreso_total = (total_ahorrado / total_costo * 100) if total_costo > 0 else 0
    return jsonify({
        'usuario': {
            'id': usuario['id'],
            'nombre_estudiante': usuario['nombre_estudiante'],
            'curso': usuario['curso'],
            'es_admin': usuario['es_admin'],
            'nombre_padre': usuario['nombre_padre']
        },
        'totales': {
            'ahorrado': total_ahorrado,
            'costo_estimado': total_costo,
            'progreso': progreso_total
        }
    })


@app.route('/api/usuario/metas')
@login_required
def api_usuario_metas():
    usuario = db.obtener_usuario(session['user_id'])
    metas = db.obtener_metas_usuario(usuario['id'])
    metas_con_progreso = []
    for meta in metas:
        balance = db.calcular_balance_meta(usuario['id'], meta['id'])
        progreso = (balance['balance'] / balance['costo_estimado'] * 100) if balance['costo_estimado'] > 0 else 0
        metas_con_progreso.append({
            **meta,
            'balance': balance,
            'progreso': progreso
        })
    return jsonify({'metas': metas_con_progreso})


@app.route('/api/usuario/meta/<int:meta_id>')
@login_required
def api_usuario_meta_detalle(meta_id):
    usuario = db.obtener_usuario(session['user_id'])
    meta = db.obtener_meta(meta_id)
    if not meta:
        return jsonify({'error': 'Meta no encontrada'}), 404
    balance = db.calcular_balance_meta(usuario['id'], meta_id)
    progreso = (balance['balance'] / balance['costo_estimado'] * 100) if balance['costo_estimado'] > 0 else 0
    return jsonify({'meta': meta, 'balance': balance, 'progreso': progreso})


@app.route('/api/usuario/meta/<int:meta_id>/movimientos')
@login_required
def api_usuario_meta_movimientos(meta_id):
    usuario = db.obtener_usuario(session['user_id'])
    meta = db.obtener_meta(meta_id)
    if not meta:
        return jsonify({'error': 'Meta no encontrada'}), 404
    movimientos = db.obtener_movimientos_meta(usuario['id'], meta_id)
    return jsonify({'movimientos': movimientos})


@app.route('/api/usuario/meta/<int:meta_id>/ahorro', methods=['POST'])
@login_required
def api_usuario_registrar_ahorro(meta_id):
    usuario = db.obtener_usuario(session['user_id'])
    meta = db.obtener_meta(meta_id)
    if not meta:
        return jsonify({'error': 'Meta no encontrada'}), 404
    monto_text = request.json.get('monto', '') if request.is_json else request.form.get('monto', '')
    try:
        monto = float(monto_text)
        if monto <= 0:
            raise ValueError
    except ValueError:
        return jsonify({'error': 'El monto debe ser un número positivo'}), 400
    ok = db.registrar_movimiento(usuario['id'], meta_id, 'ahorro', monto, "Ahorro registrado")
    if ok:
        return jsonify({'status': 'ok'}), 200
    return jsonify({'error': 'No se pudo registrar el ahorro'}), 500


@app.route('/api/usuario/meta/<int:meta_id>/salida', methods=['POST'])
@login_required
def api_usuario_registrar_salida(meta_id):
    usuario = db.obtener_usuario(session['user_id'])
    meta = db.obtener_meta(meta_id)
    if not meta:
        return jsonify({'error': 'Meta no encontrada'}), 404
    monto_text = request.json.get('monto', '') if request.is_json else request.form.get('monto', '')
    try:
        monto = float(monto_text)
        if monto <= 0:
            raise ValueError
    except ValueError:
        return jsonify({'error': 'El monto debe ser un número positivo'}), 400
    ok = db.registrar_movimiento(usuario['id'], meta_id, 'salida', monto, "Salida registrada")
    if ok:
        return jsonify({'status': 'ok'}), 200
    return jsonify({'error': 'No se pudo registrar la salida'}), 500


if __name__ == '__main__':
    # Configuración para desarrollo y producción
    port = int(os.environ.get('PORT', 5000))
    debug = os.environ.get('FLASK_ENV') != 'production'
    
    # Para producción en Render/Railway, usar host='0.0.0.0'
    app.run(debug=debug, host='0.0.0.0', port=port, threaded=True)
