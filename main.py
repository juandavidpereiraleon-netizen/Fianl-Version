"""
Aplicación principal Metafiance
Sistema de gestión de metas financieras para estudiantes
"""
from kivymd.app import MDApp
from kivymd.uix.screen import MDScreen
from kivymd.uix.boxlayout import MDBoxLayout
from kivymd.uix.label import MDLabel
from kivymd.uix.button import MDRaisedButton, MDFlatButton
from kivymd.uix.textfield import MDTextField
from kivymd.uix.menu import MDDropdownMenu
from kivymd.uix.dialog import MDDialog
from kivymd.uix.card import MDCard
from kivymd.uix.progressbar import MDProgressBar
from kivy.uix.scrollview import ScrollView
from kivy.metrics import dp
from kivy.lang import Builder
from kivy.properties import ObjectProperty, StringProperty, NumericProperty
from database import Database
from typing import Optional, List, Dict


def formatear_cop(valor: float) -> str:
    """
    Formatea un valor numérico como moneda colombiana (COP).
    Ejemplo: 3900000 -> "3.900.000"
    """
    # Convertir a entero para quitar decimales
    valor_int = int(round(valor))
    # Formatear con separadores de miles (punto)
    valor_str = f"{valor_int:,}".replace(",", ".")
    return valor_str


class HomeScreen(MDScreen):
    """Pantalla principal de la aplicación"""
    
    def go_to_login(self, instance):
        """Navega a la pantalla de login"""
        self.manager.current = "login"
    
    def go_to_register(self, instance):
        """Navega a la pantalla de registro"""
        self.manager.current = "register"


class LoginScreen(MDScreen):
    """Pantalla de inicio de sesión"""
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.db = Database()
    
    def login(self, instance):
        """Autentica al usuario"""
        email = self.ids.email.text.strip()
        password = self.ids.password.text.strip()
        
        if not email or not password:
            self.show_dialog("Error", "Por favor complete todos los campos")
            return
        
        usuario = self.db.autenticar_usuario(email, password)
        
        if usuario:
            app = MDApp.get_running_app()
            app.current_user = usuario
            
            if usuario['es_admin']:
                self.manager.current = "admin_panel"
            else:
                # Asignar metas del curso del usuario si no las tiene
                self.asignar_metas_curso(usuario)
                self.manager.current = "usuario_metas"
                # Actualizar la pantalla de metas del usuario
                usuario_metas_screen = self.manager.get_screen("usuario_metas")
                usuario_metas_screen.usuario = usuario
                usuario_metas_screen.cargar_metas()
        else:
            self.show_dialog("Error", "Credenciales incorrectas")
    
    def asignar_metas_curso(self, usuario: Dict):
        """Asigna automáticamente las metas del curso al usuario"""
        # Obtener todas las metas del curso del usuario
        todas_metas = self.db.obtener_metas()
        metas_curso = [meta for meta in todas_metas if meta['curso'] == usuario['curso']]
        
        # Obtener las metas que ya tiene asignadas el usuario
        metas_usuario = self.db.obtener_metas_usuario(usuario['id'])
        ids_metas_asignadas = {meta['id'] for meta in metas_usuario}
        
        # Asignar solo las metas del curso que el usuario aún no tiene
        for meta in metas_curso:
            if meta['id'] not in ids_metas_asignadas:
                self.db.asignar_meta_usuario(usuario['id'], meta['id'])
    
    def go_to_home(self, instance):
        """Regresa a la pantalla principal"""
        self.ids.email.text = ""
        self.ids.password.text = ""
        self.manager.current = "home"
    
    def show_dialog(self, title: str, text: str):
        """Muestra un diálogo"""
        dialog = MDDialog(
            title=title,
            text=text,
            buttons=[
                MDFlatButton(text="OK", on_release=lambda x: dialog.dismiss())
            ]
        )
        dialog.open()


class RegisterScreen(MDScreen):
    """Pantalla de registro de usuario"""

    # Propiedades para que existan antes de que Kivy llame on_kv_post
    menu_curso = ObjectProperty(allownone=True)
    menu_promocion = ObjectProperty(allownone=True)

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.db = Database()
        self.curso_seleccionado = None
        self.promocion_seleccionada = None
        # Los menús dependen de los ids definidos en el KV, que aún no
        # están disponibles durante __init__. Se inicializan después
        # de que el árbol KV se haya construido completamente.
        self.menu_curso = None
        self.menu_promocion = None

    def on_kv_post(self, base_widget):
        """
        Se ejecuta cuando los ids definidos en el archivo KV ya están
        disponibles. Aquí es seguro crear los menús desplegables.
        """
        super().on_kv_post(base_widget)
        self.ensure_menus()

    def ensure_menus(self):
        """
        Crea los menús si aún no existen.
        Se llama desde on_kv_post y también antes de intentar abrirlos
        (por seguridad).
        """
        if self.menu_curso is None or self.menu_promocion is None:
            self.setup_menus()

    def open_curso_menu(self):
        """Abre el menú de curso (spinner) de forma segura."""
        self.ensure_menus()
        if self.menu_curso:
            self.menu_curso.open()

    def open_promocion_menu(self):
        """Abre el menú de promoción (spinner) de forma segura."""
        self.ensure_menus()
        if self.menu_promocion:
            self.menu_promocion.open()
    
    def setup_menus(self):
        """Configura los menús desplegables"""
        # Requisitos solicitados:
        # - Curso: 9°, 10°, 11°
        # - Promoción: 2025/2026, 2026/2027, 2027/2028
        cursos = ["9°", "10°", "11°"]
        promociones = ["2025/2026", "2026/2027", "2027/2028"]
        
        menu_items_curso = [
            {
                "text": curso,
                "viewclass": "OneLineListItem",
                "on_release": lambda x=curso: self.seleccionar_curso(x),
            } for curso in cursos
        ]
        
        menu_items_promocion = [
            {
                "text": promocion,
                "viewclass": "OneLineListItem",
                "on_release": lambda x=promocion: self.seleccionar_promocion(x),
            } for promocion in promociones
        ]
        
        self.menu_curso = MDDropdownMenu(
            caller=self.ids.curso,
            items=menu_items_curso,
            width_mult=4,
        )
        
        self.menu_promocion = MDDropdownMenu(
            caller=self.ids.promocion,
            items=menu_items_promocion,
            width_mult=4,
        )
    
    def seleccionar_curso(self, curso: str):
        """Selecciona un curso"""
        self.curso_seleccionado = curso
        self.ids.curso.text = f"Curso: {curso}"
        self.menu_curso.dismiss()
    
    def seleccionar_promocion(self, promocion: str):
        """Selecciona una promoción"""
        self.promocion_seleccionada = promocion
        self.ids.promocion.text = f"Promoción: {promocion}"
        self.menu_promocion.dismiss()
    
    def register(self, instance):
        """Registra un nuevo usuario"""
        nombre_padre = self.ids.padre.text.strip()
        nombre_estudiante = self.ids.estudiante.text.strip()
        email = self.ids.email.text.strip()
        password = self.ids.password.text.strip()

        # Validar dominios de correo permitidos
        email_lower = email.lower()
        dominios_permitidos = ("@gmail.com", "@hotmail.com", "@yahoo.com")
        if "@" not in email_lower or not email_lower.endswith(dominios_permitidos):
            self.show_dialog(
                "Error",
                "Correo inválido. Dominios permitidos: @gmail.com, @hotmail.com, @yahoo.com",
            )
            return
        
        if not all([nombre_padre, nombre_estudiante, email, password]):
            self.show_dialog("Error", "Por favor complete todos los campos")
            return
        
        if not self.curso_seleccionado:
            self.show_dialog("Error", "Por favor seleccione un curso")
            return
        
        if not self.promocion_seleccionada:
            self.show_dialog("Error", "Por favor seleccione una promoción")
            return
        
        if self.db.registrar_usuario(nombre_padre, nombre_estudiante, email, 
                                     password, self.curso_seleccionado, 
                                     self.promocion_seleccionada):
            self.show_dialog("Éxito", "Usuario registrado correctamente")
            self.go_to_home(instance)
        else:
            self.show_dialog("Error", "El correo electrónico ya está registrado")
    
    def go_to_home(self, instance):
        """Regresa a la pantalla principal"""
        self.ids.padre.text = ""
        self.ids.estudiante.text = ""
        self.ids.email.text = ""
        self.ids.password.text = ""
        self.ids.curso.text = "Seleccionar curso"
        self.ids.promocion.text = "Seleccionar promoción"
        self.curso_seleccionado = None
        self.promocion_seleccionada = None
        self.manager.current = "home"
    
    def show_dialog(self, title: str, text: str):
        """Muestra un diálogo"""
        dialog = MDDialog(
            title=title,
            text=text,
            buttons=[
                MDFlatButton(text="OK", on_release=lambda x: dialog.dismiss())
            ]
        )
        dialog.open()


class AdminPanelScreen(MDScreen):
    """Pantalla del panel de administrador"""
    
    def go_to_metas(self, instance):
        """Navega a la pantalla de metas"""
        metas_screen = self.manager.get_screen("metas")
        metas_screen.cargar_metas()
        self.manager.current = "metas"
    
    def go_to_aportes(self, instance):
        """Navega a la pantalla de aportes (pendiente de implementar)"""
        self.show_dialog("Info", "Funcionalidad en desarrollo")
    
    def go_to_ascenso(self, instance):
        """Navega a la pantalla de ascenso (pendiente de implementar)"""
        self.show_dialog("Info", "Funcionalidad en desarrollo")
    
    def logout(self, instance):
        """Cierra la sesión"""
        app = MDApp.get_running_app()
        app.current_user = None
        self.manager.current = "home"
    
    def show_dialog(self, title: str, text: str):
        """Muestra un diálogo"""
        dialog = MDDialog(
            title=title,
            text=text,
            buttons=[
                MDFlatButton(text="OK", on_release=lambda x: dialog.dismiss())
            ]
        )
        dialog.open()


class MetasScreen(MDScreen):
    """Pantalla de gestión de metas (admin)"""
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.db = Database()
        self.meta_editando = None
    
    def on_enter(self):
        """Se ejecuta cuando se entra a la pantalla"""
        self.cargar_metas()
    
    def cargar_metas(self):
        """Carga todas las metas en la lista"""
        lista_layout = self.ids.lista_layout
        lista_layout.clear_widgets()
        
        metas = self.db.obtener_metas()
        
        for meta in metas:
            item = MetaItem(
                meta_id=meta['id'],
                nombre=meta['nombre'],
                curso=meta['curso'],
                fecha=meta['fecha_limite'],
                costo=f"COP {formatear_cop(meta['costo_estimado'])}",
                screen=self
            )
            lista_layout.add_widget(item)
    
    def agregar_meta(self, instance):
        """Abre la pantalla para agregar/editar meta"""
        agregar_screen = self.manager.get_screen("agregar_meta")
        agregar_screen.meta_editando = None
        agregar_screen.limpiar_campos()
        self.manager.current = "agregar_meta"
    
    def editar_meta(self, meta_id: int):
        """Abre la pantalla para editar una meta"""
        meta = self.db.obtener_meta(meta_id)
        if meta:
            agregar_screen = self.manager.get_screen("agregar_meta")
            agregar_screen.meta_editando = meta
            agregar_screen.cargar_datos()
            self.manager.current = "agregar_meta"
    
    def eliminar_meta(self, meta_id: int):
        """Elimina una meta"""
        if self.db.eliminar_meta(meta_id):
            self.cargar_metas()
            self.show_dialog("Éxito", "Meta eliminada correctamente")
        else:
            self.show_dialog("Error", "No se pudo eliminar la meta")
    
    def go_back(self, instance):
        """Regresa al panel de administrador"""
        self.manager.current = "admin_panel"
    
    def show_dialog(self, title: str, text: str):
        """Muestra un diálogo"""
        dialog = MDDialog(
            title=title,
            text=text,
            buttons=[
                MDFlatButton(text="OK", on_release=lambda x: dialog.dismiss())
            ]
        )
        dialog.open()


class MetaItem(MDBoxLayout):
    """Widget que representa un item de meta en la lista"""
    
    # Propiedades Kivy para que puedan usarse desde el KV (`root.*`)
    meta_id = NumericProperty(0)
    nombre = StringProperty("")
    curso = StringProperty("")
    fecha = StringProperty("")
    costo = StringProperty("")
    screen = ObjectProperty(allownone=True)

    def __init__(self, meta_id: int, nombre: str, curso: str, 
                 fecha: str, costo: str, screen: MetasScreen, **kwargs):
        super().__init__(**kwargs)
        self.meta_id = meta_id
        self.nombre = nombre
        self.curso = curso
        self.fecha = fecha
        self.costo = costo
        self.screen = screen
    
    def editar(self):
        """Edita esta meta"""
        self.screen.editar_meta(self.meta_id)
    
    def eliminar(self):
        """Elimina esta meta"""
        self.screen.eliminar_meta(self.meta_id)


class AgregarMetaScreen(MDScreen):
    """Pantalla para agregar/editar metas"""
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.db = Database()
        self.meta_editando = None
        self.curso_seleccionado = None
        self.setup_menu()
    
    def setup_menu(self):
        """Configura el menú de cursos"""
        # Para metas solo se permiten cursos de 9° a 11°
        cursos = ["9°", "10°", "11°"]
        
        menu_items = [
            {
                "text": curso,
                "viewclass": "OneLineListItem",
                "on_release": lambda x=curso: self.seleccionar_curso(x),
            } for curso in cursos
        ]
        
        self.menu_curso = MDDropdownMenu(
            caller=self.ids.curso_btn,
            items=menu_items,
            width_mult=4,
        )
    
    def seleccionar_curso(self, curso: str):
        """Selecciona un curso"""
        self.curso_seleccionado = curso
        self.ids.curso_btn.text = f"Curso: {curso}"
        self.menu_curso.dismiss()
    
    def cargar_datos(self):
        """Carga los datos de la meta que se está editando"""
        if self.meta_editando:
            self.ids.nombre.text = self.meta_editando['nombre']
            self.ids.fecha.text = self.meta_editando['fecha_limite']
            self.ids.costo.text = str(self.meta_editando['costo_estimado'])
            self.curso_seleccionado = self.meta_editando['curso']
            self.ids.curso_btn.text = f"Curso: {self.curso_seleccionado}"
    
    def limpiar_campos(self):
        """Limpia todos los campos"""
        self.ids.nombre.text = ""
        self.ids.fecha.text = ""
        self.ids.costo.text = ""
        self.ids.curso_btn.text = "Seleccionar curso"
        self.curso_seleccionado = None
    
    def guardar_meta(self, instance):
        """Guarda o actualiza una meta"""
        nombre = self.ids.nombre.text.strip()
        fecha = self.ids.fecha.text.strip()
        costo_text = self.ids.costo.text.strip()
        
        if not all([nombre, fecha, costo_text]):
            self.show_dialog("Error", "Por favor complete todos los campos")
            return
        
        if not self.curso_seleccionado:
            self.show_dialog("Error", "Por favor seleccione un curso")
            return
        
        try:
            costo = float(costo_text)
            if costo <= 0:
                raise ValueError
        except ValueError:
            self.show_dialog("Error", "El costo debe ser un número positivo")
            return
        
        if self.meta_editando:
            # Actualizar meta existente
            if self.db.actualizar_meta(self.meta_editando['id'], nombre, 
                                       self.curso_seleccionado, fecha, costo):
                self.show_dialog("Éxito", "Meta actualizada correctamente")
                self.go_back(instance)
            else:
                self.show_dialog("Error", "No se pudo actualizar la meta")
        else:
            # Crear nueva meta
            meta_id = self.db.crear_meta(nombre, self.curso_seleccionado, fecha, costo)
            if meta_id:
                self.show_dialog("Éxito", "Meta creada correctamente")
                self.go_back(instance)
            else:
                self.show_dialog("Error", "No se pudo crear la meta")
    
    def go_back(self, instance):
        """Regresa a la pantalla de metas"""
        self.limpiar_campos()
        metas_screen = self.manager.get_screen("metas")
        metas_screen.cargar_metas()
        self.manager.current = "metas"
    
    def show_dialog(self, title: str, text: str):
        """Muestra un diálogo"""
        dialog = MDDialog(
            title=title,
            text=text,
            buttons=[
                MDFlatButton(text="OK", on_release=lambda x: dialog.dismiss())
            ]
        )
        dialog.open()


class UsuarioMetasScreen(MDScreen):
    """Pantalla de metas del usuario"""
    
    # Propiedad declarada para que Kivy pueda usarla en el KV
    usuario = ObjectProperty(allownone=True)
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.db = Database()
    
    def on_enter(self):
        """Se ejecuta cuando se entra a la pantalla"""
        if self.usuario:
            self.cargar_metas()
    
    def cargar_metas(self):
        """Carga las metas del usuario"""
        if not self.usuario:
            return
        
        metas_layout = self.ids.metas_layout
        metas_layout.clear_widgets()
        
        # Actualizar título
        self.ids.titulo_metas.text = f"Gastos {self.usuario['nombre_estudiante']}"
        
        # Asegurar que el usuario tenga todas las metas de su curso asignadas
        # Obtener todas las metas del curso del usuario
        todas_metas = self.db.obtener_metas()
        metas_curso = [meta for meta in todas_metas if meta['curso'] == self.usuario['curso']]
        
        # Obtener las metas que ya tiene asignadas el usuario
        metas = self.db.obtener_metas_usuario(self.usuario['id'])
        ids_metas_asignadas = {meta['id'] for meta in metas}
        
        # Asignar solo las metas del curso que el usuario aún no tiene
        for meta in metas_curso:
            if meta['id'] not in ids_metas_asignadas:
                self.db.asignar_meta_usuario(self.usuario['id'], meta['id'])
        
        # Volver a obtener las metas actualizadas
        metas = self.db.obtener_metas_usuario(self.usuario['id'])
        
        if not metas:
            label = MDLabel(
                text="No tienes metas asignadas",
                halign="center",
                theme_text_color="Secondary"
            )
            metas_layout.add_widget(label)
            self.ids.abonado_label.text = ""
            self.ids.faltante_label.text = ""
            self.ids.progreso_porcentaje_label.text = ""
            self.ids.total_precio_label.text = ""
            self.ids.progress_bar.value = 0
            return
        
        # Calcular progreso total
        total_ahorrado = 0
        total_costo = 0
        
        for meta in metas:
            balance = self.db.calcular_balance_meta(self.usuario['id'], meta['id'])
            total_ahorrado += balance['balance']
            total_costo += balance['costo_estimado']
            
            # Crear card moderna minimalista para cada meta (con más padding)
            card = MDCard(
                orientation="vertical",
                padding=[dp(22), dp(22), dp(22), dp(22)],  # Padding aumentado
                spacing=dp(10),
                radius=[22,],
                elevation=5,
                md_bg_color=(0.98, 0.99, 1.0, 1),
                size_hint_y=None,
                height=dp(160)
            )
            
            # Nombre de la meta (con suficiente espacio)
            nombre_label = MDLabel(
                text=meta['nombre'],
                halign="left",
                text_color=(0.15, 0.15, 0.2, 1),
                font_style="H6",
                size_hint_y=None,
                height=dp(32),
                text_size=(None, None),
                valign="top"
            )
            card.add_widget(nombre_label)
            
            # Información de la meta (con espacio suficiente)
            info_text = f"Curso: {meta['curso']} | Fecha: {meta['fecha_limite']}"
            info_label = MDLabel(
                text=info_text,
                halign="left",
                text_color=(0.5, 0.5, 0.55, 1),
                font_style="Body2",
                size_hint_y=None,
                height=dp(26),
                valign="top"
            )
            card.add_widget(info_label)
            
            # Información de ahorrado y faltante (con espacio suficiente)
            progreso_label = MDLabel(
                text=f"Ahorrado: COP {formatear_cop(balance['balance'])}\nFaltante: COP {formatear_cop(balance['faltante'])}",
                halign="left",
                text_color=(0.2, 0.4, 0.7, 1),
                font_style="Body2",
                size_hint_y=None,
                height=dp(50),
                valign="top"
            )
            card.add_widget(progreso_label)
            
            # Botón para ver detalles (redondeado)
            btn = MDRaisedButton(
                text="Ver detalles",
                md_bg_color=(0.2, 0.4, 0.7, 1),
                text_color=(1, 1, 1, 1),
                elevation=3,
                radius=[20,],
                size_hint_y=None,
                height=dp(42),
                on_release=lambda x, m=meta: self.ver_detalle_meta(m)
            )
            card.add_widget(btn)
            
            metas_layout.add_widget(card)
        
        # Actualizar información de progreso total
        if total_costo > 0:
            progreso_total = (total_ahorrado / total_costo) * 100
            total_faltante = max(0, total_costo - total_ahorrado)
            self.ids.progress_bar.value = progreso_total
            # Entre título y barra: Abonado y Faltante
            self.ids.abonado_label.text = f"Abonado: COP {formatear_cop(total_ahorrado)}"
            self.ids.faltante_label.text = f"Faltante: COP {formatear_cop(total_faltante)}"
            # Debajo de la barra: Progreso porcentual y Total
            self.ids.progreso_porcentaje_label.text = f"Progreso: {progreso_total:.1f}%"
            self.ids.total_precio_label.text = f"Total: COP {formatear_cop(total_costo)}"
        else:
            self.ids.progress_bar.value = 0
            self.ids.abonado_label.text = ""
            self.ids.faltante_label.text = ""
            self.ids.progreso_porcentaje_label.text = ""
            self.ids.total_precio_label.text = ""
    
    def ver_detalle_meta(self, meta: Dict):
        """Abre la pantalla de detalle de una meta"""
        detalle_screen = self.manager.get_screen("meta_detalle")
        detalle_screen.cargar_meta(meta, self.usuario)
        self.manager.current = "meta_detalle"
    
    def go_back(self, instance):
        """Regresa a la pantalla anterior"""
        app = MDApp.get_running_app()
        app.current_user = None
        self.usuario = None
        self.manager.current = "home"


class MetaDetalleScreen(MDScreen):
    """Pantalla de detalle de una meta"""
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.db = Database()
        self.meta = None
        self.usuario = None
    
    def cargar_meta(self, meta: Dict, usuario: Dict):
        """Carga los datos de la meta"""
        self.meta = meta
        self.usuario = usuario
        
        balance = self.db.calcular_balance_meta(usuario['id'], meta['id'])
        
        self.ids.meta_nombre.text = meta['nombre']
        self.ids.meta_fecha.text = f"Fecha límite: {meta['fecha_limite']}"
        
        # Calcular progreso
        progreso = (balance['balance'] / balance['costo_estimado'] * 100) if balance['costo_estimado'] > 0 else 0
        self.ids.progress_bar_meta.value = progreso
        
        # Entre nombre y barra: Abonado y Faltante
        self.ids.meta_abonado.text = f"Abonado: COP {formatear_cop(balance['balance'])}"
        self.ids.meta_faltante.text = f"Faltante: COP {formatear_cop(balance['faltante'])}"
        
        # Debajo de la barra: Progreso porcentual y Monto completo
        self.ids.meta_progreso_porcentaje.text = f"Progreso: {progreso:.1f}%"
        self.ids.meta_total.text = f"Total: COP {formatear_cop(balance['costo_estimado'])}"
    
    def ir_registrar_ahorro(self, instance):
        """Abre la pantalla para registrar un ahorro"""
        if not self.meta or not self.usuario:
            return
        ahorro_screen = self.manager.get_screen("registrar_ahorro")
        ahorro_screen.meta = self.meta
        ahorro_screen.usuario = self.usuario
        self.manager.current = "registrar_ahorro"
    
    def ir_registrar_salida(self, instance):
        """Abre la pantalla para registrar una salida"""
        if not self.meta or not self.usuario:
            return
        salida_screen = self.manager.get_screen("registrar_salida")
        salida_screen.meta = self.meta
        salida_screen.usuario = self.usuario
        self.manager.current = "registrar_salida"
    
    def ir_historial(self, instance):
        """Abre la pantalla de historial"""
        if not self.meta or not self.usuario:
            return
        historial_screen = self.manager.get_screen("historial")
        historial_screen.cargar_historial(self.meta, self.usuario)
        self.manager.current = "historial"
    
    def ir_detalle_plan(self, instance):
        """Abre la pantalla de detalle del plan"""
        if not self.meta or not self.usuario:
            return
        plan_screen = self.manager.get_screen("detalle_plan")
        plan_screen.cargar_plan(self.meta, self.usuario)
        self.manager.current = "detalle_plan"
    
    def volver_curso(self, instance):
        """Regresa a la pantalla de metas del usuario"""
        usuario_metas_screen = self.manager.get_screen("usuario_metas")
        usuario_metas_screen.cargar_metas()
        self.manager.current = "usuario_metas"


class RegistrarAhorroScreen(MDScreen):
    """Pantalla para registrar un ahorro"""
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.db = Database()
        self.meta = None
        self.usuario = None
    
    def guardar(self):
        """Guarda el ahorro"""
        if not self.meta or not self.usuario:
            return
        
        monto_text = self.ids.monto.text.strip()
        
        if not monto_text:
            self.show_dialog("Error", "Por favor ingrese un monto")
            return
        
        try:
            monto = float(monto_text)
            if monto <= 0:
                raise ValueError
        except ValueError:
            self.show_dialog("Error", "El monto debe ser un número positivo")
            return
        
        if self.db.registrar_movimiento(self.usuario['id'], self.meta['id'], 
                                       'ahorro', monto, "Ahorro registrado"):
            self.show_dialog("Éxito", "Ahorro registrado correctamente")
            self.volver_meta()
        else:
            self.show_dialog("Error", "No se pudo registrar el ahorro")
    
    def volver_meta(self):
        """Regresa a la pantalla de detalle de meta"""
        self.ids.monto.text = ""
        detalle_screen = self.manager.get_screen("meta_detalle")
        detalle_screen.cargar_meta(self.meta, self.usuario)
        self.manager.current = "meta_detalle"
    
    def show_dialog(self, title: str, text: str):
        """Muestra un diálogo"""
        dialog = MDDialog(
            title=title,
            text=text,
            buttons=[
                MDFlatButton(text="OK", on_release=lambda x: dialog.dismiss())
            ]
        )
        dialog.open()


class RegistrarSalidaScreen(MDScreen):
    """Pantalla para registrar una salida"""
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.db = Database()
        self.meta = None
        self.usuario = None
    
    def guardar(self):
        """Guarda la salida"""
        if not self.meta or not self.usuario:
            return
        
        monto_text = self.ids.monto.text.strip()
        
        if not monto_text:
            self.show_dialog("Error", "Por favor ingrese un monto")
            return
        
        try:
            monto = float(monto_text)
            if monto <= 0:
                raise ValueError
        except ValueError:
            self.show_dialog("Error", "El monto debe ser un número positivo")
            return
        
        if self.db.registrar_movimiento(self.usuario['id'], self.meta['id'], 
                                       'salida', monto, "Salida registrada"):
            self.show_dialog("Éxito", "Salida registrada correctamente")
            self.volver_meta()
        else:
            self.show_dialog("Error", "No se pudo registrar la salida")
    
    def volver_meta(self):
        """Regresa a la pantalla de detalle de meta"""
        self.ids.monto.text = ""
        detalle_screen = self.manager.get_screen("meta_detalle")
        detalle_screen.cargar_meta(self.meta, self.usuario)
        self.manager.current = "meta_detalle"
    
    def show_dialog(self, title: str, text: str):
        """Muestra un diálogo"""
        dialog = MDDialog(
            title=title,
            text=text,
            buttons=[
                MDFlatButton(text="OK", on_release=lambda x: dialog.dismiss())
            ]
        )
        dialog.open()


class HistorialScreen(MDScreen):
    """Pantalla de historial de movimientos"""
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.db = Database()
    
    def cargar_historial(self, meta: Dict, usuario: Dict):
        """Carga el historial de movimientos"""
        lista = self.ids.lista
        lista.clear_widgets()
        
        movimientos = self.db.obtener_movimientos_meta(usuario['id'], meta['id'])
        
        if not movimientos:
            label = MDLabel(
                text="No hay movimientos registrados",
                halign="center",
                theme_text_color="Secondary"
            )
            lista.add_widget(label)
            return
        
        for mov in movimientos:
            tipo_texto = "Ahorro" if mov['tipo'] == 'ahorro' else "Salida"
            # Colores modernos: verde para ahorro, rojo para salida
            color_texto = (0.2, 0.5, 0.4, 1) if mov['tipo'] == 'ahorro' else (0.8, 0.3, 0.3, 1)
            
            # Card moderna minimalista
            card = MDCard(
                orientation="vertical",
                padding=dp(22),
                spacing=dp(10),
                radius=[22,],
                elevation=5,
                md_bg_color=(0.98, 0.99, 1.0, 1),
                size_hint_y=None,
                height=dp(115)
            )
            
            tipo_label = MDLabel(
                text=tipo_texto,
                halign="left",
                text_color=color_texto,
                font_style="H6",
                bold=True,
                size_hint_y=None,
                height=dp(28)
            )
            card.add_widget(tipo_label)
            
            monto_label = MDLabel(
                text=f"Monto: COP {formatear_cop(mov['monto'])}",
                halign="left",
                text_color=(0.15, 0.15, 0.2, 1),
                font_style="Body1",
                bold=True,
                size_hint_y=None,
                height=dp(30)
            )
            card.add_widget(monto_label)
            
            fecha_label = MDLabel(
                text=f"Fecha: {mov['fecha']}",
                halign="left",
                text_color=(0.5, 0.5, 0.55, 1),
                font_style="Body2",
                size_hint_y=None,
                height=dp(24)
            )
            card.add_widget(fecha_label)
            
            lista.add_widget(card)
    
    def volver_meta(self):
        """Regresa a la pantalla de detalle de meta"""
        self.manager.current = "meta_detalle"


class DetallePlanScreen(MDScreen):
    """Pantalla de detalle del plan"""
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.db = Database()
    
    def cargar_plan(self, meta: Dict, usuario: Dict):
        """Carga el detalle del plan"""
        balance = self.db.calcular_balance_meta(usuario['id'], meta['id'])
        
        self.ids.meta_nombre.text = meta['nombre']
        self.ids.meta_fecha.text = f"Fecha límite: {meta['fecha_limite']}"
        self.ids.meta_costo.text = f"Costo estimado: COP {formatear_cop(balance['costo_estimado'])}"
        self.ids.meta_ahorrado.text = f"Ahorrado: COP {formatear_cop(balance['balance'])}"
        self.ids.meta_faltante.text = f"Faltante: COP {formatear_cop(balance['faltante'])}"
    
    def volver_meta(self):
        """Regresa a la pantalla de detalle de meta"""
        self.manager.current = "meta_detalle"


class MetafianceApp(MDApp):
    """Aplicación principal"""
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.current_user = None
    
    def build(self):
        """Construye la aplicación"""
        from kivy.uix.screenmanager import ScreenManager

        # Cargar la interfaz definida en app.kv antes de crear las pantallas
        Builder.load_file("app.kv")

        sm = ScreenManager()
        
        # Agregar todas las pantallas
        sm.add_widget(HomeScreen(name="home"))
        sm.add_widget(LoginScreen(name="login"))
        sm.add_widget(RegisterScreen(name="register"))
        sm.add_widget(AdminPanelScreen(name="admin_panel"))
        sm.add_widget(MetasScreen(name="metas"))
        sm.add_widget(AgregarMetaScreen(name="agregar_meta"))
        sm.add_widget(UsuarioMetasScreen(name="usuario_metas"))
        sm.add_widget(MetaDetalleScreen(name="meta_detalle"))
        sm.add_widget(RegistrarAhorroScreen(name="registrar_ahorro"))
        sm.add_widget(RegistrarSalidaScreen(name="registrar_salida"))
        sm.add_widget(HistorialScreen(name="historial"))
        sm.add_widget(DetallePlanScreen(name="detalle_plan"))
        
        return sm


if __name__ == "__main__":
    MetafianceApp().run()

