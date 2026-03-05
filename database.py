"""
Módulo de base de datos para Metafiance
PostgreSQL Supabase
"""

import psycopg2
import hashlib
from typing import Optional, List, Tuple, Dict
from psycopg2.extras import RealDictCursor


class Database:
    """Clase para manejar todas las operaciones de base de datos"""

    def __init__(self):
        self.db_error = None
        try:
            self.init_database()
        except Exception as e:
            self.db_error = str(e)
            print("Error inicializando base de datos:", e)

    def get_connection(self):
        return psycopg2.connect(
            dbname="postgres",
            user="postgres.mtrqguoeowuipunkbpfs",
            password="7719*21NGC2133",
            host="aws-0-us-west-2.pooler.supabase.com",
            port="6543",
            sslmode="require"
        )

    def _execute(self, sql: str, params: Tuple | List = ()):
        conn = self.get_connection()
        try:
            cur = conn.cursor()
            cur.execute(sql, params if params else ())
            conn.commit()
            return cur
        finally:
            conn.close()

    def _fetchall_dicts(self, sql: str, params: Tuple | List = ()) -> List[Dict]:
        conn = self.get_connection()
        try:
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute(sql, params if params else ())
            rows = cur.fetchall()
            return rows
        finally:
            conn.close()

    def _fetchone_dict(self, sql: str, params: Tuple | List = ()) -> Optional[Dict]:
        rows = self._fetchall_dicts(sql, params)
        return rows[0] if rows else None

    def init_database(self):

        self._execute("""
            CREATE TABLE IF NOT EXISTS usuarios (
                id SERIAL PRIMARY KEY,
                nombre_padre TEXT NOT NULL,
                nombre_estudiante TEXT NOT NULL,
                nombre_hija TEXT,
                email TEXT UNIQUE NOT NULL,
                password TEXT NOT NULL,
                curso TEXT NOT NULL,
                promocion TEXT NOT NULL,
                es_admin BOOLEAN DEFAULT FALSE,
                fecha_registro TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        self._execute("""
            CREATE TABLE IF NOT EXISTS metas (
                id SERIAL PRIMARY KEY,
                nombre TEXT NOT NULL,
                curso TEXT NOT NULL,
                fecha_limite TEXT NOT NULL,
                costo_estimado REAL NOT NULL,
                fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        self._execute("""
            CREATE TABLE IF NOT EXISTS usuario_metas (
                id SERIAL PRIMARY KEY,
                usuario_id INTEGER NOT NULL,
                meta_id INTEGER NOT NULL,
                UNIQUE(usuario_id, meta_id),
                FOREIGN KEY (usuario_id) REFERENCES usuarios(id) ON DELETE CASCADE,
                FOREIGN KEY (meta_id) REFERENCES metas(id) ON DELETE CASCADE
            )
        """)

        self._execute("""
            CREATE TABLE IF NOT EXISTS movimientos (
                id SERIAL PRIMARY KEY,
                usuario_id INTEGER NOT NULL,
                meta_id INTEGER NOT NULL,
                tipo TEXT NOT NULL CHECK(tipo IN ('ahorro','salida')),
                monto REAL NOT NULL,
                descripcion TEXT,
                fecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (usuario_id) REFERENCES usuarios(id) ON DELETE CASCADE,
                FOREIGN KEY (meta_id) REFERENCES metas(id) ON DELETE CASCADE
            )
        """)

        self._execute("""
            CREATE TABLE IF NOT EXISTS ahorros (
                id SERIAL PRIMARY KEY,
                usuario_id INTEGER NOT NULL,
                meta_id INTEGER NOT NULL,
                monto REAL NOT NULL,
                descripcion TEXT,
                fecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        self._execute("""
            CREATE TABLE IF NOT EXISTS salidas (
                id SERIAL PRIMARY KEY,
                usuario_id INTEGER NOT NULL,
                meta_id INTEGER NOT NULL,
                monto REAL NOT NULL,
                descripcion TEXT,
                fecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        admin_email = "admin@metafinance.com"
        existing = self._fetchone_dict(
            "SELECT id FROM usuarios WHERE email = %s",
            (admin_email,)
        )

        if not existing:
            password = self.hash_password("Admin2026!")
            self._execute("""
                INSERT INTO usuarios
                (nombre_padre, nombre_estudiante, email, password, curso, promocion, es_admin)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, ("Administrador", "Admin", admin_email, password, "Admin", "2026", True))

    @staticmethod
    def hash_password(password: str) -> str:
        return hashlib.sha256(password.encode()).hexdigest()

    def registrar_usuario(self, nombre_padre: str, nombre_estudiante: str,
                          email: str, password: str, curso: str, promocion: str) -> bool:
        try:
            hashed = self.hash_password(password)
            self._execute("""
                INSERT INTO usuarios (nombre_padre, nombre_estudiante, email, password, curso, promocion)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (nombre_padre, nombre_estudiante, email, hashed, curso, promocion))
            return True
        except Exception:
            return False

    def autenticar_usuario(self, email: str, password: str) -> Optional[Dict]:
        hashed = self.hash_password(password)
        row = self._fetchone_dict("""
            SELECT id, nombre_padre, nombre_estudiante, email, curso, promocion, es_admin
            FROM usuarios WHERE email = %s AND password = %s
        """, (email, hashed))
        if row:
            return {
                'id': row['id'],
                'nombre_padre': row['nombre_padre'],
                'nombre_estudiante': row['nombre_estudiante'],
                'email': row['email'],
                'curso': row['curso'],
                'promocion': row['promocion'],
                'es_admin': bool(row['es_admin'])
            }
        return None

    def obtener_usuario(self, usuario_id: int) -> Optional[Dict]:
        row = self._fetchone_dict("""
            SELECT id, nombre_padre, nombre_estudiante, email, curso, promocion, es_admin
            FROM usuarios WHERE id = %s
        """, (usuario_id,))
        if row:
            return {
                'id': row['id'],
                'nombre_padre': row['nombre_padre'],
                'nombre_estudiante': row['nombre_estudiante'],
                'email': row['email'],
                'curso': row['curso'],
                'promocion': row['promocion'],
                'es_admin': bool(row['es_admin'])
            }
        return None

    def crear_meta(self, nombre: str, curso: str, fecha_limite: str, costo_estimado: float) -> int:
        conn = self.get_connection()
        try:
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute("""
                INSERT INTO metas (nombre, curso, fecha_limite, costo_estimado)
                VALUES (%s, %s, %s, %s)
                RETURNING id
            """, (nombre, curso, fecha_limite, costo_estimado))
            row = cur.fetchone()
            conn.commit()
            return row['id'] if row else 0
        finally:
            conn.close()

    def obtener_metas(self) -> List[Dict]:
        return self._fetchall_dicts("""
            SELECT id, nombre, curso, fecha_limite, costo_estimado, fecha_creacion
            FROM metas ORDER BY fecha_creacion DESC
        """)

    def obtener_meta(self, meta_id: int) -> Optional[Dict]:
        return self._fetchone_dict("""
            SELECT id, nombre, curso, fecha_limite, costo_estimado, fecha_creacion
            FROM metas WHERE id = %s
        """, (meta_id,))

    def actualizar_meta(self, meta_id: int, nombre: str, curso: str,
                        fecha_limite: str, costo_estimado: float) -> bool:
        try:
            self._execute("""
                UPDATE metas
                SET nombre = %s, curso = %s, fecha_limite = %s, costo_estimado = %s
                WHERE id = %s
            """, (nombre, curso, fecha_limite, costo_estimado, meta_id))
            return True
        except Exception:
            return False

    def eliminar_meta(self, meta_id: int) -> bool:
        try:
            self._execute("DELETE FROM metas WHERE id = %s", (meta_id,))
            return True
        except Exception:
            return False

    def asignar_meta_usuario(self, usuario_id: int, meta_id: int) -> bool:
        try:
            self._execute("""
                INSERT INTO usuario_metas (usuario_id, meta_id)
                VALUES (%s, %s)
                ON CONFLICT (usuario_id, meta_id) DO NOTHING
            """, (usuario_id, meta_id))
            return True
        except Exception:
            return False

    def obtener_metas_usuario(self, usuario_id: int) -> List[Dict]:
        return self._fetchall_dicts("""
            SELECT DISTINCT m.id, m.nombre, m.curso, m.fecha_limite, m.costo_estimado, m.fecha_creacion
            FROM metas m
            LEFT JOIN usuario_metas um ON m.id = um.meta_id
            WHERE um.usuario_id = %s OR m.curso = (SELECT curso FROM usuarios WHERE id = %s)
            ORDER BY m.fecha_creacion DESC
        """, (usuario_id, usuario_id))

    def registrar_movimiento(self, usuario_id: int, meta_id: int,
                             tipo: str, monto: float, descripcion: str = "") -> bool:
        try:
            self._execute("""
                INSERT INTO movimientos (usuario_id, meta_id, tipo, monto, descripcion)
                VALUES (%s, %s, %s, %s, %s)
            """, (usuario_id, meta_id, tipo, monto, descripcion))
            if tipo == 'ahorro':
                self._execute("""
                    INSERT INTO ahorros (usuario_id, meta_id, monto, descripcion)
                    VALUES (%s, %s, %s, %s)
                """, (usuario_id, meta_id, monto, descripcion))
            elif tipo == 'salida':
                self._execute("""
                    INSERT INTO salidas (usuario_id, meta_id, monto, descripcion)
                    VALUES (%s, %s, %s, %s)
                """, (usuario_id, meta_id, monto, descripcion))
            return True
        except Exception:
            return False

    def obtener_movimientos_meta(self, usuario_id: int, meta_id: int) -> List[Dict]:
        return self._fetchall_dicts("""
            SELECT id, tipo, monto, descripcion, fecha
            FROM movimientos
            WHERE usuario_id = %s AND meta_id = %s
            ORDER BY fecha DESC
        """, (usuario_id, meta_id))

    def calcular_balance_meta(self, usuario_id: int, meta_id: int) -> Dict:
        ah = self._fetchone_dict("""
            SELECT COALESCE(SUM(monto), 0) AS total FROM ahorros
            WHERE usuario_id = %s AND meta_id = %s
        """, (usuario_id, meta_id))
        ahorrado = ah['total'] if ah else 0
        sa = self._fetchone_dict("""
            SELECT COALESCE(SUM(monto), 0) AS total FROM salidas
            WHERE usuario_id = %s AND meta_id = %s
        """, (usuario_id, meta_id))
        salidas = sa['total'] if sa else 0
        cr = self._fetchone_dict("SELECT costo_estimado FROM metas WHERE id = %s", (meta_id,))
        costo_estimado = cr['costo_estimado'] if cr else 0
        balance = ahorrado - salidas
        faltante = max(0, costo_estimado - balance)
        return {
            'ahorrado': ahorrado,
            'salidas': salidas,
            'balance': balance,
            'costo_estimado': costo_estimado,
            'faltante': faltante
        }

    def listar_usuarios(self) -> List[Dict]:
        return self._fetchall_dicts("""
            SELECT id, nombre_padre, nombre_estudiante, nombre_hija, email, curso, promocion, es_admin, fecha_registro
            FROM usuarios ORDER BY fecha_registro DESC
        """)

if __name__ == "__main__":
    db = Database()

    db._execute("""
        INSERT INTO usuarios
        (nombre_padre, nombre_estudiante, nombre_hija, email, password, curso, promocion)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (
        "Carlos",
        "Juan",
        "Sofia",
        "correo@test.com",
        Database.hash_password("123456"),
        "10A",
        "2026"
    ))

    print("Usuario insertado correctamente")
