import os
import sqlite3
import psycopg2
import psycopg2.extras

SQLITE_PATH = os.environ.get("SQLITE_PATH", "metafiance.db")
SUPABASE_URL = os.environ.get("DATABASE_URL", "postgresql://postgres.nlvlxasyemgzpclmlcmr:Cr0q4ZCiu5jWD0Qf@aws-1-us-east-2.pooler.supabase.com:5432/postgres")

def read_sqlite():
    conn = sqlite3.connect(SQLITE_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    def all(sql, params=()):
        cur.execute(sql, params)
        return [dict(r) for r in cur.fetchall()]
    data = {
        "usuarios": all("SELECT id, nombre_padre, nombre_estudiante, nombre_hija, email, password, curso, promocion, es_admin, fecha_registro FROM usuarios"),
        "metas": all("SELECT id, nombre, curso, fecha_limite, costo_estimado, fecha_creacion FROM metas"),
        "usuario_metas": all("SELECT id, usuario_id, meta_id FROM usuario_metas"),
        "ahorros": all("SELECT id, usuario_id, meta_id, monto, descripcion, fecha FROM ahorros"),
        "salidas": all("SELECT id, usuario_id, meta_id, monto, descripcion, fecha FROM salidas"),
    }
    conn.close()
    return data

def ensure_schema_pg(conn):
    cur = conn.cursor()
    cur.execute("""
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
        fecha_registro TIMESTAMPTZ DEFAULT now()
    )""")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS metas (
        id SERIAL PRIMARY KEY,
        nombre TEXT NOT NULL,
        curso TEXT NOT NULL,
        fecha_limite TEXT NOT NULL,
        costo_estimado DOUBLE PRECISION NOT NULL,
        fecha_creacion TIMESTAMPTZ DEFAULT now()
    )""")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS usuario_metas (
        id SERIAL PRIMARY KEY,
        usuario_id INTEGER NOT NULL REFERENCES usuarios(id),
        meta_id INTEGER NOT NULL REFERENCES metas(id),
        UNIQUE(usuario_id, meta_id)
    )""")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS ahorros (
        id SERIAL PRIMARY KEY,
        usuario_id INTEGER NOT NULL,
        meta_id INTEGER NOT NULL,
        monto DOUBLE PRECISION NOT NULL,
        descripcion TEXT,
        fecha TIMESTAMPTZ DEFAULT now()
    )""")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS salidas (
        id SERIAL PRIMARY KEY,
        usuario_id INTEGER NOT NULL,
        meta_id INTEGER NOT NULL,
        monto DOUBLE PRECISION NOT NULL,
        descripcion TEXT,
        fecha TIMESTAMPTZ DEFAULT now()
    )""")
    conn.commit()

def migrate_to_pg(data):
    conn = psycopg2.connect(SUPABASE_URL)
    try:
        ensure_schema_pg(conn)
        cur = conn.cursor()
        for u in data["usuarios"]:
            cur.execute("""
                INSERT INTO usuarios (nombre_padre, nombre_estudiante, nombre_hija, email, password, curso, promocion, es_admin, fecha_registro)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, COALESCE(%s, now()))
                ON CONFLICT (email) DO NOTHING
            """, (u["nombre_padre"], u["nombre_estudiante"], u.get("nombre_hija"), u["email"], u["password"], u["curso"], u["promocion"], bool(u["es_admin"]), u.get("fecha_registro")))
        conn.commit()
        # Build email->id map
        cur.execute("SELECT id, email FROM usuarios")
        email_to_id = {email: uid for uid, email in cur.fetchall()}
        # metas
        for m in data["metas"]:
            cur.execute("""
                INSERT INTO metas (nombre, curso, fecha_limite, costo_estimado, fecha_creacion)
                VALUES (%s, %s, %s, %s, COALESCE(%s, now()))
                ON CONFLICT DO NOTHING
            """, (m["nombre"], m["curso"], m["fecha_limite"], float(m["costo_estimado"]), m.get("fecha_creacion")))
        conn.commit()
        # Build metas id map
        cur.execute("SELECT id, nombre, curso FROM metas")
        metas_all = cur.fetchall()
        # usuario_metas
        for um in data["usuario_metas"]:
            cur.execute("""
                INSERT INTO usuario_metas (usuario_id, meta_id)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING
            """, (um["usuario_id"], um["meta_id"]))
        conn.commit()
        # ahorros
        for a in data["ahorros"]:
            cur.execute("""
                INSERT INTO ahorros (usuario_id, meta_id, monto, descripcion, fecha)
                VALUES (%s, %s, %s, %s, COALESCE(%s, now()))
            """, (a["usuario_id"], a["meta_id"], float(a["monto"]), a.get("descripcion"), a.get("fecha")))
        # salidas
        for s in data["salidas"]:
            cur.execute("""
                INSERT INTO salidas (usuario_id, meta_id, monto, descripcion, fecha)
                VALUES (%s, %s, %s, %s, COALESCE(%s, now()))
            """, (s["usuario_id"], s["meta_id"], float(s["monto"]), s.get("descripcion"), s.get("fecha")))
        conn.commit()
    finally:
        conn.close()

def main():
    print("Leyendo datos de SQLite:", SQLITE_PATH)
    data = read_sqlite()
    print("Migrando a Supabase (PostgreSQL Pooler)")
    migrate_to_pg(data)
    print("Migración completada.")

if __name__ == "__main__":
    main()
