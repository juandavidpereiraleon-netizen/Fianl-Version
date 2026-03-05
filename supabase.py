import psycopg2

try:
    conn = psycopg2.connect(
        dbname="postgres",
        user="postgres.mtrqguoeowuipunkbpfs",
        password="7719*21NGC2133",
        host="aws-0-us-west-2.pooler.supabase.com",
        port="5432"
    )

    print("Conexión exitosa")

    cursor = conn.cursor()
    cursor.execute("SELECT version();")
    version = cursor.fetchone()
    print("Versión PostgreSQL:", version)

    cursor.close()
    conn.close()

except Exception as e:
    print("Error:", e)