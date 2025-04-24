import pyodbc
import random

username = 'apineda'
password = 'apineda'
server = 'ALBA\\SQLEXPRESS'
database = 'bdapineda2'
port = 1433
driver = 'ODBC Driver 17 for SQL Server'

# Crear conexión

conn = pyodbc.connect(
    f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'
)
cursor = conn.cursor()

# Obtenir tots els treballadors
cursor.execute("SELECT id_treballador FROM treballadors")
treballadors = [row[0] for row in cursor.fetchall()]

# Crear la nova taula si no existeix

# Comprovar si la taula existeix
cursor.execute("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'treballador_limitacio'")
exists = cursor.fetchone()[0]

if exists:
    cursor.execute("TRUNCATE TABLE dbo.treballador_limitacio")
    print("🗑️ Taula 'treballador_limitacio' buidada.")
else:
    cursor.execute("""
        CREATE TABLE treballador_limitacio (
        id_treballador INT,
        id_limitacio INT,
        FOREIGN KEY (id_treballador) REFERENCES treballadors(id_treballador),
        FOREIGN KEY (id_limitacio) REFERENCES limitacions(id_limitacio),
        PRIMARY KEY (id_treballador, id_limitacio)
    );
END
""")
conn.commit()


# Assignar limitacions aleatòries
for id_treballador in treballadors:
    quantitat = random.randint(0, 4)  # De 0 a 3 limitacions per treballador
    limitacions = set()

    while len(limitacions) < quantitat:
        id_lim = random.randint(0, 9)
        if id_lim != 0:  # Si és 0, no té cap limitació
            limitacions.add(id_lim)

    for id_limitacio in limitacions:
        cursor.execute("""
            INSERT INTO treballador_limitacio (id_treballador, id_limitacio)
            VALUES (?, ?)
        """, id_treballador, id_limitacio)
        #limitacions.to_csv("treballador_limitacio.csv", index=False, encoding="utf-8")


conn.commit()
conn.close()

print(id_treballador, limitacions)
print("✅ Limitacions assignades correctament als treballadors.")

