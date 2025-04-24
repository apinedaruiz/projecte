import pandas as pd
import pyodbc

# Connexió a la base de dades
username = 'apineda'
password = 'apineda'
server = 'ALBA\\SQLEXPRESS'
database = 'bdapineda2'
driver = 'ODBC Driver 17 for SQL Server'

conn = pyodbc.connect(
    f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'
)
cursor = conn.cursor()

# Llegir els CSV
df_limitacions = pd.read_csv("limit.csv")
df_posicions = pd.read_csv("posicions.csv")

### --- LIMITACIONS --- ###
# Comprovar si existeix la taula 'limitacions'
cursor.execute("""
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='limitacions' AND xtype='U')
               CREATE TABLE dbo.limitacions (
                id_limitacio int PRIMARY KEY,
                limitacio nvarchar(50),
                seccio nvarchar(50)
)
""")
conn.commit()

print(df_limitacions)

# Inserir dades de limitacions
insert_limitacions = """
INSERT INTO limitacions (id_limitacio, limitacio, seccio)
VALUES (?, ?, ?)
"""
cursor.executemany(insert_limitacions, df_limitacions.values.tolist())
conn.commit()
print("✅ Dades de limitacions inserides.")

### --- POSICIONS --- ###
# Comprovar si existeix la taula 'posicions'
cursor.execute("""
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='posicions' AND xtype='U')
               CREATE TABLE dbo.posicions (
                id_posicio int PRIMARY KEY,
                posicio nvarchar(50),
                clasificador int,
                id_limitacio int,
                familia nvarchar(50),
                FOREIGN KEY (id_limitacio) REFERENCES limitacions(id_limitacio)
)
""")
conn.commit()

print("✔️ Taula 'posicions' preparada.")

# Inserir dades de posicions
insert_posicions = """
INSERT INTO posicions (id_posicio, posicio, clasificador, id_limitacio, familia)
VALUES (?, ?, ?, ?, ?)
"""
cursor.executemany(insert_posicions, df_posicions.values.tolist())
conn.commit()
print("✅ Dades de posicions inserides.")

# Tancar connexió
cursor.close()
conn.close()
