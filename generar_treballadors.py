import pandas as pd
from faker import Faker
import random

import pyodbc

username = 'apineda'
password = 'apineda'
server = 'ALBA\\SQLEXPRESS'
database= 'bdapineda2'
port = 1433
driver = 'ODBC Driver 17 for SQL Server'

# Crear conexión

conn = pyodbc.connect(
    f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'
)
cursor = conn.cursor()

# Crear tabla en SQL Server si no existe
cursor.execute("""
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='treballadors' AND xtype='U')
               CREATE TABLE dbo.treballadors (
                id_treballador int PRIMARY KEY,
                dni nvarchar(9),
                nom nvarchar(50), 
                cognom1 nvarchar(50), 
                cognom2 nvarchar(50), 
                direccio nvarchar(100), 
                sexe nvarchar(10), 
                data_naixement date, 
                data_antiguitat date, 
                telefon nvarchar(15), 
                email nvarchar(50), 
                seccio nvarchar(50), 
                torn nvarchar(10)
)
""")
conn.commit()

# Inicializar Faker en español
fake = Faker("es_ES")


# Número de trabajadores a generar
num_treballadors = 150  

# Lista para almacenar los trabajadores
treballadors = []
# Lista de pueblos y provincias
pobles = ["Blanes, Girona", "Lloret de Mar, Girona", "Vidreres, Girona", "Malgrat de Mar, Barcelona", "Palafolls, Barcelona", "Tordera, Barcelona"]
for i in range(1, num_treballadors + 1):
    treballador = (
        i,
        fake.nif(),
        fake.first_name(),
        fake.last_name(),
        fake.last_name(),
        f"{fake.street_name()}, {random.choice(pobles)}",
        random.choice(["Home", "Dona"]),
        fake.date_of_birth(minimum_age=20, maximum_age=62).strftime('%Y-%m-%d'),
        fake.date_this_century(before_today=True, after_today=False).strftime('%Y-%m-%d'),
        fake.phone_number()[:14],
        fake.free_email(),
        random.choice(["Paqueteria", "Entrades", "Cargues", "RFID", "Inaguracions", "Confecció"]),
        random.choice(["Matí", "Tarda", "Nit"])
    )
    treballadors.append(treballador)

print(treballadors)



# Insertar datos en SQL Server
cursor.executemany("""
INSERT INTO treballadors (id_treballador, dni, nom, cognom1, cognom2, direccio, sexe, data_naixement, data_antiguitat, telefon, email, seccio, torn)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", treballadors)  

conn.commit()

print("✅ Datos insertados correctamente en SQL Server .")

# Cerrar conexión
cursor.close()
conn.close()

