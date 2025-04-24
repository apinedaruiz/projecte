import pandas as pd
import datetime
import pyodbc

# Definir el año
año = 2025



# Configurar conexión a Microsoft SQL Server

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

# Crear tabla en SQL Server si no existe
cursor.execute("""
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='calendari_laboral' AND xtype='U')
CREATE TABLE calendari_laboral (
    id INT IDENTITY(1,1) PRIMARY KEY,
    numero_dia INT,
    numero_setmana INT,
    numero_mes INT,
    aany INT,
    dia_setmana NVARCHAR(20),
    es_laborable BIT
)
""")
conn.commit()

# Lista de festivos (Formato: (mes, día))
festius = {
    (1, 1), (1, 6), (4, 18), (4, 21), (5, 1), (6, 9),
    (6, 24), (8, 15), (12, 25), (12, 26), (12, 31)
}

# Crear lista de datos
dades = []

# Generar los días del año 2025
for mes in range(1, 13):  # Meses de 1 a 12
    for dia in range(1, 32):  # Días de 1 a 31
        try:
            data = datetime.date(año, mes, dia)

            # Obtener valores de la fecha
            numero_dia = dia  # Día del mes (1-31)
            numero_setmana = data.isocalendar()[1]  # Semana ISO
            numero_mes = mes
            dia_setmana = data.strftime("%A").capitalize()  # Día de la semana en español

            # Determinar si es laborable (excluyendo sábados, domingos y festivos)
            es_laborable = dia_setmana not in ["Saturday", "Sunday"] and (mes, dia) not in festius

            # Agregar a la lista
            dades.append((numero_dia, numero_setmana, numero_mes, año, dia_setmana, es_laborable))

        except ValueError:
            # Si el día no existe (Ej: 30 de Febrero), se ignora
            continue

# Insertar dades en SQL Server
cursor.executemany("""
INSERT INTO calendari_laboral (numero_dia, numero_setmana, numero_mes, aany, dia_setmana, es_laborable)
VALUES (?, ?, ?, ?, ?, ?)
""", dades)

conn.commit()

print("✅ Datos insertados correctamente en SQL Server sin la columna festivo.")

# Cerrar conexión
cursor.close()
conn.close()