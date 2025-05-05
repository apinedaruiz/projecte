import pandas as pd
from minio import Minio
from io import BytesIO
from sqlalchemy import create_engine
import os

# Connexió a MinIO
minio_client = Minio(
    "localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

bucket = "assignacions-csv"
prefix = "assignacions_cpsat/"  # Ruta dins del bucket

# Connexió a SQL Server
engine = create_engine(
    "mssql+pyodbc://apineda:apineda@ALBA\\SQLEXPRESS/bdapineda2?driver=ODBC+Driver+17+for+SQL+Server"
)

# Taula de destinació
taula_destinacio = "assignacions_resum"

# Llegir tots els arxius CSV
objectes = minio_client.list_objects(bucket, prefix=prefix, recursive=True)

df_total = pd.DataFrame()

for obj in objectes:
    if obj.object_name.endswith(".csv"):
        resposta = minio_client.get_object(bucket, obj.object_name)
        df = pd.read_csv(BytesIO(resposta.data), encoding='utf-8-sig')

        # Seleccionar només les columnes necessàries
        df_resum = df[['data', 'torn', 'nom', 'posicio', 'clasificador']].copy()
        df_total = pd.concat([df_total, df_resum], ignore_index=True)
        resposta.close()
        resposta.release_conn()
#print(df_total)
# Escriure a la base de dades (afegir o crear la taula si no existeix)
df_total.to_sql(taula_destinacio, engine, if_exists='replace', index=False)

print(f"✔️ Dades carregades a la taula '{taula_destinacio}'")
