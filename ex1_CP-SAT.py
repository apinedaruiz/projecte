import pandas as pd
import numpy as np
import os
from sqlalchemy import create_engine
from ortools.sat.python import cp_model
from minio import Minio  # 🔥 NOVA línia: Minio client

# Connexió SQL Server
username = 'apineda'
password = 'apineda'
server = 'ALBA\\SQLEXPRESS'
database = 'bdapineda2'
driver = 'ODBC Driver 17 for SQL Server'
connection_url = f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver={driver}'
engine = create_engine(connection_url)

# 📥 Connexió a MiniO
minio_client = Minio(
    "localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)
bucket_name = "assignacions-csv"  # Assegura't que existeix a MiniO!

# Carregar dades
posicions = pd.read_sql("SELECT * FROM posicions", engine)
posicions['clasificador'] = posicions['clasificador'].astype(int)
posicions['familia'] = posicions['familia'].astype(int)
posicions['id_limitacio'] = posicions['id_limitacio'].astype(int)

treballadors = pd.read_sql("SELECT * FROM treballadors WHERE torn = 'matí'", engine)
limitacions_df = pd.read_sql("SELECT * FROM treballador_limitacio", engine)
df_calendari = pd.read_sql("SELECT * FROM calendari_laboral WHERE es_laborable = 1", engine)

# Obtenir els pròxims 7 dies laborables
avui = pd.Timestamp.today().normalize()
df_calendari_rename = df_calendari.rename(columns={'aany': 'year', 'numero_mes': 'month', 'numero_dia': 'day'})
df_calendari['data'] = pd.to_datetime(df_calendari_rename[['year', 'month', 'day']])
taula_laborables = df_calendari[df_calendari['data'] >= avui].sort_values('data').head(7)
dies_laborables = taula_laborables['data'].dt.strftime('%Y-%m-%d').tolist()

limitacions_dict = limitacions_df.groupby('id_treballador')['id_limitacio'].apply(set).to_dict()
output_folder = "assignacions_cpsat"
os.makedirs(output_folder, exist_ok=True)

# OR-Tools per dia
def generar_assignacions_dia(data):
    model = cp_model.CpModel()
    n_treballadors = len(treballadors)
    n_hores = 8
    n_posicions = len(posicions)

    assignacio = {}
    for t in range(n_treballadors):
        for h in range(n_hores):
            assignacio[(t, h)] = model.NewIntVar(0, n_posicions - 1, f"treb_{t}_hora_{h}")

    for t in range(n_treballadors):
        treballador = treballadors.iloc[t]
        id_treb = treballador['id_treballador']
        limitacions = limitacions_dict.get(id_treb, set())

        primer_clas = model.NewIntVar(0, 1000, f"clasificador_t{t}")
        for h in range(n_hores):
            pos_idx = assignacio[(t, h)]
            model.AddElement(pos_idx, posicions['clasificador'].tolist(), primer_clas)

        for h in range(n_hores - 1):
            f1 = model.NewIntVar(0, 1000, f"fam_{t}_{h}")
            f2 = model.NewIntVar(0, 1000, f"fam_{t}_{h+1}")
            model.AddElement(assignacio[(t, h)], posicions['familia'].tolist(), f1)
            model.AddElement(assignacio[(t, h+1)], posicions['familia'].tolist(), f2)
            model.Add(f1 != f2)

        if limitacions:
            for h in range(n_hores):
                for id_lim in limitacions:
                    lim_vec = [1 if row['id_limitacio'] == id_lim else 0 for _, row in posicions.iterrows()]
                    v = model.NewIntVar(0, 1, f"lim_{t}_{h}_{id_lim}")
                    model.AddElement(assignacio[(t, h)], lim_vec, v)
                    model.Add(v == 0)

    objectiu = model.NewIntVar(0, 10000, "objectiu")
    model.Maximize(objectiu)

    solver = cp_model.CpSolver()
    solver.parameters.max_time_in_seconds = 30
    status = solver.Solve(model)

    if status in [cp_model.OPTIMAL, cp_model.FEASIBLE]:
        assignacions = []
        for t in range(n_treballadors):
            treb = treballadors.iloc[t]
            for h in range(n_hores):
                idx = solver.Value(assignacio[(t, h)])
                pos = posicions.iloc[idx]
                assignacions.append({
                    'data': data,
                    'hora': f"{6+h:02d}:00 - {7+h:02d}:00",
                    'id_treballador': treb['id_treballador'],
                    'nom': treb['nom'],
                    'id_posicio': pos['id_posicio'],
                    'posicio': pos['posicio'],
                    'clasificador': pos['clasificador'],
                    'familia': pos['familia']
                })
        return pd.DataFrame(assignacions)
    else:
        print(f"No s'ha trobat cap solució per {data}")
        return pd.DataFrame()

# 🔥 Funció nova: pujar fitxer a MiniO
def pujar_a_minio(path_local, nom_objecte):
    minio_client.fput_object(bucket_name, nom_objecte, path_local)
    print(f"✅ Fitxer pujat a MiniO: {nom_objecte}")

# Executar per cada dia
for data in dies_laborables:
    assignacions_dia = generar_assignacions_dia(data)
    df_dia = pd.DataFrame(assignacions_dia)
    df_dia = df_dia.sort_values(by=['id_treballador', 'hora'])  # 🔄 Ordenació

    output_path = os.path.join(output_folder, f"assignacions_{data}.csv")
    df_dia.to_csv(output_path, index=False, encoding='utf-8-sig')

    # 🔥 NOVETAT: pujar el CSV acabat al bucket
    pujar_a_minio(output_path, f"assignacions_cpsat/assignacions_{data}.csv")

    print(f"✔️ Assignacions generades i pujades per {data}")
