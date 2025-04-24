import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from scipy.optimize import linear_sum_assignment
from datetime import datetime
import os

# Connexió a SQL Server amb SQLAlchemy
username = 'apineda'
password = 'apineda'
server = 'ALBA\\SQLEXPRESS'
database = 'bdapineda2'
driver = 'ODBC Driver 17 for SQL Server'

connection_url = f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver={driver}'
engine = create_engine(connection_url)

# Crear carpeta per guardar assignacions
output_folder = "assignacions_hungarian"
os.makedirs(output_folder, exist_ok=True)

# Obtenir els pròxims 7 dies laborables
df_calendari = pd.read_sql("SELECT * FROM calendari_laboral WHERE es_laborable = 1", engine)
avui = pd.Timestamp.today().normalize()

# Renombrar columnes perquè to_datetime les entengui
df_calendari_rename = df_calendari.rename(columns={'aany': 'year', 'numero_mes': 'month', 'numero_dia': 'day'})
df_calendari['data'] = pd.to_datetime(df_calendari_rename[['year', 'month', 'day']])
taula_laborables = df_calendari[df_calendari['data'] >= avui].sort_values('data').head(7)
dies_laborables = taula_laborables['data'].dt.strftime('%Y-%m-%d').tolist()

# Carreguem les dades
posicions = pd.read_sql("SELECT * FROM posicions", engine)
treballadors = pd.read_sql("SELECT * FROM treballadors WHERE torn = 'matí'", engine)
limitacions_df = pd.read_sql("SELECT * FROM treballador_limitacio", engine)

# Diccionari de limitacions per treballador
limitacions_dict = limitacions_df.groupby('id_treballador')['id_limitacio'].apply(set).to_dict()

# Assegurar que la columna 'familia' existeix
if 'familia' not in posicions.columns:
    raise Exception("La taula 'posicions' ha de tenir una columna 'familia'.")

# Funció per generar assignacions per un dia
def generar_assignacions_dia(data):
    assignacions = []
    treballadors_disponibles = treballadors.copy()
    posicions_grup = posicions.copy()

    # Seguiment de famílies assignades per treballador i classificadors fixats
    families_assignades = {treb['id_treballador']: set() for _, treb in treballadors_disponibles.iterrows()}
    clasificadors_fixats = {}

    for hora_offset in range(8):  # De 06:00 a 14:00
        hora_inici = 6 + hora_offset
        hora_fi = hora_inici + 1
        franja_horaria = f"{hora_inici:02d}:00 - {hora_fi:02d}:00"

        # Barreja i filtra famílies repetides
        posicions_hora = posicions_grup.sample(frac=1).drop_duplicates(subset='familia').reset_index(drop=True)

        # Filtra posicions segons classificadors fixats i limitacions
        posicions_filtrades = []
        for _, pos in posicions_hora.iterrows():
            for _, treb in treballadors_disponibles.iterrows():
                id_treb = treb['id_treballador']
                limitacions_treb = limitacions_dict.get(id_treb, set())
                if pos['familia'] not in families_assignades[id_treb] and pos['id_limitacio'] not in limitacions_treb:
                    clasif_fixe = clasificadors_fixats.get(id_treb)
                    if clasif_fixe is None or clasif_fixe == pos['clasificador']:
                        posicions_filtrades.append(pos)
                        break

        posicions_hora = pd.DataFrame(posicions_filtrades).drop_duplicates(subset='familia')

        n_treballadors = len(treballadors_disponibles)
        n_posicions = len(posicions_hora)
        mida = max(n_treballadors, n_posicions)

        cost_matrix = np.full((mida, mida), 1000)

        for i, treb in treballadors_disponibles.iterrows():
            id_treb = treb['id_treballador']
            limitacions_treb = limitacions_dict.get(id_treb, set())
            clasif_fixe = clasificadors_fixats.get(id_treb)

            for j, pos in posicions_hora.iterrows():
                if pos['familia'] in families_assignades[id_treb]:
                    continue
                if pos['id_limitacio'] in limitacions_treb:
                    continue
                if clasif_fixe and pos['clasificador'] != clasif_fixe:
                    continue
                cost_matrix[i, j] = 0

        fila, columna = linear_sum_assignment(cost_matrix)

        for i, j in zip(fila, columna):
            if i >= n_treballadors or j >= n_posicions:
                continue
            if cost_matrix[i, j] >= 1000:
                continue

            treb = treballadors_disponibles.iloc[i]
            pos = posicions_hora.iloc[j]
            id_treb = treb['id_treballador']

            # Fixar classificador per tot el dia
            if id_treb not in clasificadors_fixats:
                clasificadors_fixats[id_treb] = pos['clasificador']

            assignacions.append({
                'data': data,
                'hora': franja_horaria,
                'id_treballador': treb['id_treballador'],
                'nom': treb['nom'],
                'id_posicio': pos['id_posicio'],
                'posicio': pos['posicio'],
                'clasificador': pos['clasificador'],
                'familia': pos['familia']
            })

            families_assignades[id_treb].add(pos['familia'])

    return assignacions

# 🔁 Generar assignacions per cada dia i guardar CSV
for data in dies_laborables:
    assignacions_dia = generar_assignacions_dia(data)
    df_dia = pd.DataFrame(assignacions_dia)
    df_dia = df_dia.sort_values(by=['id_treballador', 'hora'])  # 🔄 Ordenació

    output_path = os.path.join(output_folder, f"assignacions_{data}.csv")
    df_dia.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"✔️ Assignacions generades per {data}: {output_path}")
