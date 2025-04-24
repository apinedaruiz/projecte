import pandas as pd
import numpy as np
import pygad
import os
from sqlalchemy import create_engine

# Connexió a SQL Server amb SQLAlchemy
username = 'apineda'
password = 'apineda'
server = 'ALBA\\SQLEXPRESS'
database = 'bdapineda2'
driver = 'ODBC Driver 17 for SQL Server'

connection_url = f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver={driver}'
engine = create_engine(connection_url)

# Crear carpeta per guardar assignacions
output_folder = "assignacions_generic"
os.makedirs(output_folder, exist_ok=True)

# Carregar les dades
posicions = pd.read_sql("SELECT * FROM posicions", engine)
treballadors = pd.read_sql("SELECT * FROM treballadors WHERE torn = 'matí'", engine)
limitacions_df = pd.read_sql("SELECT * FROM treballador_limitacio", engine)
df_calendari = pd.read_sql("SELECT * FROM calendari_laboral WHERE es_laborable = 1", engine)

# Diccionari de limitacions per treballador
limitacions_dict = limitacions_df.groupby('id_treballador')['id_limitacio'].apply(set).to_dict()

# Renombrar columnes perquè to_datetime les entengui
df_calendari_rename = df_calendari.rename(columns={'aany': 'year', 'numero_mes': 'month', 'numero_dia': 'day'})
df_calendari['data'] = pd.to_datetime(df_calendari_rename[['year', 'month', 'day']])
avui = pd.Timestamp.today().normalize()
taula_laborables = df_calendari[df_calendari['data'] >= avui].head(7)
dies_laborables = taula_laborables['data'].dt.strftime('%Y-%m-%d').tolist()

# Funció de fitness amb restriccions de clasificador i familia

def fitness_func(ga_instance, solution, solution_idx):
    n_treballadors = len(treballadors)
    n_hores = 8
    n_posicions = len(posicions)

    fitness = 0
    solution = solution.reshape((n_treballadors, n_hores))

    for i, treb in treballadors.iterrows():
        id_treb = treb['id_treballador']
        limitacions_treb = limitacions_dict.get(id_treb, set())

        classificadors_usats = set()
        familia_anterior = None
        penalitzacio_limitacions = 0
        penalitzacio_familia_consec = 0

        for hora in range(n_hores):
            idx_pos = int(solution[i, hora])
            if idx_pos >= n_posicions:
                familia_anterior = None
                continue

            pos = posicions.iloc[idx_pos]

            id_lim = pos['id_limitacio']
            if pd.notna(id_lim) and id_lim in limitacions_treb:
                penalitzacio_limitacions += 1

            classificadors_usats.add(pos['clasificador'])

            if familia_anterior is not None and familia_anterior == pos['familia']:
                penalitzacio_familia_consec += 1

            familia_anterior = pos['familia']

        if len(classificadors_usats) > 1:
            fitness -= 300

        fitness -= penalitzacio_limitacions * 50
        fitness -= penalitzacio_familia_consec * 100

        if penalitzacio_limitacions == 0 and penalitzacio_familia_consec == 0 and len(classificadors_usats) == 1:
            fitness += 200

    return fitness

# Generació d'assignacions

def generar_assignacions_genetic(data, treballadors, posicions, limitacions_dict):
    n_treballadors = len(treballadors)
    n_hores = 8
    n_posicions = len(posicions)

    ga_instance = pygad.GA(
        num_generations=150,
        num_parents_mating=20,
        fitness_func=fitness_func,
        sol_per_pop=40,
        num_genes=n_treballadors * n_hores,
        gene_type=int,
        init_range_low=0,
        init_range_high=n_posicions,
        mutation_percent_genes=15,
        mutation_type="random",
        crossover_type="single_point",
        suppress_warnings=True
    )

    ga_instance.run()

    best_solution = ga_instance.best_solution()[0]
    reshaped = best_solution.reshape((n_treballadors, n_hores))

    assignacions = []
    for i, treb in treballadors.iterrows():
        for hora in range(n_hores):
            idx_pos = int(reshaped[i, hora])
            if idx_pos >= n_posicions:
                continue
            pos = posicions.iloc[idx_pos]

            assignacions.append({
                'data': data,
                'hora': f"{6+hora:02d}:00 - {7+hora:02d}:00",
                'id_treballador': treb['id_treballador'],
                'nom': treb['nom'],
                'id_posicio': pos['id_posicio'],
                'posicio': pos['posicio'],
                'clasificador': pos['clasificador'],
                'familia': pos['familia']
            })

    return assignacions

# Bucle per dies laborables
for data in dies_laborables:
    assignacions_dia = generar_assignacions_genetic(data, treballadors, posicions, limitacions_dict)
    df_dia = pd.DataFrame(assignacions_dia)
    df_dia = df_dia.sort_values(by=['id_treballador', 'hora'])

    output_path = os.path.join(output_folder, f"assignacions_{data}.csv")
    df_dia.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"✔️ Assignacions generades per {data}: {output_path}")