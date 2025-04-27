import pandas as pd
import numpy as np
import pygad
import os
from sqlalchemy import create_engine

# CONFIGURACIÓ
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

# Diccionari de limitacions
limitacions_dict = limitacions_df.groupby('id_treballador')['id_limitacio'].apply(set).to_dict()

# Dies laborables
df_calendari_rename = df_calendari.rename(columns={'aany': 'year', 'numero_mes': 'month', 'numero_dia': 'day'})
df_calendari['data'] = pd.to_datetime(df_calendari_rename[['year', 'month', 'day']])
avui = pd.Timestamp.today().normalize()
taula_laborables = df_calendari[df_calendari['data'] >= avui].head(7)
dies_laborables = taula_laborables['data'].dt.strftime('%Y-%m-%d').tolist()


# FITNESS FUNCTION
def fitness_func(ga_instance, solution, solution_idx):
    n_treballadors = len(treballadors)
    n_hores = 8
    n_posicions = len(posicions)

    solution = solution.reshape((n_treballadors, n_hores))
    fitness = 0

    treballadors_np = treballadors[['id_treballador']].to_numpy().flatten()
    posicions_np = posicions[['id_limitacio', 'clasificador', 'familia']].to_numpy()

    for i in range(n_treballadors):
        id_treb = treballadors_np[i]
        limitacions_treb = limitacions_dict.get(id_treb, set())

        classificadors_usats = set()
        familia_anterior = None
        penalitzacio = 0

        for hora in range(n_hores):
            idx_pos = solution[i, hora]
            if idx_pos >= n_posicions:
                familia_anterior = None
                continue

            pos_info = posicions_np[idx_pos]
            id_lim, clasificador, familia = pos_info

            if pd.notna(id_lim) and id_lim in limitacions_treb:
                penalitzacio += 1000

            if familia_anterior is not None and familia_anterior == familia:
                penalitzacio += 500

            classificadors_usats.add(clasificador)
            familia_anterior = familia

        if len(classificadors_usats) > 1:
            penalitzacio += 1000

        fitness += (1000 - penalitzacio)

    return fitness


# FUNCIÓ PRINCIPAL
def generar_assignacions_genetic(data, treballadors, posicions, limitacions_dict):
    n_treballadors = len(treballadors)
    n_hores = 8
    n_posicions = len(posicions)

    best_solution = None
    best_solution_fitness = -np.inf

    for intent in range(3):  # Només 3 intents

        ga_instance = pygad.GA(
            num_generations=200,  # 200 generacions
            sol_per_pop=40,       # 40 individus
            num_parents_mating=20,
            fitness_func=fitness_func,
            num_genes=n_treballadors * n_hores,
            gene_type=int,
            init_range_low=0,
            init_range_high=n_posicions,
            mutation_percent_genes=20,
            mutation_type="random",
            crossover_type="single_point",
            suppress_warnings=True
        )

        ga_instance.run()

        candidate_solution, candidate_fitness, _ = ga_instance.best_solution()

        if candidate_fitness > best_solution_fitness:
            best_solution = candidate_solution
            best_solution_fitness = candidate_fitness

        # Si ja tenim una molt bona solució, parem
        if best_solution_fitness >= 1000 * n_treballadors:
            break

    if best_solution_fitness <= 0:
        raise Exception(f"No s'ha trobat cap assignació vàlida per {data}.")

    # Construir assignacions
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


# BUCLE PER A CADA DIA
for data in dies_laborables:
    assignacions_dia = generar_assignacions_genetic(data, treballadors, posicions, limitacions_dict)
    df_dia = pd.DataFrame(assignacions_dia)
    df_dia = df_dia.sort_values(by=['id_treballador', 'hora'])

    output_path = os.path.join(output_folder, f"assignacions_{data}.csv")
    df_dia.to_csv(output_path, index=False, encoding='utf-8-sig')
