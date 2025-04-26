import numpy as np
import random
import pandas as pd
from sqlalchemy import create_engine
import os

# Connexió a la base de dades SQL Server
username = 'apineda'
password = 'apineda'
server = 'ALBA\\SQLEXPRESS'
database = 'bdapineda2'
driver = 'ODBC Driver 17 for SQL Server'
connection_url = f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver={driver}'
engine = create_engine(connection_url)

# Càrrega de les dades
posicions = pd.read_sql("SELECT * FROM posicions", engine)
treballadors = pd.read_sql("SELECT * FROM treballadors WHERE torn = 'matí'", engine)
limitacions_df = pd.read_sql("SELECT * FROM treballador_limitacio", engine)
df_calendari = pd.read_sql("SELECT * FROM calendari_laboral WHERE es_laborable = 1", engine)

# Processar els 7 propers dies laborables
df_calendari_rename = df_calendari.rename(columns={'aany': 'year', 'numero_mes': 'month', 'numero_dia': 'day'})
df_calendari['data'] = pd.to_datetime(df_calendari_rename[['year', 'month', 'day']])
avui = pd.Timestamp.today().normalize()
taula_laborables = df_calendari[df_calendari['data'] >= avui].sort_values('data').head(7)
dies_laborables = taula_laborables['data'].tolist()

#crear carpeta per guardar les assignacions
output_folder = "assignacions_aco"
os.makedirs(output_folder, exist_ok=True)

# Diccionari de limitacions per treballador
limitacions_dict = limitacions_df.groupby('id_treballador')['id_limitacio'].apply(set).to_dict()

# Paràmetres de l'ACO
n_treballadors = len(treballadors)
n_hores = 8
n_posicions = len(posicions)
n_ants = 10
n_iter = 10
evaporacio = 0.5
intensitat = 1

def reset_feromones():
    return np.ones((n_treballadors, n_hores, n_posicions))

# Funció de fitness per avaluar la solució
# La funció de fitness avalua la qualitat d'una solució donada
# Tenint en compte les limitacions dels treballadors, la diversitat de classificadors i la repetició de famílies.
# La funció retorna un valor de fitness que es pot utilitzar per comparar diferents solucions.
# La solució es passa com un vector aplanat i es transforma en una matriu de treballadors i hores.
# Per cada treballador, es comproven les seves limitacions i es penalitzen les solucions que no les respecten.
# També es penalitzen les solucions que repeteixen la mateixa família en hores consecutives.
# Finalment, es retorna el valor de fitness total per a la solució donada.
def fitness(solution):
    fitness_value = 0
    solution = np.array(solution).reshape((n_treballadors, n_hores))

    for i, treb in treballadors.iterrows():
        id_treb = treb['id_treballador']
        limitacions_treb = limitacions_dict.get(id_treb, set())
        familia_anterior = None
        clasificadors_usats = set()

        for hora in range(n_hores):
            idx_pos = int(solution[i, hora])
            if idx_pos >= n_posicions:
                continue

            pos = posicions.iloc[idx_pos]
            familia_actual = pos['familia']
            clasificador_actual = pos['clasificador']
            clasificadors_usats.add(clasificador_actual)

            # Penalitzacions per limitacions
            if pos['id_limitacio'] in limitacions_treb:
                fitness_value -= 100
            # Penalitzacions per repetició de família
            if familia_actual == familia_anterior:
                fitness_value -= 100
            else:
                fitness_value += 10
            # Actualitzem la família anterior
            familia_anterior = familia_actual

        # Penalitzacions per diversitat de classificadors
        if len(clasificadors_usats) > 1:
            fitness_value -= 200

    return fitness_value

## Funció per seleccionar una posició basada en el vector de feromones
# Aquesta funció selecciona una posició aleatòriament basada en la probabilitat proporcional a la quantitat de feromones
# dipositades en cada posició. Si la suma de les feromones és zero, es selecciona aleatòriament entre totes les posicions.
# La funció retorna l'índex de la posició seleccionada.
def seleccionar_posicio(pheromone_vector):
    posicio_probabilitats = pheromone_vector / np.sum(pheromone_vector)
    posicio_probabilitats = np.nan_to_num(posicio_probabilitats, nan=0.0)
    if np.sum(posicio_probabilitats) == 0:
        posicio_probabilitats = np.ones_like(pheromone_vector) / len(pheromone_vector)
    return np.random.choice(range(len(pheromone_vector)), p=posicio_probabilitats)

# Funcions per a l'evaporació i actualització de feromones
# La funció d'evaporació redueix la quantitat de feromones en cada posició en una proporció determinada.
# Això ajuda a evitar que les solucions es quedin atrapades en solucions locals i fomenta la diversitat en les solucions generades.
# La funció d'actualització de feromones afegeix una quantitat de feromones a les posicions seleccionades per les formigues
# en funció de la seva qualitat (fitness). Les solucions amb millor fitness reben més feromones, mentre que les solucions amb pitjor fitness reben menys feromones.
# Això ajuda a guiar les formigues cap a solucions millors en futures iteracions.
# La funció d'actualització de feromones normalitza el fitness per assegurar-se que les quantitats de feromones afegides són proporcionals a la qualitat de les solucions.
# La normalització es fa per evitar que les solucions amb fitness molt alt o molt baix afectin excessivament la quantitat de feromones afegides.
# La normalització es fa restat el fitness mínim i dividint per l'interval de fitness (max_fit - min_fit).
# Si el fitness màxim i mínim són iguals, es fa servir un valor de 1 per evitar la divisió per zero.
# La funció d'actualització de feromones recorre totes les solucions generades per les formigues i actualitza la matriu de feromones
# per cada treballador, hora i posició seleccionada. La quantitat de feromones afegides es multiplica per la intensitat i el fitness normalitzat.
def evaporar_feromones(pheromone_matrix, evaporacio):
    pheromone_matrix *= (1 - evaporacio)
def actualitzar_feromones(pheromone_matrix, ants_solutions, ants_fitness, intensitat):
    max_fit = max(ants_fitness)
    min_fit = min(ants_fitness)
    fit_range = max_fit - min_fit if max_fit != min_fit else 1
    # Normalitzar el fitness
    for solution, fitness_value in zip(ants_solutions, ants_fitness):
        normalized_fit = (fitness_value - min_fit) / fit_range
        for t in range(n_treballadors):
            for h in range(n_hores):
                posicio = int(solution[t, h])
                pheromone_matrix[t, h, posicio] += intensitat * normalized_fit
# Funció principal de l'ACO
# Aquesta funció és la implementació principal de l'algorisme d'optimització per colònies de formigues (ACO).
# La funció inicialitza la matriu de feromones i executa un nombre determinat d'iteracions.
# En cada iteració, es generen solucions per a cada formiga i es calcula el seu fitness.
# Les solucions amb millor fitness es guarden com a la millor solució trobada fins ara.
# Després de generar les solucions, es realitza l'evaporació de feromones i l'actualització de feromones
# en funció de les solucions generades i el seu fitness.
# La funció retorna la millor solució trobada i el seu fitness associat.
# La matriu de feromones es redimensiona per tenir dimensions (n_treballadors, n_hores, n_posicions).
def ant_colony_optimization(pheromone_matrix):
    best_solution = None
    best_fitness = float('-inf')
    # Iteracions de l'ACO
    for iteration in range(n_iter):
        ants_solutions = []
        ants_fitness = []
        # Generar solucions per a cada formiga
        for ant in range(n_ants):
            solution = np.zeros((n_treballadors, n_hores), dtype=int)
            # Generar una solució per cada treballador i hora
            for t in range(n_treballadors):
                for h in range(n_hores):
                    pheromone_vector = pheromone_matrix[t, h]
                    posicio = seleccionar_posicio(pheromone_vector)
                    solution[t, h] = posicio
            # Calcular el fitness de la solució generada
            fitness_value = fitness(solution.flatten())
            ants_solutions.append(solution)
            ants_fitness.append(fitness_value)
            # Actualitzar la millor solució trobada fins ara
            if fitness_value > best_fitness:
                best_fitness = fitness_value
                best_solution = solution
        # Evaporar feromones
        # Actualitzar feromones en funció de les solucions generades
        evaporar_feromones(pheromone_matrix, evaporacio)
        actualitzar_feromones(pheromone_matrix, ants_solutions, ants_fitness, intensitat)

    return best_solution, best_fitness
## Funció per generar assignacions d'un dia
def generar_assignacions_dia(data):
    pheromone_matrix = reset_feromones()
    best_solution, _ = ant_colony_optimization(pheromone_matrix)
    assignacions = []
    # Generar assignacions per cada treballador i hora
    # La funció genera assignacions per a cada treballador i hora en un dia determinat.
    for t in range(n_treballadors):
        for h in range(n_hores):
            posicio_idx = best_solution[t, h]
            if posicio_idx >= n_posicions:
                continue
            pos = posicions.iloc[posicio_idx]
            treballador = treballadors.iloc[t]
            assignacions.append({
                'data': data.strftime('%Y-%m-%d'),
                'id_treballador': treballador['id_treballador'],
                'nom': treballador['nom'],
                'hora': f"{6 + h:02d}:00 - {7 + h:02d}:00",
                'id_posicio': pos['id_posicio'],
                'posicio': pos['posicio'],
                'clasificador': pos['clasificador'],
                'familia': pos['familia']
            })

    return assignacions

# Generar assignacions per cada dia laborable
for data in dies_laborables:
    assignacions_dia = generar_assignacions_dia(data)
    df_dia = pd.DataFrame(assignacions_dia)
    df_dia = df_dia.sort_values(by=['id_treballador', 'hora'])
    # Guardar les assignacions en un fitxer CSV
    output_path = os.path.join(output_folder, f"assignacions_{data.strftime('%Y-%m-%d')}.csv")
    df_dia.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"assignacions generades per {data.strftime('%Y-%m-%d')}: {output_path}")
