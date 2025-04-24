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

# Diccionari de limitacions per treballador
limitacions_dict = limitacions_df.groupby('id_treballador')['id_limitacio'].apply(set).to_dict()

# Paràmetres de l'ACO
n_treballadors = len(treballadors)
n_hores = 8  # De 06:00 a 14:00
n_posicions = len(posicions)
n_ants = 60
n_iter = 30
evaporacio = 1
intensitat = 2
pheromone_matrix = np.ones((n_treballadors, n_hores, n_posicions))

# Funció de fitness
def fitness(solution):
    fitness_value = 0
    solution = np.array(solution).reshape((n_treballadors, n_hores))

    for i, treb in treballadors.iterrows():
        id_treb = treb['id_treballador']
        limitacions_treb = limitacions_dict.get(id_treb, set())
        
        familia_anterior = None  # Per controlar repeticions consecutives
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
                fitness_value -= 50

            # Penalitzar si la família es repeteix consecutivament
            if familia_anterior == familia_actual:
                fitness_value -= 100  # penalització si repeteix família
            else:
                fitness_value += 10  # bonus per diversitat


            # Actualitzem valors per la següent hora
            familia_anterior = familia_actual
        
        if len(clasificadors_usats) > 1:
            fitness_value -= 50

    return fitness_value

# Funció de selecció segura
def seleccionar_posicio(pheromone_vector):
    posicio_probabilitats = pheromone_vector / np.sum(pheromone_vector)
    # En cas de problemes numèrics, normalitzem amb np.nan_to_num
    posicio_probabilitats = np.nan_to_num(posicio_probabilitats, nan=0.0)

        # Si tot són zeros, escollim aleatoriament
    if np.sum(posicio_probabilitats) == 0:
        posicio_probabilitats = np.ones_like(pheromone_vector) / len(pheromone_vector)

    return np.random.choice(range(len(pheromone_vector)), p=posicio_probabilitats)

# Funció d’evaporació
def evaporar_feromones(pheromone_matrix, evaporacio):
    pheromone_matrix *= (1 - evaporacio)

# Actualitzar feromones
def actualitzar_feromones(pheromone_matrix, ants_solutions, ants_fitness, intensitat):
    # Normalitzem fitness per evitar que les solucions dolentes sumin massa
    max_fit = max(ants_fitness)
    min_fit = min(ants_fitness)
    fit_range = max_fit - min_fit if max_fit != min_fit else 1

    for solution, fitness_value in zip(ants_solutions, ants_fitness):
        normalized_fit = (fitness_value - min_fit) / fit_range

        for t in range(n_treballadors):
            for h in range(n_hores):
                posicio = int(solution[t, h])
                pheromone_matrix[t, h, posicio] += intensitat * normalized_fit

# Algorisme ACO principal
def ant_colony_optimization():
    best_solution = None
    best_fitness = float('-inf')  # Busquem el màxim

    for iteration in range(n_iter):
        ants_solutions = []
        ants_fitness = []

        for ant in range(n_ants):
            solution = np.zeros((n_treballadors, n_hores), dtype=int)
            for t in range(n_treballadors):
                for h in range(n_hores):
                    pheromone_vector = pheromone_matrix[t, h]
                    posicio = seleccionar_posicio(pheromone_vector)
                    solution[t, h] = posicio

            fitness_value = fitness(solution.flatten())
            ants_solutions.append(solution)
            ants_fitness.append(fitness_value)

            if fitness_value > best_fitness:
                best_fitness = fitness_value
                best_solution = solution

        evaporar_feromones(pheromone_matrix, evaporacio)
        actualitzar_feromones(pheromone_matrix, ants_solutions, ants_fitness, intensitat)

        print(f"Iteració {iteration + 1}: millor fitness = {best_fitness}")

    return best_solution, best_fitness

# Executar
best_solution, best_fitness = ant_colony_optimization()

# Mostrar la millor solució
print("Millor solució trobada:")
print(best_solution)
print(f"Fitness de la millor solució: {best_fitness}")

# Crear DataFrame amb assignacions
assignacions = []
for t in range(n_treballadors):
    for h in range(n_hores):
        posicio_idx = best_solution[t, h]
        if posicio_idx >= n_posicions:
            continue
        pos = posicions.iloc[posicio_idx]
        treballador = treballadors.iloc[t]

        assignacions.append({
            'id_treballador': treballador['id_treballador'],
            'nom': treballador['nom'],
            'hora': f"{6 + h:02d}:00 - {7 + h:02d}:00",
            'id_posicio': pos['id_posicio'],
            'posicio': pos['posicio'],
            'clasificador': pos['clasificador'],
            'familia': pos['familia']
        })

df_assignacions = pd.DataFrame(assignacions)

# Guardar a CSV
output_folder = "assignacions_aco"
os.makedirs(output_folder, exist_ok=True)
output_file = os.path.join(output_folder, "assignacions_resultat.csv")
df_assignacions.to_csv(output_file, index=False, encoding='utf-8-sig')

print(f"✔️ Assignacions desades en el CSV: {output_file}")
