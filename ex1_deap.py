import pandas as pd
import random
from deap import base, creator, tools, algorithms
import numpy as np
import os
from sqlalchemy import create_engine

# Connexió a la base de dades SQL Server (modifica segons les teves necessitats)
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

# Creem els tipus per l'algoritme evolutiu
creator.create("FitnessMax", base.Fitness, weights=(1.0,))  # Maximitzar fitness
creator.create("Individual", list, fitness=creator.FitnessMax)

# Funció de fitness
def fitness(individual):
    fitness_value = 0
    n_treballadors = len(treballadors)
    n_hores = 8  # De 06:00 a 14:00
    n_posicions = len(posicions)

    # Reestructuració de la solució
    solution = np.array(individual).reshape((n_treballadors, n_hores))

    clasificadors_usats = {}

    for i, treb in treballadors.iterrows():
        id_treb = treb['id_treballador']
        limitacions_treb = limitacions_dict.get(id_treb, set())
        families_vistes = set()

        for hora in range(n_hores):
            idx_pos = int(solution[i, hora])
            if idx_pos >= n_posicions:
                continue  # assignació buida

            pos = posicions.iloc[idx_pos]

            # Penalitzacions per limitacions
            if pos['id_limitacio'] in limitacions_treb:
                fitness_value -= 50  # penalització per limitació

            # Penalitzar si la família es repeteix consecutivament
            if pos['familia'] in families_vistes:
                fitness_value -= 20
            else:
                families_vistes.add(pos['familia'])
                fitness_value += 5

            # Penalitzar canvis de classificador
            c = pos['clasificador']
            if id_treb in clasificadors_usats and clasificadors_usats[id_treb] != c:
                fitness_value -= 30
            clasificadors_usats[id_treb] = c

    return fitness_value,

# Inicialització de l'individu
def create_individual():
    return [random.randint(0, len(posicions) - 1) for _ in range(len(treballadors) * 8)]

# Crea l'objecte de l'algoritme
toolbox = base.Toolbox()
toolbox.register("individual", tools.initIterate, creator.Individual, create_individual)
toolbox.register("population", tools.initRepeat, list, toolbox.individual)
toolbox.register("mate", tools.cxOnePoint)
toolbox.register("mutate", tools.mutFlipBit, indpb=0.2)
toolbox.register("select", tools.selTournament, tournsize=3)
toolbox.register("evaluate", fitness)

# Crear la població
population = toolbox.population(n=50)

# Algoritme evolutiu
algorithms.eaSimple(population, toolbox, cxpb=0.7, mutpb=0.2, ngen=50, verbose=True)

# Obtenir la millor solució
best_individual = tools.selBest(population, 1)[0]

