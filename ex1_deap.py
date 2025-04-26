import pandas as pd
import random
from deap import base, creator, tools, algorithms
import numpy as np
import os
from sqlalchemy import create_engine

# Connexió a la base de dades SQL Server
username = 'apineda'
password = 'apineda'
server = 'ALBA\\SQLEXPRESS'
database = 'bdapineda2'
driver = 'ODBC Driver 17 for SQL Server'
connection_url = f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver={driver}'
engine = create_engine(connection_url)

# Carregar dades
posicions = pd.read_sql("SELECT * FROM posicions", engine)
treballadors = pd.read_sql("SELECT * FROM treballadors WHERE torn = 'matí'", engine)
limitacions_df = pd.read_sql("SELECT * FROM treballador_limitacio", engine)
df_calendari = pd.read_sql("SELECT * FROM calendari_laboral WHERE es_laborable = 1", engine)

# Generar assignacions per 7 dies laborables
df_calendari_rename = df_calendari.rename(columns={'aany': 'year', 'numero_mes': 'month', 'numero_dia': 'day'})
df_calendari['data'] = pd.to_datetime(df_calendari_rename[['year', 'month', 'day']])
avui = pd.Timestamp.today().normalize()
taula_laborables = df_calendari[df_calendari['data'] >= avui].sort_values('data').head(7)
dies_laborables = taula_laborables['data'].dt.strftime('%Y-%m-%d').tolist()

# Carpeta per guardar
output_folder = 'assignacions_deap'
os.makedirs(output_folder, exist_ok=True)

# Diccionari de limitacions
limitacions_dict = limitacions_df.groupby('id_treballador')['id_limitacio'].apply(set).to_dict()

# Crear tipus per DEAP
creator.create("FitnessMax", base.Fitness, weights=(1.0,))
creator.create("Individual", list, fitness=creator.FitnessMax)

# Funció de fitness
def fitness(individual):
    fitness_value = 0
    n_treballadors = len(treballadors)
    n_hores = 8
    n_posicions = len(posicions)

    solution = np.array(individual).reshape((n_treballadors, n_hores))

    for i, treb in treballadors.iterrows():
        id_treb = treb['id_treballador']
        limitacions_treb = limitacions_dict.get(id_treb, set())

        families = []
        classificadors = []
        canvi_classificador = False

        for hora in range(n_hores):
            idx_pos = int(solution[i, hora])
            if idx_pos >= n_posicions:
                continue  # Assignació buida

            pos = posicions.iloc[idx_pos]

            # Penalització per limitacions
            if pos['id_limitacio'] in limitacions_treb:
                fitness_value -= 100

            families.append(pos['familia'])
            classificadors.append(pos['clasificador'])

        # Penalitzar canvi de classificador
        primer_classificador = classificadors[0]
        for c in classificadors:
            if c != primer_classificador:
                canvi_classificador = True
                break

        if canvi_classificador:
            fitness_value -= 300
        else:
            fitness_value += 100

        # Penalitzar més de dues hores seguides mateixa família
        consecutives = 1
        for j in range(1, len(families)):
            if families[j] == families[j-1]:
                consecutives += 1
                if consecutives > 2:
                    fitness_value -= 50
            else:
                consecutives = 1

    return fitness_value,

# Crear individus
def create_individual():
    return [random.randint(0, len(posicions) - 1) for _ in range(len(treballadors) * 8)]

# Toolbox DEAP
toolbox = base.Toolbox()
toolbox.register("individual", tools.initIterate, creator.Individual, create_individual)
toolbox.register("population", tools.initRepeat, list, toolbox.individual)
toolbox.register("mate", tools.cxOnePoint)
toolbox.register("mutate", tools.mutFlipBit, indpb=0.2)
toolbox.register("select", tools.selTournament, tournsize=3)
toolbox.register("evaluate", fitness)

# Crear població
population = toolbox.population(n=50)

# Executar l'algoritme evolutiu
algorithms.eaSimple(population, toolbox, cxpb=0.7, mutpb=0.2, ngen=50, verbose=True)

# Millor individu
best_individual = tools.selBest(population, 1)[0]

# Funció per generar assignacions per un dia concret
def generar_assignacions_dia(data, individu):
    assignacions = []
    hores = [f"{h}:00" for h in range(6, 14)]  # De 6:00 a 13:00

    solution = np.array(individu).reshape((len(treballadors), 8))

    for i, treb in treballadors.iterrows():
        id_treb = treb['id_treballador']
        nom = treb['nom']
        limitacions_treb = limitacions_dict.get(id_treb, set())  # 🔥 Agafem limitacions

        # 🔥 Triar classificador fix pel dia
        primers_idx = [int(idx) for idx in solution[i]]
        primers_posicions = posicions.iloc[primers_idx]
        clasificadors = primers_posicions['clasificador'].unique()
        classificador_dia = random.choice(clasificadors)

        # 🔥 Només posicions amb aquest classificador i sense limitacions
        posicions_classificador = posicions[
            (posicions['clasificador'] == classificador_dia) &
            (~posicions['id_limitacio'].isin(limitacions_treb))  # 🔥 Evitar limitacions
        ]

        familia_anterior = None  # Control de famílies
        for h_idx, hora in enumerate(hores):
            possibles_posicions = posicions_classificador.copy()

            # 🔥 No repetir família
            if familia_anterior:
                possibles_posicions = possibles_posicions[posicions_classificador['familia'] != familia_anterior]

            if possibles_posicions.empty:
                possibles_posicions = posicions_classificador  # Relaxar si no troba (millor això que quedar-se sense assignació)

            if possibles_posicions.empty:
                # 🔥 No trobem cap posició sense limitacions, posem valor nul.
                assignacions.append({
                    'data': data,
                    'hora': hora,
                    'id_treballador': id_treb,
                    'nom': nom,
                    'posicio': None,
                    'clasificador': None,
                    'familia': None
                })
                continue

            posicio_aleatoria = possibles_posicions.sample(1).iloc[0]

            assignacions.append({
                'data': data,
                'hora': hora,
                'id_treballador': id_treb,
                'nom': nom,
                'posicio': posicio_aleatoria['posicio'],
                'clasificador': posicio_aleatoria['clasificador'],
                'familia': posicio_aleatoria['familia']
            })

            familia_anterior = posicio_aleatoria['familia']

    return assignacions






# Generar i guardar
for data in dies_laborables:
    # Crear una còpia del millor individu
    individu_dia = creator.Individual(best_individual[:])

    # Aplicar una petita mutació (ex: canviar algunes assignacions aleatòriament)
    toolbox.mutate(individu_dia)
    
    # Assignar el nou individu per al dia
    assignacions_dia = generar_assignacions_dia(data, individu_dia)
    
    df_dia = pd.DataFrame(assignacions_dia)

    # 🔥 Crear columna numèrica per ordenar correctament per hora
    df_dia['hora_num'] = df_dia['hora'].str.extract(r'(\d+)').astype(int)

    df_dia = df_dia.sort_values(by=['id_treballador', 'hora_num'])

    # ❌ Eliminar columna auxiliar
    df_dia = df_dia.drop(columns=['hora_num'])
    

    output_path = os.path.join(output_folder, f"assignacions_{data}.csv")
    df_dia.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"✔️ Assignacions generades per {data}: {output_path}")

