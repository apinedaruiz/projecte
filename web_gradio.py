import gradio as gr
import pandas as pd
from minio import Minio
from io import BytesIO
from datetime import datetime
import os
import pytz
import subprocess

#Connexió a MinIO
minio_client = Minio(
    "minio:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

bucket_name = "assignacions-csv"
carpeta_assignacions = "assignacions_cpsat/"
carpeta_validacions = "validacions_cpsat/"

zona_horaria = pytz.timezone("Europe/Madrid")

def ara_local():
    return datetime.now(zona_horaria)

#Interpretar franja horària "08:00-09:00" amb fus horari
def parse_franja_horaria(interval):
    try:
        start_str, end_str = interval.split("-")
        start_dt = zona_horaria.localize(datetime.strptime(start_str.strip(), "%H:%M"))
        end_dt = zona_horaria.localize(datetime.strptime(end_str.strip(), "%H:%M"))
        return start_dt.time(), end_dt.time()
    except Exception as e:
        print(f"Error en parse_franja_horaria: {e}")
        return None, None

#Determinar el torn actual
def obtenir_torn_actual():
    hora = ara_local().hour
    if 6 <= hora < 14:
        return "matí"
    elif 14 <= hora < 22:
        return "tarda"
    else:
        return "nit"

#Consultar assignació
def consulta(codi):
    global assignacio_actual
    assignacio_actual = None

    try:
        codi_int = int(codi)
    except:
        return "Introdueix un codi vàlid.", gr.update(visible=False), ""

    torn = obtenir_torn_actual()
    #data_avui = "2025-05-02"  # Per a proves, utilitzem una data fixa
    data_avui = ara_local().strftime('%Y-%m-%d')
    fitxer_torn = f"{carpeta_assignacions}assignacions_{data_avui}_{torn}.csv"

    try:
        response = minio_client.get_object(bucket_name, fitxer_torn)
        df = pd.read_csv(BytesIO(response.read()), encoding='utf-8-sig')
        response.close()
        response.release_conn()
    except Exception:
        return f"No hi ha assignacions disponibles per al torn de **{torn}** ({data_avui}).", gr.update(visible=False), ""

    df_treballador = df[df['id_treballador'] == codi_int]
    if df_treballador.empty:
        return "El treballador no pertany a aquest torn.", gr.update(visible=False), ""

    ara = ara_local().time()
    assignacions_valides = []

    for _, row in df_treballador.iterrows():
        start, end = parse_franja_horaria(row['hora'])
        if start and end and start <= ara <= end:
            assignacions_valides.append(row)

    if assignacions_valides:
        assignacio_actual = assignacions_valides[-1]

    if assignacio_actual is not None:
        text = f"""
### Assignació Actual

**Data:** {assignacio_actual['data']}
**Hora:** {assignacio_actual['hora']}
**Nom:** {assignacio_actual['nom']}

**Posició:** {assignacio_actual['posicio']}
**Clasificador:** {assignacio_actual['clasificador']}
"""
        return text, gr.update(visible=True), ""
    else:
        return "Ara mateix no tens cap assignació activa.", gr.update(visible=False), ""

# 🔹 Validar assignació actual
def validar():
    global assignacio_actual

    if assignacio_actual is None:
        return "❌ Cap assignació activa per validar."

    registre = {
        "timestamp": ara_local().isoformat(timespec="seconds"),
        "id_treballador": assignacio_actual['id_treballador'],
        "data": assignacio_actual['data'],
        "hora": assignacio_actual['hora'],
        "posicio": assignacio_actual['posicio'],
        "clasificador": assignacio_actual['clasificador']
    }

    fitxer_validacio = f"{carpeta_validacions}{datetime.now().strftime('%Y-%m-%d')}.csv"

    try:
        try:
            resposta = minio_client.get_object(bucket_name, fitxer_validacio)
            df_ant = pd.read_csv(BytesIO(resposta.read()), encoding='utf-8-sig')
            resposta.close()
            resposta.release_conn()
        except:
            df_ant = pd.DataFrame()

        df_total = pd.concat([df_ant, pd.DataFrame([registre])], ignore_index=True)

        buffer = BytesIO()
        df_total.to_csv(buffer, index=False, encoding='utf-8-sig')
        buffer.seek(0)

        minio_client.put_object(
            bucket_name,
            fitxer_validacio,
            data=buffer,
            length=buffer.getbuffer().nbytes,
            content_type="text/csv"
        )

    except Exception as e:
        return f"⚠️ Error al pujar la validació a MinIO: {e}"

    assignacio_actual = None
    return "✅ Validació enregistrada correctament!"

def executar_script():
    try:
        resultat = subprocess.run(["python3", "ex1_CP-SAT.py"], capture_output=True, text=True)
        return resultat.stdout or resultat.stderr
    except Exception as e:
        return f"❌ Error: {e}"


#Interfície Gradio
with gr.Blocks(theme=gr.themes.Citrus()) as app:

    # 🔸 Pàgina de menú
    with gr.Column(visible=True) as menu_page:
        gr.Markdown("## 🧭 Menú Principal")
        with gr.Row():
            btn_assignacions = gr.Button("🔍 Consultar Assignacions")
            btn_executar = gr.Button("⚙️ Executar Script Local")
        output_script = gr.Textbox(label="Resultat de l'Script", visible=False)

    # 🔸 Pàgina d’assignacions
    with gr.Column(visible=False) as assignacions_page:
        gr.Markdown("## 📋 Consulta i Validació d'Assignacions")
        codi_input = gr.Textbox(label="Codi de treballador")
        consulta_btn = gr.Button("🔍 Consultar Assignació")
        output_md = gr.Markdown()
        validar_btn = gr.Button("✅ Validar Assignació", visible=False)
        resultat_md = gr.Markdown()
        tornar_menu = gr.Button("🔙 Tornar al menú")

    # 🔸 Funcions de navegació
    def mostra_assignacions():
        return gr.update(visible=False), gr.update(visible=True)

    def torna_menu():
        return gr.update(visible=True), gr.update(visible=False)

    # 🔸 Enllaç de botons
    btn_assignacions.click(mostra_assignacions, outputs=[menu_page, assignacions_page])
    tornar_menu.click(torna_menu, outputs=[menu_page, assignacions_page])
    btn_executar.click(lambda: (executar_script(), gr.update(visible=True)), outputs=[output_script, output_script])
    consulta_btn.click(consulta, inputs=codi_input, outputs=[output_md, validar_btn, resultat_md])
    validar_btn.click(validar, outputs=resultat_md)


app.launch(server_name="0.0.0.0", server_port=7860)