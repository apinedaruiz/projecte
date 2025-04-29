import gradio as gr
import pandas as pd
from minio import Minio
from io import BytesIO
from datetime import datetime
import os

# 🔹 Connexió a MinIO
minio_client = Minio(
    "localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

bucket_name = "assignacions-csv"
carpeta_assignacions = "assignacions_cpsat/"
fitxer_avui = f"{carpeta_assignacions}assignacions_{datetime.now().strftime('%Y-%m-%d')}.csv"
carpeta_validacions = "validacions_cpsat/"
fitxer_validacions = f"{carpeta_assignacions}{datetime.now().strftime('%Y-%m-%d')}.csv"

# 🔹 Interpretar franja horària "08:00-09:00"
def parse_franja_horaria(interval):
    try:
        start_str, end_str = interval.split("-")
        start = datetime.strptime(start_str.strip(), "%H:%M").time()
        end = datetime.strptime(end_str.strip(), "%H:%M").time()
        return start, end
    except:
        return None, None

# 🔹 Consultar assignació
def consulta(codi):
    global assignacio_actual  # Per fer-la accessible al botó de validació

    try:
        codi_int = int(codi)
    except:
        return "⚠️ Introdueix un codi vàlid.", gr.update(visible=False), ""

    try:
        response = minio_client.get_object(bucket_name, fitxer_avui)
        df = pd.read_csv(BytesIO(response.read()), encoding='utf-8-sig')
        response.close()
        response.release_conn()
    except Exception as e:
        return f"⚠️ Error al llegir assignacions: {e}", gr.update(visible=False), ""

    df_treballador = df[df['id_treballador'] == codi_int]
    if df_treballador.empty:
        return "⚠️ No s'ha trobat cap treballador amb aquest codi.", gr.update(visible=False), ""

    #ara = datetime.now().time()
    HORA_MANUAL = "10:30"  # Per a proves, utilitzem una hora fixa
    ara = datetime.strptime(HORA_MANUAL, "%H:%M").time()

    assignacio_actual = None
    for _, row in df_treballador.iterrows():
        start, end = parse_franja_horaria(row['hora'])
        if start and end and start <= ara <= end:
            assignacio_actual = row
            break

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
        return "ℹ️ Ara mateix no tens cap assignació activa.", gr.update(visible=False), ""

# 🔹 Validar tasca
def validar():
    global assignacio_actual  # Afegeix això

    if assignacio_actual is None:
        return "❌ Cap assignació activa per validar."

    registre = {
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "id_treballador": assignacio_actual['id_treballador'],
        "data": assignacio_actual['data'],
        "hora": assignacio_actual['hora'],
        "posicio": assignacio_actual['posicio'],
        "clasificador": assignacio_actual['clasificador']
    }

    df_validacio = pd.DataFrame([registre])

    try:
        # Llegeix l'existent o crea un nou DataFrame
        dia_avui = datetime.now().strftime('%Y-%m-%d')
        fitxer_validacio = f"validacions_cpsat/{dia_avui}.csv"

        try:
            resposta = minio_client.get_object(bucket_name, fitxer_validacio)
            df_ant = pd.read_csv(BytesIO(resposta.read()), encoding='utf-8-sig')
            resposta.close()
            resposta.release_conn()
        except:
            df_ant = pd.DataFrame()

        df_total = pd.concat([df_ant, df_validacio], ignore_index=True)

        # Escriu a bytes
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
        return f"⚠️ Error al pujar a MinIO: {e}"
    assignacio_actual = None  # Global per ús compartit
    return "✅ Validació enregistrada correctament!"

with gr.Blocks(theme=gr.themes.Citrus()) as demo:
    gr.Markdown("## 📋 Consulta i Validació d'Assignacions")
    codi_input = gr.Textbox(label="Codi de treballador")
    consulta_btn = gr.Button("🔍 Consultar Assignació")
    output_md = gr.Markdown()
    validar_btn = gr.Button("✅ Validar Assignació", visible=False)
    resultat_md = gr.Markdown()

    consulta_btn.click(consulta, inputs=codi_input, outputs=[output_md, validar_btn, resultat_md])
    validar_btn.click(validar, outputs=resultat_md)

demo.launch()
