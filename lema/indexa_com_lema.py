import requests
import pysolr
import json
import time
import spacy
import multiprocessing

# Configurações
SOLR_URL = 'http://localhost:8983/solr/exemplo_lema'
JSON_FILE = 'quati_1M_passages.json'
BATCH_SIZE = 1000      # Número de docs enviados ao Solr por vez
LEMA_BATCH = 100       # Número de textos lematizados por processo por vez
NUM_PROCESSES = multiprocessing.cpu_count()  # Número de núcleos (ajuste se quiser)

# Inicia Solr
solr = pysolr.Solr(SOLR_URL, timeout=60)

# ---- Multiprocessing Worker ----
def worker_lemmatizer(textos):
    nlp = spacy.load("pt_core_news_sm")
    result = [" ".join([token.lemma_ for token in doc]) for doc in nlp.pipe(textos, batch_size=32)]
    return result

def lemmatize_parallel(lista_textos, num_processes=NUM_PROCESSES, batch_size=LEMA_BATCH):
    # Divide os textos em chunks menores para cada worker
    chunks = [lista_textos[i:i+batch_size] for i in range(0, len(lista_textos), batch_size)]
    lematizados = []
    with multiprocessing.Pool(processes=num_processes) as pool:
        for result in pool.imap(worker_lemmatizer, chunks):
            lematizados.extend(result)
    return lematizados

# ---- Funções de leitura e indexação ----
def carregar_json_em_lotes(caminho_arquivo, batch_size):
    with open(caminho_arquivo, 'r', encoding='utf-8') as f:
        documentos = json.load(f)
        for i in range(0, len(documentos), batch_size):
            yield documentos[i:i + batch_size]

def indexar(tipo):
    total = 0
    start_time = time.time()

    for lote in carregar_json_em_lotes(JSON_FILE, BATCH_SIZE):
        passage_texts = [doc.get('passage', '') for doc in lote]

        if tipo == "com_lema":
            # Lematização paralela!
            lematizados = lemmatize_parallel(passage_texts)
            for doc, lema in zip(lote, lematizados):
                doc['texto_com_lema'] = lema
        elif tipo == "sem_lema":
            for doc in lote:
                doc['texto_sem_lema'] = doc.get('passage', '')

        try:
            solr.add(lote, commit=False)
            total += len(lote)
            print(f"[{tipo}] {total} documentos indexados...")
        except Exception as e:
            print(f"[{tipo}] ❌ Erro ao indexar lote: {e}")

    try:
        solr.commit()
        print(f"[{tipo}] ✅ Commit final realizado.")
    except Exception as e:
        print(f"[{tipo}] ❌ Erro no commit final: {e}")

    tempo_total = time.time() - start_time
    print(f"\n⏱️ Tempo de indexação [{tipo}]: {tempo_total:.2f} segundos")

# ---- Execução ----
if __name__ == "__main__":
    print("🔄 Iniciando indexação COM lematização (paralelo)...")
    indexar("com_lema")

