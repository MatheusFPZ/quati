import pysolr
import json
import time

# Configurações
SOLR_URL = 'http://localhost:8983/solr/exemplo_stemming'
JSON_FILE = 'quati_1M_passages.json'
BATCH_SIZE = 1000

# Conectar ao Solr
solr = pysolr.Solr(SOLR_URL, timeout=60)

def carregar_json_em_lotes(caminho_arquivo, batch_size):
    with open(caminho_arquivo, 'r', encoding='utf-8') as f:
        documentos = json.load(f)
        for i in range(0, len(documentos), batch_size):
            yield documentos[i:i + batch_size]

def indexar():
    total = 0
    start_time = time.time()

    for lote in carregar_json_em_lotes(JSON_FILE, BATCH_SIZE):
        for doc in lote:
            passage_text = doc.get('passage', '')
            passage_id = doc.get('passage_id', '')
            doc['passage_id'] = passage_id
            doc['texto_com_stem'] = passage_text  # Só este campo

        try:
            solr.add(lote, commit=False)
            total += len(lote)
            print(f"[com_stem] {total} documentos indexados...")
        except Exception as e:
            print(f"[com_stem] ❌ Erro ao indexar lote: {e}")

    try:
        solr.commit()
        print(f"[com_stem] ✅ Commit final realizado.")
    except Exception as e:
        print(f"[com_stem] ❌ Erro no commit final: {e}")

    tempo_total = time.time() - start_time
    print(f"\n⏱️ Tempo de indexação: {tempo_total:.2f} segundos")

# Executar
print("🔄 Iniciando indexação COM stemming...")
indexar()
