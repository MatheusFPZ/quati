import requests, csv, time, difflib

def get_relevant_query_ids(qrels_file):
    relevant_ids = set()
    with open(qrels_file, "r") as f:
        for line in f:
            query_id, _, _, score = line.strip().split()
            if int(score) > 0:
                relevant_ids.add(query_id)
    return relevant_ids

def carregar_consultas_relevantes(topics_file, relevant_ids):
    consultas = []
    with open(topics_file, "r", encoding="utf-8") as f:
        for line in f:
            query_id, query_text = line.strip().split("\t", 1)
            if query_id in relevant_ids:
                consultas.append((query_id, query_text))
    return consultas

def consultar_solr(consultas, solr_url, output_csv, campo):
    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["número_da_consulta", "número_do_documento", "ordem_no_ranking", "score"])

        total_start = time.time()
        for query_id, query_text in consultas:
            start = time.time()
            params = {
                "q": query_text,
                "rows": 100,
                "fl": "passage_id,score",
                "df": campo
            }
            r = requests.get(solr_url, params=params).json()
            for rank, doc in enumerate(r["response"]["docs"], start=1):
                writer.writerow([
                    query_id,
                    doc["passage_id"][0] if isinstance(doc["passage_id"], list) else doc["passage_id"],
                    rank,
                    doc["score"]
                ])
            end = time.time()
            print(f"✅ Consulta {query_id} ({campo}) concluída em {end - start:.3f} segundos")
        total_end = time.time()
        print(f"\n⏱️ Tempo total de consulta [{campo}]: {total_end - total_start:.2f} segundos")

def comparar_csvs(file1, file2):
    with open(file1, encoding="utf-8") as f1, open(file2, encoding="utf-8") as f2:
        linhas1 = f1.readlines()
        linhas2 = f2.readlines()
        diferencas = list(difflib.unified_diff(linhas1, linhas2))
        if not diferencas:
            print("⚠️ Os arquivos são idênticos! Verifique se o campo df está realmente funcionando.")
        else:
            print("✅ Os arquivos são diferentes.")

# ---- Executar ----
qrels_path = "quati_1M_qrels.txt"
topics_path = "quati_all_topics.tsv"
solr_url = "http://localhost:8983/solr/quati3/select"

output_com_stop = "resultados_com_stop.csv"
output_sem_stop = "resultados_sem_stop.csv"

relevant_ids = get_relevant_query_ids(qrels_path)
consultas = carregar_consultas_relevantes(topics_path, relevant_ids)

print("🔎 Consultando campo com stopwords...")
consultar_solr(consultas, solr_url, output_com_stop, campo="texto_com_stop")

print("\n🔎 Consultando campo sem stopwords...")
consultar_solr(consultas, solr_url, output_sem_stop, campo="texto_sem_stop")

# Comparar os dois arquivos de saída
print("\n🔍 Comparando resultados...")
comparar_csvs(output_com_stop, output_sem_stop)
