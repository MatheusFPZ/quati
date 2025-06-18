import requests, csv,time

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

def consultar_solr(consultas, solr_url, output_csv):
    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["número_da_consulta","número_do_documento","ordem_no_ranking","score"])

        total_start = time.time()
        for query_id, query_text in consultas:
            start = time.time()  # cronômetro individual
            params = {
                "q": query_text,
                "rows": 100,
                "fl": "passage_id,score",
                "df": "passage"
            }
            r = requests.get(solr_url, params=params).json()
            for rank, doc in enumerate(r["response"]["docs"], start=1):
                writer.writerow([query_id, doc["passage_id"][0] if isinstance(doc["passage_id"], list) else doc["passage_id"], rank, doc["score"]])
            end = time.time()  # cronômetro individual
            print(f"✅ Consulta {query_id} concluída em {end - start:.3f} segundos")
    total_end = time.time()
    print(f"\n⏱️ Tempo total de consulta: {total_end - total_start:.2f} segundos")    
# ---- Executar ----
qrels_path = "quati_1M_qrels.txt"
topics_path = "quati_all_topics.tsv"  # formato: query_id \t query_text
solr_url = "http://localhost:8983/solr/quati_core/select"
output_csv = "resultados.csv"

relevant_ids = get_relevant_query_ids(qrels_path)
consultas = carregar_consultas_relevantes(topics_path, relevant_ids)
consultar_solr(consultas, solr_url, output_csv)

print(f"Consultas finalizadas. Resultados em {output_csv}")
