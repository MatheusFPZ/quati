import csv
from collections import defaultdict

def carregar_qrels(arquivo_qrels):
    qrels = defaultdict(dict)
    with open(arquivo_qrels, 'r', encoding='utf-8') as f:
        for linha in f:
            parts = linha.strip().split()
            if len(parts) == 4:
                consulta_id = int(parts[0])
                doc_id = parts[2]
                relevancia = int(parts[3])
                qrels[consulta_id][doc_id] = relevancia
    return qrels

def carregar_resultados(path_csv):
    resultados = {}
    with open(path_csv, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            consulta_id = int(row['número_da_consulta'])
            # Corrigir o número_do_documento removendo colchetes e aspas
            doc_id_raw = row['número_do_documento']
            doc_id = doc_id_raw.strip().strip("[]").replace("'", "").replace('"', "")
            score = float(row['score'])

            if consulta_id not in resultados:
                resultados[consulta_id] = []
            resultados[consulta_id].append((doc_id, score))
    return resultados


def calcular_ap(documentos_ranqueados, relevantes):
    acertos = 0
    soma_precisao = 0.0
    total_relevantes = sum(1 for r in relevantes.values() if r > 0)

    for i, (doc_id, _) in enumerate(documentos_ranqueados, start=1):
        relevancia = relevantes.get(doc_id, 0)
        if relevancia > 0:
            acertos += 1
            precisao = acertos / i
            soma_precisao += precisao
            print(f"Doc {doc_id} relevante! Posição {i}, precisão: {precisao:.4f}")

    if total_relevantes == 0:
        return 0.0
    return soma_precisao / total_relevantes


def calcular_map(resultados, qrels):
    aps = []
    for consulta_id in resultados:
        relevantes = qrels.get(consulta_id, {})
        if not relevantes:
            print(f"Consulta {consulta_id} ignorada (sem documentos relevantes no qrels).")
            continue

        ap = calcular_ap(resultados[consulta_id], relevantes)
        aps.append(ap)
        print(f"Consulta {consulta_id}: AP = {ap:.4f}")
    return sum(aps) / len(aps) if aps else 0.0


# Caminhos para os arquivos
arquivo_resultados = "avaliacao2.csv"
arquivo_qrels = "quati_1M_qrels.txt"

# Execução
qrels = carregar_qrels(arquivo_qrels)
resultados = carregar_resultados(arquivo_resultados)
map_score = calcular_map(resultados, qrels)

print(f"MAP: {map_score:.4f}")
