import csv
from collections import defaultdict
from scipy.stats import ttest_rel

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
    resultados = defaultdict(list)
    with open(path_csv, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            consulta_id = int(row['número_da_consulta'])
            doc_id_raw = row['número_do_documento']
            doc_id = doc_id_raw.strip().strip("[]").replace("'", "").replace('"', "")
            score = float(row['score'])
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
    if total_relevantes == 0:
        return 0.0
    return soma_precisao / total_relevantes

def calcular_aps(resultados, qrels):
    aps = []
    for consulta_id in resultados:
        relevantes = qrels.get(consulta_id, {})
        if not relevantes:
            continue
        ap = calcular_ap(resultados[consulta_id], relevantes)
        aps.append((consulta_id, ap))
    return aps

# === Arquivos ===
arquivo_com_stem = "resultados_com_stem.csv"
arquivo_sem_stem = "resultados_sem_stem.csv"
arquivo_qrels = "quati_1M_qrels.txt"

# === Execução ===
qrels = carregar_qrels(arquivo_qrels)
res_com = carregar_resultados(arquivo_com_stem)
res_sem = carregar_resultados(arquivo_sem_stem)

aps_com = calcular_aps(res_com, qrels)
aps_sem = calcular_aps(res_sem, qrels)

# === Alinhar por consultas em comum ===
consultas_comuns = set(consulta for consulta, _ in aps_com) & set(consulta for consulta, _ in aps_sem)
aps_com_dict = dict(aps_com)
aps_sem_dict = dict(aps_sem)

aps_com_vals = [aps_com_dict[c] for c in consultas_comuns]
aps_sem_vals = [aps_sem_dict[c] for c in consultas_comuns]

# === Calcular MAP ===
map_com = sum(aps_com_vals) / len(aps_com_vals)
map_sem = sum(aps_sem_vals) / len(aps_sem_vals)

# === Teste T pareado ===
t_stat, p_value = ttest_rel(aps_com_vals, aps_sem_vals)

# === Resultado final ===
print("\n📊 COMPARAÇÃO COM STEMMING")
print("-" * 40)
print(f"🔎 MAP com stemming     : {map_com:.4f}")
print(f"🔎 MAP sem stemming     : {map_sem:.4f}")
print(f"📈 Diferença de MAP     : {map_sem - map_com:.4f}")
print()
print(f"🧪 Teste T pareado (AP por consulta)")
print(f"   t = {t_stat:.4f}")
print(f"   p = {p_value:.4f}")
if p_value < 0.05:
    print("✅ Diferença estatisticamente significativa (p < 0.05)")
else:
    print("❌ Diferença não significativa (p ≥ 0.05)")
