import csv
from collections import defaultdict
from sklearn.metrics import average_precision_score
from scipy.stats import ttest_rel

# === ARQUIVOS ===
arquivo_com = "resultados_com_stop.csv"
arquivo_sem = "resultados_sem_lema.csv"
arquivo_qrels = "quati_1M_qrels.txt"

# === INSERIR TEMPOS (em segundos) ===
tempo_index_com = 460.47
tempo_index_sem = 358.24
tempo_consulta_com = 100.51
tempo_consulta_sem = 100.72

# === Carregar qrels ===
def carregar_qrels(qrels_path):
    qrels = defaultdict(set)
    with open(qrels_path, "r") as f:
        for line in f:
            query_id, _, doc_id, score = line.strip().split()
            if int(score) > 0:
                qrels[query_id].add(doc_id)
    return qrels

# === Carregar resultados de um arquivo CSV ===
def carregar_resultados(arquivo):
    resultados = defaultdict(list)
    with open(arquivo, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            consulta = row["número_da_consulta"]
            doc_id = str(row["número_do_documento"])
            score = float(row["score"])
            resultados[consulta].append((doc_id, score))
    return resultados

# === Calcular AP por consulta ===
def calcular_ap_por_consulta(resultados, qrels):
    aps = {}
    for consulta, docs in resultados.items():
        y_true = []
        y_score = []
        if consulta not in qrels:
            continue
        for doc_id, score in docs:
            y_true.append(1 if doc_id in qrels[consulta] else 0)
            y_score.append(score)
        if any(y_true):
            aps[consulta] = average_precision_score(y_true, y_score)
    return aps

# === Processamento ===
qrels = carregar_qrels(arquivo_qrels)
res_com = carregar_resultados(arquivo_com)
res_sem = carregar_resultados(arquivo_sem)

ap_com = calcular_ap_por_consulta(res_com, qrels)
ap_sem = calcular_ap_por_consulta(res_sem, qrels)

# Alinhar consultas que estão em ambos
consultas_comuns = set(ap_com.keys()) & set(ap_sem.keys())
lista_ap_com = [ap_com[c] for c in consultas_comuns]
lista_ap_sem = [ap_sem[c] for c in consultas_comuns]

# === Calcular MAP ===
map_com = sum(lista_ap_com) / len(lista_ap_com)
map_sem = sum(lista_ap_sem) / len(lista_ap_sem)

# === Teste T pareado com AP ===
t_stat, p_value = ttest_rel(lista_ap_com, lista_ap_sem)

# === Relatório ===
print("\n📊 COMPARAÇÃO COM BASE EM AVERAGE PRECISION (AP)")
print("-" * 45)
print(f"🔎 MAP com stopwords     : {map_com:.6f}")
print(f"🔎 MAP sem stopwords     : {map_sem:.6f}")
print(f"📈 Diferença de MAP      : {map_sem - map_com:.6f}")
print()
print(f"⏱️ Tempo de indexação com stopwords : {tempo_index_com:.2f}s")
print(f"⏱️ Tempo de indexação sem stopwords : {tempo_index_sem:.2f}s")
print(f"📉 Diferença (indexação)           : {tempo_index_sem - tempo_index_com:.2f}s")
print()
print(f"⏱️ Tempo de consulta com stopwords : {tempo_consulta_com:.2f}s")
print(f"⏱️ Tempo de consulta sem stopwords : {tempo_consulta_sem:.2f}s")
print(f"📉 Diferença (consulta)            : {tempo_consulta_sem - tempo_consulta_com:.2f}s")
print()
print(f"🧪 Teste T pareado (AP por consulta)")
print(f"   t = {t_stat:.6f}")
print(f"   p = {p_value:.6f}")
if p_value < 0.05:
    print("✅ Diferença estatisticamente significativa (p < 0.05)")
else:
    print("❌ Diferença não significativa (p ≥ 0.05)")
