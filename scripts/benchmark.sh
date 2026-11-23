#!/usr/bin/env bash
# Roda os dois benchmarks e grava tabelas em resultados/.
#   Benchmark 1: análise sequencial x paralela (CPU)            -> resultados/bench-analise.md
#   Benchmark 2: pipeline ponta a ponta com 1, 2 e 4 réplicas   -> resultados/bench-pipeline.md
#   Benchmark 3: relatório real via fila (banco + CPU)          -> resultados/bench-relatorio.md
source "$(dirname "$0")/lib.sh"
N_ANALISE=${N_ANALISE:-1000000}
REPS=${REPS:-3}
TOTAL=${TOTAL:-5000}
CONC=${CONC:-64}

titulo "Benchmark 1: análise sequencial x paralela ($N_ANALISE mensagens)"
docker compose run --rm -T -e JAVA_TOOL_OPTIONS=-Xmx3g ferramentas bench-analise "$N_ANALISE" 5 2 1,2,4,6,8,10 2>&1 | grep -v Picked

titulo "Benchmark 2: pipeline ponta a ponta"
rm -f resultados/bench-pipeline.csv
for R in 1 2 4; do
  passo "persistencia com $R réplica(s)"
  LOG_MSGS=false docker compose up -d --scale persistencia=$R persistencia >/dev/null 2>&1
  sleep 8
  carga_warm=$(docker compose run --rm -T -e SEM_CSV=1 ferramentas bench-pipeline http://nginx:80 1000 $CONC 8 aquecimento 2>/dev/null | tail -1)
  for CAN in 1 8; do
    for i in $(seq 1 "$REPS"); do
      docker compose run --rm -T ferramentas bench-pipeline http://nginx:80 "$TOTAL" "$CONC" "$CAN" "${R}-replica(s)" 2>/dev/null | tail -1
    done
  done
done
docker compose run --rm -T ferramentas bench-pipeline resumo 2>/dev/null
docker compose up -d --scale persistencia=2 persistencia >/dev/null 2>&1

titulo "Benchmark 3: relatório real (banco + CPU) pelo serviço de análise"
docker compose run --rm -T ferramentas seed historico 500000 2>&1 | tail -1
{
  echo "| Threads | Carregar do banco (ms) | Análise sequencial (ms) | Análise paralela (ms) | Speedup da análise | Total paralelo (banco+CPU) | Total sequencial (banco+CPU) |"
  echo "|---:|---:|---:|---:|---:|---:|---:|"
} > resultados/bench-relatorio.md
for T in 1 2 4 8 10; do
  for i in $(seq 1 "$REPS"); do
    ID=$(curl -s -X POST "$URL/api/canais/historico/relatorios" -H 'Content-Type: application/json' -d "{\"threads\":$T,\"comparar\":true}" | python3 -c 'import sys,json;print(json.load(sys.stdin)["id"])')
    until curl -s "$URL/api/relatorios/$ID" | grep -q CONCLUIDO; do sleep 0.5; done
    curl -s "$URL/api/relatorios/$ID" | python3 -c '
import sys,json; t=json.load(sys.stdin)["resultado"]["tempos"]; print(t["threads"],t["carregamentoBancoMs"],t["sequencialMs"],t["paraleloMs"])' >> /tmp/rel_bench.txt
  done
done
python3 - <<'PY' >> resultados/bench-relatorio.md
from collections import defaultdict
g=defaultdict(list)
for l in open('/tmp/rel_bench.txt'):
    t,b,s,p=map(int,l.split()); g[t].append((b,s,p))
for t in sorted(g):
    v=g[t]; n=len(v); b=sum(x[0] for x in v)/n; s=sum(x[1] for x in v)/n; p=sum(x[2] for x in v)/n
    print(f"| {t} | {b:.0f} | {s:.0f} | {p:.0f} | {s/p:.2f}x | {b+p:.0f} | {b+s:.0f} |")
PY
rm -f /tmp/rel_bench.txt
cat resultados/bench-relatorio.md
