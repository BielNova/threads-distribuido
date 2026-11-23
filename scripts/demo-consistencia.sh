#!/usr/bin/env bash
# DEMO 5 — Consistência: atualizações simultâneas tratadas corretamente.
source "$(dirname "$0")/lib.sh"

titulo "Cenário A: 2000 mensagens disputando o MESMO canal ao mesmo tempo"
passo "Réplicas de persistência ativas (cada uma com 2 consumidores concorrentes):"
docker compose ps persistencia --format '  {{.Name}}  {{.Status}}'
passo "Enviando 2000 mensagens com 50 clientes simultâneos para 1 único canal..."
carga 2000 50 1
passo "Verificação no banco: seq precisa ser exatamente 1..N, sem buraco e sem repetição"
psql_q "SELECT canal, count(*) AS mensagens, count(DISTINCT seq) AS seqs_distintas, min(seq), max(seq),
               count(DISTINCT processada_por) AS replicas_que_gravaram
        FROM mensagens WHERE canal LIKE 'carga-%' GROUP BY canal ORDER BY max(persistida_em) DESC LIMIT 1;"
passo "Distribuição do trabalho entre réplicas (mesmo canal, gravado por processos diferentes):"
psql_q "SELECT processada_por, count(*) FROM mensagens
        WHERE canal = (SELECT canal FROM mensagens WHERE canal LIKE 'carga-%' ORDER BY persistida_em DESC LIMIT 1)
        GROUP BY 1;"

titulo "Cenário B: duas pessoas editando a MESMA mensagem ao mesmo tempo (conflito)"
RESP=$(curl -s -X POST "$URL/api/canais/conflito/mensagens" -H 'Content-Type: application/json' -d '{"usuario":"X","texto":"texto original"}')
ID=$(echo "$RESP" | python3 -c 'import sys,json;print(json.load(sys.stdin)["id"])')
sleep 1
passo "Mensagem $ID criada (versao 1). Ana e Bruno leram a versao 1 e salvam ao mesmo tempo:"
curl -s -o /tmp/ed_ana.json   -w "  Ana   -> HTTP %{http_code}\n" -X PUT "$URL/api/mensagens/$ID" -H 'Content-Type: application/json' -d '{"texto":"versão da Ana","versao":1}' &
curl -s -o /tmp/ed_bruno.json -w "  Bruno -> HTTP %{http_code}\n" -X PUT "$URL/api/mensagens/$ID" -H 'Content-Type: application/json' -d '{"texto":"versão do Bruno","versao":1}' &
wait
echo "  resposta Ana:   $(cat /tmp/ed_ana.json)"
echo "  resposta Bruno: $(cat /tmp/ed_bruno.json)"
passo "Estado final no banco (só UMA edição venceu; a outra recebeu 409 e não sobrescreveu):"
psql_q "SELECT id, texto, versao FROM mensagens WHERE id = '$ID';"
