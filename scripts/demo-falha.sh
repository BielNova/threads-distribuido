#!/usr/bin/env bash
# DEMO 6 — Tolerância a falhas. Uso: scripts/demo-falha.sh [consumidor|gateway|banco|broker|todos]
source "$(dirname "$0")/lib.sh"
mkdir -p resultados/logs
CEN=${1:-todos}

falha_consumidor() {
  titulo "Falha 1: matar (kill -9) uma réplica de PERSISTÊNCIA no meio da carga"
  passo "Deixando o processamento mais lento (10 ms/msg) para a fila acumular..."
  ATRASO_MS=10 docker compose up -d persistencia >/dev/null 2>&1; sleep 6
  passo "Enviando 3000 mensagens em segundo plano..."
  carga 3000 30 1 > /tmp/carga_consumidor.txt &
  sleep 5
  stats carga | grep -E 'filaPendentes|totalMensagens'
  passo "docker kill threads-distribuido-persistencia-1  (SIGKILL: morre sem avisar ninguém)"
  docker kill threads-distribuido-persistencia-1
  sleep 4
  passo "Fila após a queda (a outra réplica continua consumindo; as mensagens sem ack voltaram para a fila):"
  stats carga | grep -E 'filaPendentes|filaConsumidores'
  passo "Subindo a réplica de novo"
  docker start threads-distribuido-persistencia-1 >/dev/null
  wait
  cat /tmp/carga_consumidor.txt
  passo "Logs: reentregas detectadas e ignoradas pela idempotência"
  docker compose logs --since 2m persistencia 2>&1 | grep -E "DUPLICADA|pronto|total gravadas" | tail -8
  docker compose logs --since 2m persistencia > resultados/logs/falha-consumidor.txt 2>&1
  ATRASO_MS=0 docker compose up -d persistencia >/dev/null 2>&1
}

falha_gateway() {
  titulo "Falha 2: derrubar um GATEWAY durante a carga (failover no nginx)"
  carga 3000 30 4 > /tmp/carga_gw.txt &
  sleep 2
  passo "docker kill threads-distribuido-gateway1-1"
  docker kill threads-distribuido-gateway1-1
  wait
  cat /tmp/carga_gw.txt
  docker compose logs --since 2m nginx 2>&1 | grep -iE "upstream|failed|refused" | tail -4
  docker compose logs --since 2m nginx gateway2 > resultados/logs/falha-gateway.txt 2>&1
  passo "Subindo gateway1 de novo"
  docker start threads-distribuido-gateway1-1 >/dev/null
}

falha_banco() {
  titulo "Falha 3: parar o POSTGRES por 15 s durante a carga"
  carga 2000 20 2 > /tmp/carga_db.txt &
  sleep 2
  passo "docker compose stop postgres  — o gateway continua aceitando (202): as mensagens esperam na fila"
  docker compose stop postgres >/dev/null 2>&1
  sleep 6
  curl -s -o /dev/null -w "  POST com banco fora do ar -> HTTP %{http_code}\n" -X POST "$URL/api/canais/geral/mensagens" -H 'Content-Type: application/json' -d '{"usuario":"X","texto":"enviada com o banco fora"}'
  docker compose exec -T rabbitmq rabbitmqctl list_queues name messages messages_unacknowledged 2>/dev/null | grep mensagens
  sleep 9
  passo "docker compose start postgres"
  docker compose start postgres >/dev/null 2>&1
  wait
  cat /tmp/carga_db.txt
  passo "Logs dos workers: retry com backoff enquanto o banco estava fora"
  docker compose logs --since 3m persistencia 2>&1 | grep -E "Falha no banco|devolvendo" | tail -6
  docker compose logs --since 3m persistencia > resultados/logs/falha-banco.txt 2>&1
}

falha_broker() {
  titulo "Falha 4: REINICIAR o RabbitMQ com mensagens na fila (filas duráveis + mensagens persistentes)"
  passo "Parando os consumidores para as mensagens ficarem acumuladas no broker"
  docker compose stop persistencia >/dev/null 2>&1
  PREF=broker-$(date +%s)
  for i in $(seq 1 200); do
    curl -s -o /dev/null -X POST "$URL/api/canais/$PREF/mensagens" -H 'Content-Type: application/json' -d "{\"usuario\":\"X\",\"texto\":\"antes do restart $i\"}"
  done
  docker compose exec -T rabbitmq rabbitmqctl list_queues name messages 2>/dev/null | grep mensagens.novas
  passo "docker compose restart rabbitmq"
  docker compose restart rabbitmq >/dev/null 2>&1
  sleep 8
  passo "Depois do restart as 200 mensagens continuam na fila:"
  docker compose exec -T rabbitmq rabbitmqctl list_queues name messages 2>/dev/null | grep mensagens.novas
  passo "Religando os consumidores"
  docker compose start persistencia >/dev/null 2>&1
  sleep 10
  psql_q "SELECT canal, count(*) FROM mensagens WHERE canal = '$PREF' GROUP BY canal;"
  docker compose logs --since 3m gateway1 gateway2 persistencia > resultados/logs/falha-broker.txt 2>&1
}

case $CEN in
  consumidor) falha_consumidor ;;
  gateway) falha_gateway ;;
  banco) falha_banco ;;
  broker) falha_broker ;;
  todos) falha_consumidor; falha_gateway; falha_banco; falha_broker ;;
esac
titulo "Consistência geral após as falhas"
stats | grep -E '"consistente"|totalMensagens|dlq' | head -3
