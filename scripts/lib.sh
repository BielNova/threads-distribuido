#!/usr/bin/env bash
# Funções comuns dos scripts de demonstração.
cd "$(dirname "$0")/.." || exit 1
URL=http://localhost:8080

titulo() { printf '\n\033[1;36m==== %s ====\033[0m\n' "$*"; }
passo()  { printf '\033[1;33m>> %s\033[0m\n' "$*"; }

# carga <total> <concorrencia> <canais>  -> roda o gerador de carga dentro da rede do compose
carga() {
  docker compose run --rm -T -e SEM_CSV=1 ferramentas bench-pipeline http://nginx:80 "$1" "$2" "${3:-1}" demo 2>/dev/null | grep -E "RESULTADO|rotulo|^demo"
}

stats() { curl -s "$URL/api/stats${1:+?prefixo=$1}" | python3 -m json.tool; }

psql_q() { docker compose exec -T postgres psql -U threads -d threads -c "$1"; }
