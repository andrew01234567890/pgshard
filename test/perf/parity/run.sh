#!/usr/bin/env bash
# Pooling-parity harness: pgbench against one PostgreSQL 18 backend through
# four front-ends on one docker network:
#   direct            pgbench -> postgres
#   pgbouncer-session pgbench -> PgBouncer (pool_mode=session) -> postgres
#   pgbouncer-txn     pgbench -> PgBouncer (pool_mode=transaction) -> postgres
#   pgshard           pgbench -> pgshard-router -> pgshard-pooler -> postgres
#
# Usage: run.sh [smoke|matrix|profile|up|down]
#   smoke   - stand up, SELECT 1 through every front-end, tear down
#   matrix  - full workload matrix, CSV under $PARITY_RESULTS
#   profile - pgbench select/prepared against pgshard while capturing CPU,
#             allocs and mutex pprof profiles from router+pooler
#             (PARITY_PROFILE_CLIENTS=10, PARITY_PROFILE_SECONDS=30)
# Env: PARITY_DURATION (s/run, default 3), PARITY_SCALE (pgbench scale, 10),
#      PARITY_CLIENTS ("1 10 100 1000"), PARITY_RESULTS (dir),
#      PARITY_KEEP=1 keeps the stack up after the run,
#      PARITY_PPROF=1 serves /debug/pprof from router (127.0.0.1:16060) and
#      pooler (127.0.0.1:16061); profile implies it.
set -euo pipefail

cmd=${1:-matrix}
duration=${PARITY_DURATION:-3}
scale=${PARITY_SCALE:-10}
clients_list=${PARITY_CLIENTS:-"1 10 100 1000"}
here=$(cd "$(dirname "$0")" && pwd)
results=${PARITY_RESULTS:-"$here/results/$(date -u +%Y%m%dT%H%M%SZ)"}

net=pgparity
pg_image=postgres:18
bouncer_image=edoburu/pgbouncer:v1.24.1-p1@sha256:3db3d7223e93af52b4116f642951a1a5fa44702a88c2a59cf7562cac19320c9e
router_image=${PARITY_ROUTER_IMAGE:-pgshard-router:dev}
pw=parity-secret

log() { echo "[parity] $*" >&2; }

teardown() {
  docker rm -f pgparity-backend pgparity-catalog pgparity-pooler pgparity-router \
    pgparity-bouncer-session pgparity-bouncer-txn pgparity-client >/dev/null 2>&1 || true
  docker network rm "$net" >/dev/null 2>&1 || true
}

up() {
  teardown
  docker network create "$net" >/dev/null
  docker run -d --name pgparity-backend --network "$net" \
    -e POSTGRES_PASSWORD="$pw" -e POSTGRES_DB=app \
    -e POSTGRES_INITDB_ARGS="--auth-host=scram-sha-256" \
    "$pg_image" postgres -c max_connections=1100 -c shared_buffers=256MB \
    -c wal_level=logical -c max_prepared_transactions=64 >/dev/null
  docker run -d --name pgparity-catalog --network "$net" \
    -e POSTGRES_PASSWORD="$pw" -e POSTGRES_DB=catalog \
    "$pg_image" postgres -c wal_level=logical -c max_prepared_transactions=64 >/dev/null
  for c in pgparity-backend pgparity-catalog; do
    for _ in $(seq 1 60); do
      docker exec "$c" pg_isready -U postgres -q 2>/dev/null && break
      sleep 1
    done
    docker exec "$c" pg_isready -U postgres -q || { log "$c not ready"; docker logs "$c" >&2; exit 1; }
  done

  docker run --rm --network "$net" -e PGSHARD_DEV_PASSWORD=app-secret "$router_image" dev-bootstrap \
    --catalog-dsn="postgres://postgres:$pw@pgparity-catalog:5432/catalog?sslmode=disable" \
    --shard-dsn="postgres://postgres:$pw@pgparity-backend:5432/postgres?sslmode=disable" \
    --pooler-endpoint=pgparity-pooler:15432 >/dev/null
  local pooler_pprof=() router_pprof=()
  if [ "${PARITY_PPROF:-0}" = 1 ]; then
    pooler_pprof=(-p 127.0.0.1:16061:6060)
    router_pprof=(-p 127.0.0.1:16060:6060)
  fi
  docker run -d --name pgparity-pooler --network "$net" --entrypoint pgshard-pooler "${pooler_pprof[@]}" "$router_image" run \
    --insecure-dev --listen=0.0.0.0:15432 ${PARITY_PPROF:+--pprof-listen=0.0.0.0:6060} --pg-host=pgparity-backend --pg-port=5432 --pg-database=app \
    --max-backends=200 --max-per-role=200 \
    --catalog-dsn="postgres://postgres:$pw@pgparity-catalog:5432/catalog?sslmode=disable" \
    --shard-set=default --shard-id=0 >/dev/null
  docker run -d --name pgparity-router --network "$net" "${router_pprof[@]}" "$router_image" serve \
    --insecure-dev --listen=0.0.0.0:6432 ${PARITY_PPROF:+--pprof-listen=0.0.0.0:6060} \
    --catalog-dsn="postgres://postgres:$pw@pgparity-catalog:5432/catalog?sslmode=disable" >/dev/null

  for mode in session transaction; do
    name=pgparity-bouncer-session; [ "$mode" = transaction ] && name=pgparity-bouncer-txn
    docker run -d --name "$name" --network "$net" \
      -e DB_HOST=pgparity-backend -e DB_NAME=app -e DB_USER=app -e DB_PASSWORD=app-secret \
      -e AUTH_TYPE=scram-sha-256 -e POOL_MODE="$mode" \
      -e MAX_CLIENT_CONN=1200 -e DEFAULT_POOL_SIZE=200 -e MAX_PREPARED_STATEMENTS=256 \
      -e LISTEN_PORT=6432 "$bouncer_image" >/dev/null
  done

  docker run -d --name pgparity-client --network "$net" --entrypoint sh \
    -e PGPASSWORD=app-secret "$pg_image" -c 'sleep infinity' >/dev/null

  # app role on the backend for the direct and pgbouncer arms (dev-bootstrap
  # created it on the shard; ensure db grants) and pgbench schema, loaded once
  # directly so every arm benches the same tables.
  docker exec pgparity-backend psql -U postgres -d app -q -c \
    "grant all on schema public to app; alter database app owner to app;" >/dev/null
  wait_frontend pgparity-backend 5432 direct
  docker exec -e PGPASSWORD=app-secret pgparity-client \
    pgbench -h pgparity-backend -p 5432 -U app -d app -i -s "$scale" -q >/dev/null 2>&1
  wait_frontend pgparity-bouncer-session 6432 pgbouncer-session
  wait_frontend pgparity-bouncer-txn 6432 pgbouncer-txn
  wait_frontend pgparity-router 6432 pgshard
}

wait_frontend() { # host port label
  for _ in $(seq 1 60); do
    if docker exec -e PGPASSWORD=app-secret pgparity-client \
      psql -h "$1" -p "$2" -U app -d app -tAc 'select 1' 2>/dev/null | grep -qx 1; then
      log "$3 ready ($1:$2)"; return 0
    fi
    sleep 1
  done
  log "$3 never served SELECT 1"; docker logs "$1" >&2 || true; return 1
}

frontend_host() { # arm -> host:port
  case "$1" in
    direct) echo pgparity-backend 5432 ;;
    pgbouncer-session) echo pgparity-bouncer-session 6432 ;;
    pgbouncer-txn) echo pgparity-bouncer-txn 6432 ;;
    pgshard) echo pgparity-router 6432 ;;
  esac
}

frontend_containers() { # arm -> containers whose CPU/RSS is the pooling overhead
  case "$1" in
    direct) echo "" ;;
    pgbouncer-session) echo pgparity-bouncer-session ;;
    pgbouncer-txn) echo pgparity-bouncer-txn ;;
    pgshard) echo "pgparity-router pgparity-pooler" ;;
  esac
}

sample_stats() { # containers... ; writes "cpu_pct rss_mib" averages to stdout on SIGTERM
  local out=$1; shift
  local n=0 cpu=0 rss=0
  trap 'awk -v c="$cpu" -v r="$rss" -v n="$n" "BEGIN{if(n==0)n=1; printf \"%.1f %.1f\", c/n, r/n}" > "$out"; exit 0' TERM
  while :; do
    if [ $# -gt 0 ]; then
      local line
      line=$(docker stats --no-stream --format '{{.CPUPerc}} {{.MemUsage}}' "$@" 2>/dev/null |
        awk '{gsub(/%/,"",$1); c+=$1; m=$2; if (m ~ /GiB/) {gsub(/GiB/,"",m); m*=1024} else {gsub(/MiB/,"",m)} r+=m} END{printf "%s %s", c+0, r+0}')
      cpu=$(awk -v a="$cpu" -v b="${line%% *}" 'BEGIN{print a+b}')
      rss=$(awk -v a="$rss" -v b="${line##* }" 'BEGIN{print a+b}')
      n=$((n+1))
    else
      sleep 0.4
    fi
    sleep 0.1
  done
}

percentiles() { # dir with pgbench per-txn logs -> "p50 p99 p999" in ms
  docker exec pgparity-client sh -c "cat $1/pgbench_log.* 2>/dev/null | awk '{print \$3}' | sort -n" |
    awk '{a[NR]=$1} END{if(NR==0){print "NA NA NA"; exit}
      printf "%.3f %.3f %.3f\n", a[int(NR*0.50)+ (NR*0.5==int(NR*0.5)?0:1)]/1000, a[int(NR*0.99)==0?1:int(NR*0.99)]/1000, a[int(NR*0.999)==0?1:int(NR*0.999)]/1000}'
}

run_pgbench() { # arm workload clients mode extra_args... -> CSV row appended
  local arm=$1 workload=$2 clients=$3 mode=$4; shift 4
  read -r host port <<<"$(frontend_host "$arm")"
  local jobs=$(( clients < 16 ? clients : 16 ))
  # High client counts need runway for the connection ramp through a pooler.
  local dur=$duration
  if [ "$clients" -ge 500 ]; then
    dur=$(( duration * 3 )); [ "$dur" -lt 10 ] && dur=10
  fi
  local logdir="/tmp/pb-$arm-$workload-$clients-$mode"
  docker exec pgparity-client sh -c "rm -rf $logdir && mkdir -p $logdir"
  local statfile="$results/.stat.$$"
  local fecontainers; fecontainers=$(frontend_containers "$arm")
  local sampler=""
  if [ -n "$fecontainers" ]; then
    # shellcheck disable=SC2086
    sample_stats "$statfile" $fecontainers & sampler=$!
  fi
  local out rc=0
  out=$(docker exec -e PGPASSWORD=app-secret -w "$logdir" pgparity-client \
    pgbench -h "$host" -p "$port" -U app -d app -n -T "$dur" \
    -c "$clients" -j "$jobs" -M "$mode" -l -r "$@" 2>&1) || rc=$?
  if [ -n "$sampler" ]; then kill -TERM "$sampler" 2>/dev/null; wait "$sampler" 2>/dev/null || true; fi
  local tps lat cpu rss p50 p99 p999 txns
  if [ $rc -ne 0 ]; then
    log "FAIL $arm/$workload c=$clients m=$mode: $(echo "$out" | tail -1)"
    echo "$arm,$workload,$clients,$mode,$dur,ERROR,NA,NA,NA,NA,NA,NA,NA,NA" >> "$results/results.csv"
    rm -f "$statfile"; return 0
  fi
  tps=$(echo "$out" | awk '/^tps =/{print $3; exit}')
  if [ -z "$tps" ]; then
    log "FAIL $arm/$workload c=$clients m=$mode: no tps in output"
    echo "$arm,$workload,$clients,$mode,$dur,ERROR,NA,NA,NA,NA,NA,NA,NA,NA" >> "$results/results.csv"
    rm -f "$statfile"; return 0
  fi
  txns=$(echo "$out" | awk '/number of transactions actually processed/{print $6; exit}' | cut -d/ -f1)
  lat=$(echo "$out" | awk '/^latency average/{print $4; exit}')
  read -r p50 p99 p999 <<<"$(percentiles "$logdir")"
  read -r cpu rss <<<"$(cat "$statfile" 2>/dev/null || echo "NA NA")"
  rm -f "$statfile"
  local cpupertxn=NA
  if [ "${cpu:-NA}" != NA ] && [ -n "$tps" ]; then
    cpupertxn=$(awk -v c="$cpu" -v t="$tps" 'BEGIN{if(t>0)printf "%.4f", (c/100)*1000/t; else print "NA"}')
  fi
  echo "$arm,$workload,$clients,$mode,$dur,$tps,$lat,$p50,$p99,$p999,${cpu:-NA},$cpupertxn,${rss:-NA},$txns" >> "$results/results.csv"
  log "$arm $workload c=$clients m=$mode tps=$tps lat=${lat}ms p99=${p99}ms cpu=${cpu:-NA}% rss=${rss:-NA}MiB"
}

run_storm() { # connection churn: new connection per transaction
  local arm=$1
  read -r host port <<<"$(frontend_host "$arm")"
  local out rc=0
  out=$(docker exec -e PGPASSWORD=app-secret pgparity-client \
    pgbench -h "$host" -p "$port" -U app -d app -n -S -C -T "$duration" -c 10 -j 10 2>&1) || rc=$?
  if [ $rc -ne 0 ]; then
    echo "$arm,storm,10,simple,$duration,ERROR,NA,NA,NA,NA,NA,NA,NA,NA" >> "$results/results.csv"
    log "FAIL $arm/storm: $(echo "$out" | tail -1)"; return 0
  fi
  local tps lat
  tps=$(echo "$out" | awk '/^tps =/{print $3; exit}')
  if [ -z "$tps" ]; then
    log "FAIL $arm/storm: no tps in output"
    echo "$arm,storm,10,simple,$duration,ERROR,NA,NA,NA,NA,NA,NA,NA,NA" >> "$results/results.csv"
    return 0
  fi
  lat=$(echo "$out" | awk '/^latency average/{print $4; exit}')
  echo "$arm,storm,10,simple,$duration,$tps,$lat,NA,NA,NA,NA,NA,NA,NA" >> "$results/results.csv"
  log "$arm storm tps=$tps lat=${lat}ms"
}

run_copy() { # large result set + COPY OUT throughput
  local arm=$1
  read -r host port <<<"$(frontend_host "$arm")"
  local t0 t1 rows rc=0
  t0=$(date +%s.%N)
  rows=$(docker exec -e PGPASSWORD=app-secret pgparity-client \
    psql -h "$host" -p "$port" -U app -d app -qAt \
    -c "copy pgbench_accounts to stdout" 2>/dev/null | wc -l) || rc=$?
  t1=$(date +%s.%N)
  if [ $rc -ne 0 ] || [ "$rows" -eq 0 ]; then
    echo "$arm,copy,1,simple,NA,ERROR,NA,NA,NA,NA,NA,NA,NA,NA" >> "$results/results.csv"
    log "FAIL $arm/copy"; return 0
  fi
  local secs rps
  secs=$(awk -v a="$t0" -v b="$t1" 'BEGIN{printf "%.2f", b-a}')
  rps=$(awk -v r="$rows" -v s="$secs" 'BEGIN{printf "%.0f", r/s}')
  echo "$arm,copy,1,simple,$secs,$rps,NA,NA,NA,NA,NA,NA,NA,$rows" >> "$results/results.csv"
  log "$arm copy $rows rows in ${secs}s (${rps} rows/s)"
}

matrix() {
  mkdir -p "$results"
  echo "frontend,workload,clients,mode,duration_s,tps,lat_avg_ms,p50_ms,p99_ms,p999_ms,fe_cpu_pct,fe_cpu_ms_per_txn,fe_rss_mib,txns" > "$results/results.csv"
  for arm in direct pgbouncer-session pgbouncer-txn pgshard; do
    for mode in simple prepared; do
      for c in $clients_list; do
        run_pgbench "$arm" select "$c" "$mode" -S
        run_pgbench "$arm" tpcb "$c" "$mode" -N
      done
    done
    run_storm "$arm"
    run_copy "$arm"
  done
  log "results: $results/results.csv"
}


profile() { # capture CPU/allocs/mutex profiles from router+pooler under load
  local secs=${PARITY_PROFILE_SECONDS:-30} clients=${PARITY_PROFILE_CLIENTS:-10}
  local dir="$results/profiles"
  mkdir -p "$dir"
  log "profile: pgbench select prepared c=$clients for ${secs}s"
  docker exec -e PGPASSWORD=app-secret pgparity-client \
    pgbench -h pgparity-router -p 6432 -U app -d app -n -S -M prepared \
    -c "$clients" -j "$clients" -T "$secs" >"$dir/pgbench.txt" 2>&1 &
  local bench=$!
  sleep 2
  local cpusecs=$(( secs - 10 )); [ "$cpusecs" -lt 5 ] && cpusecs=5
  local side port
  local curls=()
  for side in router:16060 pooler:16061; do
    port=${side##*:}; side=${side%%:*}
    curl -fsS "http://127.0.0.1:$port/debug/pprof/profile?seconds=$cpusecs" -o "$dir/$side.cpu.pb.gz" &
    curls+=($!)
  done
  wait "${curls[@]}"
  for side in router:16060 pooler:16061; do
    port=${side##*:}; side=${side%%:*}
    curl -fsS "http://127.0.0.1:$port/debug/pprof/allocs?seconds=5" -o "$dir/$side.allocs.pb.gz"
    curl -fsS "http://127.0.0.1:$port/debug/pprof/mutex" -o "$dir/$side.mutex.pb.gz"
  done
  wait "$bench" || { log "profile: pgbench failed"; cat "$dir/pgbench.txt" >&2; return 1; }
  if command -v go >/dev/null; then
    local prof
    for prof in "$dir"/*.pb.gz; do
      go tool pprof -top -nodecount=30 "$prof" > "${prof%.pb.gz}.top.txt" 2>/dev/null || true
    done
  fi
  grep -E "tps|latency average" "$dir/pgbench.txt" >&2 || true
  log "profiles: $dir"
}

smoke() {
  for arm in direct pgbouncer-session pgbouncer-txn pgshard; do
    read -r host port <<<"$(frontend_host "$arm")"
    v=$(docker exec -e PGPASSWORD=app-secret pgparity-client \
      psql -h "$host" -p "$port" -U app -d app -tAc 'select 1')
    [ "$v" = 1 ] || { log "smoke: $arm returned '$v'"; exit 1; }
    echo "SMOKE OK frontend=$arm"
  done
}

case "$cmd" in
  up) up ;;
  down) teardown ;;
  smoke)
    trap '[ "${PARITY_KEEP:-0}" = 1 ] || teardown' EXIT
    up; smoke ;;
  matrix)
    trap '[ "${PARITY_KEEP:-0}" = 1 ] || teardown' EXIT
    up; smoke; matrix ;;
  profile)
    export PARITY_PPROF=1
    trap '[ "${PARITY_KEEP:-0}" = 1 ] || teardown' EXIT
    up; profile ;;
  *) echo "usage: $0 [smoke|matrix|profile|up|down]" >&2; exit 2 ;;
esac
