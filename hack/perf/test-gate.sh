#!/usr/bin/env bash
# Self-test for hack/perf/gate.sh using fabricated benchstat CSV output.
set -euo pipefail
gate="$(dirname "$0")/gate.sh"
tmp="$(mktemp)"
trap 'rm -f "$tmp"' EXIT

csv() {
  cat > "$tmp" <<CSV
,base.txt,,head.txt,,,
,sec/op,CI,sec/op,CI,vs base,P
$1
geomean,1,,1,,+1%,

,base.txt,,head.txt,,,
,B/op,CI,B/op,CI,vs base,P
$2
geomean,,,,,+0.00%,
CSV
}

expect() { # expect <exit-code> <description>
  local want="$1" desc="$2" got=0
  "$gate" "$tmp" >/dev/null || got=$?
  if [ "$got" != "$want" ]; then echo "FAIL: $desc (exit $got, want $want)"; exit 1; fi
  echo "ok: $desc"
}

csv "GateNoop-32,4e-10,1%,7e-10,1%,+64.00%,p=0.002 n=10" ""
expect 0 "sub-threshold no-op benchmark is ignored even when significant"

csv "Noop-32,4e-10,1%,7e-10,1%,+64.00%,p=0.002 n=10" ""
expect 1 "a run where no gated benchmark exists fails loudly"

csv "GateNoop-32,5e-08,1%,2e-07,1%,+300.00%,p=0.000 n=10" ""
expect 0 "gated benchmark below the minimum base ns/op is ignored"

csv "GateParse-32,1e-06,1%,1.5e-06,1%,+50.00%,p=0.000 n=10" ""
expect 1 "gated significant large regression fails"

csv "GateParse-32,1e-06,1%,1.5e-06,1%,~,p=0.300 n=10" ""
expect 0 "gated but not significant passes"

csv "Parse-32,1e-06,1%,1.5e-06,1%,+50.00%,p=0.000 n=10
GateParse-32,1e-06,1%,1.0e-06,1%,~,p=0.900 n=10" ""
expect 0 "untagged benchmark is not gated"

csv "GateParse-32,1e-06,1%,1.1e-06,1%,+10.00%,p=0.000 n=10" ""
expect 0 "gated regression under the percent threshold passes"

csv "GateTiny-32,1.2e-07,1%,1.6e-07,1%,+33.00%,p=0.000 n=10" ""
expect 0 "gated regression under the absolute ns threshold passes"

csv "GateParse-32,1e-06,1%,1e-06,1%,~,p=1.000 n=10" "GateParse-32,100,1%,900,1%,+800.00%,p=0.000 n=10"
expect 0 "B/op section is not gated"

csv "GateParse-32,1e-06,1%,1.5e-06,1%,+50.00%,p=0.000 n=10" ""
PERF_GATE_PATTERN='^Nothing' expect 0 "PERF_GATE_PATTERN narrows the gate"
echo "gate self-test: OK"
