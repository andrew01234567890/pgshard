#!/usr/bin/env bash
# Collect enough state from a KIND job to tell a product defect apart from the
# node losing its own networking.
#
# The KIND control plane runs every PostgreSQL member, every orchestrator, the
# pooler and the manager on one node. When kube-proxy, CoreDNS, conntrack or the
# CNI on that node fails, every workload reports its own symptom and none of
# them names the cause. Workload logs alone cannot separate the two, and the
# cluster is deleted as soon as the job ends, so anything not captured here is
# gone before anyone reads the run.
#
# Every command is best effort on purpose: a source that does not exist must
# still leave the rest collected. The caller decides the job result.

set -uo pipefail

cluster="${1:?KIND cluster name is required}"
output="${2:?diagnostics output directory is required}"
node="${cluster}-control-plane"

mkdir -p "$output/logs"

# Bounds every log so a single verbose component cannot make the artifact
# unusable. Raised only for the manager, which is the primary subject.
readonly pod_log_lines=400
readonly manager_log_lines=2000
readonly journal_lines=3000

capture() {
  local file="$1"
  shift
  {
    printf '$ %s\n' "$*"
    "$@" 2>&1
    printf '\n'
  } >>"$output/$file"
}

node_capture() {
  local file="$1"
  shift
  capture "$file" docker exec "$node" "$@"
}

capture cluster.txt kubectl get nodes,namespaces --output=wide --show-labels
capture cluster.txt kubectl get \
  pods,deployments,statefulsets,services,endpointslices,leases \
  --all-namespaces --output=wide --show-labels
capture cluster.txt kubectl get --raw '/readyz?verbose'
capture cluster.txt kubectl describe nodes
capture events.txt kubectl get events --all-namespaces --sort-by=.lastTimestamp
capture describe-pods.txt kubectl describe pods --all-namespaces

capture logs/manager.log kubectl logs \
  --namespace pgshard-system deployment/pgshard-controller-manager \
  --all-containers --timestamps "--tail=$manager_log_lines"

# Every Pod on the node, kube-system included. CoreDNS, kube-proxy and kindnet
# are the components that report the failure this exists to diagnose, and none
# of them was collected before.
while read -r namespace pod; do
  [[ -n "$namespace" && -n "$pod" ]] || continue
  capture "logs/${namespace}-${pod}.log" kubectl logs \
    --namespace "$namespace" "$pod" \
    --all-containers --prefix --timestamps "--tail=$pod_log_lines"
  # A container the kubelet already restarted has its evidence in the previous
  # instance, not the running one.
  capture "logs/${namespace}-${pod}.log" kubectl logs \
    --namespace "$namespace" "$pod" \
    --all-containers --prefix --timestamps --previous "--tail=$pod_log_lines"
done < <(
  kubectl get pods --all-namespaces --no-headers \
    --output=custom-columns=NS:.metadata.namespace,NAME:.metadata.name 2>/dev/null
)

node_capture kubelet.log journalctl --unit kubelet --no-pager "--lines=$journal_lines"
node_capture containerd.log journalctl --unit containerd --no-pager "--lines=$journal_lines"
node_capture node.txt crictl ps --all
node_capture node.txt cat /proc/loadavg
node_capture node.txt cat /proc/pressure/cpu
node_capture node.txt cat /proc/pressure/io
node_capture node.txt cat /proc/pressure/memory
node_capture node.txt df --human-readable

# Connection tracking and packet-filter state. A KIND node routes every
# ClusterIP through kube-proxy, so an exhausted conntrack table or a truncated
# rule set explains connect failures that leave the API server itself healthy.
node_capture network.txt cat /proc/sys/net/netfilter/nf_conntrack_count
node_capture network.txt cat /proc/sys/net/netfilter/nf_conntrack_max
node_capture network.txt cat /proc/net/sockstat
node_capture network.txt ss --summary
node_capture network.txt ip -brief address
node_capture network.txt ip -statistics link
node_capture network.txt cat /proc/net/snmp
node_capture network.txt sh -c 'iptables-save | wc -l'
node_capture network.txt sh -c 'iptables-save -t nat | wc -l'
node_capture network.txt sh -c 'nft list ruleset | wc -l'
node_capture network.txt sh -c 'cat /etc/resolv.conf'
node_capture dmesg.txt sh -c 'dmesg --ctime | tail --lines=400'

capture runner.txt docker ps --all
capture runner.txt docker stats --no-stream
capture runner.txt free --mebi
capture runner.txt df --human-readable
capture runner.txt uptime

printf 'captured KIND diagnostics for cluster %s into %s\n' "$cluster" "$output"
