#!/usr/bin/env bash
# Runs the full plan -> run-units (fanned out) -> reconcile sequence on
# a real k8s cluster, gluing together plan-job.yaml, run-units-job.yaml,
# and reconcile-job.yaml - the wrapper plain k8s Jobs need but Argo
# Workflows / Airflow (see integrations/) would express natively as a
# real DAG instead. Prefer those for anything beyond one-off/example use.
#
# Requires: kubectl configured against your cluster, envsubst (part of
# gettext, usually already installed), k8s/secret.yaml already applied.
#
# Usage: JOB_NAME=example_fact_table PROCESSING_DATE=2026-01-15 k8s/run-all.sh
set -euo pipefail

: "${JOB_NAME:?Set JOB_NAME, e.g. example_fact_table}"
: "${PROCESSING_DATE:?Set PROCESSING_DATE, e.g. 2026-01-15}"
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

export JOB_NAME PROCESSING_DATE

echo "=== 1/3: plan ==="
kubectl delete job td2hive-plan --ignore-not-found
envsubst < "$DIR/plan-job.yaml" | kubectl apply -f -
kubectl wait --for=condition=complete --timeout=600s job/td2hive-plan

# Read the unit count back out of the shared PVC via a throwaway pod -
# there's no other way to get a file's contents out of a PVC without
# mounting it somewhere.
COMPLETIONS="$(kubectl run td2hive-plan-count --rm -i --restart=Never \
    --image=busybox --overrides='{"spec":{"containers":[{"name":"td2hive-plan-count","image":"busybox","command":["cat","/plan/unit-count"],"volumeMounts":[{"name":"plan","mountPath":"/plan"}]}],"volumes":[{"name":"plan","persistentVolumeClaim":{"claimName":"td2hive-plan"}}]}}' \
    2>/dev/null | tr -d '[:space:]')"
echo "Plan produced $COMPLETIONS unit(s)."

echo "=== 2/3: run-units (parallelism=$COMPLETIONS) ==="
export PARALLELISM="$COMPLETIONS" COMPLETIONS
kubectl delete job td2hive-run-units --ignore-not-found
envsubst < "$DIR/run-units-job.yaml" | kubectl apply -f -
kubectl wait --for=condition=complete --timeout=3600s job/td2hive-run-units

echo "=== 3/3: reconcile ==="
kubectl delete job td2hive-reconcile --ignore-not-found
envsubst < "$DIR/reconcile-job.yaml" | kubectl apply -f -
kubectl wait --for=condition=complete --timeout=600s job/td2hive-reconcile
kubectl logs job/td2hive-reconcile
