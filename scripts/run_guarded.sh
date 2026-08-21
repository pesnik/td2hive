#!/usr/bin/env bash
# Caps concurrent td2hive DataX-invoking runs to a fixed pool of slots -
# protects against many tables' cron schedules overlapping as table
# count grows. Sized off DataX's own worst-case JVM memory request
# (Xmx64g - see td2hive/datax/runner.py's DEFAULT_JVM_OPTS), not this
# host's typically-lighter actual usage: on a 503GB host, 6 slots is
# 384GB worst-case, leaving real headroom for the OS/beeline/TPT Docker
# containers/everything else. Confirmed a real, not hypothetical, risk
# 2026-08-21 - running two real jobs concurrently (well within the
# memory budget that day) still surfaced local-disk contention neither
# job hit running alone; nothing before this script stopped a third,
# fourth, fifth overlapping cron invocation from piling on further.
#
# Usage: scripts/run_guarded.sh <the real td2hive command...>
# Example:
#   scripts/run_guarded.sh /data01/.venv/bin/python3 -m td2hive.cli run \
#     --job jobs/x.yaml --processing-date 2026-01-15 ...
#
# Excess invocations past the slot pool BLOCK (checked every
# TD2HIVE_LOCK_POLL_INTERVAL seconds) until a slot frees, rather than
# failing outright or running unbounded - a cron job that fires while
# every slot is busy queues up and runs later, it doesn't get skipped or
# silently pile on.
#
# Env vars:
#   TD2HIVE_CONCURRENCY_SLOTS   - max concurrent runs (default 6)
#   TD2HIVE_LOCK_DIR            - slot lock file directory (default /var/lock/td2hive)
#   TD2HIVE_LOCK_POLL_INTERVAL  - seconds between polls when all slots are busy (default 5)
set -euo pipefail

SLOTS="${TD2HIVE_CONCURRENCY_SLOTS:-6}"
LOCK_DIR="${TD2HIVE_LOCK_DIR:-/var/lock/td2hive}"
POLL_INTERVAL="${TD2HIVE_LOCK_POLL_INTERVAL:-5}"

if [[ $# -eq 0 ]]; then
    echo "Usage: $0 <command to run under a concurrency slot>" >&2
    exit 1
fi

mkdir -p "$LOCK_DIR"

SLOT_FD=""
SLOT_NUM=""

try_acquire_slot() {
    local i lockfile fd
    for i in $(seq 1 "$SLOTS"); do
        lockfile="$LOCK_DIR/slot-$i.lock"
        exec {fd}>"$lockfile"
        if flock -n "$fd"; then
            SLOT_FD="$fd"
            SLOT_NUM="$i"
            return 0
        fi
        exec {fd}>&-
    done
    return 1
}

if ! try_acquire_slot; then
    echo "[run_guarded] all $SLOTS slot(s) busy - waiting for one to free (polling every ${POLL_INTERVAL}s)..." >&2
    while ! try_acquire_slot; do
        sleep "$POLL_INTERVAL"
    done
fi

echo "[run_guarded] acquired slot $SLOT_NUM/$SLOTS - running: $*" >&2
set +e
"$@"
exit_code=$?
set -e

flock -u "$SLOT_FD"
exec {SLOT_FD}>&-
exit "$exit_code"
