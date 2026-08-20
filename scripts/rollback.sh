#!/usr/bin/env bash
# Repoints `current` to a previously-deployed version. Never deletes
# anything - a rollback is just a symlink change, so it's instant and
# trivially reversible (roll forward again the same way).
#
# Usage: scripts/rollback.sh <app|datax> <version> [ssh-host]
#        scripts/rollback.sh <app|datax> --list [ssh-host]   # show what's deployed
set -euo pipefail

COMPONENT="${1:?Usage: rollback.sh <app|datax> <version|--list> [ssh-host]}"
TARGET="${2:?Usage: rollback.sh <app|datax> <version|--list> [ssh-host]}"
HOST="${3:-${TD2HIVE_HOST:?Usage: pass ssh-host as arg 3, or set TD2HIVE_HOST}}"

case "$COMPONENT" in
    app) REMOTE_ROOT="/data01/td2hive/app" ;;
    datax) REMOTE_ROOT="/data01/td2hive/datax" ;;
    *) echo "ERROR: component must be 'app' or 'datax'" >&2; exit 1 ;;
esac

if [[ "$TARGET" == "--list" ]]; then
    ssh "$HOST" "ls -1 $REMOTE_ROOT | grep -v current; echo 'current ->' \$(readlink $REMOTE_ROOT/current 2>/dev/null || echo '(unset)')"
    exit 0
fi

if ! ssh "$HOST" "[[ -d $REMOTE_ROOT/$TARGET ]]"; then
    echo "ERROR: $HOST:$REMOTE_ROOT/$TARGET does not exist. Use --list to see deployed versions." >&2
    exit 1
fi

PREVIOUS="$(ssh "$HOST" "readlink $REMOTE_ROOT/current" 2>/dev/null || echo '(unset)')"
ssh "$HOST" "ln -sfn $REMOTE_ROOT/$TARGET $REMOTE_ROOT/current"
echo "Rolled back $COMPONENT: current was $PREVIOUS, now -> $REMOTE_ROOT/$TARGET"
