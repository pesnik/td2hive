#!/usr/bin/env bash
# Removes one deployed version's directory. Refuses to remove whatever
# `current` points at - roll back first (scripts/rollback.sh) if you
# genuinely need to tear down the active version. This is the only
# destructive step in the deploy/rollback/teardown set, so it's the only
# one that asks for confirmation.
#
# Usage: scripts/teardown.sh <app|datax> <version> [ssh-host]
set -euo pipefail

COMPONENT="${1:?Usage: teardown.sh <app|datax> <version> [ssh-host]}"
TARGET="${2:?Usage: teardown.sh <app|datax> <version> [ssh-host]}"
HOST="${3:-${TD2HIVE_HOST:?Usage: pass ssh-host as arg 3, or set TD2HIVE_HOST}}"

case "$COMPONENT" in
    app) REMOTE_ROOT="/data01/td2hive/app" ;;
    datax) REMOTE_ROOT="/data01/td2hive/datax" ;;
    *) echo "ERROR: component must be 'app' or 'datax'" >&2; exit 1 ;;
esac

CURRENT="$(ssh "$HOST" "readlink $REMOTE_ROOT/current" 2>/dev/null || echo '')"
if [[ "$CURRENT" == "$REMOTE_ROOT/$TARGET" ]]; then
    echo "ERROR: $TARGET is the current $COMPONENT version - roll back to a different version first (scripts/rollback.sh)." >&2
    exit 1
fi

if ! ssh "$HOST" "[[ -d $REMOTE_ROOT/$TARGET ]]"; then
    echo "ERROR: $HOST:$REMOTE_ROOT/$TARGET does not exist." >&2
    exit 1
fi

read -r -p "Remove $HOST:$REMOTE_ROOT/$TARGET permanently? [y/N] " confirm
if [[ "$confirm" != "y" && "$confirm" != "Y" ]]; then
    echo "Aborted."
    exit 1
fi

ssh "$HOST" "rm -rf $REMOTE_ROOT/$TARGET"
echo "Removed $HOST:$REMOTE_ROOT/$TARGET"
