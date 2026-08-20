#!/usr/bin/env bash
# Ships a packaged DataX distribution to the target host, extracts it to a
# versioned directory, and gates promotion to `current` on
# smoke_test_datax_dist.py actually passing against real OBS. If the
# smoke test fails, `current` is left untouched - never repointed to a
# distribution that hasn't proven it can write real data. Deploy the app
# first (scripts/deploy_app.sh) - this script's smoke test needs it.
#
# Usage: scripts/deploy_datax_dist.sh <version> [ssh-host]
# Requires OBS_ACCESS_KEY, OBS_SECRET_KEY, OBS_ENDPOINT, OBS_BUCKET in the
# environment (source .env first) - used only for the smoke test, never
# printed.
set -euo pipefail

VERSION="${1:?Usage: deploy_datax_dist.sh <version> [ssh-host]}"
HOST="${2:-${TD2HIVE_HOST:?Usage: pass ssh-host as arg 2, or set TD2HIVE_HOST}}"
REMOTE_PYTHON="${TD2HIVE_PYTHON:-python3}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARBALL="$REPO_ROOT/build/td2hive-datax-${VERSION}.tar.gz"
REMOTE_ROOT="/data01/td2hive/datax"

for var in OBS_ACCESS_KEY OBS_SECRET_KEY OBS_ENDPOINT OBS_BUCKET; do
    if [[ -z "${!var:-}" ]]; then
        echo "ERROR: $var not set (source .env first)" >&2
        exit 1
    fi
done

if [[ ! -f "$TARBALL" ]]; then
    if [[ -n "${DATAX_DIST_DIR:-}" ]]; then
        echo "Building tarball first (not found: $TARBALL)"
        "$REPO_ROOT/scripts/package_datax_dist.sh" "$VERSION" "$DATAX_DIST_DIR"
    else
        echo "ERROR: $TARBALL not found. Either run scripts/package_datax_dist.sh" >&2
        echo "yourself first, or set DATAX_DIST_DIR to your built DataX distribution" >&2
        echo "so this script can build it for you." >&2
        exit 1
    fi
fi

echo "Shipping $TARBALL to $HOST..."
ssh "$HOST" "mkdir -p $REMOTE_ROOT"
scp -q "$TARBALL" "$HOST:$REMOTE_ROOT/"

echo "Extracting to $REMOTE_ROOT/$VERSION..."
ssh "$HOST" "rm -rf $REMOTE_ROOT/$VERSION && mkdir -p $REMOTE_ROOT/$VERSION && \
    tar -xzf $REMOTE_ROOT/$(basename "$TARBALL") -C $REMOTE_ROOT/$VERSION --strip-components=1 && \
    rm -f $REMOTE_ROOT/$(basename "$TARBALL")"

echo "Running smoke test against the candidate version (current is NOT yet repointed)..."
if ssh "$HOST" "DATAX_HOME=$REMOTE_ROOT/$VERSION \
    OBS_ACCESS_KEY='$OBS_ACCESS_KEY' OBS_SECRET_KEY='$OBS_SECRET_KEY' \
    OBS_ENDPOINT='$OBS_ENDPOINT' OBS_BUCKET='$OBS_BUCKET' \
    $REMOTE_PYTHON /data01/td2hive/app/current/scripts/smoke_test_datax_dist.py"; then
    echo "Smoke test PASSED. Symlinking current -> $VERSION"
    ssh "$HOST" "ln -sfn $REMOTE_ROOT/$VERSION $REMOTE_ROOT/current"
    echo "Done. current -> $(ssh "$HOST" "readlink $REMOTE_ROOT/current")"
else
    echo "Smoke test FAILED. $VERSION extracted but NOT promoted to current." >&2
    echo "Investigate at $HOST:$REMOTE_ROOT/$VERSION, or remove it with scripts/teardown.sh" >&2
    exit 1
fi
