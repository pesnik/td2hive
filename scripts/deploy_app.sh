#!/usr/bin/env bash
# Deploys the td2hive Python package (code + jobs/ + scripts/) to the
# target host as a versioned directory, symlinked as `current`. Reuses
# whatever Python environment is already set up on the target host rather
# than provisioning a new one - one Python env per host, not one per app.
# Set TD2HIVE_PYTHON to that environment's python3 (e.g.
# /path/to/venv/bin/python3) if it's not plain `python3` on the target
# host's PATH.
#
# Usage: scripts/deploy_app.sh <version> [ssh-host]
# ssh-host can also come from TD2HIVE_HOST.
set -euo pipefail

VERSION="${1:?Usage: deploy_app.sh <version> [ssh-host]}"
HOST="${2:-${TD2HIVE_HOST:?Usage: pass ssh-host as arg 2, or set TD2HIVE_HOST}}"
REMOTE_PYTHON="${TD2HIVE_PYTHON:-python3}"
REMOTE_ROOT="/data01/td2hive/app"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "Deploying td2hive app version $VERSION to $HOST:$REMOTE_ROOT/$VERSION"
ssh "$HOST" "mkdir -p $REMOTE_ROOT/$VERSION"

rsync -az --delete \
    "$REPO_ROOT/td2hive/" "$HOST:$REMOTE_ROOT/$VERSION/td2hive/"
rsync -az \
    "$REPO_ROOT/pyproject.toml" "$HOST:$REMOTE_ROOT/$VERSION/pyproject.toml"
rsync -az --delete \
    "$REPO_ROOT/scripts/" "$HOST:$REMOTE_ROOT/$VERSION/scripts/"

echo "Checking dependencies are already present in $HOST's Python environment..."
echo "(this only checks, never installs over the network - many deployment"
echo " hosts have no internet/PyPI access; install missing packages"
echo " yourself, e.g. 'pip install td2hive[mysql]' from a host that does.)"
MISSING="$(ssh "$HOST" "$REMOTE_PYTHON -m pip freeze 2>/dev/null" | python3 -c "
import re, sys
installed = {}
for line in sys.stdin:
    line = line.strip()
    if '==' in line:
        name, _, ver = line.partition('==')
        installed[name.lower().replace('_', '-')] = ver
pyproject = open('$REPO_ROOT/pyproject.toml').read()
deps_block = re.search(r'dependencies\s*=\s*\[(.*?)\]', pyproject, re.DOTALL).group(1)
missing = []
for m in re.finditer(r'\"([^\"]+)\"', deps_block):
    spec = m.group(1)
    name = re.split(r'[<>=!~]', spec, 1)[0].strip().lower().replace('_', '-')
    if name not in installed:
        missing.append(spec)
print('\n'.join(missing))
")"
if [[ -n "$MISSING" ]]; then
    echo "ERROR: missing dependencies in $HOST's Python environment ($REMOTE_PYTHON), not installed automatically:" >&2
    echo "$MISSING" >&2
    exit 1
fi
echo "All dependencies present."

echo "Verifying the deployed package imports cleanly..."
ssh "$HOST" "cd $REMOTE_ROOT/$VERSION && $REMOTE_PYTHON -c 'import td2hive.job_runner, td2hive.retention, td2hive.cli; print(\"import ok\")'"

echo "Symlinking current -> $VERSION"
ssh "$HOST" "ln -sfn $REMOTE_ROOT/$VERSION $REMOTE_ROOT/current"

echo "Done. Deployed versions:"
ssh "$HOST" "ls -1 $REMOTE_ROOT | grep -v current; echo 'current ->' \$(readlink $REMOTE_ROOT/current)"
