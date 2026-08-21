#!/usr/bin/env bash
# Packages a locally-built DataX distribution into a versioned tarball
# ready to ship. `datax-dist-dir` should contain core + the writer/reader
# plugins you need (at minimum writer/hdfswriter and reader/txtfilereader)
# - build one with `scripts/build_datax_dist.sh <hadoop-version>`, which
# handles the source build and the Hadoop-client-jar swap your cluster
# needs (see td2hive/datax/distribution.py's module docstring for why
# that's necessary). Its output (build/datax-dist/datax) is exactly the
# <datax-dist-dir> this script expects.
#
# Usage: scripts/package_datax_dist.sh <version> <datax-dist-dir>
set -euo pipefail

VERSION="${1:?Usage: package_datax_dist.sh <version> <datax-dist-dir>}"
DIST_DIR="${2:?Usage: package_datax_dist.sh <version> <datax-dist-dir>}"
OUT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/build"
TARBALL="${OUT_DIR}/td2hive-datax-${VERSION}.tar.gz"

if [[ ! -d "$DIST_DIR" ]]; then
    echo "ERROR: DataX dist dir not found: $DIST_DIR" >&2
    exit 1
fi

for plugin in reader/txtfilereader reader/streamreader writer/hdfswriter; do
    if [[ ! -d "$DIST_DIR/plugin/$plugin" ]]; then
        echo "ERROR: required plugin missing from dist: $plugin" >&2
        exit 1
    fi
done

# AppleDouble junk from macOS breaks DataX's plugin directory scanner
# (observed 2026-08-19, again 2026-08-20). Two separate things needed:
# 1. Strip any ._* files already on disk from past extractions/edits.
# 2. COPYFILE_DISABLE=1 - without it, macOS's own tar re-injects fresh
#    ._<name> AppleDouble entries INTO the archive for many files as it
#    writes it, regardless of what's on disk beforehand (confirmed
#    2026-08-20: a clean dist dir still produced a smoke-test-breaking
#    ._txtfilereader/plugin.json inside the tarball without this).
find "$DIST_DIR" -name "._*" -delete

mkdir -p "$OUT_DIR"
echo "Packaging $DIST_DIR -> $TARBALL"
COPYFILE_DISABLE=1 tar -czf "$TARBALL" -C "$(dirname "$DIST_DIR")" "$(basename "$DIST_DIR")"
echo "Done: $TARBALL ($(du -h "$TARBALL" | cut -f1))"
