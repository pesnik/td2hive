#!/usr/bin/env bash
# Builds a working DataX distribution (core + reader/txtfilereader +
# writer/hdfswriter, plus reader/streamreader for this script's own
# credential-free smoke test) from source, against the commit pinned in
# vendor/DATAX_PINNED_COMMIT, with the fixes this package's own use of
# DataX required:
#   - vendor/datax-patches/0001-fix-assembly-empty-id.patch: Maven's
#     assembly plugin rejects an empty <id></id> in package.xml - stock
#     DataX ships one.
#   - Hadoop client jars matching your cluster's Hadoop version, resolved
#     via Maven Central instead of copied off a live cluster host - see
#     scripts/resolve_hadoop_jars.sh.
#
# Requires: Java 8 (DataX's build does not work on newer JDKs - e.g. via
# sdkman: `sdk install java 8.0.442-amzn`) and Maven, both on PATH or
# JAVA_HOME pointed at a Java 8 install.
#
# Usage: scripts/build_datax_dist.sh <hadoop-version> [output-dir]
# Example: scripts/build_datax_dist.sh 3.3.6
set -euo pipefail

HADOOP_VERSION="${1:?Usage: build_datax_dist.sh <hadoop-version> [output-dir]}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUTPUT_DIR="${2:-$REPO_ROOT/build/datax-dist}"
SRC_DIR="$REPO_ROOT/build/datax-src"
PINNED_COMMIT="$(grep -v '^#' "$REPO_ROOT/vendor/DATAX_PINNED_COMMIT" | tr -d '[:space:]')"

java_version="$(java -version 2>&1 | head -1)"
if [[ "$java_version" != *'"1.8'* ]]; then
    echo "ERROR: DataX's build requires Java 8, found: $java_version" >&2
    echo "Point JAVA_HOME at a Java 8 install and re-run (e.g. via sdkman: sdk use java 8.0.442-amzn)." >&2
    exit 1
fi

echo "Fetching alibaba/DataX @ $PINNED_COMMIT into $SRC_DIR..."
rm -rf "$SRC_DIR"
mkdir -p "$SRC_DIR"
git -C "$SRC_DIR" init -q
git -C "$SRC_DIR" remote add origin https://github.com/alibaba/DataX.git
git -C "$SRC_DIR" fetch --depth 1 origin "$PINNED_COMMIT"
git -C "$SRC_DIR" checkout -q FETCH_HEAD

echo "Applying vendor/datax-patches/..."
for patch in "$REPO_ROOT"/vendor/datax-patches/*.patch; do
    echo "  $patch"
    git -C "$SRC_DIR" apply "$patch"
done

echo "Building common, core, streamreader, txtfilereader, hdfswriter (Maven)..."
mvn -q -f "$SRC_DIR/pom.xml" \
    -pl common,core,streamreader,txtfilereader,hdfswriter -am \
    package -DskipTests

echo "Assembling distribution into $OUTPUT_DIR..."
rm -rf "$OUTPUT_DIR"
mkdir -p "$OUTPUT_DIR"
# core's assembly produces the full skeleton (bin/conf/job/lib/script/tmp);
# each plugin module's assembly produces only its own plugin/<kind>/<name>/
# subtree - merge them together. (Assembly output dir name is
# "datax-<id>" where <id> comes from package.xml's <id> - "dwzip" per the
# checked-in patch above; not "datax" itself.)
cp -r "$SRC_DIR/core/target/datax-dwzip" "$OUTPUT_DIR/datax"
mkdir -p "$OUTPUT_DIR/datax/plugin"
for module in streamreader txtfilereader hdfswriter; do
    cp -r "$SRC_DIR/$module/target/datax-dwzip/plugin/"* "$OUTPUT_DIR/datax/plugin/"
done

"$REPO_ROOT/scripts/resolve_hadoop_jars.sh" "$HADOOP_VERSION" \
    "$OUTPUT_DIR/datax/plugin/writer/hdfswriter/libs"

echo ""
echo "Credential-free smoke test: streamreader -> hdfswriter, writing to a"
echo "local file:// path. Proves the Hadoop jar swap above didn't break"
echo "hdfswriter's classpath, without needing real cloud object-store"
echo "credentials. (This is NOT a substitute for"
echo "scripts/smoke_test_datax_dist.py, which validates against your real"
echo "target object storage and needs its credentials.)"
SMOKE_OUT_DIR="$(mktemp -d)"
SMOKE_JOB="$(mktemp)"
SMOKE_LOG="$(mktemp)"
cat > "$SMOKE_JOB" <<JOBJSON
{
  "job": {
    "setting": {"speed": {"channel": 1}, "errorLimit": {"record": 0, "percentage": 0.0}},
    "content": [{
      "reader": {
        "name": "streamreader",
        "parameter": {
          "column": [{"value": "smoke-test-row", "type": "string"}],
          "sliceRecordCount": 10
        }
      },
      "writer": {
        "name": "hdfswriter",
        "parameter": {
          "defaultFS": "file:///",
          "path": "$SMOKE_OUT_DIR",
          "fileName": "smoke_test",
          "fileType": "text",
          "writeMode": "append",
          "fieldDelimiter": "|",
          "column": [{"name": "col1", "type": "STRING"}]
        }
      }
    }]
  }
}
JOBJSON

# Redirect to a file and grep that afterward, rather than piping through
# `tee | grep -q` live - the live-pipe form was observed to intermittently
# report no match even though the exact same bytes, written to a file by
# `tee` in that same pipeline, plainly contained "completed successfully"
# (confirmed by direct inspection) - a pipe-timing issue specific to
# non-interactive script execution, not a real failure. A plain redirect
# followed by grep-the-file has none of that risk.
python3 "$OUTPUT_DIR/datax/bin/datax.py" "$SMOKE_JOB" > "$SMOKE_LOG" 2>&1
if grep -q "completed successfully" "$SMOKE_LOG"; then
    echo "PASS: credential-free smoke test succeeded."
    rm -f "$SMOKE_JOB" "$SMOKE_LOG"
    rm -rf "$SMOKE_OUT_DIR"
else
    echo "FAIL: credential-free smoke test did not report success - see $SMOKE_LOG" >&2
    echo "The Hadoop jar swap likely broke hdfswriter's classpath - check for" >&2
    echo "NoClassDefFoundError/NoSuchMethodError in $SMOKE_LOG." >&2
    exit 1
fi

echo ""
echo "Done: $OUTPUT_DIR/datax is ready for scripts/package_datax_dist.sh."
