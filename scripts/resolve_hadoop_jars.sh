#!/usr/bin/env bash
# Replaces hdfswriter's bundled Hadoop client jars (2.7.1 - throws
# NoSuchMethodError/NoClassDefFoundError against most modern 3.x Hadoop
# clusters) with real ones for a given Hadoop version, resolved via Maven
# Central instead of copied by hand off a live cluster host.
#
# Also fixes two real jar conflicts confirmed present in DataX's stock
# hdfswriter/libs regardless of Hadoop version, found running this at
# real production scale:
#   - Two conflicting parquet-hadoop-bundle versions ship side by side
#     (Twitter's 1.6.0, using the old `parquet.*` package DataX's compiled
#     code actually imports, and Apache's 1.10.0, renamed to
#     `org.apache.parquet.*`) - only 1.6.0 is usable; having both present
#     causes NoClassDefFoundError. This script removes 1.10.0.
#   - guava-32.1.2-jre alone is incomplete: Guava 27+ split
#     InternalFutureFailureAccess into a standalone `failureaccess`
#     artifact - missing it causes its own NoClassDefFoundError. This
#     script adds it explicitly.
#
# Cloud/vendor object-store connector jars (Huawei's hadoop-huaweicloud,
# AWS's hadoop-aws, GCS's gcs-connector, etc.) are NOT resolved here by
# default - some vendors don't publish to public Maven Central at all, so
# this script can't assume one answer for everyone. If yours is on Maven
# Central, pass its coordinates as extra arguments (groupId:artifactId:version)
# and it'll be resolved alongside the Hadoop jars. Otherwise, place it in
# <target-libs-dir> yourself after this script runs.
#
# Usage: scripts/resolve_hadoop_jars.sh <hadoop-version> <target-libs-dir> [extra-maven-coords...]
# Example (S3, via Maven Central):
#   scripts/resolve_hadoop_jars.sh 3.3.6 dist/datax/plugin/writer/hdfswriter/libs \
#     org.apache.hadoop:hadoop-aws:3.3.6 com.amazonaws:aws-java-sdk-bundle:1.12.599
set -euo pipefail

HADOOP_VERSION="${1:?Usage: resolve_hadoop_jars.sh <hadoop-version> <target-libs-dir> [extra-maven-coords...]}"
TARGET_LIBS_DIR="${2:?Usage: resolve_hadoop_jars.sh <hadoop-version> <target-libs-dir> [extra-maven-coords...]}"
shift 2
EXTRA_COORDS=("$@")

if [[ ! -d "$TARGET_LIBS_DIR" ]]; then
    echo "ERROR: target libs dir not found: $TARGET_LIBS_DIR" >&2
    exit 1
fi

SCRATCH_DIR="$(mktemp -d)"
trap 'rm -rf "$SCRATCH_DIR"' EXIT
POM="$SCRATCH_DIR/pom.xml"

{
    echo '<project xmlns="http://maven.apache.org/POM/4.0.0">'
    echo '  <modelVersion>4.0.0</modelVersion>'
    echo '  <groupId>td2hive</groupId>'
    echo '  <artifactId>hadoop-jar-resolver</artifactId>'
    echo '  <version>1.0</version>'
    echo '  <packaging>pom</packaging>'
    echo '  <dependencies>'
    for artifact in hadoop-common hadoop-hdfs hadoop-hdfs-client hadoop-annotations hadoop-auth; do
        echo "    <dependency><groupId>org.apache.hadoop</groupId><artifactId>${artifact}</artifactId><version>${HADOOP_VERSION}</version></dependency>"
    done
    # Hadoop 3.x's transitive deps that 2.7.1 (DataX's bundled default)
    # never needed - without these, hadoop-common/hdfs-client fail their
    # own classloading, independent of the writer's own logic.
    echo '    <dependency><groupId>com.google.guava</groupId><artifactId>guava</artifactId><version>32.1.2-jre</version></dependency>'
    echo '    <dependency><groupId>com.google.guava</groupId><artifactId>failureaccess</artifactId><version>1.0.2</version></dependency>'
    echo '    <dependency><groupId>com.fasterxml.woodstox</groupId><artifactId>woodstox-core</artifactId><version>5.4.0</version></dependency>'
    echo '    <dependency><groupId>org.codehaus.woodstox</groupId><artifactId>stax2-api</artifactId><version>4.2.1</version></dependency>'
    echo '    <dependency><groupId>org.apache.commons</groupId><artifactId>commons-configuration2</artifactId><version>2.10.1</version></dependency>'
    echo '    <dependency><groupId>com.google.re2j</groupId><artifactId>re2j</artifactId><version>1.1</version></dependency>'
    for coord in "${EXTRA_COORDS[@]+"${EXTRA_COORDS[@]}"}"; do
        IFS=':' read -r g a v <<< "$coord"
        echo "    <dependency><groupId>${g}</groupId><artifactId>${a}</artifactId><version>${v}</version></dependency>"
    done
    echo '  </dependencies>'
    echo '</project>'
} > "$POM"

# Resolved into a scratch dir first, not directly into TARGET_LIBS_DIR:
# Hadoop 3.x pulls in ~30 transitive jars (jackson, avro, zookeeper,
# curator, jersey, commons-*, ...) that DataX's stock hdfswriter/libs
# already bundles at old (2.7.1-era) versions. A local file:// smoke test
# doesn't touch most of those classes, so this is easy to miss - but two
# conflicting versions of the same jar sitting side by side is exactly
# the non-deterministic-classloading risk that caused a real
# NoClassDefFoundError this session (parquet-hadoop-bundle, below), just
# waiting for whichever code path exercises them (real HDFS/Kerberos via
# zookeeper/curator, Avro-format writes, etc.). Resolving separately lets
# every stale jar be identified and removed by name before anything new
# is copied in, rather than hardcoding the ~7 names that happened to
# matter for one deployment.
RESOLVED_DIR="$SCRATCH_DIR/resolved"
mkdir -p "$RESOLVED_DIR"
echo "Resolving Hadoop ${HADOOP_VERSION} client jars (+ transitive deps) via Maven Central..."
mvn -q -f "$POM" dependency:copy-dependencies \
    -DincludeScope=runtime \
    -DoutputDirectory="$RESOLVED_DIR"

echo "Removing bundled jars superseded by the resolved set from $TARGET_LIBS_DIR..."
# Maven jar names are always <artifactId>-<version>[-classifier].jar,
# where <version> starts with a digit - strip from the last such point to
# get the artifact name (works for artifactIds that themselves end in a
# digit, e.g. commons-configuration2-2.10.1.jar -> commons-configuration2).
removed_any=0
for jar in "$RESOLVED_DIR"/*.jar; do
    name="$(basename "$jar")"
    artifact="$(echo "$name" | sed -E 's/-[0-9][0-9.]*(-[A-Za-z0-9.]+)?\.jar$//')"
    for stale in "$TARGET_LIBS_DIR/${artifact}"-[0-9]*.jar; do
        [[ -f "$stale" ]] || continue
        rm -f "$stale"
        echo "  removed $(basename "$stale") (superseded by $name)"
        removed_any=1
    done
done
# DataX's stock hdfswriter/libs also ships two conflicting
# parquet-hadoop-bundle versions out of the box, independent of Hadoop
# version - Twitter's 1.6.0 (the old `parquet.*` package DataX's compiled
# code actually imports) and Apache's 1.10.0 (renamed to
# `org.apache.parquet.*`). Only 1.6.0 is usable; this isn't resolved via
# Maven above (nothing here declares a parquet-hadoop-bundle dependency),
# so it needs its own explicit removal.
if [[ -f "$TARGET_LIBS_DIR/parquet-hadoop-bundle-1.10.0.jar" ]]; then
    rm -f "$TARGET_LIBS_DIR/parquet-hadoop-bundle-1.10.0.jar"
    echo "  removed parquet-hadoop-bundle-1.10.0.jar (conflicts with bundled 1.6.0, DataX's code only imports 1.6.0's package)"
fi

echo "Copying resolved jars into $TARGET_LIBS_DIR..."
cp "$RESOLVED_DIR"/*.jar "$TARGET_LIBS_DIR/"

echo ""
echo "Done. $TARGET_LIBS_DIR now has:"
ls "$TARGET_LIBS_DIR" | grep -iE 'hadoop-(common|hdfs|annotations|auth)-|guava-|failureaccess-|parquet-hadoop-bundle-' | sort
echo ""
echo "Reminder: cloud/vendor object-store connector jars (S3/GCS/OBS/etc.)"
echo "are your responsibility unless you passed their Maven coordinates as"
echo "extra arguments above - place them in $TARGET_LIBS_DIR yourself if not."
