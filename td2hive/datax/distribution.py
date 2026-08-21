#!/usr/bin/env python3
"""Resolves and validates the DataX distribution this pipeline runs
against. Fails loudly on drift rather than letting a run silently use a
distribution missing a jar it needs - stock DataX's `hdfswriter` bundles
Hadoop 2.7.1 client jars, which throw NoSuchMethodError/NoClassDefFoundError
against most modern (3.x) Hadoop clusters.

Build a distribution with `scripts/build_datax_dist.sh <hadoop-version>`
rather than by hand - it builds DataX from a pinned source commit
(vendor/DATAX_PINNED_COMMIT), then resolves your Hadoop version's generic
client jars (hadoop-common, hadoop-hdfs, hadoop-hdfs-client,
hadoop-annotations, hadoop-auth, plus known transitive fixes) via Maven
Central (scripts/resolve_hadoop_jars.sh), removing every DataX-bundled
jar the new set supersedes. Cloud/vendor object-store connector jars
(Huawei's hadoop-huaweicloud, AWS's hadoop-aws, GCS's gcs-connector,
etc.) are NOT resolved automatically by default - some vendors don't
publish to public Maven Central at all, so this package can't assume one
answer for everyone. If yours is on Maven Central, pass its coordinates
as extra arguments to resolve_hadoop_jars.sh; otherwise it's a manual
step (place the jar in hdfswriter/libs yourself).

REQUIRED_HDFSWRITER_LIB_PREFIXES below is a validated example from one
real deployment (Huawei MRS 3.3.1 + the Huawei OBS Hadoop connector) - a
concrete starting point, not a universal requirement. Override it for
your own environment (different Hadoop distro, different object store)
by passing your own list to validate_distribution().
"""

import os
from pathlib import Path
from typing import List, Optional

REQUIRED_PLUGINS = ["reader/txtfilereader", "writer/hdfswriter"]

# Jars hdfswriter needed replaced/added beyond DataX's own bundled 2.7.1
# defaults to work against one real cluster: Huawei MRS's Hadoop 3.3.1 +
# Huawei's OBS Hadoop connector. Treat this as a worked example, not a
# hardcoded requirement for every deployment.
_EXAMPLE_HUAWEI_MRS_HDFSWRITER_LIB_PREFIXES = [
    "hadoop-common-3.3.1",
    "hadoop-hdfs-3.3.1",
    "hadoop-hdfs-client-3.3.1",
    "hadoop-annotations-3.3.1",
    "hadoop-auth-3.3.1",
    "hadoop-huaweicloud-",
    "mrs-obs-provider-",
    "esdk-obs-java-optimised-",
    "hadoop-shaded-guava-",
]


class DistributionError(RuntimeError):
    pass


def resolve_datax_home(explicit: str = "") -> Path:
    """Resolution order: explicit arg, DATAX_HOME env var, then the
    symlinked-current convention this package deploys with."""
    candidate = explicit or os.environ.get("DATAX_HOME") or "/data01/td2hive/datax/current"
    path = Path(candidate)
    if not path.exists():
        raise DistributionError(f"DATAX_HOME does not exist: {path}")
    return path


def validate_distribution(
    datax_home: Path, required_hdfswriter_lib_prefixes: Optional[List[str]] = None
) -> None:
    """Checks required plugins are present (always) and, if you pass
    required_hdfswriter_lib_prefixes, that hdfswriter's libs/ contains a
    jar matching each prefix - use this if you've had to swap Hadoop
    client jars for your own cluster (see module docstring). Left
    unchecked by default since the right jar set is cluster-specific and
    this package can't assume one."""
    plugin_dir = datax_home / "plugin"
    missing = [p for p in REQUIRED_PLUGINS if not (plugin_dir / p).is_dir()]
    if missing:
        raise DistributionError(
            f"DataX distribution at {datax_home} is missing required plugin(s): "
            f"{missing}. Rebuild/redeploy before running any job."
        )

    if not required_hdfswriter_lib_prefixes:
        return

    hdfswriter_libs = plugin_dir / "writer" / "hdfswriter" / "libs"
    present = {p.name for p in hdfswriter_libs.glob("*.jar")}
    missing_libs = [
        prefix
        for prefix in required_hdfswriter_lib_prefixes
        if not any(name.startswith(prefix) for name in present)
    ]
    if missing_libs:
        raise DistributionError(
            f"hdfswriter at {hdfswriter_libs} is missing jar(s) matching "
            f"prefixes: {missing_libs}. If these are meant to replace "
            f"DataX's bundled Hadoop 2.7.1 client jars for your cluster's "
            f"Hadoop version, this distribution will fail with "
            f"NoSuchMethodError/NoClassDefFoundError - fix the libs/ "
            f"directory before running any job against it."
        )


def list_hdfswriter_libs(datax_home: Path) -> List[str]:
    hdfswriter_libs = datax_home / "plugin" / "writer" / "hdfswriter" / "libs"
    return sorted(p.name for p in hdfswriter_libs.glob("*.jar"))
