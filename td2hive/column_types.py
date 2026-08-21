#!/usr/bin/env python3
"""Dynamic Teradata column-type resolution via DBC.ColumnsV.

Never hardcode a name->type mapping here - every table's columns must be
resolved against DBC.ColumnsV at run time. This is what makes the package
usable against any Teradata schema, not just the tables it was built
against.
"""

from typing import List, Tuple

# Teradata DBC.ColumnsV ColumnType codes we know how to map. DECIMAL/FLOAT/
# NUMBER always resolve to TPT FLOAT with an explicit CAST in the SELECT,
# regardless of native precision/scale: two source tables can declare
# different precision for the same-named column (a real scenario seen
# with production data, not hypothetical), and TPT's EXPORT operator requires the
# SELECT's actual result byte-length to exactly match DEFINE SCHEMA - "wide
# enough" isn't good enough, it must be exact. FLOAT is a fixed 8-byte width
# regardless of source precision, and matches the DOUBLE these columns land
# as in the final Hive table anyway.
_DECIMAL_LIKE = {"D", "F", "N"}
_INTEGER_LIKE = {"I", "I1", "I2", "I8"}
_CHAR_LIKE = {"CV", "CF"}
# DATE/TIMESTAMP resolve to an explicit CAST to VARCHAR, for the exact
# same reason DECIMAL casts to FLOAT: TPT's EXPORT operator needs the
# SELECT's actual result byte-length to match DEFINE SCHEMA exactly, and
# Teradata's native DATE/TIMESTAMP binary encodings aren't naturally
# TPT-portable the way a formatted string is. VARCHAR(10) covers
# 'YYYY-MM-DD'; VARCHAR(26) covers 'YYYY-MM-DD HH:MI:SS.SSSSSS' (TS(6),
# the widest fractional-second precision Teradata supports) - sized for
# the worst case rather than the source column's own declared precision,
# same reasoning DECIMAL's fixed-width FLOAT cast already uses.
_DATE_LIKE = {"DA"}
_TIMESTAMP_LIKE = {"TS", "SZ"}  # SZ = TIMESTAMP WITH TIME ZONE

ResolvedColumn = Tuple[str, str, bool]  # (name, tpt_type, needs_cast)
# When needs_cast is True, tpt_type IS the CAST target - reader.py's
# build_select_stmt casts every such column to tpt_type directly
# (`CAST(col AS {tpt_type})`), never a hardcoded type, so adding a new
# needs_cast case here (as DATE/TIMESTAMP did) never requires a reader.py
# change too.

# hdfswriter's own type vocabulary (STRING/BIGINT/DOUBLE) - a second,
# independent mapping from the same dynamically-resolved tpt_type, not a
# separate lookup against Teradata.
_TPT_TO_DATAX_READER_TYPE = {"FLOAT": "double", "INTEGER": "long"}
_TPT_TO_DATAX_WRITER_TYPE = {"FLOAT": "DOUBLE", "INTEGER": "BIGINT"}
_TPT_TO_HIVE_TYPE = {"FLOAT": "DOUBLE", "INTEGER": "INT"}


def resolve_column_types(
    cursor, schema: str, tables: List[str], columns: List[str]
) -> List[ResolvedColumn]:
    """Look up each column's Teradata type across all source tables and
    return (name, tpt_type, needs_float_cast) in the given column order.

    Looking across *all* source tables (not just one) is what catches a
    decimal precision/scale mismatch between them before it reaches TPT.
    """
    placeholders = ", ".join("?" for _ in tables)
    cursor.execute(
        f"""
        SELECT ColumnName, ColumnType, ColumnLength
        FROM DBC.ColumnsV
        WHERE DatabaseName = ? AND TableName IN ({placeholders})
        """,
        [schema, *tables],
    )
    by_name: dict = {}
    for col_name, col_type, col_length in cursor.fetchall():
        by_name.setdefault(col_name.strip().upper(), []).append(
            (col_type.strip(), col_length)
        )

    resolved: List[ResolvedColumn] = []
    for col in columns:
        key = col.strip().upper()
        variants = by_name.get(key)
        if not variants:
            raise ValueError(
                f"Column {col} not found in DBC.ColumnsV for {schema}.{tables}"
            )

        types_seen = {v[0] for v in variants}
        if types_seen & _DECIMAL_LIKE:
            resolved.append((col, "FLOAT", True))
        elif types_seen & _DATE_LIKE:
            resolved.append((col, "VARCHAR(10)", True))
        elif types_seen & _TIMESTAMP_LIKE:
            resolved.append((col, "VARCHAR(26)", True))
        elif types_seen <= _INTEGER_LIKE:
            resolved.append((col, "INTEGER", False))
        elif types_seen <= _CHAR_LIKE:
            max_len = max((v[1] or 0) for v in variants)
            # CF (fixed CHAR) declared as VARCHAR without a cast fails
            # TPT's strict schema binding - confirmed live: TPT02639
            # "Conflicting data type... Source column's data type
            # (VARCHAR) Target column's data type (CHAR)". An explicit
            # CAST(col AS VARCHAR(n)) is needed for any CF variant - also
            # correctly handles a CV/CF mix across load tables, and
            # incidentally strips CHAR's fixed-width trailing-space
            # padding, which is the right behavior for a Hive STRING
            # column anyway. Pure CV needs no cast (the SELECT already
            # returns VARCHAR, matching DEFINE SCHEMA exactly).
            needs_char_cast = bool(types_seen - {"CV"})
            resolved.append((col, f"VARCHAR({max_len or 1})", needs_char_cast))
        else:
            raise ValueError(
                f"Column {col} has unsupported/mixed type(s) {types_seen} across "
                f"{schema}.{tables} - add explicit handling before using TPT export"
            )
    return resolved


def to_datax_reader_type(tpt_type: str) -> str:
    """Map a resolved TPT type to txtfilereader's column-type vocabulary."""
    if tpt_type.startswith("VARCHAR"):
        return "string"
    return _TPT_TO_DATAX_READER_TYPE[tpt_type]


def to_datax_writer_type(tpt_type: str) -> str:
    """Map a resolved TPT type to hdfswriter's column-type vocabulary."""
    if tpt_type.startswith("VARCHAR"):
        return "STRING"
    return _TPT_TO_DATAX_WRITER_TYPE[tpt_type]


def to_hive_type(tpt_type: str) -> str:
    """Map a resolved TPT type to a Hive column DDL type."""
    return _TPT_TO_HIVE_TYPE.get(tpt_type, "STRING")
