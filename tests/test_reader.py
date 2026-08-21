"""Regression tests for the TPT export / DataX read pairing in reader.py
and datax/job_spec.py - specifically the class of bug found live
2026-08-22: a real column's value contained an embedded delimiter
character and, separately, a bare embedded carriage return, both of
which silently corrupted row/field alignment downstream when the
export wasn't quoted. Getting the fix wrong is easy and mostly
invisible until it hits real data with an unlucky value - these tests
exist so a future change can't silently regress it.

Two kinds of proof here:
  1. Structural assertions on the exact generated TPT script / job.json,
     directly encoding facts already confirmed against the real
     Teradata/DataX runtime this session (see reader.py's own comments
     for the live-debugging trail) - these can't independently verify
     TPT/DataX's own behavior, but they lock in the specific
     configuration that was confirmed correct, so a careless future
     edit (wrong attribute name, wrong value type, a stray comment
     inside the generated script) fails loudly instead of silently.
  2. An independent round-trip proof of the *quoting scheme itself*
     (RFC4180-style: wrap every field in '"', delimit with a control
     character, escape an embedded '"' by doubling it) using Python's
     own battle-tested `csv` module as a reference implementation -
     not to test TPT or DataX (neither is available to a unit test),
     but to prove the *design* correctly survives exactly the
     corruption class that was found live: an embedded delimiter, an
     embedded bare \\r, an embedded \\n, and an embedded quote
     character, all in the same row. Both TPT (confirmed via Teradata's
     own docs: doubled-quote is the default embedded-quote escaping)
     and DataX's CsvReader (the well-known javacsv library, an
     RFC4180-style implementation) are independently confirmed to
     follow this same convention - Python's csv module is a third,
     independent implementation of the identical standard, so this is
     a meaningful proof, not a tautology.
"""
import csv
import io
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from td2hive.reader import (  # noqa: E402
    FIELD_DELIMITER,
    _TPT_TEXT_DELIMITER_HEX,
    _build_tpt_job_script,
    build_select_stmt,
)
from td2hive.datax.job_spec import build_job_json, ContentSpec  # noqa: E402
from td2hive.reader import ObsConfig  # noqa: E402
from td2hive.jobspec import RunSetting  # noqa: E402


# ---------------------------------------------------------------------
# 1. Structural assertions on the generated TPT script
# ---------------------------------------------------------------------

def test_tpt_script_quotes_every_field():
    script = _build_tpt_job_script([("NAME", "VARCHAR(50)", False)])
    assert "QuotedData = 'Yes'" in script
    assert "OpenQuoteMark = '\"'" in script
    assert "CloseQuoteMark = '\"'" in script


def test_tpt_script_uses_hex_delimiter_not_plain_pipe():
    script = _build_tpt_job_script([("COL", "INTEGER", False)])
    # The original bug: '|' has no quoting/escaping, so any value
    # containing a literal '|' corrupted field alignment.
    assert "TextDelimiter = '|'" not in script
    # The first fix attempt (a Teradata SQL hex literal) failed TPT's
    # own job-script grammar (confirmed live: TPT02954) - only the
    # dedicated TextDelimiterHex attribute, as a plain quoted hex
    # string, actually works.
    assert f"TextDelimiterHex = '{_TPT_TEXT_DELIMITER_HEX}'" in script
    assert "X'01'" not in script


def test_tpt_script_has_no_sql_style_comments():
    """TPT's own job-script grammar doesn't accept '--' comments where
    an earlier attempt placed one inside the FILE_WRITER ATTRIBUTES
    block - confirmed live via TPT02954. Explanatory comments belong at
    the Python level, never inside the generated script text."""
    script = _build_tpt_job_script([("COL", "INTEGER", False)])
    for line in script.splitlines():
        assert not line.strip().startswith("--"), f"stray SQL-style comment in generated TPT script: {line!r}"


def test_tpt_script_quotes_reserved_word_column_names():
    """A real column literally named NAME - not reserved in Teradata
    SQL, but is in TPT's own job-script grammar - failed TPT02954
    unquoted in DEFINE SCHEMA."""
    script = _build_tpt_job_script([("NAME", "VARCHAR(50)", False)])
    assert '"NAME" VARCHAR(50)' in script


def test_field_delimiter_is_a_safe_control_character():
    # 0x1F (ASCII Unit Separator) - the character ASCII itself
    # designates for this purpose, essentially never present in real
    # business text, unlike '|'.
    assert FIELD_DELIMITER == "\x1f"


# ---------------------------------------------------------------------
# 2. Structural assertions on the generated DataX job.json
# ---------------------------------------------------------------------

def _build_minimal_job_json():
    return build_job_json(
        content_specs=[ContentSpec(
            local_csv_paths=[Path("/tmp/x.csv")],
            target_obs_path="obs://bucket/some/path",
            file_name="x",
        )],
        columns=[("NAME", "VARCHAR(50)", False)],
        file_type="parquet",
        field_delimiter=FIELD_DELIMITER,
        setting=RunSetting(),
        obs_config=ObsConfig(access_key="a", secret_key="b", endpoint="c"),
    )


def test_datax_reader_config_uses_literal_quote_char_not_ascii_code():
    """csvReaderConfig is applied via BeanUtils.populate() directly onto
    the real com.csvreader.CsvReader object (confirmed by disassembling
    the actual deployed txtfilereader jar). setTextQualifier(char) takes
    a char - BeanUtils' char conversion takes the FIRST CHARACTER of the
    value's string form, so an integer like 34 (meaning "ASCII code for
    the quote character", a convention shown in some DataX docs)
    silently becomes the literal character '3' instead. Confirmed live:
    every quote went unrecognized, and type conversion then failed on
    the still-quoted value."""
    job = _build_minimal_job_json()
    csv_config = job["job"]["content"][0]["reader"]["parameter"]["csvReaderConfig"]
    assert csv_config["textQualifier"] == '"'
    assert csv_config["textQualifier"] != 34
    assert csv_config["useTextQualifier"] is True


def test_datax_reader_and_writer_share_the_same_field_delimiter():
    job = _build_minimal_job_json()
    reader_param = job["job"]["content"][0]["reader"]["parameter"]
    writer_param = job["job"]["content"][0]["writer"]["parameter"]
    assert reader_param["fieldDelimiter"] == FIELD_DELIMITER
    assert writer_param["fieldDelimiter"] == FIELD_DELIMITER


# ---------------------------------------------------------------------
# 3. Independent round-trip proof of the quoting scheme itself
# ---------------------------------------------------------------------

def _tpt_style_quote_row(values):
    """Mirrors what TPT's DataConnector actually does with
    QuotedData='Yes': wrap every field in '"', double any embedded '"',
    join with FIELD_DELIMITER. Not calling any td2hive code - this is
    an independent re-implementation of the documented TPT behavior, so
    the round-trip test below isn't just checking a function against
    itself."""
    quoted = [f'"{v.replace(chr(34), chr(34) * 2)}"' for v in values]
    return FIELD_DELIMITER.join(quoted)


def test_quoting_scheme_survives_embedded_delimiter():
    original = ["1437", "VAS", f"weird{FIELD_DELIMITER}value", "ON-NET"]
    row_text = _tpt_style_quote_row(original)
    parsed = next(csv.reader(io.StringIO(row_text), delimiter=FIELD_DELIMITER, quotechar='"'))
    assert parsed == original


def test_quoting_scheme_survives_embedded_bare_carriage_return():
    """The actual bug found live: a real LOCATION_DIM value contained a
    bare \\r (not \\n) mid-field, which a line-oriented reader treats
    as a row terminator regardless of the field delimiter chosen -
    switching FIELD_DELIMITER alone (the first fix attempt) did not
    fix this; only quoting does, since a quoted field can legitimately
    span what would otherwise look like multiple physical lines."""
    original = ["469", "VAS", "Self service (call filter\r, phone backup)(7899013)", "ON-NET"]
    row_text = _tpt_style_quote_row(original)
    parsed = next(csv.reader(io.StringIO(row_text), delimiter=FIELD_DELIMITER, quotechar='"'))
    assert parsed == original


def test_quoting_scheme_survives_embedded_newline():
    original = ["1", "some\nmulti-line\nvalue", "3"]
    row_text = _tpt_style_quote_row(original)
    parsed = next(csv.reader(io.StringIO(row_text), delimiter=FIELD_DELIMITER, quotechar='"'))
    assert parsed == original


def test_quoting_scheme_survives_embedded_quote_character():
    original = ['He said "hello" to me', "plain value"]
    row_text = _tpt_style_quote_row(original)
    parsed = next(csv.reader(io.StringIO(row_text), delimiter=FIELD_DELIMITER, quotechar='"'))
    assert parsed == original


def test_quoting_scheme_survives_every_corruption_class_at_once_in_one_row():
    """The realistic case: one row, several columns, each hitting a
    different corruption class simultaneously - matching the actual
    shape of the live LOCATION_DIM failure (11 real columns, one
    genuinely corrupted by an embedded \\r)."""
    original = [
        "1119", "VAS", "VOICE",
        f"delimiter{FIELD_DELIMITER}embedded",
        "cr\rembedded",
        "newline\nembedded",
        'quote"embedded',
        "plain", "", "0", "normal value",
    ]
    row_text = _tpt_style_quote_row(original)
    parsed = next(csv.reader(io.StringIO(row_text), delimiter=FIELD_DELIMITER, quotechar='"'))
    assert parsed == original
    assert len(parsed) == 11
