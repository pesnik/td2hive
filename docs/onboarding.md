# Onboarding a new table

This turns the ad-hoc process used for the first tables migrated onto
`td2hive` into a repeatable sequence. A table has no business in
`jobs/` (production) until it's been through every step here - not
because process is a virtue in itself, but because every step exists to
catch a class of bug that has actually happened: a legacy config's
column order silently disagreeing with the real Hive DDL (positional
Parquet writing means the wrong order corrupts data with no error), a
batching default that could have combined an entire table's writes into
one oversized JVM, an unscoped verification query that overflowed
against a table with tens of thousands of partitions. None of these
were hypothetical - they were each found by actually running the tool
against a real table, not by review.

## 1. Scaffold a draft

```bash
scripts/scaffold_job.py <table_name> \
  --mysql-host ... --td-host ... > jobs/drafts/<table_name>.yaml
```

Pulls owner/load-table/partition shape from your legacy config table as
a starting point (never trusted for column *order* - see below), and
the real column order from the target Hive table's own `DESCRIBE`. If
the target table doesn't exist yet (a genuinely new table, never
onboarded under any prior pipeline), it falls back to the source
table's own physical column order (`DBC.ColumnsV`, ordered by
`ColumnId`) instead - the only order available before step 2 creates
the real table.

Watch stderr for two warnings:
- **Legacy `EXPORT_METHOD` wasn't `TPT`** - informational, not a
  problem; every table starts here.
- **Legacy config's `COLUMNS` order disagrees with the real Hive DDL
  order** - this is the exact class of bug this tool exists to catch.
  The draft always uses the real DDL order, never the legacy config's,
  but read the warning anyway: if the *set* of columns differs, not
  just the order, something's more wrong than ordering and needs a
  human look before proceeding.

## 2. Create the real target table (new tables only)

Skip this step if the table already has a Hive target (every table
migrated off an existing legacy `WRITE_NOS` load does).

```bash
td2hive create-target --job jobs/drafts/<table_name>.yaml \
  --td-host ... --obs-access-key ... --obs-secret-key ... --obs-endpoint ... --obs-bucket ...
```

Refuses to touch an already-existing table without `--force` - this
creates real, permanent Hive metadata, not a scratch table.

## 3. Validate against real data

```bash
td2hive validate --job jobs/drafts/<table_name>.yaml \
  --row-limit 2000 --partition-limit 3 \
  --td-host ... --obs-access-key ... --obs-secret-key ... --obs-endpoint ... --obs-bucket ...
```

Creates a scratch Hive table, runs a small, bounded proof load into it,
reports whether DataX's own reported write count and an independent
Hive recount agree, then tears the scratch table + its OBS data down.

`--partition-limit` matters independently of `--row-limit`: for a table
with a dynamic partition column, `plan()` enumerates one unit per
distinct partition value *before* `--row-limit` is ever applied -
`--row-limit` only bounds rows *within* each unit's export. A table
with dozens or hundreds of distinct values will still run a full
sequential pass over every one of them without `--partition-limit`
capping the *count*. (Confirmed live: a table with ~180 distinct
values ran for 25+ minutes before being killed, with `--row-limit`
alone doing nothing to shorten it.)

Read `PASS`/`FAIL` from the DataX-reported-vs-Hive-recounted comparison
printed at the end, not from the run's own `status` field - a bounded
proof run always disagrees with the full unscoped source table by
design (that's not a failure, it's the point of bounding the run).

## 4. Pick `speed_channel` from what you actually observed

**Never copy another table's `speed_channel`.** Row width varies too
much across tables for one constant to be safe - a wide table at a
given channel count can already be close to its memory ceiling while a
narrow one has plenty of headroom at the same setting (confirmed on a
real table: 16 channels sat at ~29GB/32GB, 91% of cap - a flat default
copied onto every table would have been a real production risk, not
caution for its own sake).

While step 3's validate run is in flight, watch the DataX JVM's actual
memory use on the host running it (`ps -o pid,rss,comm -C java`, or
your platform's equivalent) and note the peak RSS. Compare that against
the channel count `job.setting.speed_channel` used for that run and the
host's total available memory. Raise `speed_channel` only if there's
real, confirmed headroom at that row width - don't guess upward from a
"seems fine" impression.

Leave `job.setting.max_channels_per_job` unset (it defaults to
`speed_channel` itself - no batching) unless the table's shape
genuinely benefits from it: many *small* partitions where DataX's
JVM cold-start cost dominates, not a few wide/heavy ones. Setting it
without that combination (many small partitions **and** confirmed
memory headroom) risks silently combining a table's entire write
workload into one oversized JVM - see **Scaling** in the main README
for the specific production near-miss this caused.

Newly-promoted tables that haven't had this observation done yet should
be flagged as such in the job YAML's own comments (a plain
`speed_channel: 4` with no accompanying note is a signal the tuning
step was skipped, not that 4 was confirmed correct) - don't let "the
draft's default happened to work for the validate proof" stand in for
an actual observation at real production row volume.

## 5. Promote

Move the draft into `jobs/` (drop the `# DRAFT` header, keep any
column-order-mismatch or column-order-authority comments - they're
exactly the detail a future reader needs if this table's schema ever
looks wrong again). This is a deliberate, reviewed step - nothing in
this pipeline promotes a draft automatically.

## 6. First real run

The first non-scratch `td2hive run` for a newly-promoted table is worth
watching directly, not just firing and walking away - especially for a
table whose real row volume wasn't obvious from the legacy config alone
(a `WRITE_NOS`-era table can look small in configuration and turn out
to be hundreds of millions of rows a day once you actually query it).
If it's meaningfully bigger than anything already running, treat
promoting it into the regular schedule as a separate decision from
promoting the job spec itself.
