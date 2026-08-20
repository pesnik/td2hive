#!/usr/bin/env python3
"""SQL audit sink via SQLAlchemy - MySQL, Postgres, or SQLite from one
implementation and a connection URL, not hardcoded to any one database.
An existing audit table in your own environment is just a configuration
of this sink (a connection URL + table name), not a built-in assumption
this package makes on your behalf.
"""

from dataclasses import asdict

from sqlalchemy import (
    Column,
    DateTime,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    create_engine,
    select,
)

from . import AuditRecord


class SQLAuditSink:
    def __init__(self, connection_url: str, table_name: str = "td2hive_audit_log"):
        self.engine = create_engine(connection_url)
        self.metadata = MetaData()
        self.table = Table(
            table_name,
            self.metadata,
            Column("run_id", String(36), primary_key=True),
            Column("job_name", String(255)),
            Column("processing_date", String(10)),
            Column("source_schema", String(255)),
            Column("source_table", String(255)),
            Column("hive_schema", String(255)),
            Column("hive_table", String(255)),
            Column("source_row_count", Integer),
            Column("target_row_count", Integer),
            Column("status", String(20)),
            Column("loader", String(30)),
            Column("datax_reported_count", Integer, nullable=True),
            Column("error_detail", String(2000), nullable=True),
            Column("start_time", DateTime),
            Column("end_time", DateTime),
            Column("duration_seconds", Float),
        )
        self.metadata.create_all(self.engine, checkfirst=True)

    def record(self, run: AuditRecord) -> None:
        row = asdict(run)
        row["duration_seconds"] = run.duration_seconds
        with self.engine.begin() as conn:
            conn.execute(self.table.insert().values(**row))

    def find_success(self, job_name: str, processing_date: str) -> bool:
        with self.engine.connect() as conn:
            stmt = (
                select(self.table.c.run_id)
                .where(self.table.c.job_name == job_name)
                .where(self.table.c.processing_date == processing_date)
                .where(self.table.c.status == "success")
                .limit(1)
            )
            return conn.execute(stmt).first() is not None
