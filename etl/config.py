"""Runtime settings, read from environment variables (optionally via a local .env file)."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class Settings:
    raw_data_path: Path
    processed_dir: Path
    rejected_dir: Path
    log_dir: Path
    schema_path: Path
    indexes_path: Path
    pg_host: str
    pg_port: int
    pg_database: str
    pg_user: str
    pg_password: str | None
    s3_bucket: str | None

    def pg_connect_kwargs(self) -> dict[str, object]:
        if not self.pg_password:
            raise RuntimeError("PGPASSWORD is not set. Copy .env.example to .env and fill it in.")
        return {
            "host": self.pg_host,
            "port": self.pg_port,
            "dbname": self.pg_database,
            "user": self.pg_user,
            "password": self.pg_password,
        }


def _path(value: str) -> Path:
    path = Path(value)
    return path if path.is_absolute() else ROOT / path


def load_settings() -> Settings:
    load_dotenv(ROOT / ".env")
    return Settings(
        raw_data_path=_path(os.getenv("RAW_DATA_PATH", "data/raw/hotel_bookings_raw.csv")),
        processed_dir=ROOT / "data" / "processed",
        rejected_dir=ROOT / "data" / "rejected",
        log_dir=ROOT / "logs",
        schema_path=ROOT / "sql" / "schema.sql",
        indexes_path=ROOT / "sql" / "indexes.sql",
        pg_host=os.getenv("PGHOST", "localhost"),
        pg_port=int(os.getenv("PGPORT", "5433")),
        pg_database=os.getenv("PGDATABASE", "hotel_bookings"),
        pg_user=os.getenv("PGUSER", "etl_user"),
        pg_password=os.getenv("PGPASSWORD"),
        s3_bucket=os.getenv("S3_BUCKET") or None,
    )
