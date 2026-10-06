"""Run the hotel bookings ETL pipeline.

    python run_pipeline.py                 # extract -> transform -> validate -> load
    python run_pipeline.py --no-load       # everything except the PostgreSQL load
    python run_pipeline.py --input path/to/file.csv --log-level DEBUG
"""

from __future__ import annotations

import argparse
import logging
import sys
import uuid
from datetime import datetime
from pathlib import Path

import pandas as pd

from etl.config import Settings, load_settings
from etl.extract import extract
from etl.logging_setup import setup_logging, stage
from etl.transform import transform
from etl.validate import validate

log = logging.getLogger("run_pipeline")


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Hotel bookings ETL pipeline")
    parser.add_argument("--input", type=Path, help="Raw CSV to process (default: RAW_DATA_PATH from .env)")
    parser.add_argument("--no-load", action="store_true", help="Skip the PostgreSQL load")
    parser.add_argument("--no-s3", action="store_true", help="Skip the S3 uploads even if S3_BUCKET is set")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return parser.parse_args(argv)


def write_outputs(
    settings: Settings,
    run_label: str,
    raw: pd.DataFrame,
    valid: pd.DataFrame,
    rejected: pd.DataFrame,
    duplicate_index: pd.Index,
) -> dict[str, Path]:
    settings.processed_dir.mkdir(parents=True, exist_ok=True)
    settings.rejected_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "clean": settings.processed_dir / f"bookings_clean_{run_label}.csv",
        "rejected": settings.rejected_dir / f"rejected_{run_label}.csv",
        "duplicates": settings.rejected_dir / f"duplicates_{run_label}.csv",
    }
    valid.to_csv(paths["clean"], index=False, date_format="%Y-%m-%d", lineterminator="\n")
    rejected.to_csv(paths["rejected"], index=False, lineterminator="\n")
    duplicates = raw.loc[duplicate_index].copy()
    duplicates.insert(0, "source_line", duplicates.index + 2)
    duplicates.to_csv(paths["duplicates"], index=False, lineterminator="\n")
    for name, path in paths.items():
        log.info("Wrote %s output to %s", name, path)
    return paths


def build_rejected_frame(raw: pd.DataFrame, reasons: pd.Series) -> pd.DataFrame:
    """Rejected rows keep their original raw values so they can be inspected and replayed."""
    rejected = raw.loc[reasons.index].copy()
    rejected.insert(0, "source_line", rejected.index + 2)  # +1 for the header, +1 for 1-based lines
    rejected["rejection_reasons"] = reasons.map(";".join)
    return rejected


def run(args: argparse.Namespace) -> int:
    settings = load_settings()
    run_id = uuid.uuid4()
    run_label = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = setup_logging(settings.log_dir, run_label, args.log_level)
    source = args.input or settings.raw_data_path
    log.info("Pipeline run %s started (log: %s)", run_id, log_path)

    counts: dict[str, int] = {}
    s3_uris: dict[str, str] = {}
    conn = None
    try:
        with stage(log, "extract"):
            extracted = extract(source)
            counts["extracted"] = len(extracted.raw)

        storage = None
        if args.no_s3 or not settings.s3_bucket:
            log.info("S3 uploads disabled (%s)", "--no-s3 set" if args.no_s3 else "S3_BUCKET not set")
        else:
            from etl.storage import S3Storage

            storage = S3Storage(settings.s3_bucket, str(run_id))
            with stage(log, "s3 raw landing"):
                s3_uris["raw"] = storage.upload_raw(source, extracted.sha256)

        with stage(log, "transform"):
            transformed = transform(extracted.raw)
            counts["duplicates"] = len(transformed.duplicate_index)

        with stage(log, "validate"):
            validated = validate(transformed.clean, transformed.invalid)
            rejected = build_rejected_frame(extracted.raw, validated.rejected_reasons)
            counts["rejected"] = len(rejected)

        with stage(log, "write outputs"):
            paths = write_outputs(settings, run_label, extracted.raw, validated.valid, rejected, transformed.duplicate_index)

        if storage is not None:
            with stage(log, "s3 outputs"):
                s3_uris.update(storage.upload_outputs(paths))

        if args.no_load:
            log.info("--no-load set: skipping PostgreSQL load")
        else:
            from etl import load  # imported lazily so --no-load works without a database driver

            with stage(log, "load"):
                conn = load.connect(settings.pg_connect_kwargs())
                load.apply_schema(conn, settings.schema_path, settings.indexes_path)
                load.start_run(conn, run_id, str(source), extracted.sha256, s3_uris.get("raw"), s3_uris.get("clean"))
                counts["loaded"] = load.load_bookings(conn, run_id, validated.valid, rejected)
                load.vacuum_analyze(conn)
                load.finish_run(conn, run_id, "success", counts)
    except Exception as exc:
        log.exception("Pipeline run %s failed: %s", run_id, exc)
        if conn is not None:
            try:
                load.finish_run(conn, run_id, "failed", counts, error=str(exc))
            except Exception:
                log.exception("Could not record the failure in etl.pipeline_runs")
        return 1
    finally:
        if conn is not None:
            conn.close()

    log.info("Summary for run %s", run_id)
    log.info("  rows extracted          %8s", f"{counts['extracted']:,}")
    log.info("  duplicates removed      %8s", f"{counts['duplicates']:,}")
    log.info("  missing values filled   %8s  %s", f"{sum(transformed.filled_counts.values()):,}",
             {k: v for k, v in transformed.filled_counts.items() if v})
    log.info("  rows rejected           %8s", f"{counts['rejected']:,}")
    log.info("  rows valid              %8s", f"{len(validated.valid):,}")
    if "loaded" in counts:
        log.info("  rows loaded (upserted)  %8s", f"{counts['loaded']:,}")
    for name, uri in s3_uris.items():
        log.info("  s3 %-20s %s", name, uri)
    return 0


def main(argv: list[str] | None = None) -> int:
    return run(parse_args(argv))


if __name__ == "__main__":
    sys.exit(main())
