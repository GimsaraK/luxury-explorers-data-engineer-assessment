"""AWS S3 integration: raw landing zone, processed output and rejected-record backups.

Credentials are never passed in code. boto3 resolves them from the standard environment
variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION), which the pipeline
loads from the git-ignored .env file.

Object layout (Hive-style date partitions, so the files can later be queried by Athena/Spark):
    raw/dt=YYYY-MM-DD/<file>          the input exactly as received, before any processing
    processed/dt=YYYY-MM-DD/<file>    clean output of a run
    rejected/dt=YYYY-MM-DD/<file>     rejected and duplicate records of a run
"""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

import boto3
from botocore.exceptions import BotoCoreError, ClientError

log = logging.getLogger(__name__)


class S3Storage:
    def __init__(self, bucket: str, run_id: str, run_date: date | None = None) -> None:
        self.bucket = bucket
        self.run_id = run_id
        self.partition = f"dt={(run_date or date.today()).isoformat()}"
        self.client = boto3.client("s3")

    def _key(self, zone: str, path: Path) -> str:
        return f"{zone}/{self.partition}/{path.name}"

    def upload(self, zone: str, path: Path, sha256: str | None = None) -> str:
        key = self._key(zone, path)
        metadata = {"run-id": self.run_id}
        if sha256:
            metadata["sha256"] = sha256
        try:
            self.client.upload_file(
                str(path),
                self.bucket,
                key,
                ExtraArgs={"Metadata": metadata, "ServerSideEncryption": "AES256"},
            )
        except (BotoCoreError, ClientError) as exc:
            raise RuntimeError(f"Upload of {path.name} to s3://{self.bucket}/{key} failed: {exc}") from exc
        uri = f"s3://{self.bucket}/{key}"
        log.info("Uploaded %s (%.1f MB) to %s", path.name, path.stat().st_size / 1e6, uri)
        return uri

    def upload_raw(self, path: Path, sha256: str) -> str:
        return self.upload("raw", path, sha256)

    def upload_outputs(self, paths: dict[str, Path]) -> dict[str, str]:
        zones = {"clean": "processed", "rejected": "rejected", "duplicates": "rejected"}
        return {name: self.upload(zones[name], path) for name, path in paths.items()}
