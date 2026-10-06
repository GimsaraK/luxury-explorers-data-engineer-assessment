"""Extract: read the raw CSV exactly as delivered."""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from etl.mappings import RAW_COLUMNS

log = logging.getLogger(__name__)


@dataclass
class ExtractResult:
    raw: pd.DataFrame
    source_path: Path
    sha256: str


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def extract(path: Path) -> ExtractResult:
    if not path.exists():
        raise FileNotFoundError(f"Raw dataset not found: {path}")

    # Everything is read as text so that no value is silently coerced before cleaning.
    raw = pd.read_csv(path, dtype=str, keep_default_na=False, encoding="utf-8")

    missing = [c for c in RAW_COLUMNS if c not in raw.columns]
    if missing:
        raise ValueError(f"Raw dataset is missing expected columns: {missing}")
    extra = [c for c in raw.columns if c not in RAW_COLUMNS]
    if extra:
        log.warning("Ignoring unexpected columns: %s", extra)

    raw = raw[RAW_COLUMNS]
    sha = file_sha256(path)
    log.info("Extracted %s rows x %s columns from %s (sha256 %s)", f"{len(raw):,}", raw.shape[1], path, sha[:12])
    return ExtractResult(raw=raw, source_path=path, sha256=sha)
