"""Console + per-run file logging."""

from __future__ import annotations

import logging
import sys
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

LOG_FORMAT = "%(asctime)s %(levelname)-7s %(name)s | %(message)s"


def setup_logging(log_dir: Path, run_label: str, level: str = "INFO") -> Path:
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"pipeline_{run_label}.log"

    root = logging.getLogger()
    root.setLevel(level.upper())
    root.handlers.clear()
    formatter = logging.Formatter(LOG_FORMAT)

    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(formatter)
    root.addHandler(console)

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)
    return log_path


@contextmanager
def stage(logger: logging.Logger, name: str) -> Iterator[None]:
    logger.info("[%s] started", name)
    start = time.perf_counter()
    yield
    logger.info("[%s] finished in %.2fs", name, time.perf_counter() - start)
