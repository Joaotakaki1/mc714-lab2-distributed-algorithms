from __future__ import annotations

import logging
import os


def configure_logging(node_id: str) -> None:
    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    logging.basicConfig(
        level=level,
        format=f"%(asctime)s | node {node_id} | %(levelname)s | %(message)s",
        datefmt="%H:%M:%S",
        force=True,
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)

