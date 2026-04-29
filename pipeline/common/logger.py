"""Logging configuration"""

import logging
import sys


def get_logger(name: str) -> logging.Logger:
    """Get configured logger (idempotent, no duplicate handlers)"""
    logger = logging.getLogger(name)

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        handler.setFormatter(formatter)

        logger.addHandler(handler)
        logger.setLevel(logging.INFO)

        # ✅ Prevent duplicate logs from root logger
        logger.propagate = False

    return logger