import logging
import os


def get_plain_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    level_name = os.getenv("PLAIN_LOG_LEVEL", "INFO").upper()
    logger.setLevel(getattr(logging, level_name, logging.INFO))
    logger.propagate = False
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(handler)
    return logger
