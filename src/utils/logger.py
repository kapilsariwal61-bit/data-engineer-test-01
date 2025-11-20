# src/utils/logger.py
import logging
from pathlib import Path
import yaml

DEFAULT_LOG_PATH = Path("logs/pipeline_execution.log")


def _load_log_path_from_config(config_path: str) -> Path:
    try:
        with open(config_path) as f:
            config = yaml.safe_load(f)
        log_path = config.get("output", {}).get("log_file")
        if log_path:
            return Path(log_path)
    except FileNotFoundError:
        pass
    return DEFAULT_LOG_PATH


def get_logger(name: str, config_path: str = "src/config/config.yaml") -> logging.Logger:
    log_path = _load_log_path_from_config(config_path)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(logging.INFO)

        fh = logging.FileHandler(log_path)
        fh.setLevel(logging.INFO)

        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)

        formatter = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s - %(message)s"
        )
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)

        logger.addHandler(fh)
        logger.addHandler(ch)

    return logger
    