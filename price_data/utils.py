import logging
from datetime import datetime
from pathlib import Path


def setup_file_logger(logger: logging.Logger, class_name: str) -> None:
    if logger.handlers:
        return
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    handler = logging.FileHandler(log_dir / f"{class_name}_{timestamp}.log", encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s  %(levelname)s  %(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
