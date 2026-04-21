import logging
from datetime import datetime
from pathlib import Path


def setup_file_logger(logger: logging.Logger, class_name: str, log_dir: Path = None) -> None:
    if logger.handlers:
        return
    if log_dir is None:
        log_dir = Path(__file__).resolve().parent.parent / "logs"
    log_dir = Path(log_dir)
    log_dir.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    handler = logging.FileHandler(log_dir / f"{class_name}_{timestamp}.log", encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s  %(levelname)s  %(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
