import logging
from pathlib import Path
from logging.handlers import RotatingFileHandler


def define_logger(name: str, log_file: str | None = None) -> logging.Logger:
    """
    Log handler for console and file logs.
    Automatically recreates log directories if missing.
    Prevents duplicate handlers on reload.
    """

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # 🔒 Prevent duplicate handlers (uvicorn reload / multiprocessing)
    if logger.handlers:
        return logger

    # ---------- FILE HANDLER ----------
    if log_file:
        log_path = Path(log_file)

        # ✅ recreate logs folder if deleted
        log_path.parent.mkdir(parents=True, exist_ok=True)

        file_handler = RotatingFileHandler(
            log_path,
            maxBytes=5 * 1024 * 1024,  # 5MB
            backupCount=5,
            encoding="utf-8"
        )
        file_handler.setLevel(logging.INFO)

        file_formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)

    # ---------- CONSOLE HANDLER ----------
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    console_formatter = logging.Formatter(
        "%(levelname)s - %(message)s"
    )
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    return logger
