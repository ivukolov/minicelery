import logging.config

from minicelery.config import Settings


def build_logging_config(settings: Settings) -> dict:
    level = "DEBUG" if settings.debug else settings.log_level.upper()
    console_formatter = "debug" if settings.debug else "default"

    handlers: dict = {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": console_formatter,
        },
    }

    if settings.log_file is not None:
        settings.log_file.parent.mkdir(parents=True, exist_ok=True)
        handlers["file"] = {
            "class": "logging.handlers.RotatingFileHandler",
            "filename": str(settings.log_file),
            "maxBytes": settings.log_max_bytes,
            "backupCount": settings.log_backup_count,
            "encoding": "utf-8",
            "formatter": "debug",
        }

    return {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "default": {
                "format": "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
            },
            "debug": {
                "format": "%(asctime)s | %(levelname)-8s | %(name)s:%(funcName)s:%(lineno)d | %(message)s",
            },
        },
        "handlers": handlers,
        "loggers": {
            "minicelery": {
                "level": level,
                "handlers": list(handlers),
                "propagate": False,
            },
            "redis": {"level": "WARNING"},
        },
    }


def setup_logging(settings: Settings) -> None:
    logging.config.dictConfig(build_logging_config(settings))