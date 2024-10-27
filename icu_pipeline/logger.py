import logging
import os

import colorlog


class ICULogger:
    logger: logging.Logger | None = None

    @staticmethod
    def __init_logger() -> None:
        if ICULogger.logger is not None:
            return
        _logger: logging.Logger = logging.getLogger()  # Get the root logger
        _logger.setLevel(logging.DEBUG)  # Set your desired log level

        formatter = colorlog.ColoredFormatter(
            "%(log_color)s%(levelname)s:%(name)s:%(message)s%(reset)s",
            log_colors={
                "DEBUG": "cyan",
                "INFO": "green",
                "WARNING": "yellow",
                "ERROR": "red",
                "CRITICAL": "red,bg_white",
            },
        )

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        _logger.addHandler(console_handler)

        log_file_path = "logs/icu_pipeline.log"
        # Ensure the directory exists
        os.makedirs(os.path.dirname(log_file_path), exist_ok=True)

        file_handler = logging.FileHandler(log_file_path)
        file_handler.setFormatter(formatter)
        _logger.addHandler(file_handler)
        _logger.info("New Logger Created")
        ICULogger.logger = _logger

    @staticmethod
    def get_logger() -> logging.Logger:
        ICULogger.__init_logger()
        assert ICULogger.logger is not None
        return ICULogger.logger
