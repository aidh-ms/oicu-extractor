import logging
import unittest
from io import StringIO

from icu_pipeline.logger import ICULogger


class LoggerTestCase(unittest.TestCase):
    def setUp(self):
        self.logger = ICULogger.get_logger()

        # Create a StringIO object to capture the logging output
        self.log_output = StringIO()
        self.log_handler = logging.StreamHandler(self.log_output)
        self.logger.addHandler(self.log_handler)

    def tearDown(self):
        self.logger.removeHandler(self.log_handler)
        self.log_output.close()

    def test_info_logging(self):
        message = "This is an info message"
        self.logger.info(message)
        log_output = self.log_output.getvalue().strip()

        self.assertIn(message, log_output)
        self.assertIn("info", log_output)

    def test_warning_logging(self):
        message = "This is a warning message"
        self.logger.warning(message)
        log_output = self.log_output.getvalue().strip()

        self.assertIn(message, log_output)
        self.assertIn("warning", log_output)

    def test_error_logging(self):
        message = "This is an error message"
        self.logger.error(message)
        log_output = self.log_output.getvalue().strip()

        self.assertIn(message, log_output)
        self.assertIn("error", log_output)

    def test_logger_file_output(self):
        message = "This is a message to be logged to a file"
        self.logger.info(message)

        # Assert
        with open("logs/icu_pipeline.log", "r") as log_file:
            log_content = log_file.read()
            self.assertIn(message, log_content)
            self.assertIn("info", log_content)
