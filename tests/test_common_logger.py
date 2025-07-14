import logging
import pytest
from src.common.logger import setup_logger


class TestLogger:
    def test_setup_logger_default(self):
        logger = setup_logger("test_logger")
        
        assert logger.name == "test_logger"
        assert logger.level == logging.INFO
        assert len(logger.handlers) > 0
        
    def test_setup_logger_debug_level(self):
        logger = setup_logger("test_debug", level="DEBUG")
        
        assert logger.level == logging.DEBUG
        
    def test_setup_logger_custom_level(self):
        logger = setup_logger("test_warning", level="WARNING")
        
        assert logger.level == logging.WARNING
        
    def test_logger_formatter(self):
        logger = setup_logger("test_formatter")
        
        handler = logger.handlers[0]
        formatter = handler.formatter
        assert formatter is not None
        assert "%(asctime)s" in formatter._fmt
        assert "%(name)s" in formatter._fmt
        assert "%(levelname)s" in formatter._fmt
        assert "%(message)s" in formatter._fmt