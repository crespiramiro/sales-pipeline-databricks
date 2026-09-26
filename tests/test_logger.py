import os
import logging
import sys
from pathlib import Path

# Add project root and ingestion to sys.path
sys.path.insert(0, str(Path(__file__).parent.parent / "ingestion"))

from utils.logger import setup_logger


def test_setup_logger_creates_directory_and_returns_logger(tmp_path):
    log_dir = tmp_path / "test_logs"
    logger = setup_logger(name="TestLogger", log_dir=str(log_dir))

    assert isinstance(logger, logging.Logger)
    assert logger.name == "TestLogger"
    assert os.path.exists(log_dir)
    assert os.path.exists(os.path.join(log_dir, "etl.log"))
