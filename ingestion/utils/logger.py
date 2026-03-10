import logging
from logging.handlers import TimedRotatingFileHandler
import os

def setup_logger(name="ETL", log_dir="logs"):
    os.makedirs(log_dir, exist_ok=True)
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    log_file = os.path.join(log_dir, "etl.log")
    handler = TimedRotatingFileHandler(log_file, when="midnight", backupCount=7)
    formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # También loggea a consola
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger