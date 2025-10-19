from datetime import datetime
import logging, os


def init_logger(name: str, log_path: str = None) -> logging.Logger:
    """
    Initialize a logger with the given name.

    Args:
        name (str): The name of the logger.

    Returns:
        logging.Logger: Configured logger instance.
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    # Create console handler and set level to debug
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

    # Create formatter and add it to the handler
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    ch.setFormatter(formatter)

    logger.addHandler(ch)

    if log_path:
        os.makedirs(os.path.dirname(log_path), exist_ok=True)
        fh = logging.FileHandler(log_path)
        fh.setLevel(logging.INFO)
        fh.setFormatter(formatter)
        logger.addHandler(fh)
    return logger


timestamp = datetime.now().strftime("%Y-%m-%d")
log_path = "./logs/"
log_file = os.path.join(log_path, f"sample_dag_{timestamp}.log")
logger_log = init_logger("sample_dag_etl", log_file)
