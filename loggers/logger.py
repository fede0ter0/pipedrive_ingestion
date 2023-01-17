import logging
import sys
import os

service_name = 'logger'

def app_logger(logger_name=service_name, file_name=None):
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(formatter)
    logger.handlers.clear()
    logger.addHandler(sh)
    if file_name:
        filepath = os.path.dirname(file_name)
        os.umask(0)
        if not os.path.exists(filepath):
            os.makedirs(filepath)
        fh = logging.FileHandler(filename=file_name, mode='w')
        fh.setFormatter(formatter)
        logger.addHandler(fh)
    return logger


def get_logger(module_name):
    return logging.getLogger(service_name).getChild(module_name)
