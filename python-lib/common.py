import logging, sys
from logging import Logger

def get_logger() -> Logger:
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    return logging.getLogger("snowflake-hdfs-plugin")
