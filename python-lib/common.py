import logging, sys
from logging import Logger


def get_logger() -> Logger:
    """
    Returns an instance of Logger customised for this plugin
    :return:
    """
    # we're ditching the timestamp output because it already gets added by the java logger DSS uses
    logging.basicConfig(
        stream=sys.stdout,
        level=logging.INFO,
        format='%(name)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger("snowflake-hdfs-plugin")
