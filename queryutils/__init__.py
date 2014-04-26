from logging import getLogger as get_logger
from logging.handlers import RotatingFileHandler
import logging

BYTES_IN_MB = 1048576
FIVE_MB = 5*BYTES_IN_MB

logger = get_logger("queryutils")
logger.setLevel(logging.DEBUG)
handler = RotatingFileHandler("queryutils.log", maxBytes=FIVE_MB)
handler.setLevel(logging.DEBUG)
logger.addHandler(handler)
