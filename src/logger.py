import logging
import sys

def setup_logger():
    logger = logging.getLogger(__name__)
    hdlr = logging.StreamHandler(sys.stdout)
    formatter = MyFormatter()
 
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr)
    logger.setLevel(logging.DEBUG)
    return logger

class MyFormatter(logging.Formatter):

    format_strs  ={
            logging.WARNING:  "  \033[93m WARNING:\033[0m {}",
            logging.ERROR:  "  \033[91m ERROR:\033[0m {}",
            logging.CRITICAL:  "  \033[91m CRITICAL:\033[0m {}",
            logging.INFO:  "> {}",
            logging.DEBUG:  "  \033[94m DEBUG:\033[0m {}",
            }

    def format(self, record):
        try:
            return MyFormatter.format_strs[record.levelno].format(record.msg)
        except KeyError as e:
            return self._fmt.format(record)
