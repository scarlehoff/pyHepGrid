import logging
import sys

logging.VALUES = 15
logging.addLevelName(logging.VALUES, "VALUES")

def values(self, message, *args, **kws):
    # Yes, logger takes its '*args' as 'args'.
    if self.isEnabledFor(logging.VALUES):
        self._log(logging.VALUES, message, args, **kws) 

logging.Logger.values = values

def setup_logger(debuglevel):

    # Add custom level for value initialisation

    logger = logging.getLogger(__name__)
    hdlr = logging.StreamHandler(sys.stdout)
    formatter = MyFormatter()
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr)
    logger.setLevel(debuglevel)



    return logger

class MyFormatter(logging.Formatter):

    format_strs  ={
            logging.WARNING:  "  \033[93m WARNING:\033[0m {}",
            logging.ERROR:  "  \033[91m ERROR:\033[0m {}",
            logging.CRITICAL:  "  \033[91m CRITICAL ERROR:\033[0m {}",
            logging.INFO:  "> {}",
            logging.DEBUG:  "  \033[94m DEBUG:\033[0m {}",
            logging.VALUES:  "\033[92mValue set:\033[0m {}",
            }

    def format(self, record):
        try:
            return MyFormatter.format_strs[record.levelno].format(record.msg)
        except KeyError as e:
            return self._fmt.format(record)
