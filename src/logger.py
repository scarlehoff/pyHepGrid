import logging
import sys
import os

################### README ###################
# Use:
# For all print/error/warning statements, import logger from src.header
#
#             LOG FUNCTION     PURPOSE                                         LEVEL
# Then use -> logger.debug     debug statement                                   10
#             logger.value     value setting in src.header                       15 
#             logger.info      generic print                                     20
#             logger.warning   warning message, continue with running            30
#             logger.error     error message, continue with running              40
#             logger.critical  error message, exit with non-zero error code      50
#
# Doing this allows consistent formatting and easy piping to log files in future :)
# It also allows us to keep a lot of debug statements in the code that are ignored 
# at runtime when by default we only print messages with levels of VALUES or higher. 
# This can be changed with the command line argument debuglevel = [debug/value/...]
# which will then print only statements of that debug level or higher

logging.VALUES = 15
logging.addLevelName(logging.VALUES, "VALUES")
logging.Logger._critical = logging.Logger.critical

def value(self, attrname, attrval, location_set, *args, **kws):
    # Args: attrname, attrval, location_set
    # Yes, logger takes its '*args' as 'args'.
    valattrs = {
        "attrname":attrname,
        "attrval":attrval,
        "location_set":location_set
        }
    try:
        kws["extra"].update(valattrs)
    except KeyError as e:
        kws["extra"]=valattrs

    if self.isEnabledFor(logging.VALUES):
        self._log(logging.VALUES, "", args, **kws) 


def critical_with_exit(self, *args, **kwargs):
    logging.Logger._critical(self, *args, **kwargs)
    sys.exit(-1)

logging.Logger.value = value
logging.Logger.critical = critical_with_exit

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
            logging.WARNING:  "  \033[93m WARNING:\033[0m {msg}",
            logging.ERROR:  "  \033[91m ERROR:\033[0m {msg}",
            logging.CRITICAL:  "  \033[91m CRITICAL ERROR:\033[0m {msg}",
            logging.INFO:  "> {msg}",
            logging.DEBUG:  "  \033[94m DEBUG:\033[0m {msg}",
            logging.VALUES:  "\033[92mValue set:\033[0m {location_set:20} {attrname:<15} : {attrval}",
            }

    def format(self, record):
        print_data = {}
        for attr in dir(record):
            try:
                print_data[attr] = os.path.relpath(getattr(record,attr))
            except (AttributeError,ValueError,TypeError) as e:
                print_data[attr] = getattr(record,attr)
        try:
            return MyFormatter.format_strs[record.levelno].format(**print_data)
        except KeyError as e:
            return self._fmt.format(record)
