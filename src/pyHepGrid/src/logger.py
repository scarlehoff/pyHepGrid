import logging
import sys
import os

###################################### README #########################################
# Use:
# For all print/error/warning statements, import logger from pyHepGrid.src.header
#
#             LOG FUNCTION     PURPOSE                                         LEVEL
# Then use -> logger.debug     debug statement                                   10
#             logger.value     value setting in pyHepGrid.src.header                       15
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

class LevelFilter(logging.Filter):
    """ Anything with level below level will go through """
    def __init__(self, level):
        self.level = level
    def filter(self, record):
        return record.levelno < self.level

def setup_logger(debuglevel):

    logger = logging.getLogger(__name__)
    logger.setLevel(debuglevel)

    # Set up formatter
    formatter = MyFormatter()

    # Decide what goes into stderr and what into stdout
    # In principle from WARNING onwards goes to stderr
    # atm regardless of debuglevel
    partitioner = LevelFilter(logging.WARNING)

    # Add custom level for value initialisation
    out_handler = logging.StreamHandler(sys.stdout)
    out_handler.setFormatter(formatter)
    out_handler.addFilter( partitioner )

    err_handler = logging.StreamHandler(sys.stderr)
    err_handler.setFormatter( formatter )
    err_handler.setLevel( logging.WARNING )

    logger.addHandler(err_handler)
    logger.addHandler(out_handler)

    return logger

class MyFormatter(logging.Formatter):

    format_strs  ={
            logging.WARNING:  "  \033[93m WARNING:\033[0m {msg}",
            logging.ERROR:  "  \033[91m ERROR:\033[0m {msg}",
            logging.CRITICAL:  "  \033[91m CRITICAL:\033[0m {msg}",
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
