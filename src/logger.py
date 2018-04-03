import logging
import sys
import os

logging.VALUES = 15
logging.addLevelName(logging.VALUES, "VALUES")

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

logging.Logger.value = value

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
