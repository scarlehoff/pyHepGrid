#!/usr/bin/env python3.4

##### Call argument parser
from src.argument_parser import arguments as args

from src.header import arctable, diractable, dbname, dbfields, logger
from src.dbapi  import database
from src.modes import do_proxy, do_run, do_initialise, do_test, do_management


if __name__ == "__main__":
    modes = {"pro":do_proxy, "run":do_run, "ini":do_initialise,
             "tes":do_test, "man":do_management}
    rcard = args.runcard
    rmode = args.mode

    # Checks on modes
    db = database(dbname, tables = [arctable, diractable], fields = dbfields)

    mode = rmode[:3]
    try:
        modes[mode](args,rcard)
    except KeyError as e:
        logger.critical("Invalid mode {0} selected. Exiting".format(rmode))
