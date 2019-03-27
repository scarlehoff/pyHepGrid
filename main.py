#!/usr/bin/env python3.6

##### Call argument parser
from src.argument_parser import arguments as args

from src.header import arctable, diractable, arcprodtable, slurmprodtable, \
    dbname, dbfields, logger, slurmtable
from src.dbapi  import database
from src.modes import do_proxy, do_run, do_initialise, do_test, do_management


if __name__ == "__main__":
    modes = {"pro":do_proxy, "run":do_run, "ini":do_initialise,
             "tes":do_test, "man":do_management}
    rcard = args.runcard
    rmode = args.mode

    # Checks on modes
    db = database(dbname, tables = [arctable, diractable, slurmtable,
                                    arcprodtable,slurmprodtable], 
                  fields = dbfields)

    mode = rmode[:3]

    if mode in modes.keys():
        modes[mode](args,rcard)
    else:
        logger.critical("Invalid mode {0} selected. Exiting".format(rmode))
