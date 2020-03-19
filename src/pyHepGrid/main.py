#!/usr/bin/env python3

# Call argument parser
from pyHepGrid.src.argument_parser import arguments as args
from pyHepGrid.src.header import arctable, diractable, arcprodtable, \
    slurmprodtable, dbname, dbfields, logger, slurmtable
from pyHepGrid.src.dbapi import database
from pyHepGrid.src.modes import do_proxy, do_run, do_initialise, do_test, \
    do_management


def main():
    # import os
    # os.chdir(os.path.dirname(__file__))
    modes = {"pro": do_proxy,
             "run": do_run,
             "ini": do_initialise,
             "tes": do_test,
             "man": do_management}

    rcard = args.runcard
    rmode = args.mode.lower()

    # Checks on modes
    db = database(dbname, tables=[arctable, diractable, slurmtable,
                                  arcprodtable, slurmprodtable],
                  fields=dbfields, logger=logger)

    mode = rmode[:3]

    if mode in modes.keys():
        modes[mode](args, rcard)
    else:
        logger.critical("Invalid mode {0} selected. Exiting".format(rmode))
