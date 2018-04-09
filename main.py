#!/usr/bin/env python3.4

##### Call argument parser
from src.argument_parser import arguments as args
rcard = args.runcard
rmode = args.mode

from src.header import arctable, diractable, dbname, dbfields, logger
from src.dbapi  import database
from src.modes import do_proxy, do_run, do_initialise, do_test, do_management


if __name__ == "__main__":
    modes = {"pro":do_proxy, "run":do_run, "ini":do_initialise,
             "tes":do_test, "man":do_management}

    # Checks on modes
    if len(rmode) < 3:
        logger.critical("Mode ", rmode, " not valid")

    if (rmode[:3] == "run" and not "runcard" in rmode) or rmode[:3] == "man" :
        if args.runDirac and args.runArc:
            if not args.idjob == "all":
                logger.critical("Please choose only Dirac (-D) or Arc (-A) or Arc Production Mode (-B) (unless using -j all)")
        if not args.runDirac and not args.runArc and not args.runArcProduction:
            logger.critical("Please choose either Dirac (-D) or Arc (-A) or Arc Production Mode (-B)")
    db = database(dbname, tables = [arctable, diractable], fields = dbfields)

    mode = rmode[:3]
    try:
        modes[mode](args,rcard)
    except KeyError as e:
        logger.critical("Invalid mode {0} selected. Exiting".format(rmode))
