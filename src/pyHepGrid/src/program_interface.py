""" Interface for different runtime programs.

Includes all functions which programs need to define for full functionality.
In certain cases, useful defaults have been included as an example.

These functions are overcomplete, i.e. not all are required to run pyHepGrid ok.
This should be fixed in future. The main functions required are:
init_production()
init_warmup()
"""
from pyHepGrid.src.header import logger, local_run_directory, grid_warmup_dir
import sys
import os


class ProgramInterface(object):
    # Add the possibility of massaging a list of arguments
    def include_arguments(self, argument_dict):
        return argument_dict
    # Add production arguments (by default, defer to generic include_arguments)

    def include_production_arguments(self, argument_dict):
        return self.include_arguments(argument_dict)
    # Add warmup arguments (by default, defer to generic include_arguments)

    def include_warmup_arguments(self, argument_dict):
        return self.include_arguments(argument_dict)

    # Checks for the grid storage system
    def get_grid_from_stdout(self, jobid, jobinfo):
        raise NotImplementedError("{0} not implemented for {1}".format(
            sys._getframe().f_code.co_name, self.__class__.__bases__[0]))

    # Initialisation functions
    def init_single_local_warmup(self, runcard, tag, continue_warmup=False,
                                 provided_warmup=False):
        raise NotImplementedError("{0} not implemented for {1}".format(
            sys._getframe().f_code.co_name, self.__class__.__bases__[0]))

    def init_single_local_production(self, runcard, tag, provided_warmup=False):
        raise NotImplementedError("{0} not implemented for {1}".format(
            sys._getframe().f_code.co_name, self.__class__.__bases__[0]))

    def init_warmup(self, provided_warmup=None,
                    continue_warmup=False, local=False):
        raise NotImplementedError("{0} not implemented for {1}".format(
            sys._getframe().f_code.co_name, self.__class__.__bases__[0]))

    def init_production(self, provided_warmup=None,
                        continue_warmup=False, local=False):
        raise NotImplementedError("{0} not implemented for {1}".format(
            sys._getframe().f_code.co_name, self.__class__.__bases__[0]))

    def check_warmup_files(self, db_id, rcard, resubmit=False):
        raise NotImplementedError("{0} not implemented for {1}".format(
            sys._getframe().f_code.co_name, self.__class__.__bases__[0]))

    # A couple of defaults
    def warmup_name(self, runcard, rname):
        out = F"output{runcard}-warm-{rname}.tar.gz"
        return out

    def warmup_sockets_dirname(self, runcard, rname):
        out = F"output{runcard}-warm-{rname}"
        return out

    def output_name(self, runcard, rname, seed):
        out = F"output{runcard}-{rname}-{seed}.tar.gz"
        return out

    def get_local_dir_name(self, runcard, tag):
        # Suitable defaults
        runname = F"{runcard}-{tag}"
        dir_name = os.path.join(local_run_directory, runname)
        logger.info(F"Run directory: {dir_name}")
        return dir_name

    def get_stdout_dir_name(self, run_dir):
        return os.path.join(run_dir, "stdout/")

    def _exe_fullpath(self, executable_src_dir, executable_exe):
        return os.path.join(executable_src_dir, executable_exe)

    def set_overwrite_warmup(self):
        self.overwrite_warmup = True

    def check_for_existing_warmup(self, r, rname):
        logger.info(
            "Checking for prior warmup output which this warmup run will "
            "overwrite...")
        checkname = self.warmup_name(r, rname)
        if self.gridw.checkForThis(checkname, grid_warmup_dir):
            self._press_yes_to_continue(
                F"Prior warmup output file {checkname} already exists at "
                "gfal:~/{grid_warmup_dir}. Do you want to remove it?")
            self.gridw.delete(checkname, grid_warmup_dir)
        else:
            logger.info("None found.")

        logger.info("Checking for prior socket warmup backups which this warmup"
                    " run will overwrite...")
        checkname = self.warmup_sockets_dirname(r, rname)
        if self.gridw.checkForThis(checkname, grid_warmup_dir):
            self._press_yes_to_continue(
                F"Prior socketed warmup backups {checkname} exist at "
                "gfal:~/{grid_warmup_dir}. Do you want to remove the directory "
                "and its contents?")
            self.gridw.delete_directory(checkname, grid_warmup_dir)
        else:
            logger.info("None found.")

    def init_local_warmups(self, provided_warmup=None, continue_warmup=False,
                           local=False):
        rncards, dCards = util.expandCard()
        for runcard in rncards:
            self.init_single_local_warmup(runcard, dCards[runcard],
                                          provided_warmup=provided_warmup,
                                          continue_warmup=continue_warmup)

    def init_local_production(self, provided_warmup=None, local=False):
        rncards, dCards = util.expandCard()
        for runcard in rncards:
            self.init_single_local_production(runcard, dCards[runcard],
                                              provided_warmup=provided_warmup)

    # Checks for the grid storage system
    def check_for_existing_output(self, r, rname):
        """
        Check whether given runcard already has output in the grid needs testing
        as it needs to be able to remove (many) things for production run It
        relies on the base seed from the src.header file to remove the output
        """
        from pyHepGrid.src.header import grid_output_dir, logger
        logger.debug(F"Checking whether runcard {rname} has output for seeds "
                     "that you are trying to submit...")
        checkname = r + "-" + rname
        files = self.gridw.get_dir_contents(grid_output_dir)
        first = True
        if checkname in files:
            from pyHepGrid.src.header import baseSeed, producRun
            for seed in range(baseSeed, baseSeed + producRun):
                filename = self.output_name(r, rname, seed)
                if filename in files:
                    if first:
                        self._press_yes_to_continue(
                            "This runcard already has at least one output file "
                            "at gfal:output with a seed you are trying to "
                            F"submit (looked for '{checkname}').\n"
                            "If you continue, it will be removed now.")
                        logger.warning(
                            F"Runcard {r} has at least one file at output")
                        first = False
                    self.gridw.delete(filename, grid_output_dir)
            logger.info("Output check complete")

    # helper functions
    def _file_exists(self, file, logger):
        import os.path
        if not os.path.exists(file):
            logger.critical(
                "File {0} required for initialisation.".format(file))

    def _press_yes_to_continue(self, msg, error=None, fallback=None):
        """
        Press 'y' to continue, or 'n' to either return "fallback" (if set),
        or exit the program with error code -1 (if not).
        """
        if self.assume_yes:
            return 0
        if msg is not None:
            print(msg)
        yn = input("Do you want to continue? (y/n) ").lower()
        if yn.startswith("y"):
            return 0
        else:
            if fallback:
                return fallback
            if error:
                raise Exception(error)
            else:
                sys.exit(-1)
