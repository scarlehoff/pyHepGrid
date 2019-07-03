""" Interface for different runtime programs.

Includes all functions which programs need to define for full functionality.
In certain cases, useful defaults have been included as an example.

These functions are overcomplete, i.e. not all are required to run pyHepGrid ok.
This should be fixed in future. The main functions required are:
init_production()
init_warmup()
"""
from pyHepGrid.src.header import logger, local_run_directory, lfn_warmup_dir
import sys
import os


class ProgramInterface(object):
    def _check_warmup(self, runcard, continue_warmup=False):
        logger.critical("{0} not implemented".format(sys._getframe().f_code.co_name))

    def set_overwrite_warmup(self):
        logger.critical("{0} not implemented".format(sys._getframe().f_code.co_name))

    # Checks for the grid storage system
    def _checkfor_existing_warmup(self, r, rname):

        logger.info("Checking whether this runcard is already at lfn:warmup")
        checkname = self.warmup_name(r, rname)
        if self.gridw.checkForThis(checkname, lfn_warmup_dir):
            self._press_yes_to_continue("File {1} already exists at lfn:{0}, do you want to remove it?".format(lfn_warmup_dir, checkname))
            self.gridw.delete(checkname, lfn_warmup_dir)

    def _checkfor_existing_output(self, r, rname):
        logger.critical("{0} not implemented".format(sys._getframe().f_code.co_name))

    def get_grid_from_stdout(self, jobid, jobinfo):
        logger.critical("{0} not implemented".format(sys._getframe().f_code.co_name))

    # Initialisation functions
    def get_local_dir_name(self, runcard, tag):
        # Suitable defaults
        runname = "{0}-{1}".format(runcard, tag)
        dir_name = os.path.join(local_run_directory, runname)
        logger.info("Run directory: {0}".format(dir_name))
        return dir_name

    def get_stdout_dir_name(self, run_dir):
        logger.critical("{0} not implemented".format(sys._getframe().f_code.co_name))

    def init_single_local_warmup(self, runcard, tag, continue_warmup=False,
                                 provided_warmup=False):
        logger.critical("{0} not implemented".format(sys._getframe().f_code.co_name))

    def init_local_warmups(self, provided_warmup=None,
                           continue_warmup=False, local=False):
        logger.critical("{0} not implemented".format(sys._getframe().f_code.co_name))

    def init_warmup(self, provided_warmup=None,
                    continue_warmup=False, local=False):
        logger.critical("{0} not implemented".format(sys._getframe().f_code.co_name))

    def init_local_production(self, provided_warmup=None, local=False):
        logger.critical("{0} not implemented".format(sys._getframe().f_code.co_name))

    def init_production(self, provided_warmup=None,
                        continue_warmup=False, local=False):
        logger.critical("{0} not implemented".format(sys._getframe().f_code.co_name))

    def get_local_warmup_name(self, matchname, provided_warmup):
        logger.critical("{0} not implemented".format(sys._getframe().f_code.co_name))

    def check_warmup_files(self, db_id, rcard, resubmit=False):
        logger.critical("{0} not implemented".format(sys._getframe().f_code.co_name))

    # A couple of defaults
    def warmup_name(self, runcard, rname):
        out = "output{0}-warm-{1}.tar.gz".format(runcard, rname)
        return out

    def output_name(self, runcard, rname, seed):
        out = "output{0}-{1}-{2}.tar.gz".format(runcard, rname, seed)
        return out
