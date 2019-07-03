from src.header import logger
import sys

class ProgramInterface(object):
    def init_single_local_production(self, runcard, tag, provided_warmup=False):
        logger.critical("{0} not implemented".format(sys._getframe().f_code.co_name))

    def _check_warmup(self, runcard, continue_warmup=False):
        logger.critical("{0} not implemented".format(sys._getframe().f_code.co_name))

    def set_overwrite_warmup(self):
        logger.critical("{0} not implemented".format(sys._getframe().f_code.co_name))

    # Checks for the grid storage system
    def _checkfor_existing_warmup(self, r, rname):
        logger.critical("{0} not implemented".format(sys._getframe().f_code.co_name))

    def _checkfor_existing_output(self, r, rname):
        logger.critical("{0} not implemented".format(sys._getframe().f_code.co_name))

    def _bring_warmup_files(self, runcard, rname, shell=False, check_only=False):
        logger.critical("{0} not implemented".format(sys._getframe().f_code.co_name))

    def get_grid_from_stdout(self,jobid, jobinfo):
        logger.critical("{0} not implemented".format(sys._getframe().f_code.co_name))

    ### Initialisation functions
    def get_local_dir_name(self,runcard, tag):
        logger.critical("{0} not implemented".format(sys._getframe().f_code.co_name))

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
    def init_single_local_production(self, runcard, tag, provided_warmup=False):
        logger.critical("{0} not implemented".format(sys._getframe().f_code.co_name))

    def init_production(self, provided_warmup=None, 
                        continue_warmup=False, local=False):
        logger.critical("{0} not implemented".format(sys._getframe().f_code.co_name))

    def get_local_warmup_name(self, matchname, provided_warmup):
        logger.critical("{0} not implemented".format(sys._getframe().f_code.co_name))

    def check_warmup_files(self, db_id, rcard, resubmit=False):
        logger.critical("{0} not implemented".format(sys._getframe().f_code.co_name))
