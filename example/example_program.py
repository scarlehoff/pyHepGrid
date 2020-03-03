import os
import sys
import pyHepGrid.src.utilities as util
import pyHepGrid.src.header as header
from pyHepGrid.src.header import logger

from pyHepGrid.src.program_interface import ProgramInterface


class ProgramClass(ProgramInterface):

    # list of 'warmup' (resource) files to include in tarball
    _WARMUP_FILES = []

    def init_production(self, provided_warmup=None, continue_warmup=False,
                        local=False):
        """
        Initialises a production run. If a warmup file is provided
        retrieval step is skipped/
        Steps are:
            0 - Retrieve warmup from the grid/local
            1 - tar up executable, runcard and necessary files
            2 - sent it to the grid storage
        """
        import tempfile
        from pyHepGrid.src.header import runcardDir as runFol
        from pyHepGrid.src.header import executable_exe, executable_src_dir, grid_input_dir

        if local:
            self.init_local_production(provided_warmup=provided_warmup)
            return

        rncards, dCards = util.expandCard()
        path_to_exe_full = self._exe_fullpath(executable_src_dir, executable_exe)

        origdir = os.path.abspath(os.getcwd())
        tmpdir = tempfile.mkdtemp()

        # if provided warmup is a relative path, ensure we have the full path
        # before we change to the tmp directory
        if provided_warmup:
            if provided_warmup[0] != "/":
                provided_warmup = "{0}/{1}".format(origdir, provided_warmup)

        if provided_warmup:
            warmup_base = provided_warmup
        elif header.provided_warmup_dir:
            warmup_base = header.provided_warmup_dir
        else:
            # print("Retrieving warmup file from grid")
            # warmupFiles = self._bring_warmup_files(i, dCards[i], shell=True)
            logger.critical("Retrieving warmup file from grid: Not implemented")

        os.chdir(tmpdir)
        logger.debug("Temporary directory: {0}".format(tmpdir))

        # if not os.path.isfile(path_to_exe_full):
        #     logger.critical("Could not find executable at {0}".format(path_to_exe_full))
        # copy(path_to_exe_full, os.getcwd())
        # files = [executable_exe]
        for idx, i in enumerate(rncards):
            local = False

            tarfile = i + "+" + dCards[i] + ".tar.gz"
            base_folder = i.split("-")[0] + "/"
            logger.info("Initialising {0} to {1} [{2}/{3}]".format(i, tarfile, idx + 1, len(rncards)))

            # runcards
            run_dir = runFol + base_folder
            runFiles = dCards[i].split("+")
            print(runFiles)
            for f in runFiles:
                f = run_dir + f
                self._file_exists(f, logger)
                os.system("cp -r " + f + " " + tmpdir)

            # warmup files
            for f in self._WARMUP_FILES:
                f = warmup_base + base_folder + f
                self._file_exists(f, logger)
                os.system("cp -r " + f + " " + tmpdir)

            # tar up & send to grid storage
            self.tarw.tarFiles(self._WARMUP_FILES + runFiles, tarfile)

            if self.gridw.checkForThis(tarfile, grid_input_dir):
                logger.info("Removing old version of {0} from Grid Storage".format(tarfile))
                self.gridw.delete(tarfile, grid_input_dir)
            logger.info("Sending {0} to {1}".format(tarfile, grid_input_dir))
            self.gridw.send(tarfile, grid_input_dir, shell=True)

        # clean up afterwards
        os.chdir(origdir)
        os.system("rm -r " + tmpdir)
