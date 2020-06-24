import os
import pyHepGrid.src.utilities as util
import pyHepGrid.src.header as header
from pyHepGrid.src.header import logger

from pyHepGrid.src.ProgramInterface import ProgramInterface


class ExampleProgram(ProgramInterface):
    # For this simple example we only implement the "Production" mode for
    # Arc/Dirac, if you need anything else please implement the corresponding
    # function(s) from the `ProgramInterface`

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
        from pyHepGrid.src.header import executable_exe, executable_src_dir, \
            grid_input_dir

        if local:
            self.init_local_production(provided_warmup=provided_warmup)
            return

        runFolders, dCards = util.expandCard()
        path_to_exe_full = self._exe_fullpath(
            executable_src_dir, executable_exe)

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
            logger.critical("Retrieving warmup file from grid: Not implemented")

        os.chdir(tmpdir)
        logger.debug("Temporary directory: {0}".format(tmpdir))

        if not os.path.isfile(path_to_exe_full):
            logger.critical(
                "Could not find executable at {0}".format(path_to_exe_full))
        else:
            tar_name = os.path.basename(header.grid_executable)
            grid_exe_dir = os.path.dirname(header.grid_executable)
            exe_name = header.executable_exe
            os.system("cp -r " + path_to_exe_full + " " + exe_name)
            upload_exe = True
            if self.gridw.checkForThis(tar_name, grid_exe_dir):
                if not self._press_yes_to_continue(
                        "Old executable found. Do you want to remove it?",
                        fallback=1):
                    logger.info(
                        F"Removing old version of {tar_name} from Grid Storage")
                    self.gridw.delete(tar_name, grid_exe_dir)
                else:
                    upload_exe = False
            if upload_exe:
                self.tarw.tarFiles([exe_name], tar_name)
                self.gridw.send(tar_name, grid_exe_dir)

        for idx, runName in enumerate(runFolders):
            local = False

            tarfile = runName + "+" + dCards[runName] + ".tar.gz"
            base_folder = runName.split("-")[0]
            logger.info(
                "Initialising {0} to {1} [{2}/{3}]".format(
                    runName, tarfile, idx + 1, len(runFolders)))

            # runcards
            run_dir = os.path.join(runFol, base_folder)
            runFiles = dCards[runName].split("+")
            for f in runFiles:
                f = os.path.join(run_dir, f)
                self._file_exists(f, logger)
                os.system("cp -r " + f + " " + tmpdir)

            # warmup files
            for f in self._WARMUP_FILES:
                f = os.path.join(warmup_base, base_folder, f)
                self._file_exists(f, logger)
                os.system("cp -r " + f + " " + tmpdir)

            # tar up & send to grid storage
            self.tarw.tarFiles(self._WARMUP_FILES + runFiles, tarfile)

            if self.gridw.checkForThis(tarfile, grid_input_dir):
                logger.info(
                    "Removing old version of {0} from Grid Storage".format(
                        tarfile))
                self.gridw.delete(tarfile, grid_input_dir)
            logger.info("Sending {0} to {1}".format(tarfile, grid_input_dir))
            self.gridw.send(tarfile, grid_input_dir)

        # clean up afterwards
        os.chdir(origdir)
        os.system("rm -r " + tmpdir)

    def include_arguments(self, argument_dict):
        # Pass custom argument to run script
        argument_dict["executable_location"] = header.grid_executable
        return argument_dict
