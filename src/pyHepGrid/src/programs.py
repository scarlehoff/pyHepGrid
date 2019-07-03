import os
import sys
from pyHepGrid.src.header import logger

import pyHepGrid.src.utilities as util
import pyHepGrid.src.header as header
import pyHepGrid.src.runmodes
from pyHepGrid.src.runcard_parsing import PROGRAMruncard
from pyHepGrid.src.program_interface import ProgramInterface

class NNLOJET(ProgramInterface):
    def warmup_name(self, runcard, rname):
        out = "output" + runcard + "-warm-" + rname + ".tar.gz"
        return out

    def output_name(self, runcard, rname, seed):
        out = "output" + runcard + "-" + rname + "-" + str(seed) + ".tar.gz"
        return out


# Checks for the runcard
    def _check_production(self, runcard):
        logger.info("Checking production in runcard {0}".format(runcard.name))
        if runcard.is_warmup():
            self._press_yes_to_continue("Warmup is active in runcard")
        if not runcard.is_production():
            self._press_yes_to_continue("Production is not active in runcard")

    def _check_warmup(self, runcard, continue_warmup = False):
        logger.info("Checking warmup in runcard {0}".format(runcard.name))
        if not runcard.is_warmup():
            self._press_yes_to_continue("Warmup is not active in runcard")
        if continue_warmup and not runcard.is_continuation():
            self._press_yes_to_continue("Continue warmup is not active in runcard")
        if runcard.is_production():
            self._press_yes_to_continue("Production is active in runcard")

    def set_overwrite_warmup(self):
        self.overwrite_warmup = True

    # Checks for the grid storage system
    def _checkfor_existing_warmup(self, r, rname):
        """ Check whether given runcard already has a warmup output in the grid """
        logger.info("Checking whether this runcard is already at lfn:warmup")
        checkname = self.warmup_name(r, rname)
        if self.gridw.checkForThis(checkname, header.lfn_warmup_dir):
            self._press_yes_to_continue("File {1} already exists at lfn:{0}, do you want to remove it?".format(header.lfn_warmup_dir,checkname))
            self.gridw.delete(checkname, header.lfn_warmup_dir)

    def _checkfor_existing_output(self, r, rname):
        """ Check whether given runcard already has output in the grid
        needs testing as it needs to be able to remove (many) things for production run
        It relies on the base seed from the src.header file to remove the output
        """
        from pyHepGrid.src.header import lfn_output_dir, logger
        logger.info("Checking whether runcard {0} has output for seeds that you are trying to submit...".format(rname))
        checkname = r + "-" + rname
        files = self.gridw.get_dir_contents(lfn_output_dir)
        first = True
        if checkname in files:
            from pyHepGrid.src.header import baseSeed, producRun
            for seed in range(baseSeed, baseSeed + producRun):
                filename = "output" + checkname + "-" + str(seed) + ".tar.gz"
                if filename in files:
                    if first:
                        self._press_yes_to_continue("It seems this runcard already has at least one file at lfn:output with a seed you are trying to submit (looked for {}). Do you want to remove it/them?".format(checkname))
                        logger.warning("Runcard " + r + " has at least one file at output")
                        first = False
                    self.gridw.delete(filename, lfn_output_dir)
            logger.info("Output check complete")


    def _checkfor_existing_output_local(self, r, rname, baseSeed, producRun):
        """ Check whether given runcard already has output in the local run dir (looks for log files)
        """
        import re
        logger.info("Checking whether runcard {0} has output for seeds that you are trying to submit...".format(rname))
        local_dir_name = self.get_local_dir_name(r,rname)
        files = os.listdir(local_dir_name)
        runcard = PROGRAMruncard(runcard_file=os.path.join(local_dir_name,r),logger=logger,
                                 grid_run=False)
        runcard_id = runcard.runcard_dict_case_preserving["id"]
        logs = [f for f in files if f.endswith(".log") and runcard_id in f]
        logseed_regex = re.compile(r".s([0-9]+)\.[^\.]+$")
        existing_seeds = set([int(logseed_regex.search(i).group(1)) for i
                              in logs])
        submission_seeds = set(range(baseSeed,baseSeed+producRun))
        overlap = existing_seeds.intersection(submission_seeds)
        if overlap:
            logger.warning("Log files for seed(s) {0} already exist in run folders. Running will overwrite the logfiles already present.".format(" ".join(str(i) for i in overlap)))
            self._press_yes_to_continue(None)
        return


    def _bring_warmup_files(self, runcard, rname, shell = False, check_only = False):
        """ Download the warmup file for a run to local directory
        extracts Vegas grid and log file and returns a list with their names

        check_only flag doesn't error out if the warmup doesn't exist, instead just returns
        and empty list for later use [intended for checkwarmup mode so multiple warmups can
        be checked consecutively.
        """
        from pyHepGrid.src.header import lfn_warmup_dir
        gridFiles = []
        suppress_errors = False
        if check_only:
            suppress_errors = True
        ## First bring the warmup .tar.gz
        outnm = self.warmup_name(runcard, rname)
        logger.debug("Warmup LFN name: {0}".format(outnm))
        tmpnm = "tmp.tar.gz"
        logger.debug("local tmp tar name: {0}".format(tmpnm))
        success = self.gridw.bring(outnm, lfn_warmup_dir, tmpnm, shell = shell,
                                   suppress_errors=suppress_errors)

        if not success and not check_only:
            logger.critical("Grid files failed to copy from the LFN. Did the warmup complete successfully?")
        elif not success:
            return []

        ## Now extract only the Vegas grid files and log file
        gridp = [".RRa", ".RRb", ".vRa", ".vRb", ".vBa", ".vBb",
                 ".V", ".R", ".LO", ".RV", ".VV"]
        gridp += [i+"_channel" for i in gridp]
        extractFiles = self.tarw.extract_extensions(tmpnm, gridp+[".log",".txt","channels"])
        try:
            gridFiles = [i for i in extractFiles if ".log" not in i]
            logfile = [i for i in extractFiles if ".log" in i][0]
        except IndexError as e:
            if not check_only:
                logger.critical("Logfile not found. Did the warmup complete successfully?")
            else:
                return []
        if gridFiles == [] and not check_only: # No grid files found
            logger.critical("Grid files not found in warmup tarfile. Did the warmup complete successfully?")
        elif gridFiles == []:
            return []

        ## Tag log file as -warmup
        newlog = logfile + "-warmup"
        os.rename(logfile, newlog)
        # Remove temporary tar files
        os.remove(tmpnm)
        gridFiles.append(newlog)
        # Make sure access to the file is correct!
        for i in gridFiles:
            util.spCall(["chmod", "a+wrx", i])
        return gridFiles


    def get_grid_from_stdout(self,jobid, jobinfo):
        from pyHepGrid.src.header import warmup_base_dir, default_runfolder
        import re, os

        stdout = "\n".join(self.cat_job(jobid, jobinfo, store = True))

        try:
            gridname = [i for i in stdout.split("\n") if "Writing grid" in i][0].split()[-1].strip()
            logger.info("Grid name from stdout: {0}".format(gridname))
        except IndexError as e:
            logger.critical("No grid filename found in stdout logs. Did the warmup write a grid?")

        result = re.search('vegas warmup to stdout(.*)End', stdout,flags=re.S) # Thanks StackOverflow

        try:
            grid = result.group(1)
        except IndexError as e:
            logger.critical("No grid found in stdout logs. Did the warmup write a grid?")

        logger.info("Grid extracted successfully")
        if default_runfolder is None:
            base = header.warmup_base_dir
        else:
            base = header.default_runfolder

        outloc = os.path.join(base,jobinfo["runfolder"],jobinfo["runcard"])
        grid_fname = os.path.join(outloc,gridname)
        os.makedirs(outloc,exist_ok=True)
        if os.path.exists(grid_fname):
            self._press_yes_to_continue("  \033[93m WARNING:\033[0m Grid file already exists at {0}. do you want to overwrite it?".format(grid_fname))
        with open(grid_fname,"w") as gridfile:
            gridfile.write(grid)
        logger.info("Grid written locally to {0}".format(os.path.relpath(grid_fname)))

    ### Initialisation functions
    def get_local_dir_name(self,runcard, tag):
        from pyHepGrid.src.header import local_run_directory
        runname = "{0}-{1}".format(runcard, tag)
        dir_name = os.path.join(local_run_directory,runname)
        logger.info("Run directory: {0}".format(dir_name))
        return dir_name

    def get_stdout_dir_name(self, run_dir):
        return os.path.join(run_dir,"stdout/")

        
    def _exe_fullpath(self, executable_src_dir, executable_exe):
        return os.path.join(executable_src_dir, "driver", executable_exe)


    def init_single_local_warmup(self, runcard, tag, continue_warmup = False,
                                 provided_warmup=False):
        import shutil
        from pyHepGrid.src.header import executable_src_dir, executable_exe, runcardDir, slurm_kill_exe
        run_dir = self.get_local_dir_name(runcard, tag)
        os.makedirs(run_dir,exist_ok=True)
        stdoutdir = self.get_stdout_dir_name(run_dir)
        os.makedirs(stdoutdir,exist_ok=True) # directory for slurm stdout files
        path_to_exe_full = self._exe_fullpath(executable_src_dir, executable_exe)
        shutil.copy(path_to_exe_full, run_dir)
        runcard_file = runcardDir + "/" + runcard
        runcard_obj = PROGRAMruncard(runcard_file, logger=logger, grid_run=False)
        self._check_warmup(runcard_obj, continue_warmup)
        logger.debug("Copying runcard {0} to {1}".format(runcard_file, run_dir))
        shutil.copy(runcard_file, run_dir)
        shutil.copy(slurm_kill_exe, run_dir)
        if provided_warmup:
            # Copy warmup to rundir
            match, local = self.get_local_warmup_name(runcard_obj.warmup_filename(), provided_warmup)
            shutil.copy(match, run_dir)
        if runcard_obj.is_continuation():
            # Assert warmup is present in dir. Will error out if not
            if continue_warmup:
                match, local = self.get_local_warmup_name(runcard_obj.warmup_filename(), run_dir)
            else:
                logger.critical("Continuation set in warmup but not requested at run time")

    def init_local_warmups(self, provided_warmup = None, continue_warmup=False, local=False):
        rncards, dCards = util.expandCard()
        for runcard in rncards:
            self.init_single_local_warmup(runcard, dCards[runcard],
                                          provided_warmup=provided_warmup,
                                          continue_warmup=continue_warmup)

    def init_warmup(self, provided_warmup = None, continue_warmup=False, local=False):
        """ Initialises a warmup run. An warmup file can be provided and it will be
        added to the .tar file sent to the grid storage.
        Steps are:
            1 - tar up executable, runcard and necessary files
            2 - sent it to the grid storage
        """
        from shutil import copy
        import tempfile
        from pyHepGrid.src.header import executable_src_dir, executable_exe, logger
        from pyHepGrid.src.header import runcardDir as runFol

        if local:
            self.init_local_warmups(provided_warmup = provided_warmup,
                                    continue_warmup=continue_warmup)
            return
        origdir = os.path.abspath(os.getcwd())
        tmpdir = tempfile.mkdtemp()

        # if provided warmup is a relative path, ensure we have the full path
        # before we change to the tmp directory
        if provided_warmup:
            if provided_warmup[0] != "/":
                provided_warmup = "{0}/{1}".format(origdir, provided_warmup)

        os.chdir(tmpdir)

        logger.debug("Temporary directory: {0}".format(tmpdir))

        rncards, dCards = util.expandCard()
        path_to_exe_full = self._exe_fullpath(executable_src_dir, executable_exe)
        if not os.path.isfile(path_to_exe_full):
            logger.critical("Could not find executable at {0}".format(path_to_exe_full))
        copy(path_to_exe_full, os.getcwd())
        files = [executable_exe]
        for idx,i in enumerate(rncards):
            logger.info("Initialising {0} [{1}/{2}]".format(i,idx+1,len(rncards)))
            local = False
            warmupFiles = []
            # Check whether warmup/production is active in the runcard
            if not os.path.isfile(runFol + "/" + i):
                self._press_yes_to_continue("Could not find runcard {0}".format(i), error="Could not find runcard")
            runcard_file = runFol + "/" + i
            runcard_obj = PROGRAMruncard(runcard_file, logger=logger)
            self._check_warmup(runcard_obj, continue_warmup)
            if provided_warmup:
                # Copy warmup to current dir if not already there
                match, local = self.get_local_warmup_name(runcard_obj.warmup_filename(), provided_warmup)
                files += [match]
            rname   = dCards[i]
            tarfile = i + rname + ".tar.gz"
            copy(runFol + "/" + i, os.getcwd())
            if self.overwrite_warmup:
                checkname = self.warmup_name(i, rname)
                if self.gridw.checkForThis(checkname, header.lfn_warmup_dir):
                    print("Warmup found in lfn:{0}!".format(header.lfn_warmup_dir))
                    warmup_files = self._bring_warmup_files(i, rname, shell=True)
                    # if not warmup_files: # check now done in bring warmup files
                    #     logger.critical("No warmup grids found in warmup tar!")
                    files += warmup_files
                    print("Warmup files found: {0}".format(" ".join(i for i in warmup_files)))

            self.tarw.tarFiles(files + [i], tarfile)
            if self.gridw.checkForThis(tarfile, "input"): # Could we cache this? Just to speed up ini
                print("Removing old version of " + tarfile + " from Grid Storage")
                self.gridw.delete(tarfile, "input")
            if self.gridw.gfal:
                print("Sending " + tarfile + " to gfal input/")
            else:
                print("Sending " + tarfile + " to lfn input/")
            self.gridw.send(tarfile, "input", shell=True)
            if not local:
                for j in warmupFiles:
                    os.remove(j)
            os.remove(i)
            os.remove(tarfile)
        os.remove(executable_exe)
        os.chdir(origdir)

    def init_local_production(self, provided_warmup = None, local=False):
        rncards, dCards = util.expandCard()
        for runcard in rncards:
            self.init_single_local_production(runcard, dCards[runcard],
                                              provided_warmup=provided_warmup)

    def init_single_local_production(self, runcard, tag, provided_warmup=False):
        """ Initialise single production run for the local environment. Can probably be
        more tightly integrated with the warmup equivalent in future - lots of shared code
        that can be refactored."""
        import shutil
        from pyHepGrid.src.header import executable_src_dir, executable_exe, runcardDir
        run_dir = self.get_local_dir_name(runcard, tag)
        os.makedirs(run_dir,exist_ok=True)
        stdoutdir = self.get_stdout_dir_name(run_dir)
        os.makedirs(stdoutdir,exist_ok=True) # directory for slurm stdout files
        path_to_exe_full = self._exe_fullpath(executable_src_dir, executable_exe)
        shutil.copy(path_to_exe_full, run_dir)
        runcard_file = runcardDir + "/" + runcard
        runcard_obj = PROGRAMruncard(runcard_file, logger=logger)
        self._check_production(runcard_obj)
        logger.debug("Copying runcard {0} to {1}".format(runcard_file, run_dir))
        shutil.copy(runcard_file, run_dir)
        if provided_warmup:
            # Copy warmup to rundir
            match, local = self.get_local_warmup_name(runcard_obj.warmup_filename(), provided_warmup)
            shutil.copy(match, run_dir)
        else:
            # check warmup is in dir - check is case insensitive - be careful!
            rundirfiles = [i.lower() for i in os.listdir(run_dir)]
            if runcard_obj.warmup_filename() not in rundirfiles:
                logger.critical("No warmup found in run folder and no warmup provided manually")

    def init_production(self, provided_warmup = None, continue_warmup=False, local=False):
        """ Initialises a production run. If a warmup file is provided
        retrieval step is skipped
        Steps are:
            0 - Retrieve warmup from the grid/local
            1 - tar up executable, runcard and necessary files
            2 - sent it to the grid storage
        """
        from shutil import copy
        import tempfile
        from pyHepGrid.src.header import runcardDir as runFol
        from pyHepGrid.src.header import executable_exe, executable_src_dir, logger

        if local:
            self.init_local_production(provided_warmup = provided_warmup)
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

        os.chdir(tmpdir)
        logger.debug("Temporary directory: {0}".format(tmpdir))


        if not os.path.isfile(path_to_exe_full):
            logger.critical("Could not find executable at {0}".format(path_to_exe_full))
        copy(path_to_exe_full, os.getcwd())
        files = [executable_exe]
        for idx,i in enumerate(rncards):
            logger.info("Initialising {0} [{1}/{2}]".format(i,idx+1,len(rncards)))
            local = False
            # Check whether warmup/production is active in the runcard
            runcard_file = runFol + "/" + i
            runcard_obj = PROGRAMruncard(runcard_file, logger=logger)
            self._check_production(runcard_obj)
            rname   = dCards[i]
            tarfile = i + rname + ".tar.gz"
            copy(runFol + "/" + i, os.getcwd())
            if provided_warmup:
                match, local = self.get_local_warmup_name(runcard_obj.warmup_filename(),
                                                          provided_warmup)
                warmupFiles = [match]
            elif header.provided_warmup_dir:
                match, local = self.get_local_warmup_name(runcard_obj.warmup_filename(),
                                                          header.provided_warmup_dir)
                warmupFiles = [match]
            else:
                print("Retrieving warmup file from grid")
                warmupFiles = self._bring_warmup_files(i, rname,  shell = True)
            self.tarw.tarFiles(files + [i] +  warmupFiles, tarfile)
            if self.gridw.checkForThis(tarfile, "input"):
                print("Removing old version of " + tarfile + " from Grid Storage")
                self.gridw.delete(tarfile, "input")
            print("Sending " + tarfile + " to lfn:input/")
            self.gridw.send(tarfile, "input", shell = True)
            if local:
                util.spCall(["rm", i, tarfile])
            else:
                util.spCall(["rm", i, tarfile] + warmupFiles)
        os.remove(executable_exe)
        os.chdir(origdir)

    def get_local_warmup_name(self, matchname, provided_warmup):
        from shutil import copy
        from pyHepGrid.src.header import logger
        print(matchname, provided_warmup)
        if os.path.isdir(provided_warmup):
            matches = []
            potential_files = os.listdir(provided_warmup)
            for potfile in potential_files:
                if potfile.lower().startswith(matchname) and\
                        not potfile.endswith(".txt") and not potfile.endswith(".log"):
                    matches.append(potfile)
            if len(matches) > 1:
                logger.critical("Multiple warmup matches found in {1}: {0}".format(" ".join(i for i in matches), provided_warmup))
            elif len(matches) ==0 :
                logger.critical("No warmup matches found in {0}.".format(provided_warmup))
            else:
                match = os.path.join(provided_warmup,matches[0])
        else:
            match = provided_warmup
        print("Using warmup {0}".format(match))
        if not match in os.listdir(sys.path[0]):
            local_match  =False
            copy(match,os.path.basename(match))
            match = os.path.basename(match)
        else:
            local_match = True
        return match, local_match


    def check_warmup_files(self, db_id, rcard, resubmit=False):
        """ Provides stats on whether a warmup file exists for a given run and optionally
        resubmit if absent"""
        from shutil import copy
        import tempfile
        import tarfile
        from pyHepGrid.src.header import logger

        origdir = os.path.abspath(os.getcwd())
        tmpdir = tempfile.mkdtemp()

        os.chdir(tmpdir)
        logger.debug("Temporary directory: {0}".format(tmpdir))
        rncards, dCards = util.expandCard()
        tags = ["runcard", "runfolder"]
        runcard_info = self.dbase.list_data(self.table, tags, db_id)[0]
        runcard = runcard_info["runcard"]
        rname = runcard_info["runfolder"]
        try:
            warmup_files = self._bring_warmup_files(runcard, rname, check_only=True, shell=True)
            if warmup_files == []:
                status = "\033[93mMissing\033[0m"
            else:
                status = "\033[92mPresent\033[0m"
        except tarfile.ReadError as e:
            status = "\033[91mCorrupted\033[0m"
        run_id = "{0}-{1}:".format(runcard, rname)
        logger.info("[{0}] {2:55} {1:>20}".format(db_id, status, run_id))
        os.chdir(origdir)

        if resubmit and "Present" not in status:
            done, wait, run, fail, unk = self.stats_job(db_id, do_print=False)
            if run+wait>0: # Be more careful in case of unknown status
                logger.warning("Job still running. Not resubmitting for now")
            else:
                # Need to override dictCard for single job submission
                expandedCard = ([runcard], {runcard:rname})
                logger.info("Warmup not found and job ended. Resubmitting to ARC")
                from pyHepGrid.src.runArcjob import runWrapper
                runWrapper(rcard, expandedCard = expandedCard)

    def _get_prod_args(self, runcard, runtag, seed):
        """ Returns all arguments for production running. These arguments should
        match all those required by nnlorun.py"""
        base_string = self._make_base_argstring(runcard, runtag)
        production_dict = {'Production' : None,
                        'seed' : seed }

        production_str = self._format_args(production_dict)
        return base_string + production_str

    def _get_warmup_args(self, runcard, runtag, threads = 1, sockets = None, port=header.port):
        """ Returns all necessary arguments for warmup running. These arguments
        should match those required by nnlorun.py."""
        base_string = self._make_base_argstring(runcard, runtag)
        warmup_dict = { 'Warmup' : None,
                        'threads' : threads }
        if sockets:
            warmup_dict['port'] = port
            warmup_dict['Host'] = header.server_host
            warmup_dict['Sockets'] = None

        warmup_str = self._format_args(warmup_dict)
        return base_string + warmup_str


class HEJ(NNLOJET):

    def _exe_fullpath(self, executable_src_dir, executable_exe):
        return os.path.join(executable_src_dir, executable_exe)

    def warmup_name(self, runcard, rname):
        out = runcard + "+" + rname + ".tar.gz"
        return out

    def output_name(self, runcard, rname, seed):
        out = "output-" + runcard + "-" + rname + "-" + str(seed) + ".tar.gz"
        return out

    def init_production(self, provided_warmup = None, continue_warmup=False, 
                        local=False):
        """ Initialises a production run. If a warmup file is provided
        retrieval step is skipped
        Steps are:
            0 - Retrieve warmup from the grid/local
            1 - tar up executable, runcard and necessary files
            2 - sent it to the grid storage
        """
        import tempfile
        from pyHepGrid.src.header import runcardDir as runFol
        from pyHepGrid.src.header import executable_exe, executable_src_dir, lfn_input_dir

        if local:
            self.init_local_production(provided_warmup = provided_warmup)
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

        os.chdir(tmpdir)
        logger.debug("Temporary directory: {0}".format(tmpdir))

        # if not os.path.isfile(path_to_exe_full):
        #     logger.critical("Could not find executable at {0}".format(path_to_exe_full))
        # copy(path_to_exe_full, os.getcwd())
        # files = [executable_exe]
        for idx,i in enumerate(rncards):
            local = False

            tarfile = i +"+"+ dCards[i] + ".tar.gz"
            base_folder = i.split("-")[0] + "/"
            logger.info("Initialising {0} to {1} [{2}/{3}]".format(i,tarfile,idx+1,len(rncards)))

            # runcards
            run_dir = runFol + base_folder
            runFiles = [dCards[i]+".yml"]
            for f in runFiles:
                os.system("cp -r "+run_dir+f+" "+tmpdir)

            # warmup files
            if provided_warmup:
                warmup_dir = provided_warmup + base_folder
            elif header.provided_warmup_dir:
                warmup_dir = header.provided_warmup_dir + base_folder
            else:
                print("Retrieving warmup file from grid")
                warmupFiles = self._bring_warmup_files(i, dCards[i], shell = True)
            warmupFiles = ["Process","Run.dat","Results.db"]
            for f in warmupFiles:
                os.system("cp -r "+warmup_dir+f+" "+tmpdir)

            # tar up & send to grid storage
            self.tarw.tarFiles(warmupFiles+runFiles, tarfile)

            if self.gridw.checkForThis(tarfile, lfn_input_dir):
                print("Removing old version of " + tarfile + " from Grid Storage")
                self.gridw.delete(tarfile, lfn_input_dir)
            print("Sending " + tarfile + " to "+lfn_input_dir)
            self.gridw.send(tarfile, lfn_input_dir, shell = True)

        # clean up afterwards
        os.chdir(origdir)
        os.system("rm -r "+tmpdir)


    def get_local_warmup_name(self, matchname, provided_warmup):
        from shutil import copy
        exclude_patterns = [".txt",".log",".tex",".lhe",".bak",".yoda"]
        print(matchname, provided_warmup)
        if os.path.isdir(provided_warmup):
            matches = []
            potential_files = os.listdir(provided_warmup)
            for potfile in potential_files:
                if potfile.lower().startswith(matchname) \
                    and not any(potfile.endswith(p) for p in exclude_patterns):
                    matches.append(potfile)
            if len(matches) > 1:
                logger.critical("Multiple warmup matches found in {1}: {0}".format(" ".join(i for i in matches), provided_warmup))
            elif len(matches) ==0 :
                logger.critical("No warmup matches found in {0}.".format(provided_warmup))
            else:
                match = os.path.join(provided_warmup,matches[0])
        else:
            match = provided_warmup
        print("Using warmup {0}".format(match))
        if not match in os.listdir(sys.path[0]):
            local_match = False
            copy(match,os.path.basename(match))
            match = os.path.basename(match)
        else:
            local_match = True
        return match, local_match
