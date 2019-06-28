import os
import sys
from src.header import logger

import src.utilities as util
import src.header as header

counter = None
def init_counter(args):
    global counter
    counter = args

class Backend(object):
    """ Abstract class
    """
    cDONE = 1
    cWAIT = 0
    cFAIL = -1
    cRUN  = 2
    cUNK  = 99

    ### IMPORTANT: NAMING FUNCTIONS SHOULD BE THE SAME IN ARC.py AND DIRAC.py
    def warmup_name(self, runcard, rname):
        out = "output" + runcard + "-warm-" + rname + ".tar.gz"
        return out

    def output_name(self, runcard, rname, seed):
        out = "output" + runcard + "-" + rname + "-" + str(seed) + ".tar.gz"
        return out

    def output_name_array(self, runcard, rname, seeds):
        return [self.output_name(runcard, rname, seed) for seed in seeds]
    #########################################################################

    def set_oneliner_output(self):
        self.stats_one_line = True

    def stats_print_setup(self, runcard_info, dbid = ""):
        from src.header import short_stats
        if dbid == "":
            string = ""
        else:
            string = "{0:5} ".format("["+dbid+"]")
        if self.stats_one_line:
            print("{0}-{1}: ".format(dbid, runcard_info["runcard"]), end="")
            return
        if not short_stats:
            string += "=> {0}: {1}".format(runcard_info["runcard"],
                                       runcard_info["runfolder"])
            print(string)
        else:
            string += "{0:20}: {1:10} ".format(runcard_info["runcard"],
                                         runcard_info["runfolder"])
            print(string, end="")


    def __init__(self, act_only_on_done = False):
        from src.header import dbname, baseSeed
        import src.dbapi
        self.overwrite_warmup = False
        self.tarw  = util.TarWrap()
        self.gridw = util.GridWrap()
        self.dbase = src.dbapi.database(dbname)
        self.table = None
        self.bSeed = baseSeed
        self.jobtype_get = {
                'P' : self._get_data_production,
                'W' : self._get_data_warmup,
                'S' : self._get_data_warmup
                }
        self.assume_yes = False
        self.act_only_on_done = act_only_on_done
        self.stats_one_line = False

    # Helper functions and wrappers
    def dont_ask_dont_tell(self):
        self.assume_yes = True

    def set_list_disabled(self):
        self.dbase.set_list_disabled()

    def _press_yes_to_continue(self, msg, error = None, fallback = None):
        """ Press y to continue
            or n to exit the program
        """
        if self.assume_yes:
            return 0
        if msg is not None:
            print(msg)
        yn = input("Do you want to continue (y/n) ").lower()
        if yn.startswith("y"):
            return 0
        else:
            if fallback:
                return fallback
            if error:
                raise Exception(error)
            else:
                sys.exit(-1)

    def _db_list(self, fields, search_string = None, search_fields = ["runcard", "runfolder", "jobtype"]):
        """ Returns a list with a dict for each member of the list.
            If a search_string is provided, only entries matching searc_string in search_fields
            will be returned
        """
        if search_string:
            return self.dbase.find_and_list(self.table, fields, search_fields, search_string)
        else:
            return self.dbase.list_data(self.table, fields)

    def _get_computing_element(self, id_example):
        """ Given a list jobids, returns the computing elements they are being run on
        if it is not Durham. Since there is no pattern? let's assume anything relevant
        happens before .ac.uk _and_ after the first ."""
        comp_element = id_example.split('.ac.uk')[0]
        if "dur" not in comp_element and "." in comp_element:
            return " at {}".format(comp_element.split('.',1)[1])
        else:
            return ""

    def _multirun(self, function, arguments, n_threads = 15,
                  arglen=None, use_counter = False, timeout = False):
        """ Wrapper for multiprocessing
            For ARC only single thread is allow as the arc database needs
            to be locked
        """
        from multiprocessing import Pool, Value
        # If required # calls is lower than the # threads given, use the minimum
        if arglen is None:
            arglen = n_threads
        threads = max(min(n_threads, arglen),1)

        if use_counter:
            counter = Value('i', 0)
            pool   = Pool(threads, initializer = init_counter, initargs = (counter,))
        else:
            pool   = Pool(threads)
        self.dbase.close()
        result = pool.map(function, arguments, chunksize = 1)
        self.dbase.reopen()
        pool.close()
        pool.join()
        return result

    def _check_id_type(self, db_id):
        """ Checks whether a job is production/warmup/socketed
        """
        production = 'P'
        socketed_warmup = 'S'
        warmup = 'W'
        jobtype = self.dbase.list_data(self.table, ["jobtype"], db_id)[0]["jobtype"]
        if not jobtype: # legacy suport (database returns None)
            idout = self.dbase.list_data(self.table, ["jobid"], db_id)[0]["jobid"]
            if len(idout.split(" ")) > 1:
                return production
            else:
                return warmup
        if "Production" in jobtype:
            return production
        elif "Warmup" in jobtype:
            return warmup
        elif "Socket" in jobtype:
            return socketed_warmup
        else:
            idout = self.dbase.list_data(self.table, ["jobid"], db_id)[0]["jobid"]
            if len(idout.split(" ")) > 1:
                return production
            else:
                return warmup

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
        from src.header import logger
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
        from src.header import lfn_output_dir, logger
        logger.info("Checking whether runcard {0} has output for seeds that you are trying to submit...".format(rname))
        checkname = r + "-" + rname
        files = self.gridw.get_dir_contents(lfn_output_dir)
        first = True
        if checkname in files:
            from src.header import baseSeed, producRun
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
        from src.header import logger
        import re
        from src.runcard_parsing import runcard_parsing
        logger.info("Checking whether runcard {0} has output for seeds that you are trying to submit...".format(rname))
        local_dir_name = self.get_local_dir_name(r,rname)
        files = os.listdir(local_dir_name)
        runcard = runcard_parsing(runcard_file=os.path.join(local_dir_name,r),logger=logger,
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
        from src.header import lfn_warmup_dir, logger
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

    def get_completion_stats(self, jobid, jobinfo, args):
        from collections import Counter
        from src.gnuplot import do_plot
        job_outputs = self.cat_job(jobid, jobinfo, store = True)
        vals = []
        for idx,job_stdout in enumerate(job_outputs):
            for line in reversed(job_stdout.split("\n")):
                if "Current progress" in line and ".uk" not in line:
                    vals.append(int(line.split("\r")[-1].split()[-1].strip()[:-1]))
                    break
                elif "Commencing" in line:
                    vals.append(0)
                    break
        histogram = sorted(list(zip(Counter(vals).keys(),Counter(vals).values())), key = lambda x: x[0])
        format_string = "| {0:<4}"
        val_line = "{0:<13}".format("% Completion")
        count_line =  "{0:<13}".format("# Jobs")
        for element in histogram:
            val_line += format_string.format(str(element[0])+"%")
            count_line += format_string.format(element[1])
        divider = "-"*len(val_line)
        print(val_line)
        print(divider)
        print(count_line)

        if not args.gnuplot:
            return

        rawvals = [i[0] for i in histogram]
        rawcounts = [i[1] for i in histogram]

        newvals = []
        newcounts = []
        for i in range(0, 105, 5):
            match = False
            for j in histogram:
                if i == j[0]:
                    newvals.append(j[0])
                    newcounts.append(j[1])
                    match = True
                    break
            if not match:
                newvals.append(i)
                newcounts.append(0)

        do_plot(newvals, newcounts, title="Completion % Histogram (Unnormalised)",
                xlabel="% Completion", ylabel = "No. of jobs")

    def get_grid_from_stdout(self,jobid, jobinfo):
        from src.header import logger, warmup_base_dir, default_runfolder
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


    # External functions for database management
    def get_id(self, db_id):
        """ Returns a list of DIRAC/ARC jobids
        for a given database entry
        """
        from src.header import logger
        jobid = self.dbase.list_data(self.table, ["jobid"], db_id)
        try:
            idout = jobid[0]['jobid']
        except IndexError:
            print("Selected job is %s out of bounds" % jobid)
            idt   = input("> Select id to act upon: ")
            idout = self.get_id(idt)
        jobid_list = idout.split(" ")
        if self.act_only_on_done:
            new_list = []
            status_list = self._get_old_status(db_id)
            if status_list:
                for jobid, stat in zip(jobid_list, status_list):
                    if stat == self.cDONE:
                        new_list.append(jobid)
                return new_list
            else:
                logger.critical("In order to act only on 'done' jobs you need to have that info in the db!")
        else:
            return jobid_list

    def get_date(self, db_id):
        """ Returns date from a given database entry
        """
        jobid = self.dbase.list_data(self.table, ["date"], db_id)
        try:
            idout = jobid[0]['date']
        except IndexError:
            print("Selected job is %s out of bounds" % jobid)
            idt   = input("> Select id to act upon: ")
            idout = self.get_date(idt)
        return idout

    def disable_db_entry(self, db_id):
        """ Disable database entry
        """
        self.dbase.disable_entry(self.table, db_id)

    def enable_db_entry(self, db_id):
        """ Enable database entry
        """
        self.dbase.disable_entry(self.table, db_id, revert = True)

    ### Initialisation functions
    def get_local_dir_name(self,runcard, tag):
        from src.header import local_run_directory
        runname = "{0}-{1}".format(runcard, tag)
        dir_name = os.path.join(local_run_directory,runname)
        logger.info("Run directory: {0}".format(dir_name))
        return dir_name

    def get_stdout_dir_name(self, run_dir):
        return os.path.join(run_dir,"stdout/")


    def init_single_local_warmup(self, runcard, tag, continue_warmup = False,
                                 provided_warmup=False):
        import shutil
        from src.header import executable_src_dir, executable_exe, runcardDir, slurm_kill_exe
        run_dir = self.get_local_dir_name(runcard, tag)
        os.makedirs(run_dir,exist_ok=True)
        stdoutdir = self.get_stdout_dir_name(run_dir)
        os.makedirs(stdoutdir,exist_ok=True) # directory for slurm stdout files
        path_to_exe_full = executable_src_dir + executable_exe
        shutil.copy(path_to_exe_full, run_dir)
        runcard_file = runcardDir + "/" + runcard
        runcard_obj = runcard_parsing(runcard_file, logger=logger, grid_run=False)
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
        from src.header import executable_src_dir, executable_exe, logger
        from src.header import runcardDir as runFol

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
        path_to_exe_full = executable_src_dir + executable_exe
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
            runcard_obj = runcard_parsing(runcard_file, logger=logger)
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
        from src.header import executable_src_dir, executable_exe, runcardDir
        run_dir = self.get_local_dir_name(runcard, tag)
        os.makedirs(run_dir,exist_ok=True)
        stdoutdir = self.get_stdout_dir_name(run_dir)
        os.makedirs(stdoutdir,exist_ok=True) # directory for slurm stdout files
        path_to_exe_full = executable_src_dir + executable_exe
        shutil.copy(path_to_exe_full, run_dir)
        runcard_file = runcardDir + "/" + runcard
        runcard_obj = runcard_parsing(runcard_file, logger=logger)
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
        from src.header import runcardDir as runFol
        from src.header import executable_exe, executable_src_dir, logger

        if local:
            self.init_local_production(provided_warmup = provided_warmup)
            return

        rncards, dCards = util.expandCard()
        path_to_exe_full = executable_src_dir + executable_exe

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
            runcard_obj = runcard_parsing(runcard_file, logger=logger)
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
        from src.header import logger
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


    def check_warmup_files(self, db_id, rcard, resubmit=False):
        """ Provides stats on whether a warmup file exists for a given run and optionally
        resubmit if absent"""
        from shutil import copy
        import tempfile
        import tarfile
        from src.header import logger

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
                from src.runArcjob import runWrapper
                runWrapper(rcard, expandedCard = expandedCard)


    # src.Backend "independent" management options
    # (some of them need backend-dependent definitions but work the same
    # for both ARC and DIRAC)
    def stats_job(self, dbid, do_print=True):
        """ Given a list of jobs, returns the number of jobs which
        are in each possible state (done/waiting/running/etc)
        """
        jobids = self.get_id(dbid)
        current_status = self._get_old_status(dbid)
        arglen = len(jobids)

        if isinstance(current_status, list):
            if len(current_status)==arglen:
                jobids_lst = zip(jobids, current_status)
            else: # Current status corrupted somehow... Start again
                jobids_lst = jobids
        else:
            jobids_lst = jobids

        tags = ["runcard", "runfolder", "date"]
        runcard_info = self.dbase.list_data(self.table, tags, dbid)[0]

        n_threads = header.finalise_no_cores
        status = self._multirun(self._do_stats_job, jobids_lst,
                                n_threads, arglen=arglen)
        done = status.count(self.cDONE)
        wait = status.count(self.cWAIT)
        run = status.count(self.cRUN)
        fail = status.count(self.cFAIL)
        unk = status.count(self.cUNK)
        if do_print:
            self.stats_print_setup(runcard_info, dbid=dbid)
            total = len(jobids)
            self.print_stats(done, wait, run, fail, unk, total)
        self._set_new_status(dbid, status)
        return done, wait, run, fail, unk


    def _get_old_status(self, db_id):
        field_name = "sub_status"
        status_dic = self.dbase.list_data(self.table, [field_name], db_id)
        try:
            status_str = status_dic[0][field_name]
            if status_str.lower() == "none":
                return None
            else:
                outlst = [int(i) for i in status_str.split()]
                return outlst
        except:
            return None

    def _set_new_status(self, db_id, status):
        field_name = "sub_status"
        status_str = ' '.join(map(str,status))
        self.dbase.update_entry(self.table, db_id, field_name, status_str)

    def print_stats(self, done, wait, run, fail, unk, total):
        import datetime
        from src.header import short_stats
        total2 = done + wait + run + fail + unk
        time = datetime.datetime.now().strftime("%H:%M:%S %d-%m-%Y")

        if self.stats_one_line:
            string = "Done: [{0}/{1}];\n".format(done, total)
            print(string)
            return

        if short_stats:
            def addline(name, val, colour):
                if val > 0:
                    return "{0}{1}: {2:4}  \033[0m".format(colour, name, val)
                else:
                    return  "{0}: {1:4}  ".format(name, val)

            string = addline("Done", done, '\033[92m')
            string += addline("Waiting", wait, '\033[93m')
            string += addline("Running", run, '\033[94m')
            string += addline("Failed", fail, '\033[91m')
            string += "Total: {0:4}".format(total2)
            print(string)
        else:
            print(" >> Total number of subjobs: {0:<20} {1}".format(total, time))
            print("    >> Done:    {0}".format(done))
            print("    >> Waiting: {0}".format(wait))
            print("    >> Running: {0}".format(run))
            print("    >> Failed:  {0}".format(fail))
            print("    >> Unknown: {0}".format(unk))
            print("    >> Sum      {0}".format(total2))

    def _do_stats_job(self, jobid_raw):
        """ version of stats job multithread ready
        """
        import src.header as header
        if isinstance(jobid_raw, tuple):
            if jobid_raw[1] == self.cDONE or jobid_raw[1] == self.cFAIL:
                return jobid_raw[1]
            else:
                jobid = jobid_raw[0]
        else:
            jobid = jobid_raw
        cmd = [self.cmd_stat, jobid.strip(), "-j", header.arcbase]
        strOut = util.getOutputCall(cmd, suppress_errors=True)
        if "Done" in strOut or "Finished" in strOut:
            return self.cDONE
        elif "Waiting" in strOut or "Queuing" in strOut:
            return self.cWAIT
        elif "Running" in strOut:
            return self.cRUN
        elif "Failed" in strOut:
            return self.cFAIL
        else:
            return self.cUNK



    def _get_data_warmup(self, db_id):
        """ Given a database entry, retrieve its data from the
        warmup folder to the folder defined in said database entry
        For arc jobs stdoutput will be downloaded in said folder as well
        """
        # Retrieve data from database
        from src.header import arcbase, lfn_warmup_dir
        fields    =  ["runcard","runfolder", "jobid", "pathfolder"]
        data      =  self.dbase.list_data(self.table, fields, db_id)[0]
        runfolder =  data["runfolder"]
        finfolder =  data["pathfolder"] + "/" + runfolder
        runcard   =  data["runcard"]
        jobids    =  data["jobid"].split()
        util.spCall(["mkdir", "-p", finfolder])
        print("Retrieving ARC output into " + finfolder)
        try:
            # Retrieve ARC standard output for every job of this run
            for jobid in jobids:
                print(jobid)
                cmd       =  [self.cmd_get, "-j", arcbase, jobid.strip()]
                output    = util.getOutputCall(cmd)
                outputfol = output.split("Results stored at: ")[1].rstrip()
                outputfolder = outputfol.split("\n")[0]
                if outputfolder == "" or (len(outputfolder.split(" ")) > 1):
                    print("Running mv and rm command is not safe here")
                    print("Found blank spaces in the output folder")
                    print("Nothing will be moved to the warmup global folder")
                else:
                    destination = finfolder + "/" + "arc_out_" + runcard + outputfolder
                    util.spCall(["mv", outputfolder, destination])
                    #util.spCall(["rm", "-rf", outputfolder])
        except:
            print("Couldn't find job output in the ARC server")
            print("jobid: " + jobid)
            print("Run arcstat to check the state of the job")
            print("Trying to retrieve data from grid storage anyway")
        # Retrieve warmup from the grid storage warmup folder
        wname = self.warmup_name(runcard, runfolder)
        self.gridw.bring(wname, lfn_warmup_dir, finfolder + "/" + wname)

    def _get_data_production(self, db_id):
        """ Given a database entry, retrieve its data from
        the output folder to the folder defined in said db entry
        """
        print("You are going to download all folders corresponding to this runcard from lfn:output")
        print("Make sure all runs are finished using the -s or -S options!")
        fields       = ["runfolder", "runcard", "jobid", "pathfolder", "iseed"]
        data         = self.dbase.list_data(self.table, fields, db_id)[0]
        self.rcard   = data["runcard"]
        self.rfolder = data["runfolder"]
        pathfolderTp = data["pathfolder"]
        initial_seed = data["iseed"]
        pathfolder   = util.sanitiseGeneratedPath(pathfolderTp, self.rfolder)
        jobids       = data["jobid"].split(" ")
        finalSeed    = int(initial_seed) + len(jobids)
        if initial_seed == "None":
            initial_seed = self.bSeed
        else:
            initial_seed = int(initial_seed)
        while True:
            firstName = self.output_name(self.rcard, self.rfolder, initial_seed)
            finalName = self.output_name(self.rcard, self.rfolder, finalSeed)
            print("The starting filename is {}".format(firstName))
            print("The final filename is {}".format(finalName))
            yn = self._press_yes_to_continue("If you are ok with this, press y", fallback = -1)
            if yn == 0:
                break
            initial_seed = int(input("Please, introduce the starting seed (ex: 400): "))
            finalSeed  = int(input("Please, introduce the final seed (ex: 460): "))
        try:
            os.makedirs(self.rfolder)
        except OSError as err:
            if err.errno == 17:
                print("Tried to create folder %s in this directory".format(self.rfolder))
                print("to no avail. We are going to assume the directory was already there")
                self._press_yes_to_continue("", "Folder {} already exists".format(self.rfolder))
            else:
                raise
        os.chdir(self.rfolder)
        try:
            os.makedirs("log")
            os.makedirs("dat")
        except: # todo: macho... this is like mkdir -p :P
            pass
        seeds    =  range(initial_seed, finalSeed)
        # If we are only act on a subrange of jobids (ie, the ones which are done...) choose only those seeds
        if self.act_only_on_done:
            old_status = self._get_old_status(db_id)
            if old_status:
                new_seed = []
                for seed, stat in zip(seeds, old_status):
                    if stat == self.cDONE:
                        new_seed.append(seed)
            seeds = new_seed

        from src.header import finalise_no_cores as n_threads
        # Check which of the seeds actually produced some data
        all_remote = self.output_name_array(self.rcard, self.rfolder, seeds)
        all_output = self.gridw.get_dir_contents(header.lfn_output_dir).split()
        remote_tarfiles = list(set(all_remote) & set(all_output))
        print("Found data for {0} of the {1} seeds.".format(len(remote_tarfiles), len(seeds)))

        # Download said data
        tarfiles = self._multirun(self._do_get_data, remote_tarfiles, n_threads, use_counter = True)
        tarfiles = list(filter(None, tarfiles))
        print("Downloaded 0 files", end ='\r')
        print("Downloaded {0} files, extracting...".format(len(tarfiles)))

        # Extract some information from the first tarfile
        for tarfile in tarfiles:
            if self._extract_output_warmup_data(tarfile):
                break

        # Extract all
        dummy    =  self._multirun(self._do_extract_outputData, tarfiles, n_threads)
        os.chdir("..")
        print("Everything saved at {0}".format(pathfolder))
        util.spCall(["mv", self.rfolder, pathfolder])

    def _do_get_data(self, filename):
        """ Multithread wrapper used in get_data_production
        to download information from the grid storage
        """
        local_name = filename.replace("output", "")
        local_file = self.rfolder + "/" + local_name
        self.gridw.bring(filename, header.lfn_output_dir, local_file, timeout = header.timeout)
        from os.path import isfile
        if isfile(local_name):
            global counter
            if counter:
                counter.value += 1
                print("Downloaded {0} files ".format(counter.value), end='\r')
            return local_name
        else:
            return None

    def _extract_output_warmup_data(self, tarfile):
        """
        Extracts runcard and warmup from a tarfile.
        """
        extensions = ["run", "vRa", "vRb", "vBa", "vBb", "RRa", "RRb", "log-warmup"]
        return self.tarw.extract_extensions(tarfile, extensions)

    def _do_extract_outputData(self, tarfile):
        """ Multithread wrapper used in get_data_production
            for untaring files
        """
        # It assumes log and dat folder are already there
        if not os.path.isfile(tarfile):
            print(tarfile + " not found")
            return -1

        out_dict = {".log" : "log/", ".dat" : "dat/" }
        self.tarw.extract_extension_to(tarfile, out_dict)

        util.spCall(["rm", tarfile])

        return 0

    def get_data(self, db_id, jobid = None, custom_get = None):
        """ External interface for the get_data routines.
        If a custom_get is defined in the src.header, it will be used
        instead of the 'native' _get_data_{production/warmup}.
        Custom scripts need to have a public "do_finalise()" function for this to work
        """
        if custom_get:
            from importlib import import_module
            custom_get = custom_get.replace("/",".")
            import_module(custom_get).do_finalise()
        else:
            # Check whether we are in a production or a warmup run before continuing
            # and call the corresponding get_function
            jobtype = self._check_id_type(db_id)
            self.jobtype_get[jobtype](db_id)


    def list_runs(self, search_string = None):
        """ List all runs in the database.
        If a search_string is provided, only those runs matching the search_string in
        runcard or runfolder will apear
        """
        fields = ["rowid", "jobid", "runcard", "runfolder", "date", "jobtype", "iseed"]
        dictC  = self._db_list(fields, search_string)
        print("Active runs: " + str(len(dictC)))

        # Could easily be optimised
        offset = 2
        id_width = max(list(len(str(i["rowid"])) for i in dictC)+[2])+offset
        runcard_width = max(list(len(i["runcard"].strip()) for i in dictC)+[7])+offset
        runname_width = max(list(len(i["runfolder"].strip()) for i in dictC)+[7])+offset
        date_width = max(list(len(str(i['date']).split('.')[0].strip()) for i in dictC)+[10])+offset
        misc_width = 7

        print("|".join(["id".center(id_width),"runcard".center(runcard_width),"runname".center(runname_width),"date".center(date_width),"misc".center(misc_width)]))
        for i in dictC:
            rid = str(i['rowid']).center(id_width)
            ruc = str(i['runcard']).center(runcard_width)
            run = str(i['runfolder']).center(runname_width)
            dat = str(i['date']).split('.')[0].center(date_width)
            misc = str(" "+i['jobtype'])
            jobids = str(i['jobid'])
            initial_seed = str(i['iseed'])
            no_jobs = len(jobids.split(" "))
            if no_jobs > 1:
                if initial_seed and initial_seed != "None":
                    misc += " ({0}, is: {1})".format(no_jobs, initial_seed)
                else:
                    misc += " ({0})".format(no_jobs)
            misc += self._get_computing_element(jobids)
            misc_text = misc.center(misc_width)
            print("|".join([rid,ruc,run,dat,misc_text]))

    def get_active_dbids(self):
        field_name = "rowid"
        dictC = self._db_list([field_name])
        all_ids = []
        for i in dictC:
            all_ids.append(str(i[field_name]))
        return all_ids

    def _format_args(self, *args, **kwargs):
        raise Exception("Any children classes of src.Backend.py should override this method")

    def _get_default_args(self):
        # Defaults arguments that can always go in
        dictionary = {
                'gfal_location' : header.cvmfs_gfal_location,
                'executable' : header.executable_exe,
                'lfndir' : header.lfndir,
                'input_folder' : header.lfn_input_dir,
                'output_folder' : header.lfn_output_dir,
                'warmup_folder' : header.lfn_warmup_dir,
                'lhapdf_grid' : header.lhapdf_grid_loc,
                'lhapdf_local' : header.lhapdf_loc,
                'debug' : str(header.debug_level),
                'gfaldir': header.gfaldir,
                'use_gfal' : str(header.use_gfal),
                'events' : str(header.events)
                }
        return dictionary

    def _make_base_argstring(self, runcard, runtag):
        dictionary = self._get_default_args()
        dictionary['runcard'] = runcard
        dictionary['runname'] = runtag
        return self._format_args(dictionary)

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

def generic_initialise(runcard, warmup=False, production=False, grid=None,
                       overwrite_grid=False, local=False):
    from src.header import logger
    logger.info("Initialising runcard: {0}".format(runcard))
    back = Backend()

    if warmup:
        if overwrite_grid:
            back.set_overwrite_warmup()
        back.init_warmup(grid, continue_warmup=overwrite_grid, local=local)
    elif production:
        back.init_production(grid, continue_warmup=overwrite_grid, local=local)
    else:
        logger.critical("Both warmup and production not selected. What do you want me to initialise?")
