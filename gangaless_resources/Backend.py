import os
import utilities as util

class Backend(object):
    """ Abstract class
    """
    cDONE = 0
    cWAIT = 1
    cRUN = 2
    cFAIL = -1
    cUNK = 99

    ### IMPORTANT: NAMING FUNCTIONS SHOULD BE THE SAME IN ARC.py AND DIRAC.py
    def warmup_name(self, runcard, rname):
        out = "output" + runcard + "-warm-" + rname + ".tar.gz"
        return out

    def output_name(self, runcard, rname, seed):
        out = "output" + runcard + "-" + rname + "-" + str(seed) + ".tar.gz"
        return out
    #########################################################################

    def __init__(self):
        from header import dbname, baseSeed
        import dbapi
        self.tarw  = util.TarWrap()
        self.gridw = util.GridWrap()
        self.dbase = dbapi.database(dbname)
        self.table = None
        self.bSeed = baseSeed
        self.jobtype_get = {
                'P' : self._get_data_production,
                'W' : self._get_data_warmup,
                'S' : self._get_data_warmup
                }

    # Helper functions and wrappers
    def _press_yes_to_continue(self, msg, error = None):
        """ Press y to continue
            or n to exit the program
        """
        print(msg)
        yn = input("Do you want to continue (y/n) ").lower()
        if yn.startswith("y"):
            return 0
        else:
            if error:
                raise Exception(error)
            else:
                from sys import exit
                exit(-1)

    def _db_list(self, fields, search_string = None, search_fields = ["runcard", "runfolder"]):
        """ Returns a list with a dict for each member of the list.
            If a search_string is provided, only entries matching searc_string in search_fields
            will be returned
        """
        if search_string:
            return self.dbase.find_and_list(self.table, fields, search_fields, search_string)
        else:
            return self.dbase.list_data(self.table, fields)

    def _multirun(self, function, arguments, n_threads = 5):
        """ Wrapper for multiprocessing
            For ARC only single thread is allow as the arc database needs
            to be locked 
        """
        from multiprocessing import Pool 
        if str(self) == "Arc":
            threads = 1
        else:
            threads = n_threads
        pool   = Pool(threads)
        result = pool.map(function, arguments)
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
    def _check_production_warmup(self, r, runcard_dir, warmup, production):
        """ Check whether production/warmup are active in the runcard """
        warm_string = "warmup"
        prod_string = "production"
        print("Checking warmup/production in runcard %s" % r)
        with open(runcard_dir + "/" + r, 'r') as f:
            for line_raw in f:
                line = line_raw.lower()
                if warm_string in line:
                    if warmup and ".false." in line:
                        self._press_yes_to_continue("Warmup is not active")
                    elif not warmup and ".true." in line:
                        self._press_yes_to_continue("Warmup is active")
                if prod_string in line:
                    if production and ".false." in line:
                        self._press_yes_to_continue("Production is not active")
                    elif not production and ".true." in line:
                        self._press_yes_to_continue("Production is active")

    def _check_production(self, r, runcard_dir):
        self._check_production_warmup(r, runcard_dir, warmup = False, production = True)

    def _check_warmup(self, r, runcard_dir):
        self._check_production_warmup(r, runcard_dir, warmup = True, production = False)

    # Checks for the grid storage system
    def _checkfor_existing_warmup(self, r, rname):
        """ Check whether given runcard already has a warmup output in the grid """
        print("Checking whether this runcard is already at lfn:warmup")
        checkname = self.warmup_name(r, rname)
        if self.gridw.checkForThis(checkname, "warmup"):
            self._press_yes_to_continue("File {} already exists at lfn:warmup, do wou want to remove it?".format(checkname))
            self.gridw.delete(checkname, "warmup")

    def _checkfor_existing_output(self, r, rname):
        """ Check whether given runcard already has output in the grid
        needs testing as it needs to be able to remove (many) things for production run 
        It relies on the base seed from the header file to remove the output
        """
        print("Checking whether this runcard has something on the output folder...")
        checkname = r + "-" + rname
        if self.gridw.checkForThis(checkname, "output"):
            self._press_yes_to_continue("It seems this runcard already has at least one file at lfn:output (looked for {}). Do you want to remove it/them?".format(checkname))
            print("Runcard " + r + " has at least one file at output")
            from header import baseSeed, producRun
            for seed in range(baseSeed, baseSeed + producRun):
                filename = "output" + checkname + "-" + str(seed) + ".tar.gz"
                self.gridw.delete(filename, "output")


    def _bring_warmup_files(self, runcard, rname):
        """ Download the warmup file for a run to local directory
        extracts Vegas grid and log file and returns a list with their names
        TODO: use a unique /tmp directory instead of local dir
        """
        gridFiles = []
        ## First bring the warmup .tar.gz
        outnm = self.warmup_name(runcard, rname)
        tmpnm = "tmp.tar.gz"
        self.gridw.bring(outnm, "warmup", tmpnm)
        ## Now list the files inside the .tar.gz and extract only the Vegas grid file
        gridp = [".RRa", ".RRb", ".vRa", ".vRb", ".vBa", ".vBb"]
        outlist = self.tarw.listFilesTar(tmpnm)
        logfile = ""
        for fileRaw in outlist:
            if ".log" in fileRaw:
                file = fileRaw.split(" ")[-1]
                logfile = file
            if len(fileRaw.split(".y")) == 1: 
                continue
            file = fileRaw.split(" ")[-1]
            for grid in gridp:
                if grid in file: 
                    gridFiles.append(file)
        ## And now extract only those files
        extractFiles = gridFiles + [logfile]
        self.tarw.extractThese(tmpnm, extractFiles)
        ## Tag log file as -warmup
        newlog = logfile + "-warmup"
        cmd = ["mv", logfile, newlog]
        util.spCall(cmd)
        # Remove temporary tar files
        util.spCall(["rm", "tmp.tar.gz"])
        gridFiles.append(newlog)
        # Make sure access to the file is correct!
        for i in gridFiles:
            util.spCall(["chmod", "a+wrx", i])
        return gridFiles

    # External functions for database management
    def get_id(self, db_id):
        """ Returns a list of DIRAC/ARC jobids
        for a given database entry
        """
        jobid = self.dbase.list_data(self.table, ["jobid"], db_id)
        try:
            idout = jobid[0]['jobid']
        except IndexError:
            print("Selected job is %s out of bounds" % jobid)
            idt   = input("> Select id to act upon: ")
            idout = self.get_id(idt)
        return idout.split(" ")

    def get_date(self, db_id):
        """ Returns a list of DIRAC/ARC jobids
        for a given database entry
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
    def init_warmup(self, runcard, provided_warmup = None):
        """ Initialises a warmup run. An warmup file can be provided and it will be 
        added to the .tar file sent to the grid storage. 
        Steps are:
            1 - tar up NNLOJET, runcard and necessary files
            2 - sent it to the grid storage
        """
        from shutil import copy
        from header import NNLOJETdir, NNLOJETexe
        from header import runcardDir as runFol
        rncards, dCards = util.expandCard(runcard)
        nnlojetfull = NNLOJETdir + "/driver/" + NNLOJETexe
        if not os.path.isfile(nnlojetfull): 
            raise Exception("Could not find NNLOJET executable")
        copy(nnlojetfull, os.getcwd())
        files = [NNLOJETexe]
        if provided_warmup: 
            files = files + [provided_warmup]
        for i in rncards:
            # Check whether warmup/production is active in the runcard
            if not os.path.isfile(runFol + "/" + i):
                print("Could not find runcard %s" % i)
                yn = input("Do you want to continue? (y/n): ").lower()
                if yn.startswith('y'):
                    continue
                else:
                    raise Exception("Could not find runcard")
            self._check_warmup(i, runFol)
            rname   = dCards[i]
            tarfile = i + rname + ".tar.gz"
            copy(runFol + "/" + i, os.getcwd())
            self.tarw.tarFiles(files + [i], tarfile)
            if self.gridw.checkForThis(tarfile, "input"):
                print("Removing old version of " + tarfile + " from Grid Storage")
                self.gridw.delete(tarfile, "input")
            print("Sending " + tarfile + " to lfn:input/")
            self.gridw.send(tarfile, "input")
            util.spCall(["rm", i, tarfile])
        util.spCall(["rm", NNLOJETexe])

    def init_production(self, runcard, provided_warmup = None):
        """ Initialises a production run. If a warmup file is provided
        retrieval step is skipped
        Steps are:
            0 - Retrieve warmup from the grid
            1 - tar up NNLOJET, runcard and necessary files
            2 - sent it to the grid storage
        """
        from shutil import copy
        from header import runcardDir as runFol
        from header import NNLOJETexe, NNLOJETdir
        rncards, dCards = util.expandCard(runcard)
        nnlojetfull = NNLOJETdir + "/driver/" + NNLOJETexe
        if not os.path.isfile(nnlojetfull): 
            raise Exception("Could not find NNLOJET executable")
        copy(nnlojetfull, os.getcwd())
        files = [NNLOJETexe]
        for i in rncards:
            # Check whether warmup/production is active in the runcard
            self._check_production(i, runFol)
            rname   = dCards[i]
            tarfile = i + rname + ".tar.gz"
            copy(runFol + "/" + i, os.getcwd())
            if provided_warmup:
                warmupFiles = [provided_warmup]
            else:
                warmupFiles = self._bring_warmup_files(i, rname)
            self.tarw.tarFiles(files + [i] + warmupFiles, tarfile)
            if self.gridw.checkForThis(tarfile, "input"):
                print("Removing old version of " + tarfile + " from Grid Storage")
                self.gridw.delete(tarfile, "input")
            print("Sending " + tarfile + " to lfn:input/")
            self.gridw.send(tarfile, "input")
            util.spCall(["rm", i, tarfile] + warmupFiles)
        util.spCall(["rm"] + files)

    # Backend "independent" management options
    # (some of them need backend-dependent definitions but work the same
    # for both ARC and DIRAC)
    def stats_job(self, jobids):
        """ Given a list of jobs, returns the number of jobs which
        are in each possible state (done/waiting/running/etc)
        """
        from header import finalise_no_cores as n_threads
        status = self._multirun(self._do_stats_job, jobids, n_threads)
        done = status.count(self.cDONE)
        wait = status.count(self.cWAIT)
        run = status.count(self.cRUN)
        fail = status.count(self.cFAIL)
        unk = status.count(self.cUNK)
        self.print_stats(done, wait, run, fail, unk, jobids)

    def print_stats(self, done, wait, run, fail, unk, jobids):
        import datetime
        total = len(jobids)
        total2 = done + wait + run + fail + unk 
        time = datetime.datetime.now().strftime("%H:%M:%S %d-%m-%Y")
        print(" >> Total number of subjobs: {0:<20} {1}".format(total, time))
        print("    >> Done:    {0}".format(done))
        print("    >> Waiting: {0}".format(wait))
        print("    >> Running: {0}".format(run))
        print("    >> Failed:  {0}".format(fail))
        print("    >> Unknown: {0}".format(unk))
        print("    >> Sum      {0}".format(total2))

    def _do_stats_job(self, jobid):
        """ version of stats job multithread ready
        """
        # When used with ARC, it assumes -j database is not needed (ie, default db is being used)
        cmd = [self.cmd_stat, jobid.strip()]
        strOut = util.getOutputCall(cmd)
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
        from header import arcbase
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
        self.gridw.bring(wname, "warmup", finfolder + "/" + wname)

    def _get_data_production(self, db_id):
        """ Given a database entry, retrieve its data from
        the output folder to the folder defined in said db entry
        """
        print("You are going to download all folders corresponding to this runcard from lfn:output")
        print("Make sure all runs are finished using the -s or -S options!")
        fields       = ["runfolder", "jobid", "runcard", "pathfolder", "iseed"]
        data         = self.dbase.list_data(self.table, fields, db_id)[0]
        self.rcard   = data["runcard"]
        self.rfolder = data["runfolder"]
        pathfolderTp = data["pathfolder"]
        initial_seed = data["iseed"]
        pathfolder   = util.sanitiseGeneratedPath(pathfolderTp, self.rfolder)
        jobids       = data["jobid"].split(" ")
        finalSeed    = self.bSeed + len(jobids)
        while True:
            firstName = self.output_name(self.rcard, self.rfolder, initial_seed)
            finalName = self.output_name(self.rcard, self.rfolder, finalSeed)
            print("The starting filename is {}".format(firstName))
            print("The final filename is {}".format(finalName))
            yn = input("Are these parameters correct? (y/n) ").lower()
            if yn.startswith("y"): 
                break
            self.bSeed = int(input("Please, introduce the starting seed (ex: 400): "))
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
        seeds    =  range(self.bSeed, finalSeed)
        from header import finalise_no_cores as n_threads
        tarfiles =  self._multirun(self._do_get_data, seeds, n_threads)
        # Don't try to untar files that do not exist...
        dummy    =  self._multirun(self._do_extract_outputData, tarfiles, n_threads)
        os.chdir("..")
        print("Everything saved at %s" % pathfolder)
        util.spCall(["mv", self.rfolder, pathfolder])


    def _do_get_data(self, seed):
        """ Multithread wrapper used in get_data_production 
        to download information from the grid storage
        """
        filenm   = self.output_name(self.rcard, self.rfolder, seed)
        remotenm = filenm + ".tar.gz"
        localfil = self.rfolder + "-" + str(seed) + ".tar.gz"
        localnm  = self.rfolder + "/" + localfil
        self.gridw.bring(filenm, "output", localnm)
        return localfil

    def _do_extract_outputData(self, tarfile):
        """ Multithread wrapper used in get_data_production
            for untaring files
        """
        # It assumes log and dat folder are already there
        if not os.path.isfile(tarfile):
            print(tarfile + " not found")
            return -1
        files =  self.tarw.listFilesTar(tarfile)
        datf = []
        logf = []
        logw = None
        runf = None
        for fil in files:
            f = fil.strip()
            f = f.split(" ")[-1].strip()
            if ".dat" in fil and "lhapdf/" not in fil:
                datf.append(f)
            elif ".log" in fil:
                if "warm" in fil:
                    logw = f
                else:
                    logf.append(f)
            elif ".run" in fil:
                runf = f
        dtarfile = "../" + tarfile

        try:
            os.chdir("log")
        except:
            print("Somehow we could not enter 'log' folder")
            print(os.getcwd())
            print(os.system("ls"))
        if runf:
            if not os.path.isfile(runf):
                self.tarw.extractThese(dtarfile, [runf])
        if logw:
            if not os.path.isfile(logw):
                self.tarw.extractThese(dtarfile, [logw])
        try:
            self.tarw.extractThese(dtarfile, logf)
            os.chdir("../dat")
            self.tarw.extractThese(dtarfile, datf)
            os.chdir("..")
            util.spCall(["rm", tarfile])
        except:
            print("Once again, something went wrong with os and chdir")
            print(os.getcwd())
            print(os.system("ls"))

        return 0

    def get_data(self, db_id, jobid = None, custom_get = None):
        """ External interface for the get_data routines.
        If a custom_get is defined in the header, it will be used
        instead of the 'native' _get_data_{production/warmup}.
        Custom scripts need to have a public "do_finalise()" function for this to work
        """
        if custom_get:
            from importlib import import_module
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
        print("id".center(5) + " | " + "runcard".center(22) + " | " + "runname".center(25) + " |" +  "date".center(20))
        for i in dictC:
            multirun_flag = ""
            rid = str(i['rowid']).center(5)
            ruc = str(i['runcard']).center(22)
            run = str(i['runfolder']).center(25)
            dat = str(i['date']).split('.')[0] + " " + str(i['jobtype'])
            dat = dat.center(20)
            jobids = str(i['jobid'])
            initial_seed = str(i['iseed'])
            no_jobs = len(jobids.split(" "))
            if no_jobs > 1:
                if initial_seed and initial_seed != "None":
                    multirun_flag = " ({0}, is: {1})".format(no_jobs, initial_seed)
                else:
                    multirun_flag = " ({0})".format(no_jobs)
            print(rid + " | " + ruc + " | " + run + " | " + dat + multirun_flag)
