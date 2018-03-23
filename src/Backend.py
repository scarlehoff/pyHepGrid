import os
import sys
import src.utilities as util
import src.header as header

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


    def stats_print_setup(self, runcard_info, dbid = ""):
        from src.header import short_stats
        if dbid == "":
            string = ""
        else:
            string = "{0:5} ".format("["+dbid+"]")
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

    def _multirun(self, function, arguments, n_threads = 5, 
                  arglen=None):
        """ Wrapper for multiprocessing
            For ARC only single thread is allow as the arc database needs
            to be locked 
        """
        from multiprocessing import Pool 
        # If required # calls is lower than the # threads given, use the minimum
        if arglen is None:
            arglen = n_threads
        threads = max(min(n_threads, arglen),1)
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
    def _check_production_warmup(self, r, runcard_dir, warmup, production, continue_warmup=False):
        """ Check whether production/warmup are active in the runcard """
        warm_string = "warmup"
        prod_string = "production"
        info = {}
        print("Checking warmup/production in runcard %s" % r)
        channels_flag = False
        channels_string = "channels"
        born = "vB"
        real = "vR"
        dreal = "RR"
        sufix_dict = {'b' : born, 'lo' : born, 'v': born, 'vv': born, 'r' : real, 'rv' : real, 'rr': dreal}
        with open(runcard_dir + "/" + r, 'r') as f:
            for idx,line_raw in enumerate(f):
                line = line_raw.lower()
                if warm_string in line:
                    if continue_warmup and not warmup:
                        print("    \033[91m ERROR:\033[0m Continue warmup selected, but submission not in warmup mode. Exiting. ")
                        sys.exit(-1)
                    elif continue_warmup and warmup and (warmup and "2" not in line):
                        self._press_yes_to_continue("Continue warmup is not active in runcard")
                    elif (warmup and ".false." in line) or (warmup and "0" in line):
                        self._press_yes_to_continue("Warmup is not active in runcard")
                    elif (not warmup and ".true." in line) or (not warmup and "1" in line):
                        self._press_yes_to_continue("Warmup is active in runcard")
                if prod_string in line:
                    if (production and ".false." in line) or (production and "0" in line) \
                            or (production and "2" in line):
                        self._press_yes_to_continue("Production is not active in runcard")
                    elif (not production and ".true." in line) or (not production and "1" in line):
                        self._press_yes_to_continue("Production is active in runcard")
                if idx == 1:
                    info["id"] = line_raw.split()[0].strip()
                if idx == 2:
                    info["proc"] = line_raw.split()[0].strip()
                if idx == 17:
                    region = line_raw.split()[0].strip()
                if idx == 14:
                    info["tc"] = line_raw.split()[0].strip()
                if channels_flag:
                    # This will fail if you try to send a job with "all" in regions
                    # It is not a bug, it's a feature
                    if line.strip() in sufix_dict: # Only works for LO/R/V/RV/VV/RR
                        info["channel_sufix"] = sufix_dict[line.strip()] + region
                    else:
                        info["channel_sufix"] = ""
                    channels_flag = False
                if channels_string == line.strip():
                    channels_flag = True

        return info

    def _check_production(self, r, runcard_dir, continue_warmup = False):
        return self._check_production_warmup(r, runcard_dir, warmup = False, production = True, 
                                      continue_warmup = continue_warmup)

    def _check_warmup(self, r, runcard_dir, continue_warmup = False):
        return self._check_production_warmup(r, runcard_dir, warmup = True, production = False, 
                                      continue_warmup = continue_warmup)

    def set_overwrite_warmup(self):
        self.overwrite_warmup = True

    # Checks for the grid storage system
    def _checkfor_existing_warmup(self, r, rname):
        """ Check whether given runcard already has a warmup output in the grid """
        print("Checking whether this runcard is already at lfn:warmup")
        checkname = self.warmup_name(r, rname)
        if self.gridw.checkForThis(checkname, header.lfn_warmup_dir):
            self._press_yes_to_continue("File {1} already exists at lfn:{0}, do you want to remove it?".format(header.lfn_warmup_dir,checkname))
            self.gridw.delete(checkname, header.lfn_warmup_dir)

    def _checkfor_existing_output(self, r, rname):
        """ Check whether given runcard already has output in the grid
        needs testing as it needs to be able to remove (many) things for production run 
        It relies on the base seed from the src.header file to remove the output
        """
        from src.header import lfn_output_dir
        print("Checking whether runcard {0} has output for seeds that you are trying to submit...".format(rname))
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
                        print("Runcard " + r + " has at least one file at output")
                        first = False
                    self.gridw.delete(filename, lfn_output_dir)
            print("Output check complete")

    def _bring_warmup_files(self, runcard, rname):
        """ Download the warmup file for a run to local directory
        extracts Vegas grid and log file and returns a list with their names
        TODO: use a unique /tmp directory instead of local dir
        """
        from src.header import lfn_warmup_dir
        gridFiles = []
        ## First bring the warmup .tar.gz
        outnm = self.warmup_name(runcard, rname)
        tmpnm = "tmp.tar.gz"
        success = self.gridw.bring(outnm, lfn_warmup_dir, tmpnm)
        if not success:
            print("  \033[91m ERROR:\033[0m Grid files failed to copy from the LFN. Did the warmup complete successfully?")
            sys.exit(-1)
        ## Now extract only the Vegas grid files and log file
        gridp = [".RRa", ".RRb", ".vRa", ".vRb", ".vBa", ".vBb"]
        extractFiles = self.tarw.extract_extensions(tmpnm, gridp+[".log"])
        try:
            gridFiles = [i for i in extractFiles if ".log" not in i]
            logfile = [i for i in extractFiles if ".log" in i][0]
        except IndexError as e:
            print("  \033[91m ERROR:\033[0m Logfile not found. Did the warmup complete successfully?")
            sys.exit(-1)

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
                print("In order to act only on 'done' jobs you need to have that info in the db!")
                sys.exit(-1)
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
    def init_warmup(self, provided_warmup = None, continue_warmup=False):
        """ Initialises a warmup run. An warmup file can be provided and it will be 
        added to the .tar file sent to the grid storage. 
        Steps are:
            1 - tar up NNLOJET, runcard and necessary files
            2 - sent it to the grid storage
        """
        from shutil import copy
        from src.header import NNLOJETdir, NNLOJETexe
        from src.header import runcardDir as runFol
        rncards, dCards = util.expandCard()
        nnlojetfull = NNLOJETdir + "/driver/" + NNLOJETexe
        if not os.path.isfile(nnlojetfull): 
            print("    \033[91mERROR:\033[0m Could not find NNLOJET executable at {0}".format(nnlojetfull))
            sys.exit(-1)
        copy(nnlojetfull, os.getcwd()) 
        files = [NNLOJETexe]
        for i in rncards:
            local = False
            warmupFiles = []
            # Check whether warmup/production is active in the runcard
            if not os.path.isfile(runFol + "/" + i):
                self._press_yes_to_continue("Could not find runcard {0}".format(i), error="Could not find runcard")
            info = self._check_warmup(i, runFol, continue_warmup=continue_warmup)
            if provided_warmup: 
                # Copy warmup to current dir if not already there
                match, local = self.get_local_warmup_name(info,provided_warmup)
                files += [match]
            rname   = dCards[i]
            tarfile = i + rname + ".tar.gz"
            copy(runFol + "/" + i, os.getcwd())
            if self.overwrite_warmup:
                checkname = self.warmup_name(i, rname)
                if self.gridw.checkForThis(checkname, header.lfn_warmup_dir):
                    print("Warmup found in lfn:{0}!".format(header.lfn_warmup_dir))
                    warmup_files = self._bring_warmup_files(i, rname)
                    if not warmup_files:
                        print("    \033[91mERROR:\033[0m No warmup grids found in warmup tar!")
                        sys.exit(-1)
                    files += warmup_files
                    print("Warmup files found: {0}".format(" ".join(i for i in warmup_files)))

            self.tarw.tarFiles(files + [i], tarfile)
            if self.gridw.checkForThis(tarfile, "input"):
                print("Removing old version of " + tarfile + " from Grid Storage")
                self.gridw.delete(tarfile, "input")
            print("Sending " + tarfile + " to lfn:input/")
            self.gridw.send(tarfile, "input")
            if not local:
                for j in warmupFiles:
                    os.remove(j)
            os.remove(i)
            os.remove(tarfile)
        os.remove(NNLOJETexe)

    def init_production(self, provided_warmup = None, continue_warmup=False):
        """ Initialises a production run. If a warmup file is provided
        retrieval step is skipped
        Steps are:
            0 - Retrieve warmup from the grid
            1 - tar up NNLOJET, runcard and necessary files
            2 - sent it to the grid storage
        """
        from shutil import copy
        from src.header import runcardDir as runFol
        from src.header import NNLOJETexe, NNLOJETdir
        rncards, dCards = util.expandCard()
        nnlojetfull = NNLOJETdir + "/driver/" + NNLOJETexe
        if not os.path.isfile(nnlojetfull): 
            print("    \033[91mERROR:\033[0m Could not find NNLOJET executable at {0}".format(nnlojetfull))
            sys.exit(-1)
        copy(nnlojetfull, os.getcwd())
        files = [NNLOJETexe]
        for i in rncards:
            local = False
            # Check whether warmup/production is active in the runcard
            info = self._check_production(i, runFol, continue_warmup = continue_warmup)
            rname   = dCards[i]
            tarfile = i + rname + ".tar.gz"
            copy(runFol + "/" + i, os.getcwd())
            if provided_warmup:
                match, local = self.get_local_warmup_name(info,provided_warmup)
                warmupFiles = [match]
            else:
                print("Retrieving warmup file from grid")
                warmupFiles = self._bring_warmup_files(i, rname)
            self.tarw.tarFiles(files + [i] + warmupFiles, tarfile)
            if self.gridw.checkForThis(tarfile, "input"):
                print("Removing old version of " + tarfile + " from Grid Storage")
                self.gridw.delete(tarfile, "input")
            print("Sending " + tarfile + " to lfn:input/")
            self.gridw.send(tarfile, "input")
            if local:
                util.spCall(["rm", i, tarfile])
            else:
                util.spCall(["rm", i, tarfile] + warmupFiles)
        os.remove(NNLOJETexe)

    def get_local_warmup_name(self,info,provided_warmup):
        from shutil import copy
        if os.path.isdir(provided_warmup):
            matches = []
            potential_files = os.listdir(provided_warmup)
            matchname = "{0}.{1}.y{2}.{3}".format(info["proc"],info["id"],info["tc"],info["channel_sufix"])
            for potfile in potential_files:
                if matchname in potfile and\
                        not potfile.endswith(".txt") and not potfile.endswith(".log"):
                    matches.append(potfile)
            if len(matches) > 1:
                print("\033[93m Multiple warmup matches found in {1}: {0}\033[0m ".format(" ".join(i for i in matches), provided_warmup))
                print("Exiting.")
                sys.exit()
            elif len(matches) ==0 :
                print("\033[93m No warmup matches found in {0}.\033[0m ".format(provided_warmup))
                print("Exiting.")
                sys.exit()
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


    # src.Backend "independent" management options
    # (some of them need backend-dependent definitions but work the same
    # for both ARC and DIRAC)
    def stats_job(self, dbid):
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

        self.stats_print_setup(runcard_info, dbid=dbid)
        total = len(jobids)
        self.print_stats(done, wait, run, fail, unk, total)
        self._set_new_status(dbid, status)

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
        tarfiles = self._multirun(self._do_get_data, remote_tarfiles, n_threads)
        tarfiles = list(filter(None, tarfiles))
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
        self.gridw.bring(filename, header.lfn_output_dir, local_file)
        from os.path import isfile
        if isfile(local_name):
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
        print("id".center(5) + " | " + "runcard".center(22) + " | " + "runname".center(25) + " | " +  "date".center(22) + " | " + "misc".center(20))
        for i in dictC:
            rid = str(i['rowid']).center(5)
            ruc = str(i['runcard']).center(22)
            run = str(i['runfolder']).center(25)
            dat = str(i['date']).split('.')[0].center(22)
            misc = str(i['jobtype'])
            jobids = str(i['jobid'])
            initial_seed = str(i['iseed'])
            no_jobs = len(jobids.split(" "))
            if no_jobs > 1:
                if initial_seed and initial_seed != "None":
                    misc += " ({0}, is: {1})".format(no_jobs, initial_seed)
                else:
                    misc += " ({0})".format(no_jobs)
            misc += self._get_computing_element(jobids)
            misc_text = misc.center(20)
            print(rid + " | " + ruc + " | " + run + " | " + dat + " | " + misc_text)

    def get_active_dbids(self):
        field_name = "rowid"
        dictC = self._db_list([field_name])
        all_ids = []
        for i in dictC:
            all_ids.append(str(i[field_name]))
        return all_ids

    def _format_args(self):
        raise Exception("Any children classes of src.Backend.py should override this method")

    def _get_default_args(self):
        # Defaults arguments that can always go in
        dictionary = {
                'executable' : header.NNLOJETexe,
                'lfndir' : header.lfndir,
                'input_folder' : header.lfn_input_dir,
                'output_folder' : header.lfn_output_dir,
                'warmup_folder' : header.lfn_warmup_dir,
                'lhapdf_grid' : header.lhapdf_grid_loc,
                'lhapdf_local' : header.lhapdf_loc,
                'debug' : str(header.debug_level),
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

    def stats_job_cheat(self, dbid):
        try:
            if self.__first_stats_job_cheat:
                pass
        except AttributeError as e:
             print("  \033[93m WARNING:\033[0m The selected backend does not override the cheat version of the status command. Falling back to the standard version.")
             self.__first_stats_job_cheat = False
        self.stats_job(dbid)

def generic_initialise(runcard, warmup=False, production=False, grid=None, overwrite_grid=False):
    print("Initialising runcard: {0}".format(runcard))
    back = Backend()

    if warmup:
        if overwrite_grid:
            back.set_overwrite_warmup()
        back.init_warmup(grid, continue_warmup=overwrite_grid)
    elif production:
        back.init_production(grid, continue_warmup=overwrite_grid)
    else:
        print("What do you want me to initialise?")
        sys.exit(-1)


