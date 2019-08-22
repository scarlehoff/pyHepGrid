from collections import Counter
import datetime
import os
import pyHepGrid.src.dbapi
from pyHepGrid.src.header import logger
import pyHepGrid.src.utilities as util
import pyHepGrid.src.header as header
import pyHepGrid.src.runmodes
import multiprocessing as mp
import sys

counter = None
_mode = pyHepGrid.src.runmodes.mode_selector[header.runmode.upper()]


def init_counter(args):
    global counter
    counter = args


class Backend(_mode):
    """ Abstract class
    """
    cDONE = 1
    cWAIT = 0
    cFAIL = -1
    cRUN  = 2
    cUNK  = 99

    def output_name_array(self, runcard, rname, seeds):
        return [self.output_name(runcard, rname, seed) for seed in seeds]

    def set_oneliner_output(self):
        self.stats_one_line = True

    def stats_print_setup(self, runcard_info, dbid=""):
        if dbid == "":
            string = ""
        else:
            string = "{0:5} ".format("[{0}]".format(dbid))

        if self.stats_one_line:
            logger.plain("{0}-{1}: ".format(dbid, runcard_info["runcard"]), end="")
            return
        if not header.short_stats:
            string += "=> {0}: {1}".format(runcard_info["runcard"],
                                           runcard_info["runfolder"])
            logger.plain(string)
        else:
            string += "{0:20}: {1:10} ".format(runcard_info["runcard"],
                                               runcard_info["runfolder"])
            logger.plain(string, end="")

    def __init__(self, act_only_on_done=False):
        from pyHepGrid.src.header import dbname, baseSeed
        self.overwrite_warmup = False
        self.tarw = util.TarWrap()
        self.gridw = util.GridWrap()
        self.dbase = pyHepGrid.src.dbapi.database(dbname, logger=header.logger)
        self.table = None
        self.bSeed = baseSeed
        self.jobtype_get = {
                'P': self._get_data_production,
                'W': self._get_data_warmup,
                'S': self._get_data_warmup
                }
        self.assume_yes = False
        self.act_only_on_done = act_only_on_done
        self.stats_one_line = False

    # Helper functions and wrappers
    def dont_ask_dont_tell(self):
        self.assume_yes = True

    def set_list_disabled(self):
        self.dbase.set_list_disabled()

    def _db_list(self, fields, search_string=None,
                 search_fields=["runcard", "runfolder", "jobtype"]):
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
            return " at {}".format(comp_element.split('.', 1)[1])
        else:
            return ""

    def _multirun(self, function, arguments, n_threads=15,
                  arglen=None, use_counter=False, timeout=False):
        """ Wrapper for multiprocessing
            For ARC only single thread is allow as the arc database needs
            to be locked
        """
        # If required # calls is lower than the # threads given, use the minimum
        if arglen is None:
            arglen = n_threads
        threads = max(min(n_threads, arglen),1)

        if use_counter:
            counter = mp.Value('i', 0)
            pool = mp.Pool(threads, initializer=init_counter, initargs=(counter,))
        else:
            pool = mp.Pool(threads)
        self.dbase.close()

        result = pool.map(function, arguments, chunksize=1)
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


    def get_completion_stats(self, jobid, jobinfo, args):
        from pyHepGrid.src.gnuplot import do_plot
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
        logger.plain(val_line)
        logger.plain(divider)
        logger.plain(count_line)

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


    # External functions for database management
    def get_id(self, db_id):
        """ Returns a list of DIRAC/ARC jobids
        for a given database entry
        """
        jobid = self.dbase.list_data(self.table, ["jobid"], db_id)
        try:
            idout = jobid[0]['jobid']
        except IndexError:
            logger.info("Selected job is %s out of bounds" % jobid)
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
            logger.info("Selected job is %s out of bounds" % jobid)
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
        total2 = done + wait + run + fail + unk
        time = datetime.datetime.now().strftime("%H:%M:%S %d-%m-%Y")

        if self.stats_one_line:
            string = "Done: [{0}/{1}];\n".format(done, total)
            print(string)
            return

        if header.short_stats:
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
            logger.plain(string)
        else:
            logger.plain(" >> Total number of subjobs: {0:<20} {1}".format(total, time))
            logger.plain("    >> Done:    {0}".format(done))
            logger.plain("    >> Waiting: {0}".format(wait))
            logger.plain("    >> Running: {0}".format(run))
            logger.plain("    >> Failed:  {0}".format(fail))
            logger.plain("    >> Unknown: {0}".format(unk))
            logger.plain("    >> Sum      {0}".format(total2))

    def _do_stats_job(self, jobid_raw):
        """ version of stats job multithread ready
        """
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
        from pyHepGrid.src.header import arcbase, grid_warmup_dir
        fields    =  ["runcard","runfolder", "jobid", "pathfolder"]
        data      =  self.dbase.list_data(self.table, fields, db_id)[0]
        runfolder =  data["runfolder"]
        finfolder =  data["pathfolder"] + "/" + runfolder
        runcard   =  data["runcard"]
        jobids    =  data["jobid"].split()
        util.spCall(["mkdir", "-p", finfolder])
        logger.info("Retrieving ARC output into " + finfolder)
        try:
            # Retrieve ARC standard output for every job of this run
            for jobid in jobids:
                logger.info(jobid)
                cmd       =  [self.cmd_get, "-j", arcbase, jobid.strip()]
                output    = util.getOutputCall(cmd)
                outputfol = output.split("Results stored at: ")[1].rstrip()
                outputfolder = outputfol.split("\n")[0]
                if outputfolder == "" or (len(outputfolder.split(" ")) > 1):
                    logger.info("Running mv and rm command is not safe here")
                    logger.info("Found blank spaces in the output folder")
                    logger.info("Nothing will be moved to the warmup global folder")
                else:
                    destination = finfolder + "/" + "arc_out_" + runcard + outputfolder
                    util.spCall(["mv", outputfolder, destination])
                    #util.spCall(["rm", "-rf", outputfolder])
        except:
            logger.info("Couldn't find job output in the ARC server")
            logger.info("jobid: " + jobid)
            logger.info("Run arcstat to check the state of the job")
            logger.info("Trying to retrieve data from grid storage anyway")
        # Retrieve warmup from the grid storage warmup folder
        wname = self.warmup_name(runcard, runfolder)
        self.gridw.bring(wname, grid_warmup_dir, finfolder + "/" + wname)

    def _get_data_production(self, db_id):
        """ Given a database entry, retrieve its data from
        the output folder to the folder defined in said db entry
        """
        logger.info("You are going to download all folders corresponding to this runcard from grid output")
        logger.info("Make sure all runs are finished using the -s or -S options!")
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
            logger.info("The starting filename is {}".format(firstName))
            logger.info("The final filename is {}".format(finalName))
            yn = self._press_yes_to_continue("If you are ok with this, press y", fallback = -1)
            if yn == 0:
                break
            initial_seed = int(input("Please, introduce the starting seed (ex: 400): "))
            finalSeed  = int(input("Please, introduce the final seed (ex: 460): "))
        try:
            os.makedirs(self.rfolder)
        except OSError as err:
            if err.errno == 17:
                logger.info("Tried to create folder %s in this directory".format(self.rfolder))
                logger.info("to no avail. We are going to assume the directory was already there")
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

        from pyHepGrid.src.header import finalise_no_cores as n_threads
        # Check which of the seeds actually produced some data
        all_remote = self.output_name_array(self.rcard, self.rfolder, seeds)
        all_output = self.gridw.get_dir_contents(header.grid_output_dir).split()
        remote_tarfiles = list(set(all_remote) & set(all_output))
        logger.info("Found data for {0} of the {1} seeds.".format(len(remote_tarfiles), len(seeds)))

        # Download said data
        tarfiles = self._multirun(self._do_get_data, remote_tarfiles, n_threads, use_counter = True)
        tarfiles = list(filter(None, tarfiles))
        logger.info("Downloaded 0 files", end ='\r')
        logger.info("Downloaded {0} files, extracting...".format(len(tarfiles)))

        # Extract some information from the first tarfile
        for tarfile in tarfiles:
            if self._extract_output_warmup_data(tarfile):
                break

        # Extract all
        dummy    =  self._multirun(self._do_extract_outputData, tarfiles, n_threads)
        os.chdir("..")
        logger.info("Everything saved at {0}".format(pathfolder))
        util.spCall(["mv", self.rfolder, pathfolder])

    def _do_get_data(self, filename):
        """ Multithread wrapper used in get_data_production
        to download information from the grid storage
        """
        local_name = filename.replace("output", "")
        local_file = self.rfolder + "/" + local_name
        self.gridw.bring(filename, header.grid_output_dir, local_file, timeout = header.timeout)
        if os.path.isfile(local_name):
            global counter
            if counter:
                counter.value += 1
                logger.info("Downloaded {0} files ".format(counter.value), end='\r')
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
            logger.info("{0} not found".format(tarfile))
            return -1

        out_dict = {".log" : "log/", ".dat" : "dat/" }
        self.tarw.extract_extension_to(tarfile, out_dict)

        util.spCall(["rm", tarfile])

        return 0

    def get_data(self, db_id, jobid = None, custom_get = None):
        """ External interface for the get_data routines.
        If a custom_get is defined in the pyHepGrid.src.header, it will be used
        instead of the 'native' _get_data_{production/warmup}.
        Custom scripts need to have a public "do_finalise()" function for this to work
        """
        if custom_get:
            import importlib
            sys.path.append(os.path.dirname(os.path.expanduser(custom_get)))
            finalise_mod = importlib.import_module(
                os.path.basename(custom_get.replace(".py","")) )
            finalise_mod.do_finalise()
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
        logger.plain("Active runs: " + str(len(dictC)))

        # Could easily be optimised
        offset = 2
        id_width = max(list(len(str(i["rowid"])) for i in dictC)+[2])+offset
        runcard_width = max(list(len(i["runcard"].strip()) for i in dictC)+[7])+offset
        runname_width = max(list(len(i["runfolder"].strip()) for i in dictC)+[7])+offset
        date_width = max(list(len(str(i['date']).split('.')[0].strip()) for i in dictC)+[10])+offset
        misc_width = 7

        header = "|".join(["id".center(id_width),"runcard".center(runcard_width),
                               "runname".center(runname_width),"date".center(date_width),
                               "misc".center(misc_width)])
        logger.plain(header)
        logger.plain("-"*len(header))

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
            logger.plain("|".join([rid,ruc,run,dat,misc_text]))

    def get_active_dbids(self):
        field_name = "rowid"
        dictC = self._db_list([field_name])
        all_ids = []
        for i in dictC:
            all_ids.append(str(i[field_name]))
        return all_ids

    def _format_args(self, *args, **kwargs):
        raise Exception("Any children classes of pyHepGrid.src.Backend.py should override this method")

    def _get_default_args(self):
        # Defaults arguments that can always go in
        dictionary = {
            'gfal_location' : header.cvmfs_gfal_location,
            'executable' : header.executable_exe,
            'input_folder' : header.grid_input_dir,
            'output_folder' : header.grid_output_dir,
            'warmup_folder' : header.grid_warmup_dir,
            'lhapdf_grid' : header.lhapdf_grid_loc,
            'lhapdf_local' : header.lhapdf_loc,
            'debug' : str(header.debug_level),
            'gfaldir': header.gfaldir,
            'use_gfal' : str(header.use_gfal),
            'events' : str(header.events)
        }
        if header.use_cvmfs_lhapdf:
            dictionary.update({
            "use_cvmfs_lhapdf":header.use_cvmfs_lhapdf,
            "cvmfs_lhapdf_location":header.cvmfs_lhapdf_location})
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
        production_dict = {'Production': None,
                        'seed': seed }

        production_str = self._format_args(production_dict)
        return base_string + production_str

    def _get_warmup_args(self, runcard, runtag, threads=1,
                         sockets=None, port=header.port):
        """ Returns all necessary arguments for warmup running. These arguments
        should match those required by nnlorun.py."""
        base_string = self._make_base_argstring(runcard, runtag)
        warmup_dict = {'Warmup': None,
                       'threads': threads}
        if sockets:
            warmup_dict['port'] = port
            warmup_dict['Host'] = header.server_host
            warmup_dict['Sockets'] = None

        warmup_str = self._format_args(warmup_dict)
        return base_string + warmup_str


def generic_initialise(runcard, warmup=False, production=False, grid=None,
                       overwrite_grid=False, local=False):
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
