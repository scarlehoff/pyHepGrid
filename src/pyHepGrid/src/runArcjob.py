from pyHepGrid.src.Backend import Backend
from datetime import datetime
import pyHepGrid.src.utilities as util
import pyHepGrid.src.header as header
import pyHepGrid.src.socket_api as sapi

class RunArc(Backend):
    def __init__(self, prod=False, arcscript = None, **kwargs):
        super(RunArc, self).__init__(**kwargs)
        if not prod:
            self.table     = header.arctable
        else:
            self.table     = header.arcprodtable
        self.arcbd     = header.arcbase
        if arcscript:
            self.templ = arcscript
        else:
            self.templ = header.ARCSCRIPTDEFAULT
        self.runfolder = header.runcardDir
        self.gridw     = util.GridWrap()
        self.tarw      = util.TarWrap()

    def _format_args(self, input_args):
        if isinstance(input_args, dict):
            string_arg = ""
            for key in input_args.keys():
                arg_value = input_args[key]
                if arg_value is not None:
                    string_arg += " \"--{0}\" \"{1}\"".format(key, arg_value)
                else:
                    string_arg += " \"--{0}\" ".format(key)
            return string_arg
        elif isinstance(input_args, str):
            return "\"{}\"".format(input_args)
        elif isinstance(input_args, list):
            return "\"{}\"".format("\" \"".join(input_args))
        else:
            header.logger.warning("Arguments: {}".format(input_args))
            raise Exception("Type of input arguments: {} not regocnised in ARC ._format_args".format(type(input_args)))

    def _write_XRSL(self, dictData, filename = None):
        """ Writes a unique XRSL file
        which instructs the arc job to run
        """
        if not filename:
            filename = util.unique_filename()
        with open(filename, 'w') as f:
            for i in self.templ:
                f.write(i)
                f.write('\n')
            for key in dictData:
                f.write("(" + key)
                argument_value = dictData[key].strip()
                if argument_value[0] == "\"" and argument_value[-1] == "\"":
                    f.write(" = {})\n".format(argument_value))
                else:
                    f.write(" = \"{}\")\n".format(argument_value))
        return filename

    def _run_XRSL(self, filename, test = False):
        """ Sends XRSL to the queue defined in header
        If test = True, use test queue
        """
        import random
        from pyHepGrid.src.header import arc_direct
        from pyHepGrid.src.header import split_dur_ce
        if test:
            from pyHepGrid.src.header import ce_test as ce
        else:
            from pyHepGrid.src.header import ce_base as ce
            if split_dur_ce and ".dur.scotgrid.ac.uk" in ce: # Randomise ce at submission time to reduce load
                ce = random.choice(["ce1.dur.scotgrid.ac.uk","ce2.dur.scotgrid.ac.uk"])

        cmd = "arcsub -c {0} {1} -j {2}".format(ce, filename, self.arcbd)
        # Can only use direct in Durham. Otherwise fails!
        # Speeds up submission (according to Stephen)
        if arc_direct and ".dur.scotgrid.ac.uk" in ce:
            cmd += " -S org.nordugrid.gridftpjob --direct "
        output = util.getOutputCall(cmd.split())
        jobid = output.split("jobid:")[-1].rstrip().strip()
        return jobid

    # Runs for ARC
    def run_wrap_warmup(self, test = None, expandedCard = None):
        """ Wrapper function. It assumes the initialisation stage has already happend
            Writes XRSL file with the appropiate information and send one single job
            (or n_sockets jobs) to the queue

            ExpandedCard is an override for util.expandCard for use in auto-resubmission
        """
        from pyHepGrid.src.header import warmupthr, jobName, warmup_base_dir
        # runcard names (of the form foo.run)
        # dCards, dictionary of { 'runcard' : 'name' }, can also include extra info
        if expandedCard is None:
            rncards, dCards = util.expandCard()
        else:
            rncards, dCards = expandedCard
        if test:
            from pyHepGrid.src.header import ce_test as ce
        else:
            from pyHepGrid.src.header import ce_base as ce

        if header.sockets_active > 1:
            sockets = True
            n_sockets = header.sockets_active
            if ".dur.scotgrid.ac.uk" not in ce:
                #Can't submit sockets elsewhere than Durham!!!!!!!
                header.logger.info("Current submission computing element: {0}".format(ce))
                header.logger.critical("Can't submit socketed warmups to locations other than Durham")
        else:
            sockets = False
            n_sockets = 1
            if test:
                job_type = "Warmup Test"
            else:
                job_type = "Warmup"

        # Sanity checks for test queue
        if test and warmupthr > 2:
            self._press_yes_to_continue("  \033[93m WARNING:\033[0m About to submit job(s) to the test queue with {0} threads each.".format(warmupthr))
        if test and n_sockets > 2:
            self._press_yes_to_continue("  \033[93m WARNING:\033[0m About to submit job(s) to the test queue with {0} sockets each.".format(n_sockets))

        self.runfolder = header.runcardDir
        # loop over all .run files defined in runcard.py

        header.logger.info("Runcards selected: {0}".format(" ".join(r for r in rncards)))
        port = header.port
        for r in rncards:
            if n_sockets > 1:
                # Automagically activates the socket and finds the best port for it!
                port = sapi.fire_up_socket_server(header.server_host, port, n_sockets,
                                                  header.wait_time, header.socket_exe,
                                                  tag="{0}-{1}".format(r,dCards[r]))
                job_type = "Socket={}".format(port)

            # Check whether this run has something on the gridStorage
            self.check_for_existing_warmup(r, dCards[r])
            # Generate the XRSL file
            arguments = self._get_warmup_args(r, dCards[r], threads=warmupthr,
                                              sockets=sockets, port=port)
            dictData = {'arguments'   : arguments,
                        'jobName'     : jobName,
                        'count'       : str(warmupthr),
                        'countpernode': str(warmupthr),}
            xrslfile = self._write_XRSL(dictData)
            header.logger.debug(" > Path of xrsl file: {0}".format(xrslfile))

            jobids = []
            keyquit = None
            try:
                for i_socket in range(n_sockets):
                    # Run the file
                    jobids.append(self._run_XRSL(xrslfile, test=test))
            except Exception as interrupt:
                print("\n")
                header.logger.error("Submission error encountered. Inserting all successful submissions to database")
                keyquit = interrupt
            # Create daily path
            if warmup_base_dir is not None:
                pathfolder = util.generatePath(warmup=True)
            else:
                pathfolder = "None"
            # Create database entry
            dataDict = {'jobid'     : ' '.join(jobids),
                        'date'      : str(datetime.now()),
                        'pathfolder': pathfolder,
                        'runcard'   : r,
                        'runfolder' : dCards[r],
                        'jobtype'   : job_type,
                        'status'    : "active",}
            if len(jobids) > 0:
                self.dbase.insert_data(self.table, dataDict)
            else:
                header.logger.critical("No jobids returned, no database entry inserted for submission: {0} {1}".format(r, dCards[r]))
            port += 1
            if keyquit is not None:
                raise keyquit

    def run_wrap_production(self, test = None):
        """ Wrapper function. It assumes the initialisation stage has already happend
            Writes XRSL file with the appropiate information and send a producrun
            number of jobs to the arc queue
        """
        from pyHepGrid.src.header import baseSeed, producRun, jobName, lhapdf_grid_loc, lhapdf_loc, executable_exe

        # runcard names (keys)
        # dCards, dictionary of { 'runcard' : 'name' }
        rncards, dCards = util.expandCard()
        self.runfolder = header.runcardDir
        job_type = "Production"

        header.logger.info("Runcards selected: {0}".format(" ".join(r for r in rncards)))
        for r in rncards:
            joblist = []
            # Check whether this run has something on the gridStorage
            self.check_for_existing_output(r, dCards[r])
            # use the same unique name for all seeds since
            # we cannot multiprocess the arc submission
            xrslfile = None
            keyquit = None

            # Sanity check for test queue
            if test and producRun > 5:
                self._press_yes_to_continue("  \033[93m WARNING:\033[0m About to submit a large number ({0}) of jobs to the test queue.".format(producRun))

            try:
                for seed in range(baseSeed, baseSeed + producRun):
                    arguments = self._get_prod_args(r, dCards[r], seed)
                    dictData = {'arguments'   : arguments,
                                'jobName'     : jobName,
                                'count'       : str(1),
                                'countpernode': str(1),}
                    xrslfile = self._write_XRSL(dictData, filename = xrslfile)
                    if(seed == baseSeed):
                        header.logger.debug(" > Path of xrsl file: {0}".format(xrslfile))
                # Run the file
                    jobid = self._run_XRSL(xrslfile, test=test)
                    joblist.append(jobid)
            except Exception as interrupt:
                print("\n")
                header.logger.error("Submission error encountered. Inserting all successful submissions to database")
                keyquit = interrupt
            # Create daily path
            pathfolder = util.generatePath(warmup=False)
            # Create database entry
            jobStr = ' '.join(joblist)
            dataDict = {'jobid'     : jobStr,
                        'date'      : str(datetime.now()),
                        'pathfolder': pathfolder,
                        'runcard'   : r,
                        'jobtype'   : job_type,
                        'runfolder' : dCards[r],
                        'iseed'     : str(baseSeed),
                        'no_runs'   : str(producRun),
                        'status'    : "active",}
            if len(joblist) > 0:
                self.dbase.insert_data(self.table, dataDict)
            else:
                header.logger.critical("No jobids returned, no database entry inserted for submission: {0} {1}".format(r, dCards[r]))
            if keyquit is not None:
                raise keyquit

def runWrapper(runcard, test = None, expandedCard = None):
    header.logger.info("Running arc job for {0}".format(runcard))
    arc = RunArc(arcscript=header.ARCSCRIPTDEFAULT)
    arc.run_wrap_warmup(test, expandedCard)

def runWrapperProduction(runcard, test=None):
    header.logger.info("Running arc job for {0}".format(runcard))
    arc = RunArc(prod=True,arcscript=header.ARCSCRIPTDEFAULTPRODUCTION)
    arc.run_wrap_production(test)


####### Testing routines - just a wrapper to get the args for nnlojob

def testWrapper(r, dCards):
    header.logger.info("Running arc job for {0}".format(r))
    arc = RunArc(arcscript=header.ARCSCRIPTDEFAULT)
    return arc._get_warmup_args(r, dCards[r], threads=header.warmupthr,
                                                        sockets=False)

def testWrapperProduction(r, dCards):
    header.logger.info("Running arc job for {0}".format(r))
    arc = RunArc(prod=True,arcscript=header.ARCSCRIPTDEFAULTPRODUCTION)
    return arc._get_prod_args(r, dCards[r], 1)

# Code graveyard
def iniWrapper(runcard, warmup=None):
    header.logger.info("Initialising Arc for {0}".format(runcard))
    arc = RunArc()
    arc.init_warmup(warmup)

def iniWrapperProduction(runcard, warmup=None):
    header.logger.info("Initialising Arc for {0}".format(runcard))
    arc = RunArc()
    if warmup:
        arc.init_warmup(warmup)
    else:
        arc.init_production()
