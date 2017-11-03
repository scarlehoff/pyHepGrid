from Backend import Backend
from datetime import datetime
import utilities as util
import header

class RunArc(Backend):
    def __init__(self, arcscript = None): 
        super(RunArc, self).__init__()
        self.table     = header.arctable
        self.arcbd     = header.arcbase
        if arcscript:
            self.templ = arcscript
        else:
            self.templ = header.ARCSCRIPTDEFAULT
        self.runfolder = header.runcardDir
        self.gridw     = util.GridWrap()
        self.tarw      = util.TarWrap()

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
                f.write(" = \"" + dictData[key])
                f.write("\")\n")
        return filename

    def _run_XRSL(self, filename, test = False):
        """ Sends XRSL to the queue defined in header
        If test = True, use test queue
        """
        if test:
            from header import ce_test as ce
        else:
            from header import ce_base as ce
        cmd = "arcsub -c {0} {1}".format(ce, filename)
        output = util.getOutputCall(cmd.split())
        jobid = output.split("jobid:")[-1].rstrip().strip()
        return jobid

    # Runs for ARC
    def run_wrap_warmup(self, runcard, test = None):
        """ Wrapper function. It assumes the initialisation stage has already happend
            Writes XRSL file with the appropiate information and send one single job
            (or n_sockets jobs) to the queue
        """
        # runcard names (of the form foo.run)
        # dCards, dictionary of { 'runcard' : 'name' }, can also include extra info
        rncards, dCards = util.expandCard(runcard)
        # check whether this is a socketed run
        if "sockets_active" in dCards.keys():
            sockets = True
            n_sockets = int(dCards["sockets_active"])
            if "port" in dCards.keys():
                port = int(dCards["port"])
            else:
                port = 8888
            job_type = "Socket={}".format(port)
        else:
            sockets = False
            n_sockets = 1
            if test:
                job_type = "Warmup Test"
            else:
                job_type = "Warmup"

        self.runfolder = header.runcardDir
        from header import warmupthr, lhapdf_grid_loc, lfndir, lhapdf_loc, jobName
        # loop over al .run files defined in runcard.py
        for r in rncards:
            # Check whether this run has something on the gridStorage
            self._checkfor_existing_warmup(r, dCards[r])
            # Generate the XRSL file
            argument_base  = "" + r + "\""
            argument_base += " \"" + dCards[r] + "\""
            argument_base += " \"" + str(warmupthr) + "\""
            argument_base += " \"" + lhapdf_grid_loc + "\""
            argument_base += " \"" + lfndir + "\""
            argument_base += " \"" + lhapdf_loc + ""
            jobids = []
            for i_socket in range(n_sockets):
                arguments = argument_base
                if sockets:
                    arguments += "\" \"" + str(port) + "\""
                    arguments += " \"" + str(n_sockets) + "\""
                    arguments += " \"" + str(i_socket+1) + ""
                dictData = {'arguments'   : arguments,
                            'jobName'     : jobName,
                            'count'       : str(warmupthr),
                            'countpernode': str(warmupthr),}
                xrslfile = self._write_XRSL(dictData)
                # Run the file
                jobids.append(self._run_XRSL(xrslfile, test=test))
            # Create daily path
            pathfolder = util.generatePath(warmup=True)
            # Create database entry
            dataDict = {'jobid'     : ' '.join(jobids),
                        'date'      : str(datetime.now()),
                        'pathfolder': pathfolder,
                        'runcard'   : r,
                        'runfolder' : dCards[r],
                        'jobtype'   : job_type,
                        'status'    : "active",}
            self.dbase.insert_data(self.table, dataDict)

    def run_wrap_production(self, runcard, test = None):
        """ Wrapper function. It assumes the initialisation stage has already happend
            Writes XRSL file with the appropiate information and send a producrun 
            number of jobs to the arc queue
        """
        # runcard names (keys)
        # dCards, dictionary of { 'runcard' : 'name' }
        rncards, dCard = util.expandCard(runcard)
        self.runfolder = header.runcardDir
        job_type = "Production"
        from header import baseSeed, producRun, jobName
        for r in rncards:
            joblist = []
            # Check whether this run has something on the gridStorage
            self._checkfor_existing_output(r, dCards[r])
            # use the same unique name for all seeds since 
            # we cannot multiprocess the arc submission
            xrslfile = None 
            for seed in range(baseSeed, baseSeed + producRun):
                arguments  = "" + r + "\""
                arguments += " \"" + dCards[r] + "\""
                arguments += " \"" + str(seed) + ""
                dictData = {'arguments'   : arguments,
                            'jobName'     : jobName,
                            'count'       : str(1),
                            'countpernode': str(1),}
                xrslfile = self._write_XRSL(dictData, filename = xrslfile)
                # Run the file
                jobid = self._run_XRSL(xrslfile, test=test)
                joblist.append(jobid)
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
                        'status'    : "active",}
            self.dbase.insert_data(self.table, dataDict)

def runWrapper(runcard, test = None):
    print("Running arc job for {0}".format(runcard))
    arc = RunArc(header.ARCSCRIPTDEFAULT)
    arc.run_wrap_warmup(runcard, test)

def runWrapperProduction(runcard, test=None):
    print("Running arc job for {0}".format(runcard))
    arc = RunArc(header.ARCSCRIPTDEFAULTPRODUCTION)
    arc.run_wrap_production(runcard, test)

def iniWrapper(runcard, warmup=None):
    print("Initialising Arc for {0}".format(runcard))
    arc = RunArc()
    arc.init_warmup(runcard, warmup)

def iniWrapperProduction(runcard, warmup=None):
    print("Initialising Arc for {0}".format(runcard))
    arc = RunArc()
    if warmup:
        arc.init_warmup(runcard, warmup)
    else:
        arc.init_production(runcard)
