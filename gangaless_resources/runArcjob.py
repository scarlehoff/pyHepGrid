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
            print("Arguments: {}".format(input_args))
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

        if header.sockets_active > 1:
            sockets = True
            n_sockets = header.sockets_active
            port = header.port
            job_type = "Socket={}".format(port)
        else:
            sockets = False
            n_sockets = 1
            if test:
                job_type = "Warmup Test"
            else:
                job_type = "Warmup"

        self.runfolder = header.runcardDir
        from header import warmupthr, jobName, warmup_base_dir
        # loop over al .run files defined in runcard.py

        for r in rncards:
            # Check whether this run has something on the gridStorage
            self._checkfor_existing_warmup(r, dCards[r])
            # Generate the XRSL file
            arguments = self._get_warmup_args(r, dCards[r], threads=warmupthr, sockets = sockets)
            dictData = {'arguments'   : arguments,
                        'jobName'     : jobName,
                        'count'       : str(warmupthr),
                        'countpernode': str(warmupthr),}
            xrslfile = self._write_XRSL(dictData)
            print(" > Path of xrsl file: {0}".format(xrslfile))

            jobids = []
            for i_socket in range(n_sockets):
                # Run the file
                jobids.append(self._run_XRSL(xrslfile, test=test))

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
            self.dbase.insert_data(self.table, dataDict)

    def run_wrap_production(self, runcard, test = None):
        """ Wrapper function. It assumes the initialisation stage has already happend
            Writes XRSL file with the appropiate information and send a producrun 
            number of jobs to the arc queue
        """
        # runcard names (keys)
        # dCards, dictionary of { 'runcard' : 'name' }
        rncards, dCards = util.expandCard(runcard)
        self.runfolder = header.runcardDir
        job_type = "Production"
        from header import baseSeed, producRun, jobName, lhapdf_grid_loc, lfndir, lhapdf_loc, NNLOJETexe, lfn_output_dir
        for r in rncards:
            joblist = []
            # Check whether this run has something on the gridStorage
            self._checkfor_existing_output(r, dCards[r])
            # use the same unique name for all seeds since 
            # we cannot multiprocess the arc submission
            xrslfile = None 
            for seed in range(baseSeed, baseSeed + producRun):
                arguments = self._get_prod_args(r, dCards[r], seed)
                dictData = {'arguments'   : arguments,
                            'jobName'     : jobName,
                            'count'       : str(1),
                            'countpernode': str(1),}
                xrslfile = self._write_XRSL(dictData, filename = xrslfile)
                if(seed == baseSeed):
                    print(" > Path of xrsl file: {0}".format(xrslfile))
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
                        'iseed'     : str(baseSeed),
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




# Code graveyard
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
