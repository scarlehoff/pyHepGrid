from Backend import Backend
from datetime import datetime
import utilities as util
import header

class RunDirac(Backend):
    def __init__(self):
        super(RunDirac, self).__init__()
        self.table     = header.diractable
        self.templ     = header.DIRACSCRIPTDEFAULT
        self.runfolder = None
        self.gridw     = util.GridWrap()
        self.tarw      = util.TarWrap()

    #
    # XRSL file utilities
    # 
    def _write_JDL(self, list_data, start_seed, no_runs, filename = None):
        """ Writes a unique JDL file 
        which instructs the dirac job to run
        """
        if not filename:
            filename = util.unique_filename()
        with open(filename, 'w') as f:
            for i in self.templ:
                f.write(i)
                f.write("\n")
            f.write("Arguments = \"")
            for j in list_data:
                f.write(j)
                f.write(" ")
            f.write("\";\n") 
            f.write("Parameters = {0};\n".format(no_runs))
            f.write("ParameterStart = {0};\n".format(start_seed))
            f.write("ParameterStep = 1;\n".format(start_seed))
            f.write("ParameterFactor = 1;\n".format(start_seed))
        return filename

    def _run_JDL(self, filename):
        """ Sends JDL file to the dirac 
        management system
        """
        cmd = "dirac-wms-job-submit {}".format(filename)
        output  = util.getOutputCall(cmd.split())
        jobids = output.rstrip().strip().split("]")[0].split("[")[-1]
        jobids = jobids.split(", ")
        return jobids

    # Run for DIRAC
    def run_wrap_production(self, runcard):
        """ Wrapper function. It assumes the initialisation stage has already happened
        Writes JDL file with the appropiate information and send procrun number of jobs
        to the diract management system
        """
        rncards, dCards = util.expandCard(runcard)
        self.runfolder  = header.runcardDir
        from header    import baseSeed, producRun, lhapdf_grid_loc, lfndir, lhapdf_loc, NNLOJETexe
        for r in rncards:
            print("> Submitting {0} job(s) for {2} to Dirac, beginning at seed {1}.".format(producRun, baseSeed, r))
            self._checkfor_existing_output(r, dCards[r])
            jdlfile = None
            argbase = [r, dCards[r]]
            args    = argbase + ["%s", lhapdf_grid_loc]
            args = args + [lfndir, lhapdf_loc, NNLOJETexe]
            jdlfile = self._write_JDL(args, baseSeed, producRun)
            joblist   = self._run_JDL(jdlfile)
            # Create daily path
            pathfolder = util.generatePath(False)
            # Create database entr
            jobStr   = ' '.join(joblist)
            dataDict = {'jobid'     : jobStr,
                        'date'      : str(datetime.now()),
                        'pathfolder': pathfolder,
                        'runcard'   : r,
                        'runfolder' : dCards[r],
                        'iseed'     : str(baseSeed),
                        'jobtype'   : "Production",
                        'status'    : "active",}
            self.dbase.insert_data(self.table, dataDict)

def runWrapper(runcard, test = None):
    print("Running dirac job for {0}".format(runcard))
    dirac = RunDirac()
    dirac.run_wrap_production(runcard)

def iniWrapper(runcard, warmupProvided = None):
    print("Initialising dirac for {0}".format(runcard))
    dirac = RunDirac()
    dirac.init_production(runcard, warmupProvided)
