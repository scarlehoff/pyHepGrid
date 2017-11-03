from Backend import Backend
class RunDirac(Backend):
    def __init__(self):
        super(RunDirac, self).__init__()
        from header    import diractable, DIRACSCRIPTDEFAULT
        from utilities import GridWrap, TarWrap
        self.table     = diractable
        self.templ     = DIRACSCRIPTDEFAULT
        self.runfolder = None
        self.gridw     = GridWrap()
        self.tarw      = TarWrap()
        self.jdlfile   = "runDiracJob.jdl"

        #
    # XRSL file utilities
    # 
    def writeJDL(self, list_data):
        with open(self.jdlfile, 'w') as f:
            for i in self.templ:
                f.write(i)
                f.write("\n")
            f.write("Arguments = \"")
            for j in list_data:
                f.write(j)
                f.write(" ")
            f.write("\";\n")

    def runJDL(self):
        from utilities import getOutputCall
        cmdbase = ["dirac-wms-job-submit"]
        cmd     = cmdbase + [self.jdlfile]
        output  = getOutputCall(cmd)
        jobid   = output.rstrip().strip().split(" ")[-1]
        return jobid

    # Run for DIRAC
    def runWrap(self, runcard):
        from header    import baseSeed, producRun, lhapdf_grid_loc, lfndir, lhapdf_loc, NNLOJETexe
        from header import runcardDir as runFol
        from utilities import expandCard, generatePath
        from datetime  import datetime
        rncards, dCards = expandCard(runcard)
        self.runfolder  = runFol
        for r in rncards:
            print("> Submitting {0} job(s) for {2} to Dirac, beginning at seed {1}.".format(producRun, baseSeed, r))
            joblist = []
            #self._checkfor_existing_output(r, dCards[r])
            for seed in range(baseSeed, baseSeed + producRun):
                # From DIRAC.py
                # RUNCARD = sys.argv[1]
                # RUNNAME = sys.argv[2]
                # SEED    = sys.argv[3] 
                # lhapdf_grid_loc = sys.argv[4] 
                # LFNDIR = sys.argv[5]
                # LHAPDF_LOC = sys.argv[6]
                # Genereate and run a file per seed number
                argbase = [r, dCards[r]]
                args    = argbase + [str(seed), lhapdf_grid_loc]
                args = args + [lfndir, lhapdf_loc, NNLOJETexe]
                self.writeJDL(args)
                jobid   = self.runJDL()
                joblist.append(jobid)
            # Create daily path
            pathfolder = generatePath(False)
            # Create database entr
            jobStr   = ' '.join(joblist)
            dataDict = {'jobid'     : jobStr,
                        'date'      : str(datetime.now()),
                        'pathfolder': pathfolder,
                        'runcard'   : r,
                        'runfolder' : dCards[r],
                        'status'    : "active",}
            self.dbase.insert_data(self.table, dataDict)

def runWrapper(runcard, test = None):
    print("Running dirac job for {0}".format(runcard))
    dirac = RunDirac()
    dirac.runWrap(runcard)

def iniWrapper(runcard, warmupProvided = None):
    print("Initialising dirac for {0}".format(runcard))
    dirac = RunDirac()
    dirac.init_production(runcard, warmupProvided)
