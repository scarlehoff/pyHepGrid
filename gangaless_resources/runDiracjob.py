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
    def writeJDL(self, listData):
        with open(self.jdlfile, 'w') as f:
            for i in self.templ:
                f.write(i)
                f.write("\n")
            f.write("Arguments = \"")
            for j in listData:
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
        from header    import baseSeed, producRun
        from utilities import expandCard, generatePath
        from datetime  import datetime
        rncards, dCards, runFol = expandCard(runcard)
        self.runfolder          = runFol
        for r in rncards:
            joblist = []
            #self.checkExistingOutput(r, dCards[r])
            for seed in range(baseSeed, baseSeed + producRun):
                # Genereate and run a file per seed number
                argbase = [r, dCards[r]]
                args    = argbase + [str(seed)]
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
            self.dbase.insertData(self.table, dataDict)

def runWrapper(runcard, test = None):
    print("Running dirac job for ", runcard)
    dirac = RunDirac()
    dirac.runWrap(runcard)

def iniWrapper(runcard, warmupProvided = None):
    print("Initialising dirac for ", runcard)
    dirac = RunDirac()
    dirac.iniProduction(runcard, warmupProvided)
