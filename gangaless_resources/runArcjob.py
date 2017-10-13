from Backend import Backend
class RunArc(Backend):
    def __init__(self, arcscript = None): 
        super(RunArc, self).__init__()
        from header import  arctable, arcbase, ARCSCRIPTDEFAULT
        from utilities import GridWrap, TarWrap
        self.table     = arctable
        self.arcbd     = arcbase
        if arcscript:
            self.templ = arcscript
        else:
            self.templ = ARCSCRIPTDEFAULT
        self.runfolder = None
        self.gridw     = GridWrap()
        self.tarw      = TarWrap()
        self.xrslfile  = "runArcJob.xrsl"

    #
    # XRSL file utilities
    # 
    def writeXRSL(self, dictData):
        with open(self.xrslfile, 'w') as f:
            for i in self.templ:
                f.write(i)
                f.write('\n')
            for key in dictData:
                f.write("(" + key)
                f.write(" = \"" + dictData[key])
                f.write("\")\n")

    def runXRSL(self, test = False):
        from utilities import getOutputCall
        from header import ce_base, ce_test
        if test:
            from header import ce_test as ce
        else:
            from header import ce_base as ce
        cmdbase = ['arcsub', '-c', ce]
        cmd     = cmdbase + [self.xrslfile]
        output  = getOutputCall(cmd)
        jobid   = output.split("jobid:")[-1].rstrip().strip()
        return jobid

    # Runs for ARC
    def runWrapWarmup(self, runcard, test = None):
        from utilities import expandCard, generatePath
        from header import warmupthr, jobName
        from datetime import datetime
        # runcard names (keys)
        # dCards, dictionary of { 'runcard' : 'name' }
        # runFol = folder where the runcards are
        rncards, dCards, runFol = expandCard(runcard)
        self.runfolder = runFol
        for r in rncards:
            # Check whether this run has something on the gridStorage
            self.checkExistingWarmup(r, dCards[r])
            # Generate the XRSL file
            arguments  = "" + r + "\""
            arguments += " \"" + dCards[r] + "\""
            arguments += " \"" + str(warmupthr) + ""
            dictData = {'arguments'   : arguments,
                        'jobName'     : jobName,
                        'count'       : str(warmupthr),
                        'countpernode': str(warmupthr),}
            self.writeXRSL(dictData)
            # Run the file
            jobid = self.runXRSL(test)
            # Create daily path
            pathfolder = generatePath(True)
            # Create database entry
            dataDict = {'jobid'     : jobid,
                        'date'      : str(datetime.now()),
                        'pathfolder': pathfolder,
                        'runcard'   : r,
                        'runfolder' : dCards[r],
                        'status'    : "active",}
            self.dbase.insertData(self.table, dataDict)

    def runWrapProduction(self, runcard, test = None):
        from utilities import expandCard, generatePath
        from header import jobName, baseSeed, producRun
        from datetime import datetime
        # runcard names (keys)
        # dCards, dictionary of { 'runcard' : 'name' }
        # runFol = folder where the runcards are
        rncards, dCards, runFol = expandCard(runcard)
        self.runfolder = runFol
        for r in rncards:
            joblist = []
            # Check whether this run has something on the gridStorage
            self.checkExistingOutput(r, dCards[r])
            # Generate the XRSL file
            for seed in range(baseSeed, baseSeed + producRun):
                arguments  = "" + r + "\""
                arguments += " \"" + dCards[r] + "\""
                arguments += " \"" + str(seed) + ""
                dictData = {'arguments'   : arguments,
                            'jobName'     : jobName,
                            'count'       : str(1),
                            'countpernode': str(1),}
                self.writeXRSL(dictData)
                # Run the file
                jobid = self.runXRSL(test)
                joblist.append(jobid)
            # Create daily path
            pathfolder = generatePath(False)
            # Create database entry
            jobStr = ' '.join(joblist)
            dataDict = {'jobid'     : jobStr,
                        'date'      : str(datetime.now()),
                        'pathfolder': pathfolder,
                        'runcard'   : r,
                        'runfolder' : dCards[r],
                        'status'    : "active",}
            self.dbase.insertData(self.table, dataDict)

def runWrapper(runcard, test = None):
    print("Running arc job for {0}".format(runcard))
    from header import ARCSCRIPTDEFAULT
    arc = RunArc(ARCSCRIPTDEFAULT)
    arc.runWrapWarmup(runcard, test)

def runWrapperProduction(runcard, test=None):
    print("Running arc job for {0}".format(runcard))
    from header import ARCSCRIPTDEFAULTPRODUCTION
    arc = RunArc(ARCSCRIPTDEFAULTPRODUCTION)
    arc.runWrapProduction(runcard, test)

def iniWrapper(runcard, warmup=None):
    print("Initialising Arc for {0}".format(runcard))
    arc = RunArc()
    arc.iniWarmup(runcard, warmup)

def iniWrapperProduction(runcard, warmup=None):
    print("Initialising Arc for {0}".format(runcard))
    arc = RunArc()
    if warmup:
        arc.iniWarmup(runcard, warmup)
    else:
        arc.iniProduction(runcard)
