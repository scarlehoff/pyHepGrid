from Backend import Backend
class RunArc(Backend):
    def __init__(self):
        super(RunArc, self).__init__()
        from header import  arctable, arcbase, ARCSCRIPTDEFAULT
        from utilities import GridWrap, TarWrap
        self.table     = arctable
        self.arcbd     = arcbase
        self.templ     = ARCSCRIPTDEFAULT
        self.runfolder = None
        self.gridw     = GridWrap()
        self.tarw      = TarWrap()
        self.xrslfile  = "runArcJob.xrsl"

    #
    # Check/probe functions
    #
    def checkWarmup(self, r, runcardDir):
        #
        # Checks whether warmup and production are active
        # in the runcard
        #
        print("Checking warmup/production in the runcard")
        with open(runcardDir + "/" + r, 'r') as f:
            for line in f:
                if "Warmup" in line and ".false." in line:
                    print("Warmup is off")
                    yn = self.input("Do you want to continue (y/n) ")
                    if yn[0] == "y" or yn[0] == "Y":
                        pass
                    else:
                        raise Exception("WRONG RUNCARD")
                if "Production" in line and ".true." in line:
                    print("Production is on")
                    yn = self.input("Do you want to continue (y/n) ")
                    if yn[0] == "y" or yn[0] == "Y":
                        pass
                    else:
                        raise Exception("WRONG RUNCARD")

    def checkExistingWarmup(self, r, rname):
        #
        # Checks for any output from this runcard in the grid 
        #
        print("Checking whether this runcard is already at lfn:warmup")
        checknm = self.warmupName(r, rname)
        if self.gridw.checkForThis(checknm, "warmup"):
            print("File " + checknm + " already exist at lfn:warmup")
            yn = self.input("Do you want to delete this file? (y/n) ")
            if yn == "y":
                self.gridw.delete(checknm, "warmup")
            else:
                print("Not deleting... exiting")
                raise Exception("Runcard already exists")

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

    #
    # Wrappers
    # 
    def iniWrap(self, runcard, warmup = None):
        from utilities import expandCard, spCall
        from shutil import copy
        from os import getcwd, path
        rncards, dCards, runFol = expandCard(runcard)
        if "NNLOJETdir" not in dCards:
            from header import NNLOJETdir
        else:
            NNLOJETdir = dCards["NNLOJETdir"]
        from header import NNLOJETexe
        nnlojetfull = NNLOJETdir + "/driver/" + NNLOJETexe
        if not path.isfile(nnlojetfull): 
            raise Exception("Could not find NNLOJET executable")
        copy(nnlojetfull, getcwd())
        files = [NNLOJETexe]
        if warmup: 
            files = files + [warmup]
        for i in rncards:
            # Check whether warmup/production is active in the runcard
            if not path.isfile(runFol + "/" + i):
                print("Could not find runcard %s", i)
                yn = self.input("Do you want to continue? (y/n): ")
                if yn == y:
                    continue
                else:
                    raise Exception("Could not find runcard")
            self.checkWarmup(i, runFol)
            rname   = dCards[i]
            tarfile = i + rname + ".tar.gz"
            copy(runFol + "/" + i, getcwd())
            self.tarw.tarFiles(files + [i], tarfile)
            if self.gridw.checkForThis(tarfile, "input"):
                print("Removing old version of " + tarfile + " from Grid Storage")
                self.gridw.delete(tarfile, "input")
            print("Sending " + tarfile + " to lfn:input/")
            self.gridw.send(tarfile, "input")
            spCall(["rm", i, tarfile])
        spCall(["rm", NNLOJETexe])

    def runWrap(self, runcard, test = None):
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

def runWrapper(runcard, test = None):
    print("Running arc job for ", runcard)
    arc = RunArc()
    arc.runWrap(runcard, test)

def iniWrapper(runcard, warmup=None):
    print("Initialising Arc for ", runcard)
    arc = RunArc()
    arc.iniWrap(runcard, warmup)
