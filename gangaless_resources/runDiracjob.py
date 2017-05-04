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
    # Check/probe functions
    #
    def checkProduction(self, r, runcardDir):
        #
        # Checks whether warmup and production are active
        # in the runcard
        #
        print("Checking warmup/production in the runcard")
        with open(runcardDir + "/" + r, 'r') as f:
            for line in f:
                if "Warmup" in line and ".true." in line:
                    print("Warmup is on")
                    yn = self.input("Do you want to continue (y/n) ")
                    if yn[0] == "y" or yn[0] == "Y":
                        pass
                    else:
                        raise Exception("WRONG RUNCARD")
                if "Production" in line and ".false." in line:
                    print("Production is off")
                    yn = self.input("Do you want to continue (y/n) ")
                    if yn[0] == "y" or yn[0] == "Y":
                        pass
                    else:
                        raise Exception("WRONG RUNCARD")

    def checkExistingOutput(self, r, rname):
        print("Checking whether this runcard has something on the output folder...")
        checknm = r + "-" + rname
        print("Not sure whether check for output works")
        if self.gridw.checkForThis(checknm, "output"):
            print("Runcard " + r + " has at least one file at output")
            yn = self.input("Do you want to delete them all? (y/n) ")
            if yn == "y":
                from header import baseSeed, producRun
                for seed in range(baseSeed, baseSeed + producRun):
                    filename = "output" + checkname + "-" + str(seed) + ".tar.gz"
                    self.gridw.delete(filename, "output")
            else:
                print("Not deleting... exiting...")
                raise Exception("Runcard already exists")

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

    def bringWarmupFiles(self, runcard, rname):
        from utilities import getOutputCall, spCall
        gridFiles = []
        outnm = self.warmupName(runcard, rname)
        tmpnm = "tmp.tar.gz"
        ## First bring the warmup .tar.gz
        self.gridw.bring(outnm, "warmup", tmpnm)
        gridp = [".RRa", ".RRb", ".vRa", ".vRb", ".vBa", ".vBb"]
        ## Now list the files inside the .tar.gz and extract the grid files
        outlist = self.tarw.listFilesTar(tmpnm)
        logfile = ""
        for fileRaw in outlist:
            if ".log" in fileRaw:
                file = fileRaw.split(" ")[-1]
                logfile = file
            if len(fileRaw.split(".y")) == 1: continue
            file = fileRaw.split(" ")[-1]
            for grid in gridp:
                if grid in file: gridFiles.append(file)
        ## And now extract those particular files
        extractFiles = gridFiles + [logfile]
        self.tarw.extractThese(tmpnm, extractFiles)
        ## Tag log file as warmup
        newlog = logfile + "-warmup"
        cmd = ["mv", logfile, newlog]
        spCall(cmd)
        spCall(["rm", "tmp.tar.gz"])
        gridFiles.append(newlog)
        for i in gridFiles:
            spCall(["chmod", "a+wrx", i])
        return gridFiles

    #
    # Wrappers
    # 
    def iniWrap(self, runcard, warmupProvided = None):
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
        for i in rncards:
            # Check whether warmup/production is active in the runcard
            self.checkProduction(i, runFol)
            rname   = dCards[i]
            tarfile = i + rname + ".tar.gz"
            copy(runFol + "/" + i, getcwd())
            if warmupProvided:
                warmupFiles = [warmupProvided]
            else:
                warmupFiles = self.bringWarmupFiles(i, rname)
            self.tarw.tarFiles(files + [i] + warmupFiles, tarfile)
            if self.gridw.checkForThis(tarfile, "input"):
                print("Removing old version of " + tarfile + " from Grid Storage")
                self.gridw.delete(tarfile, "input")
            print("Sending " + tarfile + " to lfn:input/")
            self.gridw.send(tarfile, "input")
            spCall(["rm", i, tarfile] + warmupFiles)
        spCall(["rm"] + files)

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
    dirac.iniWrap(runcard, warmupProvided)
