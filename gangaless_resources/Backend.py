class Backend(object):
    cDONE = 0
    cWAIT = 1
    cRUN = 2
    cFAIL = -1
    cUNK = 99

    def __init__(self):
        from utilities import TarWrap, GridWrap
        from header import dbname, baseSeed
        import dbapi
        self.tarw  = TarWrap()
        self.gridw = GridWrap()
        self.dbase = dbapi.database(dbname)
        self.table = None
        self.bSeed = baseSeed
        self.jobtype_get = {
                'P' : self.getDataProduction,
                'W' : self.getDataWarmup,
                'S' : self.getDataWarmup
                }

    # Check/Probe functions
    def checkProduction(self, r, runcardDir):
        #
        # Checks whether warmup and production are active
        # in the runcard
        #
        print("Checking warmup/production in runcard %s" % r)
        with open(runcardDir + "/" + r, 'r') as f:
            for line in f:
                if "Warmup" in line and ".true." in line.lower():
                    print("Warmup is on")
                    yn = input("Do you want to continue (y/n) ").lower()
                    if yn.startswith("y"):
                        pass
                    else:
                        raise Exception("WRONG RUNCARD")
                if "Production" in line and ".false." in line.lower():
                    print("Production is off")
                    yn = input("Do you want to continue (y/n) ").lower()
                    if yn.startswith("y"):
                        pass
                    else:
                        raise Exception("WRONG RUNCARD")

    def checkWarmup(self, r, runcardDir):
        #
        # Checks whether warmup and production are active
        # in the runcard
        #
        print("Checking warmup/production in runcard %s" % r)
        with open(runcardDir + "/" + r, 'r') as f:
            for line in f:
                if "Warmup" in line and ".false." in line.lower():
                    print("Warmup is off")
                    yn = input("Do you want to continue (y/n) ").lower()
                    if yn.startswith("y"):
                        pass
                    else:
                        raise Exception("WRONG RUNCARD")
                if "Production" in line and ".true." in line.lower():
                    print("Production is on")
                    yn = input("Do you want to continue (y/n) ").lower()
                    if yn.startswith("y"):
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
            yn = input("Do you want to delete this file? (y/n) ").lower()
            if yn.startswith("y"):
                self.gridw.delete(checknm, "warmup")
            else:
                print("Not deleting... exiting")
                raise Exception("Runcard already exists")

    # Check whether the output folders already exist in the grid storage system
    def checkExistingOutput(self, r, rname):
        print("Checking whether this runcard has something on the output folder...")
        checkname = r + "-" + rname
        print("Not sure whether check for output works")
        if self.gridw.checkForThis(checkname, "output"):
            print("Runcard " + r + " has at least one file at output")
            yn = input("Do you want to delete them all? (y/n) ").lower()
            if yn.startswith("y"):
                from header import baseSeed, producRun
                for seed in range(baseSeed, baseSeed + producRun):
                    filename = "output" + checkname + "-" + str(seed) + ".tar.gz"
                    self.gridw.delete(filename, "output")
            else:
                print("Not deleting... exiting...")
                raise Exception("Runcard already exists")




    def multiRun(self, function, arguments, n_threads = 5):
        from multiprocessing.dummy import Pool as ThreadPool
        if str(self) == "Arc":
            threads = 1
        else:
            threads = n_threads
        pool   = ThreadPool(threads)
        result = pool.map(function, arguments)
        pool.close()
        return result

    def dbList(self, fields, search_string = None):
        if search_string:
            search_fields = ["runcard", "runfolder"]
            return self.dbase.find_and_list(self.table, fields, search_fields, search_string)
        else:
            return self.dbase.list_data(self.table, fields)

    # If any of the "naming" function changes
    # they need to be changed as well at ARC.py/DIRAC.py
    def warmupName(self, runcard, rname):
        out = "output" + runcard + "-warm-" + rname + ".tar.gz"
        return out

    def outputName(self, runcard, rname, seed):
        out = "output" + runcard + "-" + rname + "-" + str(seed) + ".tar.gz"
        return out

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

    ### Initialisation functions
    def iniWarmup(self, runcard, warmup = None):
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
                print("Could not find runcard %s" % i)
                yn = input("Do you want to continue? (y/n): ").lower()
                if yn.startswith('y'):
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

    def iniProduction(self, runcard, warmupProvided = None):
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

    ### General functions for database management

    def checkIdType(self, db_id):
        production = 'P'
        socketed_warmup = 'S'
        warmup = 'W'
        jobtype = self.dbase.list_data(self.table, ["jobtype"], db_id)[0]["jobtype"]
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

    def getId(self, db_id):
        jobid = self.dbase.list_data(self.table, ["jobid"], db_id)
        try:
            idout = jobid[0]['jobid']
        except IndexError:
            print("Selected job is %s out of bounds" % jobid)
            idt   = input("> Select id to act upon: ")
            idout = self.getId(idt)
        return idout.split(" ")

    def desactivateJob(self, db_id):
        self.dbase.disable_entry(self.table, db_id)
        return 0

    def reactivateJob(self, db_id):
        self.dbase.disable_entry(self.table, db_id, revert = True)
        return 0

#
# Management options which are backend independent
#

    def statsJob(self, jobids):
        import datetime
        time = datetime.datetime.now().strftime("%H:%M:%S %d-%m-%Y")
        status = self.multiRun(self.do_statsJob, jobids, n_threads=12)
        done = status.count(self.cDONE)
        wait = status.count(self.cWAIT)
        run = status.count(self.cRUN)
        fail = status.count(self.cFAIL)
        unk = status.count(self.cUNK)
        total = len(jobids)
        total2 = done + wait + run + fail + unk 
        print(" >> Total number of subjobs: {0:<20} {1}".format(total, time))
        print("    >> Done:    {0}".format(done))
        print("    >> Waiting: {0}".format(wait))
        print("    >> Running: {0}".format(run))
        print("    >> Failed:  {0}".format(fail))
        print("    >> Unknown: {0}".format(unk))
        print("    >> Sum      {0}".format(total2))

    def do_statsJob(self, jobid):
        # When used with ARC, it assumes -j database is not needed (ie, default db is being used)
        cmd = [self.cmd_stat, jobid.strip()]
        from utilities import getOutputCall
        strOut = getOutputCall(cmd)
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

    def getDataWarmup(self, db_id):
        # Retrieve data from database
        from header import arcbase
        from utilities import getOutputCall, spCall
        fields    =  ["runcard","runfolder", "jobid", "pathfolder"]
        data      =  self.dbase.list_data(self.table, fields, db_id)[0]
        runfolder =  data["runfolder"]
        finfolder =  data["pathfolder"] + "/" + runfolder
        runcard   =  data["runcard"]
        jobids    =  data["jobid"].split()
        spCall(["mkdir", "-p", finfolder])
        print("Retrieving ARC output into " + finfolder)
        try:
            # Retrieve ARC standard output
            for jobid in jobids:
                print(jobid)
                cmd       =  [self.cmd_get, "-j", arcbase, jobid.strip()]
                output    = getOutputCall(cmd)
                outputfol = output.split("Results stored at: ")[1].rstrip()
                outputfolder = outputfol.split("\n")[0]
                if outputfolder == "" or (len(outputfolder.split(" ")) > 1):
                    print("Running mv and rm command is not safe here")
                    print("Found blank spaces in the output folder")
                    print("Nothing will be moved to the warmup global folder")
                else:
                    destination = finfolder + "/" + "arc_out_" + runcard + outputfolder
                    spCall(["mv", outputfolder, destination])
                    #spCall(["rm", "-rf", outputfolder])
        except:
            print("Couldn't find job output in the ARC server")
            print("jobid: " + jobid)
            print("Run arcstat to check the state of the job")
            print("Trying to retrieve data from grid storage anyway")
        # Retrieve ARC grid storage output
        wname = self.warmupName(runcard, runfolder)
        self.gridw.bring(wname, "warmup", finfolder + "/" + wname)

    def getDataProduction(self, db_id):
        from utilities import sanitiseGeneratedPath
        print("You are going to download all folders corresponding to this runcard from lfn:output")
        print("Make sure all runs are finished using the -i option")
        fields       = ["runfolder", "jobid", "runcard", "pathfolder"]
        data         = self.dbase.list_data(self.table, fields, db_id)[0]
        self.rcard   = data["runcard"]
        self.rfolder = data["runfolder"]
        pathfolderTp = data["pathfolder"]
        pathfolder   = sanitiseGeneratedPath(pathfolderTp, self.rfolder)
        jobids       = data["jobid"].split(" ")
        finalSeed    = self.bSeed + len(jobids)
        while True:
            firstName = self.outputName(self.rcard, self.rfolder, self.bSeed)
            finalName = self.outputName(self.rcard, self.rfolder, finalSeed)
            print("The starting filename is %s" % firstName)
            print("The final filename is %s" % finalName)
            yn = input("Are these parameters correct? (y/n) ").lower()
            if yn.startswith("y"): break
            self.bSeed = int(input("Please, introduce the starting seed (ex: 400): "))
            finalSeed  = int(input("Please, introduce the final seed (ex: 460): ")) + 1
        from os import makedirs, chdir
        try:
            makedirs(self.rfolder)
        except OSError as err:
            if err.errno == 17:
                print("Tried to create folder %s in this directory" % self.rfolder)
                print("to no avail. We are going to assume the directory was already there")
                yn = input("Do you want to continue? (y/n) ").lower()
                if yn.startswith("n"): raise Exception("Folder %s already exists" % self.rfolder)
            else:
                raise 
        chdir(self.rfolder)
        try:
            makedirs("log")
            makedirs("dat")
        except:
            pass
        seeds    =  range(self.bSeed, finalSeed)
        tarfiles =  self.multiRun(self.do_getData, seeds, 15)
        dummy    =  self.multiRun(self.do_extractOutputData, tarfiles, 1)
        chdir("..")
        from utilities import spCall
        print("Everything saved at %s" % pathfolder)
        spCall(["mv", self.rfolder, pathfolder])


    def do_getData(self, seed):
        filenm   = self.outputName(self.rcard, self.rfolder, seed)
        remotenm = filenm + ".tar.gz"
        localfil = self.rfolder + "-" + str(seed) + ".tar.gz"
        localnm  = self.rfolder + "/" + localfil
        self.gridw.bring(filenm, "output", localnm)
        return localfil

    def do_extractOutputData(self, tarfile):
        # It assumes log and dat folder are already there
        from os import chdir, path
        if not path.isfile(tarfile):
            print(tarfile + " not found")
            return -1
        files =  self.tarw.listFilesTar(tarfile)
        datf  =  []
        runf  =  []
        logf  =  []
        for fil in files:
            f = fil.strip()
            f = f.split(" ")[-1].strip()
            if ".run" in fil: runf.append(f)
            if ".log" in fil: logf.append(f)
            if ".dat" in fil and 'lhapdf/' not in fil: datf.append(f)
        dtarfile = "../" + tarfile
        chdir("log")
        self.tarw.extractThese(dtarfile, runf)
        self.tarw.extractThese(dtarfile, logf)
        chdir("../dat")
        self.tarw.extractThese(dtarfile, datf)
        chdir("..")
        from utilities import spCall
        spCall(["rm", tarfile])
        return 0



#
# List all runs
#
    def listRuns(self, search_string = None):
        fields = ["rowid", "jobid", "runcard", "runfolder", "date", "jobtype"]
        productionFlag = ""
        dictC  = self.dbList(fields, search_string)
        print("Active runs: " + str(len(dictC)))
        print("id".center(5) + " | " + "runcard".center(22) + " | " + "runname".center(25) + " |" +  "date".center(20))
        for i in dictC:
            rid = str(i['rowid']).center(5)
            ruc = str(i['runcard']).center(22)
            run = str(i['runfolder']).center(25)
            dat = str(i['date']).split('.')[0] + " " + str(i['jobtype'])
            dat = dat.center(20)
            jobids = str(i['jobid'])
            if len(jobids.split(" ")) > 1:
                productionFlag = " (+)"
            print(rid + " | " + ruc + " | " + run + " | " + dat + productionFlag)


