#
# Utility script to run Dirac jobs
#
DIRACSCRIPTDEFAULT = [
        "JobName    = \"gridjob1\";",
        "Executable = \"DIRAC.py\";",
        #   "Arguments  = \"some\";",
        "StdOutput  = \"StdOut\";",
        "StdError   = \"StdErr\";",
        "InputSandbox  = {\"DIRAC.py\"};",
        "OutputSandbox = {\"StdOut\",\"StdErr\"};",
        ]

from header import dbname
table  = 'diracjobs'
fields = ['jobids', 'date', 'runcard', 'runfolder', 'status']

def runJDL(jdlfile, rcard, rname, njobs):
    from datetime import datetime
    from header import baseseed
    import subprocess as sp
    import dbapi
    cmdbase = ["dirac-wms-job-submit"]
    joblist = []
    argbase = [rcard, rname]
    for seed in range(baseseed, baseseed + int(njobs)):
        args   = argbase + [str(seed)]
        writeJDL(jdlfile, args)
        cmd    = cmdbase + [jdlfile]
        output = sp.Popen(cmd, stdout = sp.PIPE).communicate()[0]
        jobid  = output.rstrip().strip().split(" ")[-1]
        joblist.append(jobid)
    databas = dbapi.database(dbname, table, fields)
    jobliststr = " "
    for i in joblist: jobliststr += i + " "
    dataDict = {'jobids'   : jobliststr,
                'date'     : str(datetime.now()),
                'runcard'  : rcard,
                'runfolder': rname,
                'status'   : "active",
                }
    databas.insertData(table, dataDict)

def writeJDL(jdlfile, listData):
    with open(jdlfile, 'w') as f:
        for i in DIRACSCRIPTDEFAULT:
            f.write(i)
            f.write("\n")
        f.write("Arguments = \"")
        for j in listData:
            f.write(j)
            f.write(" ")
        f.write("\";\n")

def checkProduction(r):
    from header import runcardDir
    print("Checking warmup")
    with open(runcardDir + "/" + r, 'r') as f:
        for line in f:
            if "Warmup" in line and ".true." in line:
                print("Warmup is on")
                yn = raw_input("Do you want to continue (y/n) ")
                if yn[0] == "y" or yn[0] == "Y":
                    pass
                else:
                    raise Exception("WRONG RUNCARD")
            if "Production" in line and ".false." in line:
                print("Production is off")
                yn = raw_input("Do you want to continue (y/n) ")
                if yn[0] == "y" or yn[0] == "Y":
                    pass
                else:
                    raise Exception("WRONG RUNCARD")

def checkExistingOutput(r, rname):
    print("Checking whether this runcard has something on the output folder...")
    import subprocess as sp
    cmd = ["lfc-ls", "output"]
    output  = sp.Popen(cmd, stdout = sp.PIPE).communicate()[0]
    print("Content of lfn:output: ")
    print(output)
    for i in output.split('\n'):
        checkname = r + "-" + rname
        if checkname in i: 
            print("Runcard " + r + " has at least one file at output")
            yn = raw_input("Do you want to delete them all? (y/n) ")
            if yn == "y":
                from header import deleteFromGrid, baseseed, producrun
                for seed in range(baseseed, baseseed + producrun):
                    filename = "output" + checkname + "-" + str(seed) + ".tar.gz"
                    deleteFromGrid(filename, "output")
                break
            else:
                print("Not deleting... exiting...")
                raise Exception("Runcard already exists")


def runDirac(runcard):
    from os import getcwd
    from header import producrun, expandCard
    runcards, dictCards = expandCard(runcard)
    jdlfile = "jdlfiletemplate.jdl"
    for r in runcards:
        checkProduction(r)
        checkExistingOutput(r, dictCards[r])
        runJDL(jdlfile, r, dictCards[r], producrun)

### MANAGEMENT OPTIONS
def listRuns():
    import dbapi
    databas = dbapi.database(dbname, table, fields)
    dictC = databas.listData(table, ["rowid", "jobids", "runcard", "date"])
    print("id".center(10) + " | " + "runcard".center(20) + " | " + "date".center(20))
    for i in dictC:
        rid = str(i['rowid']).center(10)
        ruc = str(i['runcard']).center(20)
        dat = str(i['date']).center(20)
        print(rid + " | " + ruc + " | " + dat)

def getId(id):
    import dbapi
    databas = dbapi.database(dbname, table, fields)
    jobids = databas.listData(table, ["jobids"], id)[0]
    listIds = jobids['jobids'].rstrip().strip().split(" ")
    return listIds

def extractData(tarfile):
    import subprocess as sp
    from os import chdir
    cmd = ["tar", "-tvf", tarfile]
    out = sp.Popen(cmd, stdout = sp.PIPE).communicate()[0]
    datfiles  = []
    logfiles  = []
    runfiles  = []
    listfiles = out.split("\n")
    for file in listfiles:
        f = file.strip()
        f = f.split(" ")[-1].strip()
        if ".dat" in file: datfiles.append(f)
        if ".run" in file: runfiles.append(f)
        if ".log" in file: logfiles.append(f)
    cmdbase = ["tar", "-xzf", "../" + tarfile]
    chdir("log")
    sp.call(cmdbase + runfiles)
    sp.call(cmdbase + logfiles)
    chdir("../dat")
    sp.call(cmdbase + datfiles)
    chdir("..")
    sp.call(["rm", tarfile])

def checkIfThere(dirPath,file):
    from os import path
    from glob import glob
    if not path.exists(dirPath + "/" + file):
        return False
    else:
        return True

def movetodaily(dirname):
    from os import path, makedirs, environ
    from subprocess import call
    from datetime import datetime
    date     = datetime.now()
    month    = date.strftime("%B")
    day      = str(date.day)
    homePath = environ['HOME']
    basePath = homePath + "/ResultsRunGrids"
    if not path.exists(basePath):
        print("Creating the basepath at " + basePath)
        makedirs(basePath)
    monthlyPath = basePath + "/" + month
    if not path.exists(monthlyPath):
        print("Creating monthly path at " + monthlyPath)
        makedirs(monthlyPath)
    dailyPath = monthlyPath + "/" + day
    if not path.exists(dailyPath):
        print("Creating daily path at " + dailyPath)
        makedirs(dailyPath)
    i = 0
    finalname = dirname + "-n0"
    while checkIfThere(dailyPath, finalname):
        i += 1
        finalname = dirname + "-n" + str(i)
    finalPlacement = dailyPath + "/" + finalname
    print("Moving " + dirname + " to " + finalPlacement)
    call(["mv", dirname, finalPlacement])


def getData(id):
    from subprocess import call
    from os import makedirs, chdir
    from header import baseseed
    import dbapi
    extract = True
    print("You are going to download all folders corresponding to this runcard from lfn:output")
    print("Make sure all runs are finished using the -i option")
    databas = dbapi.database(dbname)
    data = databas.listData(table, ["runfolder", "jobids", "runcard"], id)[0]
    runcard    = data["runcard"]
    runfolder  = data["runfolder"]
    jobids     = getId(id)
    wbaseseed = baseseed
    finalseed = baseseed + len(jobids)
    while True:
        startingnm = "output" + runcard + "-" + runfolder + "-" + str(wbaseseed) 
        finalnm    = "output" + runcard + "-" + runfolder + "-" + str(finalseed - 1) 
        print("The starting filename is %s" % startingnm)
        print("The final filename is %s" % finalnm)
        yn = raw_input("Are these parameters correct? (y/n) ")
        if yn == "y": break
        wbaseseed = int(raw_input("Please, introduce the starting seed (ex: 400): "))
        finalseed = int(raw_input("Please, introduce the final seed (ex: 460): ")) + 1
    cmdbase = ["bash", "-c"]
    cmdlfn  = "lcg-cp lfn:output/"
    call(cmdbase + ["which lcg-cp"])
    makedirs(runfolder)
    chdir(runfolder)
    if extract: 
        makedirs("log")
        makedirs("dat")
    for seed in range(wbaseseed, finalseed):
        filenm = "output" + runcard + "-" + runfolder + "-" + str(seed) 
        remotn = filenm + ".tar.gz"
#        localn = runfolder + "/" + runfolder + "-" + str(seed) + ".tar.gz"
        localn = runfolder + "-" + str(seed) + ".tar.gz"
        cmdlf  = cmdlfn + remotn + " " + localn
        cmd    = cmdbase + [cmdlf]
        call(cmd)
        if extract: extractData(localn)
    chdir("../")
    print("Results stored at " + runfolder)
    if extract: movetodaily(runfolder)

    


def killJob(jobid):
    from subprocess import call
    from header     import arcbase
    print("WARNING! You are about to kill the job!")
    yn = raw_input("Do you want to continue? (y/n) ")
    if yn != "y":
        from sys import exit
        exit(0)
    cmd = ["arckill", "-j", arcbase, jobid.strip()]
    call(cmd)

def cleanJob(jobid):
    from subprocess import call
    from header     import arcbase
    print("WARNING! You are about to clean the job!")
    yn = raw_input("Do you want to continue?i (y/n) ")
    if yn != "y":
        from sys import exit
        exit(0)
    cmd = ["arcclean", "-j", arcbase, jobid.strip()]
    call(cmd)

def catJob(jobids):
    from subprocess import call
    print("Looking at the last job of the list of " + str(len(jobids)))
    jobid = jobids[-1]
    cmd = ["dirac-wms-job-peek", jobid.strip()]
    call(cmd)

def statusJob(jobids):
    from subprocess import call
    from header     import arcbase
    print("Printing status for all jobs... might take a while...")
    for jobid in jobids:
        cmd = ["dirac-wms-job-status", jobid]
        call(cmd)

def infoJobs(jobid):
    pass

def desactivateJob(id):
    import dbapi
    databas = dbapi.database(dbname, table, fields)
    databas.desactivateEntry(table, id)
    return 0
