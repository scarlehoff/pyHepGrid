#
# Utility scripts to run ARC jobs
#

ARCSCRIPTDEFAULT = ["&",
        "(executable   = \"ARC.py\")",
#        "(arguments    = \"testing.run\" \"TESTINGFOLDER\" \"4\")",
#        "(jobName      = \"gridjob1\")",
        "(outputFiles  = (\"outfile.out\" \"\") )",
        "(stdout       = \"stdout\")",
        "(stderr       = \"stderr\")",
        "(gmlog        = \"testjob.log\")",
        "(memory       = \"100\")",
#        "(count        = \"4\")",
#        "(countpernode = \"4\")",
        ]

#dbname = 'arcdb.dat'
from header import dbname
table  = 'arcjobs'
fields = ['jobid', 'date', 'runcard', 'runfolder', 'status']

# Running functions

def runXRSL(xrslfile, rcard, rname):
    from datetime import datetime
    import subprocess as sp
    import dbapi
    ce      = "ce2.dur.scotgrid.ac.uk"
    cmdbase = ['arcsub', '-c', ce]
    cmd     = cmdbase + [xrslfile]
    databas = dbapi.database(dbname, table, fields)
    output  = sp.Popen(cmd, stdout = sp.PIPE).communicate()[0]
    jobid   = output.split("jobid:")[-1].rstrip().strip()
    dataDict = {'jobid'    : jobid,
                'date'     : str(datetime.now()),
                'runcard'  : rcard,
                'runfolder': rname,
                'status'   : "active",}
    databas.insertData(table, dataDict)

def writeXRSL(xrslfile, dictData):
    with open(xrslfile, 'w') as f:
        for i in ARCSCRIPTDEFAULT:
            f.write(i)
            f.write('\n')
        for key in dictData:
            f.write("(" + key)
            f.write(" = \"" + dictData[key])
            f.write("\")\n")

def checkWarmup(r):
    from header import runcardDir
    print("Checking warmup")
    with open(runcardDir + "/" + r, 'r') as f:
        for line in f:
            if "Warmup" in line and ".false." in line:
                print("Warmup is off")
                yn = raw_input("Do you want to continue (y/n) ")
                if yn[0] == "y" or yn[0] == "Y":
                    pass
                else:
                    raise Exception("WRONG RUNCARD")
            if "Production" in line and ".true." in line:
                print("Production is on")
                yn = raw_input("Do you want to continue (y/n) ")
                if yn[0] == "y" or yn[0] == "Y":
                    pass
                else:
                    raise Exception("WRONG RUNCARD")

def checkExistingWarmup(r, rname):
    print("Checking whether this runcard is already at lfn:warmup")
    import subprocess as sp
    cmd    = ["lfc-ls", "warmup"]
    output  = sp.Popen(cmd, stdout = sp.PIPE).communicate()[0]
    checknm = "output" + r + "-warm-" + rname + ".tar.gz"
    if checknm in output:
        print("File " + checknm + " already exist at lfn:warmup")
        yn = raw_input("Do you want to delete this file? (y/n) ")
        if yn == "y":
            from header import deleteFromGrid
            deleteFromGrid(checknm, "warmup")
        else:
            print("Not deleting... exiting")
            raise Exception("Runcard already exists")


def runArc(runcard):
    from os import getcwd
    from header import warmupthr, expandCard
    runcards, dictCards = expandCard(runcard)
    xrslfile = "runArcJob.xrsl"
    jobName  = "gridjob1"
    for r in runcards:
        checkWarmup(r)
        checkExistingWarmup(r, dictCards[r])
        arguments  = "" + r + "\""
        arguments += " \"" + dictCards[r] + "\""
        arguments += " \"" + str(warmupthr) + ""
        dictData = {'arguments'   : arguments,
                    'jobName'     : jobName,
                    'count'       : str(warmupthr),
                    'countpernode': str(warmupthr),}
        writeXRSL(xrslfile, dictData)
        runXRSL(xrslfile, r, dictCards[r])

### MANAGEMENT OPTIONS

def listRuns():
    import dbapi
    databas = dbapi.database(dbname, table, fields)
    dictC = databas.listData(table, ["rowid", "jobid", "runcard", "runfolder", "date"])
    print("id".center(5) + " | " + "runcard".center(22) + " | " + "runname".center(25) + " |" +  "date".center(20))
    for i in dictC:
        rid = str(i['rowid']).center(5)
        ruc = str(i['runcard']).center(22)
        run = str(i['runfolder']).center(25)
        dat = str(i['date']).split('.')[0]
        dat = dat.center(20)
        print(rid + " | " + ruc + " | " + run + " | " + dat)

def getData(id):
    import subprocess as sp
    from header import arcbase
    import dbapi
    databas = dbapi.database(dbname)
    data = databas.listData(table, ["runcard","runfolder", "jobid"], id)[0]
    runfolder = data["runfolder"]
    jobid     = data["jobid"]
    cmd = ["arcget", "-j", arcbase, jobid.strip()]
    print("Retrieving ARC output into " + runfolder)
    try:
        output  = sp.Popen(cmd, stdout = sp.PIPE).communicate()[0]
        outputfol = output.split("Results stored at: ")[1].rstrip()
        outputfolder = outputfol.split("\n")[0]
        sp.call(["mv", outputfolder, runfolder])
    except:
        print("Couldn't find job output in the ARC server")
        print("jobid: " + jobid)
        print("Run arcstat to check the state of the job")
    yn = raw_input("Do you also want to retrieve the warmup directory? (y/n) ")
    if yn == "y":
        wname = "output" + data["runcard"] + "-warm-" + runfolder + ".tar.gz"
#        wname = data["runcard"] + "-w.tar.gz"
        lfncm = "lfn:warmup/" + wname
        tmpnm = "tmp.tar.gz"
        cmd   = ["lcg-cp", lfncm, tmpnm]
        sp.call(cmd)
        sp.call(["mv", tmpnm, runfolder + "/"])


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

def catJob(jobid):
    from subprocess import call
    from header     import arcbase
    cmd = ["arccat", "-j", arcbase, jobid.strip()]
    call(cmd)

def getId(id):
    import dbapi
    databas = dbapi.database(dbname, table, fields)
    jobid = databas.listData(table, ["jobid"], id)
    return jobid[0]['jobid']

def statusJob(jobid):
    from subprocess import call
    from header     import arcbase
    cmd = ["arcstat", "-j", arcbase, jobid.strip()]
    call(cmd)

def desactivateJob(id):
    import dbapi
    databas = dbapi.database(dbname, table, fields)
    databas.desactivateEntry(table, id)
    return 0

def reactivateJob(id):
    import dbapi
    databas = dbapi.database(dbname, table, fields)
    databas.desactivateEntry(table, id, revert = True)
    return 0
