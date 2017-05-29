#!/usr/bin/env python3

#
# Backend Management classes
#

#
# Todo: 
#      -  Database initialisation (crate tables)
# 

from header import dbname, arcbase
from utilities import getOutputCall, spCall
from Backend import Backend

class Arc(Backend):
    cmd_print = "arccat"
    cmd_get   = "arcget"
    cmd_kill  = "arckill"
    cmd_clean = "arcclean"
    cmd_stat  = "arcstat"
    cmd_renew = "arcrenew"

    def __init__(self):
        # Might not work on python2?
        super(Arc, self).__init__()
        from header import arctable
        self.table = arctable

    def __str__(self):
        return "Arc"

    def updateStdOut(self):
        from os import path, makedirs
        fields = ["rowid", "jobid", "pathfolder", "runfolder"]
        dictC  = self.dbList(fields)
        for job in dictC:
            # Retrieve data from database
            id      = str(job['rowid'])
            jobid   = str(job['jobid'])
            rfold   = str(job['runfolder']) 
            pfold   = str(job['pathfolder']) + "/" + rfold
            flnam   = pfold + "/stdout"
            # Create target folder if it doesn't exist
            if not path.exists(pfold): makedirs(pfold)
            cmd     = self.cmd_print + ' ' +  jobid.strip()
            # It seems script is the only right way to save data with arc
            stripcm = ['script', '-c', cmd, '-a', 'tmpscript.txt']
            mvcmd   = ['mv', 'tmpscript.txt', flnam]
            spCall(stripcm)
            spCall(mvcmd)

    def renewProxy(self, jobid):
        cmd = [self.cmd_renew, jobid.strip()]
        spCall(cmd)


    def getData(self, id):
        # Retrieve data from database
        fields    =  ["runcard","runfolder", "jobid", "pathfolder"]
        data      =  self.dbase.listData(self.table, fields, id)[0]
        runfolder =  data["runfolder"]
        finfolder =  data["pathfolder"] + "/" + runfolder
        jobid     =  data["jobid"]
        cmd       =  [self.cmd_get, "-j", arcbase, jobid.strip()]
        print("Retrieving ARC output into " + finfolder)
        try:
            # Retrieve ARC standard output
            output    = getOutputCall(cmd)
            outputfol = output.split("Results stored at: ")[1].rstrip()
            outputfolder = outputfol.split("\n")[0]
            if outputfolder == "" or (len(outputfolder.split(" ")) > 1):
                print("Running mv and rm command is not safe here")
                print("Found blank spaces in the output folder")
                print("Nothing will be moved to the warmup global folder")
            else:
                spCall(["mv", outputfolder, finfolder])
                #spCall(["rm", "-rf", outputfolder])
        except:
            print("Couldn't find job output in the ARC server")
            print("jobid: " + jobid)
            print("Run arcstat to check the state of the job")
            print("Trying to retrieve data from grid storage anyway")
        # Retrieve ARC grid storage output
        wname = self.warmupName(data["runcard"], runfolder)
        self.gridw.bring(wname, "warmup", finfolder + "/" + wname)

    def killJob(self, jobid):
        print("WARNING! You are about to kill the job!")
        yn = self.input("Do you want to continue? (y/n) ")
        if yn != "y":
            from sys import exit
            exit(0)
        cmd = [self.cmd_kill, "-j", arcbase, jobid.strip()]
        spCall(cmd)

    def cleanJob(self, jobid):
        print("WARNING! You are about to clean the job!")
        yn = self.input("Do you want to continue? (y/n) ")
        if yn != "y":
            from sys import exit
            exit(0)
        cmd = [self.cmd_clean, "-j", arcbase, jobid.strip()]
        spCall(cmd)

    def catJob(self, jobid):
        cmd = [self.cmd_print, "-j", arcbase, jobid.strip()]
        spCall(cmd)

    def statusJob(self, jobid):
        cmd = [self.cmd_stat, "-j", arcbase, jobid.strip()]
        spCall(cmd)

### End Class Arc

class Dirac(Backend):
    def __str__(self):
        return "Dirac"

    cmd_print = "dirac-wms-job-peek"
    cmd_kill  = "dirac-wms-job-kill"
    cmd_stat  = "dirac-wms-job-status"

    def __init__(self):
        super(Dirac, self).__init__()
        from header import diractable
        self.table = diractable
    
    def catJob(self, jobids):
        print("Printing the last 20 lines of the last job")
        jobid = jobids.split(" ")[-1]
        cmd = [self.cmd_print, jobid.strip()]
        spCall(cmd)

    def statusJob(self, jobids):
        jobid_arr = jobids.split(" ")
        self.multiRun(self.do_statusJob, jobid_arr)

    def do_statusJob(self, jobid):
        cmd = [self.cmd_stat, jobid.strip()]
        spCall(cmd)
        return 1

    def killJob(self, jobids):
        cmd = [self.cmd_kill] + jobids.split(" ")
        spCall(cmd)

    def getData(self, bdid):
        from utilities import sanitiseGeneratedPath
        print("You are going to download all folders corresponding to this runcard from lfn:output")
        print("Make sure all runs are finished using the -i option")
        fields       = ["runfolder", "jobid", "runcard", "pathfolder"]
        data         = self.dbase.listData(self.table, fields, bdid)[0]
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
            yn = self.input("Are these parameters correct? (y/n) ")
            if yn == "y": break
            self.bSeed = int(self.input("Please, introduce the starting seed (ex: 400): "))
            finalSeed  = int(self.input("Please, introduce the final seed (ex: 460): ")) + 1
        from os import makedirs, chdir
        try:
            makedirs(self.rfolder)
        except OSError as err:
            if err.errno == 17:
                print("Tried to create folder %s in this directory", self.rfolder)
                print("to no avail. We are going to assume the directory was already there")
                yn = self.input("Do you want to continue? (y/n) ")
                if yn == "n": raise Exception("Folder %s already exists", self.rfolder)
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
        print("Everything saved at %s", pathfolder)
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




if __name__ == '__main__':
    from sys import version_info
    print("Test for backendManagement.py")
    print("Running with: Python ", version_info.major)
    print("This test needs to be ran at gridui")
    arc   = Arc() ; dirac = Dirac()
    print("Instantiate classes")

