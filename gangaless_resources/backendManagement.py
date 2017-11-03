#!/usr/bin/env python3

#
# Backend Management classes
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
        dictC  = self._db_list(fields)
        for job in dictC:
            # Retrieve data from database
            jobid   = str(job['jobid'])
            rfold   = str(job['runfolder']) 
            pfold   = str(job['pathfolder']) + "/" + rfold
            flnam   = pfold + "/stdout"
            # Create target folder if it doesn't exist
            if not path.exists(pfold): 
                makedirs(pfold)
            cmd     = self.cmd_print + ' ' +  jobid.strip()
            # It seems script is the only right way to save data with arc
            stripcm = ['script', '-c', cmd, '-a', 'tmpscript.txt']
            mvcmd   = ['mv', 'tmpscript.txt', flnam]
            spCall(stripcm)
            spCall(mvcmd)

    def renewProxy(self, jobids):
        for jobid in jobids:
            cmd = [self.cmd_renew, jobid.strip()]
            spCall(cmd)

    def killJob(self, jobids):
        print("WARNING! You are about to kill the job!")
        yn = input("Do you want to continue? (y/n) ").lower()
        if not yn.startswith("y"):
            from sys import exit
            exit(0)
        for jobid in jobids:
            cmd = [self.cmd_kill, "-j", arcbase, jobid.strip()]
            spCall(cmd)

    def cleanJob(self, jobids):
        print("WARNING! You are about to clean the job!")
        yn = input("Do you want to continue? (y/n) ").lower()
        if not yn.startswith("y"):
            from sys import exit
            exit(0)
        for jobid in jobids:
            cmd = [self.cmd_clean, "-j", arcbase, jobid.strip()]
            spCall(cmd)

    def catJob(self, jobids):
        for jobid in jobids:
            cmd = [self.cmd_print, "-j", arcbase, jobid.strip()]
            spCall(cmd)

    def catLogJob(self, jobids):
        """Sometimes the std output doesn't get updated
        but we can choose to access the logs themselves"""
        output_folder = ["file:///tmp/"]
        cmd_base =  ["globus-url-copy", "-v"]
        cmd_str = "cat /tmp/"
        for jobid in jobids:
            cmd = cmd_base + [jobid + "/*.log"] + output_folder
            output = getOutputCall(cmd).split()
            for text in output:
                if ".log" in text:
                    spCall((cmd_str + text).split())

    def statusJob(self, jobids, verbose = False):
        for jobid in jobids:
            cmd = [self.cmd_stat, "-j", arcbase, jobid.strip()]
            if verbose:
                cmd += ["-l"]
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
        jobid = jobids[-1]
        cmd = [self.cmd_print, jobid.strip()]
        spCall(cmd)

    def statusJob(self, jobids, verbose = False):
        self._multirun(self.do_statusJob, jobids, 10)

    def do_statusJob(self, jobid):
        cmd = [self.cmd_stat, jobid]
        spCall(cmd)
        return 0

    def get_status(self, status):
        from utilities import getOutputCall
        return set(getOutputCall(['dirac-wms-select-jobs','--Status={0}'.format(status)]).split("\n")[-2].split(", "))

    def stats_job_cheat(self, jobids):
        """ When using Dirac, instead of asking for each job individually
        we can ask for batchs of jobs in a given state and compare.
        In order to use this function you need to modify "dirac-wms-select-jobs.py"
        comment out lines 87-89
        """
        print("Function under construction")
        waiting_jobs = self.get_status('Waiting')
        jobids = set(jobids)
        done_jobs = self.get_status('Done')
        running_jobs = self.get_status('Running')
        fail_jobs = self.get_status('Failed')
        unk_jobs = self.get_status('Unknown')
        wait = len(jobids & waiting_jobs)
        run = len(jobids & running_jobs)
        fail = len(jobids & fail_jobs)
        done = len(jobids & done_jobs)
        unk = len(jobids & unk_jobs)
        total = len(jobids)
        total2 = done + wait + run + fail + unk 
        print(" >> Total number of subjobs: {0:<20} {1}".format(total, time))
        print("    >> Done:    {0}".format(done))
        print("    >> Waiting: {0}".format(wait))
        print("    >> Running: {0}".format(run))
        print("    >> Failed:  {0}".format(fail))
        print("    >> Unknown: {0}".format(unk))
        print("    >> Sum      {0}".format(total2))




    def killJob(self, jobids):
        cmd = [self.cmd_kill] + jobids
        spCall(cmd)


if __name__ == '__main__':
    from sys import version_info
    print("Test for backendManagement.py")
    print("Running with: Python ", version_info.major)
    print("This test needs to be ran at gridui")
    arc   = Arc() ; dirac = Dirac()
    print("Instantiate classes")

