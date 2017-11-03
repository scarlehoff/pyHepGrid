#!/usr/bin/env python3

#
# Backend Management classes
#
import utilities as util
import header
from Backend import Backend

import os

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
        self.table = header.arctable

    def __str__(self):
        return "Arc"

    def update_stdout(self):
        """ retrieves stdout of all running jobs and store the current state 
        into its correspondent folder
        """
        fields = ["rowid", "jobid", "pathfolder", "runfolder"]
        dictC  = self._db_list(fields)
        for job in dictC:
            # Retrieve data from database
            jobid   = str(job['jobid'])
            rfold   = str(job['runfolder']) 
            pfold   = str(job['pathfolder']) + "/" + rfold
            flnam   = pfold + "/stdout"
            # Create target folder if it doesn't exist
            if not os.path.exists(pfold): 
                os.makedirs(pfold)
            cmd     = self.cmd_print + ' ' +  jobid.strip()
            # It seems script is the only right way to save data with arc
            stripcm = ['script', '-c', cmd, '-a', 'tmpscript.txt']
            mvcmd   = ['mv', 'tmpscript.txt', flnam]
            util.spCall(stripcm)
            util.spCall(mvcmd)

    def renew_proxy(self, jobids):
        """ renew proxy for a given job """
        for jobid in jobids:
            cmd = [self.cmd_renew, jobid.strip()]
            util.spCall(cmd)

    def kill_job(self, jobids):
        """ kills given job """
        self._press_yes_to_continue("WARNING! You are about to kill the job!")
        for jobid in jobids:
            cmd = [self.cmd_kill, "-j", header.arcbase, jobid.strip()]
            util.spCall(cmd)

    def clean_job(self, jobids):
        """ remove the sandbox of a given job (including its stdout!) from
        the arc storage """
        self._press_yes_to_continue("WARNING! You are about to clean the job!")
        for jobid in jobids:
            cmd = [self.cmd_clean, "-j", header.arcbase, jobid.strip()]
            util.spCall(cmd)

    def cat_job(self, jobids):
        """ print stdandard output of a given job"""
        for jobid in jobids:
            cmd = [self.cmd_print, "-j", header.arcbase, jobid.strip()]
            util.spCall(cmd)

    def cat_log_job(self, jobids):
        """Sometimes the std output doesn't get updated
        but we can choose to access the logs themselves"""
        output_folder = ["file:///tmp/"]
        cmd_base =  ["globus-url-copy", "-v"]
        cmd_str = "cat /tmp/"
        for jobid in jobids:
            cmd = cmd_base + [jobid + "/*.log"] + output_folder
            output = util.getOutputCall(cmd).split()
            for text in output:
                if ".log" in text:
                    util.spCall((cmd_str + text).split())

    def status_job(self, jobids, verbose = False):
        """ print the current status of a given job """
        for jobid in jobids:
            cmd = [self.cmd_stat, "-j", header.arcbase, jobid.strip()]
            if verbose:
                cmd += ["-l"]
            util.spCall(cmd)


class Dirac(Backend):
    cmd_print = "dirac-wms-job-peek"
    cmd_kill  = "dirac-wms-job-kill"
    cmd_stat  = "dirac-wms-job-status"

    def __init__(self):
        super(Dirac, self).__init__()
        self.table = header.diractable

    def __str__(self):
        return "Dirac"
    
    def cat_job(self, jobids):
        print("Printing the last 20 lines of the last job")
        jobid = jobids[-1]
        cmd = [self.cmd_print, jobid.strip()]
        header.spCall(cmd)

    def status_job(self, jobids, verbose = False):
        """ query dirac on a job-by-job basis about the status of the job """
        self._multirun(self.do_status_job, jobids, header.finalise_no_cores)

    def do_status_job(self, jobid):
        """ multiproc wrapper for status_job """
        cmd = [self.cmd_stat, jobid]
        header.spCall(cmd)
        return 0

    def get_status(self, status, date):
        return set(util.getOutputCall(['dirac-wms-select-jobs','--Status={0}'.format(status),
                                  '--Owner={0}'.format(header.dirac_name),
                                  '--Date={0}'.format(date)]).split("\n")[-2].split(", "))

    def stats_job_cheat(self, jobids, date):
        """ When using Dirac, instead of asking for each job individually
        we can ask for batchs of jobs in a given state and compare.
        In order to use this function you need to modify 
        "DIRAC/Interfaces/scripts/dirac-wms-select-jobs.py" to comment out 
        lines 87-89.
        """
        print("Stats function under testing/debugging. Use with care...")
        date = date.split()[0]
        jobids = set(jobids)
        waiting_jobs = self.get_status('Waiting', date)
        done_jobs = self.get_status('Done', date)
        running_jobs = self.get_status('Running', date)
        fail_jobs = self.get_status('Failed', date)
        unk_jobs = self.get_status('Unknown', date)
        wait = len(jobids & waiting_jobs)
        run = len(jobids & running_jobs)
        fail = len(jobids & fail_jobs)
        done = len(jobids & done_jobs)
        unk = len(jobids & unk_jobs)
        self.print_stats(done, wait, run, fail, unk, jobids)


    def kill_job(self, jobids):
        """ kill all jobs associated with this run """
        self._press_yes_to_continue("WARNING! You are about to kill all jobs for this run!")
        cmd = [self.cmd_kill] + jobids
        header.spCall(cmd)


if __name__ == '__main__':
    from sys import version_info
    print("Test for backendManagement.py")
    print("Running with: Python ", version_info.major)
    print("This test needs to be ran at gridui")
    arc   = Arc() ; dirac = Dirac()
    print("Instantiate classes")

