#!/usr/bin/env python3

#
# src.Backend Management classes
#
import src.utilities as util
import src.header as header
from src.Backend import Backend

import os

class Arc(Backend):
    cmd_print = "arccat"
    cmd_get   = "arcget"
    cmd_kill  = "arckill"
    cmd_clean = "arcclean"
    cmd_stat  = "arcstat"
    cmd_renew = "arcrenew"

    def __init__(self, **kwargs):
        # Might not work on python2?
        super(Arc, self).__init__(**kwargs)
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
        self._press_yes_to_continue("  \033[93m WARNING:\033[0m You are about to kill the job!")
        for jobid in jobids:
            cmd = [self.cmd_kill, "-j", header.arcbase, jobid.strip()]
            util.spCall(cmd)

    def clean_job(self, jobids):
        """ remove the sandbox of a given job (including its stdout!) from
        the arc storage """
        self._press_yes_to_continue("  \033[93m WARNING:\033[0m You are about to clean the job!")
        for jobid in jobids:
            cmd = [self.cmd_clean, "-j", header.arcbase, jobid.strip()]
            util.spCall(cmd)

    def cat_job(self, jobids, print_stderr = None):
        """ print stdandard output of a given job"""
        for jobid in jobids:
            cmd = [self.cmd_print, "-j", header.arcbase, jobid.strip()]
            if print_stderr:
                cmd += ["-e"]
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

    def bring_current_warmup(self, db_id):
        """ Sometimes we want to retrieve the warmup before the job finishes """
        output_folder = ["file:///tmp/"]
        cmd_base =  ["globus-url-copy", "-v"]
        fields = ["pathfolder", "runfolder", "jobid"]
        data = self.dbase.list_data(self.table, fields, db_id)[0]
        runfolder =  data["runfolder"]
        finfolder =  pathfolder = data["pathfolder"] + "/" + runfolder + "/"
        if header.finalisation_script is not None:
            finfolder = header.default_runfolder
        jobids    =  data["jobid"].split()
        output_folder = ["file://" + finfolder]
        for jobid in jobids:
            cmd = cmd_base + [jobid + "/*.y*"] + output_folder
            util.spCall(cmd)
            cmd = cmd_base + [jobid + "/*.log"] + output_folder
            util.spCall(cmd)
        print("Warmup stored at {0}".format(finfolder))

    def status_job(self, jobids, verbose = False):
        """ print the current status of a given job """
        # for jobid in jobids:
        #     cmd = [self.cmd_stat, "-j", header.arcbase, jobid.strip()]
        #     if verbose:
        #         cmd += ["-l"]
        #     util.spCall(cmd)
        cmd = [self.cmd_stat, "-j", header.arcbase]
        print(header.arcbase)
        jobids = [jobid.strip() for jobid in jobids]
        cmd = cmd + jobids
        if verbose:
            cmd += ["-l"]
        util.spCall(cmd)

class Dirac(Backend):
    cmd_print = "dirac-wms-job-peek"
    cmd_kill  = "dirac-wms-job-kill"
    cmd_stat  = "dirac-wms-job-status"

    def __init__(self, **kwargs):
        super(Dirac, self).__init__(**kwargs)
        self.table = header.diractable

    def __str__(self):
        return "Dirac"
    
    def cat_job(self, jobids, print_stderr = None):
        print("Printing the last 20 lines of the last job")
        jobid = jobids[-1]
        cmd = [self.cmd_print, jobid.strip()]
        util.spCall(cmd)

    def status_job(self, jobids, verbose = False):
        """ query dirac on a job-by-job basis about the status of the job """
        self._multirun(self.do_status_job, jobids, header.finalise_no_cores)

    def do_status_job(self, jobid):
        """ multiproc wrapper for status_job """
        cmd = [self.cmd_stat, jobid]
        util.spCall(cmd, suppress_errors=True)
        return 0

    def get_status(self, status, date):
        return set(util.getOutputCall(['dirac-wms-select-jobs','--Status={0}'.format(status),
                                  '--Owner={0}'.format(header.dirac_name),
                                  '--Date={0}'.format(date)]).split("\n")[-2].split(", "))

    def stats_job_cheat(self, dbid):
        """ When using Dirac, instead of asking for each job individually
        we can ask for batchs of jobs in a given state and compare.
        In order to use this function you need to modify 
        "DIRAC/Interfaces/scripts/dirac-wms-select-jobs.py" to comment out 
        lines 87-89.
        """
        jobids = self.get_id(dbid)
        tags = ["runcard", "runfolder", "date"]
        runcard_info = self.dbase.list_data(self.table, tags, dbid)[0]

        try:
            self.__first_call_stats
        except AttributeError as e:
            print("Stats function under testing/debugging. Use with care...")
            self.__first_call_stats = False
        date = runcard_info["date"].split()[0]
        jobids_set = set(jobids)
        # Get all jobs in each state
        waiting_jobs = self.get_status('Waiting', date)
        done_jobs = self.get_status('Done', date)
        running_jobs = self.get_status('Running', date)
        fail_jobs = self.get_status('Failed', date)
        unk_jobs = self.get_status('Unknown', date)
        failed_jobs_set = jobids_set & fail_jobs
        done_jobs_set = jobids_set & done_jobs
        # Count how many jobs we have in each state
        fail = len(failed_jobs_set)
        done = len(done_jobs_set)
        wait = len(jobids_set & waiting_jobs)
        run = len(jobids_set & running_jobs)
        unk = len(jobids_set & unk_jobs)
        # Save done and failed jobs to the database
        status = len(jobids)*[0]
        for jobid in failed_jobs_set:
            status[jobids.index(jobid)] = self.cFAIL
        for jobid in done_jobs_set:
            status[jobids.index(jobid)] = self.cDONE
        self.stats_print_setup(runcard_info,dbid = dbid)
        total = len(jobids)
        self.print_stats(done, wait, run, fail, unk, total)
        self._set_new_status(dbid, status)


    def kill_job(self, jobids):
        """ kill all jobs associated with this run """
        self._press_yes_to_continue("  \033[93m WARNING:\033[0m You are about to kill all jobs for this run!")
        cmd = [self.cmd_kill] + jobids
        util.spCall(cmd)

class Slurm(Backend):
    # cmd_print = "arccat"
    # cmd_get   = "arcget"
    # cmd_kill  = "arckill"
    # cmd_clean = "arcclean"
    # cmd_stat  = "arcstat"
    # cmd_renew = "arcrenew"

    def __init__(self, **kwargs):
        # Might not work on python2?
        super(Slurm, self).__init__(**kwargs)
        self.table = header.slurmtable

    def __str__(self):
        return "Slurm"

    def get_data(*args, **kwargs):
        header.logger.critical("Get_data not implemented for SLURM")

    # def list_runs(*args, **kwargs):
    #     header.logger.critical("list_runs not implemented for SLURM")

    def get_status(self, jobid, status):
        stat = len([i for i in util.getOutputCall(["squeue", "-j{0}".format(jobid),"-r","-t",status],
                                                  suppress_errors=True).split("\n")[1:] 
                    if "error" not in i]) #strip header from results
        if stat >0:
            stat = stat-1
        return stat

    def stats_job(self, dbid):
        tags = ["runcard", "runfolder", "date"]
        jobids = self.get_id(dbid) # only have one array id for SLURM
        runcard_info = self.dbase.list_data(self.table, tags, dbid)[0]
        running, waiting, fail, tot = 0,0,0,0
        for jobid in jobids:
            running += self.get_status(jobid,"R")
            waiting += self.get_status(jobid,"PD")
            fail += self.get_status(jobid,"F")+self.get_status(jobid,"CA")
            tot += self.get_status(jobid,"all")
        done = tot-fail-waiting-running
        self.stats_print_setup(runcard_info,dbid = dbid)
        total = len(jobids)
        self.print_stats(done, waiting, running, fail, 0, tot)


    def kill_job(self,jobids):
        for jobid in jobids:
            util.spCall(["scancel",str(jobid)])

if __name__ == '__main__':
    from sys import version_info
    print("Test for src.backendManagement.py")
    print("Running with: Python ", version_info.major)
    print("This test needs to be ran at gridui")
    arc   = Arc()
    dirac = Dirac()
    slurm = Slurm()
    print("Instantiate classes")

