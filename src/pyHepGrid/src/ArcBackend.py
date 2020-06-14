#!/usr/bin/env python3

import pyHepGrid.src.utilities as util
import pyHepGrid.src.header as header
from pyHepGrid.src.Backend import Backend
import os


class Arc(Backend):
    """
    Backend subclass for Arc submission.
    """
    cmd_print = "arccat"
    cmd_get = "arcget"
    cmd_kill = "arckill"
    cmd_clean = "arcclean"
    cmd_stat = "arcstat"
    cmd_renew = "arcrenew"

    def __init__(self, production=False, **kwargs):
        # Might not work on python2?
        super(Arc, self).__init__(**kwargs)
        if production:
            self.table = header.arcprodtable
        else:
            self.table = header.arctable
        self.production = production

    def __str__(self):
        retstr = "Arc"
        if self.production:
            retstr += " Production"
        return retstr

    def update_stdout(self):
        """ retrieves stdout of all running jobs and store the current state
        into its correspondent folder
        """
        fields = ["rowid", "jobid", "pathfolder", "runfolder"]
        dictC = self._db_list(fields)
        for job in dictC:
            # Retrieve data from database
            jobid = str(job['jobid'])
            rfold = str(job['runfolder'])
            pfold = str(job['pathfolder']) + "/" + rfold
            flnam = pfold + "/stdout"
            # Create target folder if it doesn't exist
            if not os.path.exists(pfold):
                os.makedirs(pfold)
            cmd = self.cmd_print + ' ' + jobid.strip()
            # It seems script is the only right way to save data with arc
            stripcm = ['script', '-c', cmd, '-a', 'tmpscript.txt']
            mvcmd = ['mv', 'tmpscript.txt', flnam]
            util.spCall(stripcm)
            util.spCall(mvcmd)

    def renew_proxy(self, jobids):
        """ renew proxy for a given job """
        for jobid in jobids:
            cmd = [self.cmd_renew, jobid.strip()]
            util.spCall(cmd)

    def kill_job(self, jobids, jobinfo):
        """ kills given job """
        self._press_yes_to_continue(
            "  \033[93m WARNING:\033[0m You are about to kill the job!")

        if len(jobids) == 0:
            header.logger.critical(
                "No jobids stored associated with this database entry, "
                "therefore nothing to kill.")

        # Kill in groups of 150 for speeeed
        for jobid_set in util.batch_gen(jobids, 150):
            stripped_set = [i.strip() for i in jobid_set]
            cmd = [self.cmd_kill, "-j", header.arcbase] + stripped_set
            header.logger.debug(
                "job_kill batch length:{0}".format(len(stripped_set)))
            util.spCall(cmd)

    def clean_job(self, jobids):
        """ remove the sandbox of a given job (including its stdout!) from
        the arc storage """
        self._press_yes_to_continue(
            "  \033[93m WARNING:\033[0m You are about to clean the job!")
        for jobid in jobids:
            cmd = [self.cmd_clean, "-j", header.arcbase, jobid.strip()]
            util.spCall(cmd)

    def cat_job(self, jobids, jobinfo, print_stderr=None, store=False):
        """ print standard output of a given job"""
        out = []
        for jobid in jobids:
            cmd = [self.cmd_print, "-j", header.arcbase, jobid.strip()]
            if print_stderr:
                cmd += ["-e"]
            if not store:
                util.spCall(cmd)
            else:
                out.append(util.getOutputCall(cmd, include_return_code=False))
        if store:
            return out

    def cat_log_job(self, jobids, jobinfo):
        """Sometimes the std output doesn't get updated
        but we can choose to access the logs themselves"""
        output_folder = ["file:///tmp/"]
        cmd_base = ["arccp", "-i"]
        cmd_str = "cat /tmp/"
        for jobid in jobids:
            files = util.getOutputCall(
                ["arcls", jobid], include_return_code=False).split()
            logfiles = [i for i in files if i.endswith(".log")]
            for logfile in logfiles:
                cmd = cmd_base + [os.path.join(jobid, logfile)] + output_folder
                output = util.getOutputCall(
                    cmd, include_return_code=False).split()
                for text in output:
                    if ".log" in text:
                        util.spCall((cmd_str + text).split())

    def bring_current_warmup(self, db_id):
        """ Sometimes we want to retrieve the warmup before the job finishes """
        output_folder = ["file:///tmp/"]
        cmd_base = ["gfal-copy", "-v"]
        fields = ["pathfolder", "runfolder", "jobid"]
        data = self.dbase.list_data(self.table, fields, db_id)[0]
        runfolder = data["runfolder"]
        finfolder = data["pathfolder"] + "/" + runfolder + "/"
        if header.finalisation_script is not None:
            finfolder = header.default_runfolder
        jobids = data["jobid"].split()
        output_folder = ["file://" + finfolder]
        for jobid in jobids:
            cmd = cmd_base + [jobid + "/*.y*"] + output_folder
            util.spCall(cmd)
            cmd = cmd_base + [jobid + "/*.log"] + output_folder
            util.spCall(cmd)
        print("Warmup stored at {0}".format(finfolder))

    def status_job(self, jobids, verbose=False):
        """ print the current status of a given job """
        cmd = [self.cmd_stat, "-j", header.arcbase]
        jobids = [jobid.strip() for jobid in jobids]
        if len(jobids) == 0:
            header.logger.critical("No jobs selected")
        cmd = cmd + jobids
        if verbose:
            cmd += ["-l"]
        util.spCall(cmd)

    def _do_stats_job(self, jobid_raw):
        """ version of stats job multithread ready
        """
        if isinstance(jobid_raw, tuple):
            if (jobid_raw[1] == self.cDONE or
                    jobid_raw[1] == self.cFAIL or
                    jobid_raw[1] == self.cMISS):
                return jobid_raw[1]
            else:
                jobid = jobid_raw[0]
        else:
            jobid = jobid_raw
        cmd = [self.cmd_stat, jobid.strip(), "-j", header.arcbase]
        strOut = util.getOutputCall(
            cmd, suppress_errors=True, include_return_code=False)
        if "Done" in strOut or "Finished" in strOut:
            return self.cDONE
        elif "Waiting" in strOut or "Queuing" in strOut:
            return self.cWAIT
        elif "Running" in strOut:
            return self.cRUN
        elif "Failed" in strOut:
            # if we still have a return code 0 something is odd
            if "Exit Code: 0" in strOut:
                return self.cMISS
            return self.cFAIL
        else:
            return self.cUNK

    def stats_job(self, dbid, do_print=True):
        """ Given a list of jobs, returns the number of jobs which
        are in each possible state (done/waiting/running/etc)
        """
        jobids = self.get_id(dbid)
        current_status = self._get_old_status(dbid)
        arglen = len(jobids)

        if isinstance(current_status, list):
            if len(current_status) == arglen:
                jobids_lst = zip(jobids, current_status)
            else:  # Current status corrupted somehow... Start again
                jobids_lst = jobids
        else:
            jobids_lst = jobids

        tags = ["runcard", "runfolder", "date"]
        runcard_info = self.dbase.list_data(self.table, tags, dbid)[0]

        n_threads = header.finalise_no_cores
        status = self._multirun(self._do_stats_job, jobids_lst,
                                n_threads, arglen=arglen)
        done = status.count(self.cDONE)
        wait = status.count(self.cWAIT)
        run = status.count(self.cRUN)
        fail = status.count(self.cFAIL)
        miss = status.count(self.cMISS)
        unk = status.count(self.cUNK)
        if do_print:
            self.stats_print_setup(runcard_info, dbid=dbid)
            total = len(jobids)
            self._print_stats(done, wait, run, fail, miss, unk, total)
        self._set_new_status(dbid, status)
        return done, wait, run, fail, unk


if __name__ == '__main__':
    from sys import version_info
    print("Test for pyHepGrid.src.ArcBackend.py")
    print("Running with: Python ", version_info.major)
    print("This test needs to be ran at gridui")
    arc = Arc()
    print("Instantiate classes")
