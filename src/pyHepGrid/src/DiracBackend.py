#!/usr/bin/env python3

import pyHepGrid.src.utilities as util
import pyHepGrid.src.header as header
from pyHepGrid.src.Backend import Backend


class Dirac(Backend):
    """
    Backend subclass for Dirac submission.
    """
    cmd_print = "dirac-wms-job-peek"
    cmd_kill = "dirac-wms-job-kill"
    cmd_stat = "dirac-wms-job-status"

    def __init__(self, **kwargs):
        super(Dirac, self).__init__(**kwargs)
        self.table = header.diractable

    def __str__(self):
        return "Dirac"

    def cat_job(self, jobids, jobinfo, print_stderr=None):
        print("Printing the last 20 lines of the last job")
        jobid = jobids[-1]
        cmd = [self.cmd_print, jobid.strip()]
        util.spCall(cmd)

    def status_job(self, jobids, verbose=False):
        """ query dirac on a job-by-job basis about the status of the job """
        self._multirun(self.do_status_job, jobids, header.finalise_no_cores)

    def do_status_job(self, jobid):
        """ multiproc wrapper for status_job """
        cmd = [self.cmd_stat, jobid]
        util.spCall(cmd, suppress_errors=True)
        return 0

    def _get_status(self, status, date):
        output = set(util.getOutputCall(
            ['dirac-wms-select-jobs',
             F'--Status={status}',
             F'--Owner={header.dirac_name}',
             '--Maximum=0',  # 0 lists ALL jobs, which is nice :)
             F'--Date={date}'],
            include_return_code=False).split("\n")[-2].split(","))
        header.logger.debug(output)
        return output

    def stats_job(self, dbid):
        """ When using Dirac, instead of asking for each job individually
        we can ask for batchs of jobs in a given state and compare.
        """
        jobids = self.get_id(dbid)
        tags = ["runcard", "runfolder", "date"]
        runcard_info = self.dbase.list_data(self.table, tags, dbid)[0]

        try:
            self.__first_call_stats
        except AttributeError:
            self.__first_call_stats = False
        date = runcard_info["date"].split()[0]
        jobids_set = set(jobids)
        # Get all jobs in each state
        waiting_jobs = self._get_status('Waiting', date)
        done_jobs = self._get_status('Done', date)
        running_jobs = self._get_status('Running', date)
        fail_jobs = self._get_status('Failed', date)
        unk_jobs = self._get_status('Unknown', date)
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
        self.stats_print_setup(runcard_info, dbid=dbid)
        total = len(jobids)
        self._print_stats(done, wait, run, fail, 0, unk, total)
        self._set_new_status(dbid, status)

    def kill_job(self, jobids, jobinfo):
        """ kill all jobs associated with this run """
        self._press_yes_to_continue(
            "  \033[93m WARNING:\033[0m You are about to kill all jobs for "
            "this run!")

        if len(jobids) == 0:
            header.logger.critical(
                "No jobids stored associated with this database entry, "
                "therefore nothing to kill.")

        cmd = [self.cmd_kill] + jobids
        util.spCall(cmd)


if __name__ == '__main__':
    import os
    from sys import version_info
    print("Test for pyHepGrid.src.DiracBackend.py")
    if os.path.isdir("./test/"):
        raise FileExistsError("'src/pyHepGrid/src/test' directory not empty.  Aborting.")
    print("Running with: Python ", version_info.major)
    header.dbname = "./test/test_db"
    dirac = Dirac()
    print("Instantiate classes")
    assert os.path.isfile(header.dbname)
    # clean up
    os.remove("./test/test_db")
    os.removedirs("./test")
    print("Success")
