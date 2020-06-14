#!/usr/bin/env python3

import pyHepGrid.src.utilities as util
import pyHepGrid.src.header as header
from pyHepGrid.src.Backend import Backend
import shutil
import os


class Slurm(Backend):
    """
    Backend subclass for Slurm submission.
    """
    def __init__(self, production=False, **kwargs):
        # Might not work on python2?
        super(Slurm, self).__init__(**kwargs)
        if production:
            self.table = header.slurmprodtable
        else:
            self.table = header.slurmtable
        self.production = production

    def __str__(self):
        retstr = "Slurm"
        if self.production:
            retstr += " Production"
        return retstr

    def _get_data_warmup(self, db_id):
        fields = ["runcard", "runfolder", "jobid", "pathfolder"]
        data = self.dbase.list_data(self.table, fields, db_id)[0]
        warmup_output_dir = self.get_local_dir_name(
            data["runcard"], data["runfolder"])
        warmup_extensions = (".RRa", ".RRb", ".vRa",
                             ".vRb", ".vBa", ".vBb", ".log")
        warmup_files = [i for i in os.listdir(
            warmup_output_dir) if i.endswith(warmup_extensions)]
        header.logger.info("Found files: {0}".format(", ".join(warmup_files)))
        warmup_dir = os.path.join(header.warmup_base_dir, data["runfolder"])
        os.makedirs(warmup_dir, exist_ok=True)
        for warmfile in warmup_files:
            orig = os.path.join(warmup_output_dir, warmfile)
            new = os.path.join(warmup_dir, warmfile)
            shutil.copy(orig, new)
        header.logger.info("Warmup stored in {0}".format(warmup_dir))

    def _get_data_production(self, db_id):
        fields = ["runcard", "runfolder", "jobid", "pathfolder"]
        data = self.dbase.list_data(self.table, fields, db_id)[0]
        production_output_dir = self.get_local_dir_name(
            data["runcard"], data["runfolder"])
        dat_files = [i for i in os.listdir(production_output_dir)
                     if i.endswith(".dat")]
        log_files = [i for i in os.listdir(production_output_dir)
                     if i.endswith(".log")]
        header.logger.info(dat_files, data["pathfolder"])
        production_dir = os.path.join(
            header.production_base_dir, data["runfolder"])
        os.makedirs(production_dir, exist_ok=True)
        results_folder = production_dir
        os.makedirs(results_folder, exist_ok=True)
        for prodfile in dat_files:
            orig = os.path.join(production_output_dir, prodfile)
            new = os.path.join(results_folder, prodfile)
            shutil.copy(orig, new)
        log_folder = os.path.join(results_folder, "log")
        os.makedirs(log_folder, exist_ok=True)
        for logfile in log_files:
            orig = os.path.join(production_output_dir, logfile)
            new = os.path.join(log_folder, logfile)
            shutil.copy(orig, new)

    def cat_log_job(self, jobids, jobinfo):
        import re
        import glob
        run_dir = self.get_local_dir_name(
            jobinfo["runcard"], jobinfo["runfolder"])
        log_files = [i for i in os.listdir(run_dir) if i.endswith(".log")]

        if jobinfo["iseed"] is None:
            jobinfo["iseed"] = 1
        expected_seeds = set(range(int(jobinfo["iseed"]), int(
            jobinfo["iseed"])+int(jobinfo["no_runs"])))

        logseed_regex = re.compile(r".s([0-9]+)\.[^\.]+$")
        logseeds_in_dir = set([int(logseed_regex.search(i).group(1)) for i
                               in glob.glob('{0}/*.log'.format(run_dir))])
        seeds_to_print = (logseeds_in_dir.union(expected_seeds))

        cat_logs = []
        for log_file in log_files:
            for seed in seeds_to_print:
                if F".s{seed}." in log_file:
                    cat_logs.append(log_file)
                    seeds_to_print.remove(seed)
                    break

        for log in cat_logs:
            cmd = ["cat", os.path.join(run_dir, log)]
            util.spCall(cmd)

    def _get_status(self, jobid, status):
        stat = len(
            [i for i in util.getOutputCall(
                ["squeue", F"-j{jobid}", "-r", "-t", status],
                suppress_errors=True, include_return_code=False
            ).split("\n")[1:]
                if "error" not in i])  # strip header from results
        if stat > 0:
            stat = stat-1
        return stat

    def stats_job(self, dbid):
        tags = ["runcard", "runfolder", "date"]
        jobids = self.get_id(dbid)  # only have one array id for SLURM
        runcard_info = self.dbase.list_data(self.table, tags, dbid)[0]
        running, waiting, fail, tot = 0, 0, 0, 0
        for jobid in jobids:
            running += self._get_status(jobid, "R")
            waiting += self._get_status(jobid, "PD")
            fail += self._get_status(jobid, "F")+self._get_status(jobid, "CA")
            tot += self._get_status(jobid, "all")
        done = tot-fail-waiting-running
        self.stats_print_setup(runcard_info, dbid=dbid)
        self._print_stats(done, waiting, running, fail, 0, 0, tot)

    def cat_job(self, jobids, jobinfo, print_stderr=None, store=False):
        """ print standard output of a given job"""
        dir_name = self.get_stdout_dir_name(self.get_local_dir_name(
            jobinfo["runcard"], jobinfo["runfolder"]))
        # jobids = length 1 for SLURM jobs - just take the only element here
        jobid = jobids[0]
        output = []
        if jobinfo["jobtype"] == "Production" or "Socket" in jobinfo["jobtype"]:
            for subjobno in range(1, int(jobinfo["no_runs"])+1):
                stdoutfile = os.path.join(
                    dir_name, "slurm-{0}_{1}.out".format(jobid, subjobno))
                if print_stderr:
                    stdoutfile = stdoutfile.replace(".out", ".err")
                cmd = ["cat", stdoutfile]
                if not store:
                    util.spCall(cmd)
                else:
                    output.append(util.getOutputCall(cmd, suppress_errors=True,
                                                     include_return_code=False))
        else:
            stdoutfile = os.path.join(dir_name, F"slurm-{jobid}.out")
            if print_stderr:
                stdoutfile = stdoutfile.replace(".out", ".err")
            cmd = ["cat", stdoutfile]
            if not store:
                util.spCall(cmd)
            else:
                output.append(util.getOutputCall(cmd, suppress_errors=True,
                                                 include_return_code=False))
        if store:
            return output

    def kill_job(self, jobids, jobinfo):
        header.logger.debug(jobids, jobinfo)
        if len(jobids) == 0:
            header.logger.critical(
                "No jobids stored associated with this database entry, "
                "therefore nothing to kill.")

        for jobid in jobids:
            util.spCall(["scancel", str(jobid)])
        # Kill the socket server if needed
        # if "Socket" in jobinfo["jobtype"]:
        #     hostname = header.server_host
        #     port  = jobinfo["jobtype"].split("=")[-1]
        #     self._press_yes_to_continue(
        #       "  \033[93m WARNING:\033[0m Killing "
        #       "TMUX server for job at {0}:{1}".format(hostname,port))
        #     import pyHepGrid.src.socket_api as sapi
        #     sapi.socket_sync_str(hostname,port,b"bye!")

    def status_job(self, jobids, verbose=False):
        """ print the current status of a given job """
        running, waiting, fail, tot = 0, 0, 0, 0
        for jobid in jobids:
            running += self._get_status(jobid, "R")
            waiting += self._get_status(jobid, "PD")
            fail += self._get_status(jobid, "F")+self._get_status(jobid, "CA")
            tot += self._get_status(jobid, "all")
        done = tot-fail-waiting-running
        self._print_stats(done, waiting, running, fail, 0, 0, tot)


if __name__ == '__main__':
    from sys import version_info
    print("Test for pyHepGrid.src.SlurmBackend.py")
    print("Running with: Python ", version_info.major)
    print("This test needs to be ran at gridui")
    slurm = Slurm()
    print("Instantiate classes")
