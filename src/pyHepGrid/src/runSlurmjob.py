from datetime import datetime
from pyHepGrid.src.Backend import Backend
import pyHepGrid.src.header as header
import pyHepGrid.src.socket_api as sapi
import pyHepGrid.src.utilities as util


class RunSlurm(Backend):
    """ Generic class for running Slurm scripts, both production and warmup"""

    def __init__(self, prod=False, slurmscript=None, **kwargs):
        super(RunSlurm, self).__init__(**kwargs)
        if prod:
            self.table = header.slurmprodtable
        else:
            self.table = header.slurmtable
        if slurmscript:
            self.templ = slurmscript
        else:
            self.templ = header.SLURMSCRIPTDEFAULT
        self.prodtempl = header.SLURMSCRIPTDEFAULT_PRODUCTION
        self.runfolder = header.runcardDir
        self.tarw = util.TarWrap()

    def __do_common_args(self, args, threads, queue):
        """ Setup for all arguments common to production and warmup"""
        from pyHepGrid.src.header import slurm_exclusive, slurm_exclude, jobName

        if slurm_exclusive:
            args["exclusive"] = "#SBATCH --exclusive"
        else:
            args["exclusive"] = ""
        if len(slurm_exclude) > 0:
            args["exclude_list"] = "#SBATCH --exclude={0}".format(
                ",".join(slurm_exclude)
            )
        else:
            args["exclude_list"] = ""
        args["stderrfile"] = args["stdoutfile"].replace(".out", ".err")
        args["stacksize"] = header.stacksize
        args["memsize"] = int(threads * header.stacksize * 1.2)
        args["jobName"] = jobName
        if queue is not None:
            args["partition"] = "#SBATCH --partition {0}".format(queue)
        else:
            args["partition"] = ""
        args["exe"] = header.executable_exe
        return args

    def _get_warmup_args(
        self,
        runcard,
        tag,
        threads=1,
        n_sockets=1,
        sockets=None,
        port=header.port,
        array=False,
        queue=None,
    ):
        """
        Sets and returns arguments to be passed as sbatch commands, which are
        substituted into the warmup SLURM template file using string formatting.

        In order to add new options, add <option_name> into the args dictionary,
        with the corresponding value. Then in the template file, {<option_name>}
        will be replaced by the corresponding value.
        """
        args = {
            "runcard": runcard,
            "runcard_dir": self.get_local_dir_name(runcard, tag),
            "threads": threads,
            "sockets": sockets,
            "port": port,
            "host": header.server_host,
            "socketstr": "",
            "array": "",
        }
        if sockets:
            args["socketstr"] = (
                " -port {0} -sockets {1} -host {2} -ns " "$((${{SLURM_ARRAY_TASK_ID}}))"
            ).format(port, n_sockets, header.server_host)
            args["array"] = "#SBATCH --array=1-{0}".format(n_sockets)
        if array or sockets:
            args["stdoutfile"] = (
                self.get_stdout_dir_name(args["runcard_dir"]) + "slurm-%A_%a.out"
            )
        else:
            args["stdoutfile"] = (
                self.get_stdout_dir_name(args["runcard_dir"]) + "slurm-%j.out"
            )
        args = self.__do_common_args(args, threads, queue)
        return args

    def _get_production_args(
        self, runcard, tag, baseSeed, producRun, threads, array=True, queue=None
    ):
        """
        Sets and returns arguments to be passed as sbatch commands, which are
        substituted into the production SLURM template file using string
        formatting.

        In order to add new options, add <option_name> into the args dictionary,
        with the corresponding value. Then in the template file, {<option_name>}
        will be replaced by the corresponding value.
        """
        args = {
            "runcard": runcard,
            "runcard_dir": self.get_local_dir_name(runcard, tag),
            "baseSeed": baseSeed,
            "producRun": producRun,
            "threads": threads,
        }
        if array:
            args["stdoutfile"] = (
                self.get_stdout_dir_name(args["runcard_dir"]) + "slurm-%A_%a.out"
            )
        else:
            args["stdoutfile"] = (
                self.get_stdout_dir_name(args["runcard_dir"]) + "slurm-%j.out"
            )
        args = self.__do_common_args(args, threads, queue)
        # Add arguments coming from the parent interface
        args = super().include_arguments(args)
        return args

    def _run_SLURM(self, filename, args, queue, test=False, socket=None, n_sockets=1):
        """ Takes a slurm runfile and submits it to the SLURM batch system.

        Returns the jobid and queue used for submission"""
        if queue is not None:
            queuetag = "-p {0}".format(queue)
        else:
            queuetag = ""
        cmd = "sbatch {0} {1}".format(filename, queuetag)
        header.logger.debug(cmd)
        output = util.getOutputCall(cmd.split(), include_return_code=False)
        jobid = output.strip().split()[-1]
        return jobid, queue

    def _write_SLURM(self, dictData, template, filename=None):
        """ Writes a unique SLURM file to disk for warmup/production runs based
        on the template file given. Substitutes the args given in dictData into
        the template"""
        if not filename:
            filename = util.unique_filename()
        with open(filename, "w") as f:
            slurmfile = template.format(**dictData)
            f.write(slurmfile)
        header.logger.debug(slurmfile)
        return filename

    def run_wrap_warmup(self, test=None, expandedCard=None):
        """
        Wrapper function. It assumes the initialisation stage has already
        happend Writes sbatch file with the appropiate information and send one
        single job (or n_sockets jobs) to the queue

        ExpandedCard is an override for util.expandCard for use in
        auto-resubmission
        """
        from pyHepGrid.src.header import warmupthr

        if test:
            from pyHepGrid.src.header import test_queue as queue
        else:
            from pyHepGrid.src.header import warmup_queue as queue

        # runcard names (of the form foo.run)
        # dCards, dictionary of { 'runcard' : 'name' },
        # can also include extra informations
        if expandedCard is None:
            rncards, dCards = util.expandCard()
        else:
            rncards, dCards = expandedCard

        if header.sockets_active > 1:
            sockets = True
            n_sockets = header.sockets_active
        else:
            sockets = False
            n_sockets = 1
            if test:
                job_type = "Warmup Test"
            else:
                job_type = "Warmup"

        self.runfolder = header.runcardDir
        # loop over all .run files defined in runcard.py

        header.logger.info("Runcards selected: {0}".format(" ".join(rncards)))
        port = header.port
        for r in rncards:
            if n_sockets > 1:
                # Automatically activates the socket and finds the best port!
                port = sapi.fire_up_socket_server(
                    header.server_host,
                    port,
                    n_sockets,
                    None,
                    header.socket_exe,
                    tag="{0}-{1}".format(r, dCards[r]),
                    tmuxloc=header.tmux_location,
                )
                job_type = "Socket={}".format(port)
            # TODO check if warmup exists? nah

            # Generate the SLURM file
            if n_sockets > 1:
                array = True
            else:
                array = False
            arguments = self._get_warmup_args(
                r,
                dCards[r],
                n_sockets=n_sockets,
                threads=warmupthr,
                sockets=sockets,
                port=port,
                array=array,
                queue=queue,
            )
            slurmfile = self._write_SLURM(arguments, self.templ)
            header.logger.debug(" > Path of slurm file: {0}".format(slurmfile))
            jobids = []

            jobid, runqueue = self._run_SLURM(
                slurmfile, arguments, queue, test=test, n_sockets=n_sockets
            )
            jobids.append(jobid)

            # Create database entry
            dataDict = {
                "jobid": " ".join(jobids),
                "no_runs": str(n_sockets),
                "date": str(datetime.now()),
                "pathfolder": arguments["runcard_dir"],
                "runcard": r,
                "runfolder": dCards[r],
                "jobtype": job_type,
                "queue": str(runqueue),
                "status": "active",
            }
            if len(jobids) > 0:
                self.dbase.insert_data(self.table, dataDict)
            else:
                header.logger.critical(
                    "No jobids returned, no database entry inserted for "
                    f"submission: {r} {dCards[r]}"
                )
            port += 1

    def run_wrap_production(self, test=None):
        """
        Wrapper function. It assumes the initialisation stage has already
        happend Writes sbatch file with the appropiate information and sends
        producrun # of jobs to the queue
        """
        # runcard names (of the form foo.run)
        # dCards, dictionary of { 'runcard' : 'name' }
        # can also include extra info
        rncards, dCards = util.expandCard()
        if test:
            from pyHepGrid.src.header import test_queue as queue
        else:
            from pyHepGrid.src.header import production_queue as queue
        job_type = "Production"
        self.runfolder = header.runcardDir
        from pyHepGrid.src.header import producRun, baseSeed, production_threads

        # loop over all .run files defined in runcard.py

        header.logger.info("Runcards selected: {0}".format(" ".join(rncards)))
        for r in rncards:
            self.check_for_existing_output_local(r, dCards[r], baseSeed, producRun)

            # Generate the SLURM file
            arguments = self._get_production_args(
                r,
                dCards[r],
                baseSeed,
                producRun,
                production_threads,
                array=True,
                queue=queue,
            )
            slurmfile = self._write_SLURM(arguments, self.prodtempl)
            header.logger.debug("Path of slurm file: {0}".format(slurmfile))
            jobids = []
            jobid, runqueue = self._run_SLURM(slurmfile, arguments, queue, test=test)
            jobids.append(jobid)
            # Create database entry
            dataDict = {
                "jobid": " ".join(jobids),
                "date": str(datetime.now()),
                "pathfolder": arguments["runcard_dir"],
                "runcard": r,
                "runfolder": dCards[r],
                "jobtype": job_type,
                "queue": str(runqueue),
                "iseed": str(baseSeed),
                "no_runs": str(producRun),
                "status": "active",
            }
            if len(jobids) > 0:
                self.dbase.insert_data(self.table, dataDict)
            else:
                header.logger.critical(
                    "No jobids returned, no database entry inserted for "
                    f"submission: {r} {dCards[r]}"
                )


def runWrapper(runcard, test=None, expandedCard=None):
    header.logger.info("Running SLURM job for {0}".format(runcard))
    slurm = RunSlurm()
    slurm.run_wrap_warmup(test, expandedCard)


def runWrapperProduction(runcard, test=None, expandedCard=None):
    header.logger.info("Running SLURM production job for {0}".format(runcard))
    slurm = RunSlurm(prod=True)
    slurm.run_wrap_production(test)


def iniWrapper(runcard, warmup=None):
    header.logger.info("Initialising SLURM for {0}".format(runcard))
    slurm = RunSlurm()
    slurm.init_warmup(warmup)
