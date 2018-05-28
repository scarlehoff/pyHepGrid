from src.Backend import Backend
import src.header as header
import src.socket_api as sapi
import src.utilities as util
from datetime import datetime

class RunSlurm(Backend):
    def __init__(self, slurmscript = None, **kwargs): 
        super(RunSlurm, self).__init__(**kwargs)
        self.table     = header.slurmtable
        if slurmscript:
            self.templ = slurmscript
        else:
            self.templ = header.SLURMSCRIPTDEFAULT
        self.prodtempl = header.SLURMSCRIPTDEFAULT_PRODUCTION
        self.runfolder = header.runcardDir
        self.tarw      = util.TarWrap()

    def _get_warmup_args(self, runcard, tag, threads=1, n_sockets=1,
                         sockets=None, port=header.port, array=False):
        args = {"runcard":runcard, "runcard_dir":self.get_local_dir_name(runcard, tag),
                "threads":threads, "sockets":sockets, "port":port, "host":header.server_host,
                "socketstr":"", "array":""}
        if sockets:
            args["socketstr"] = " -port {0} -sockets {1} -host {2} -ns $((${{SLURM_ARRAY_TASK_ID}}))".format(port, n_sockets, header.server_host)
            args["array"] = "#SBATCH --array=1-{0}".format(n_sockets)
        if array or sockets:
            args["stdoutfile"]=self.get_stdout_dir_name(args["runcard_dir"])+"slurm-%A_%a.out"
        else:
            args["stdoutfile"]=self.get_stdout_dir_name(args["runcard_dir"])+"slurm-%j.out"
        return args

    def _get_production_args(self, runcard, tag, baseSeed, producRun, threads, array=True):
        args = {"runcard":runcard, "runcard_dir":self.get_local_dir_name(runcard, tag),
                "baseSeed":baseSeed, "producRun":producRun-1,"threads":threads}
        if array:
            args["stdoutfile"]=self.get_stdout_dir_name(args["runcard_dir"])+"slurm-%A_%a.out"
        else:
            args["stdoutfile"]=self.get_stdout_dir_name(args["runcard_dir"])+"slurm-%j.out"
        return args

    def _run_SLURM(self, filename, args, queue, test=False, socket=None):
        if queue is not None:
            queuetag = "-p {0}".format(queue)
        else:
            queuetag = ""
        cmd = "sbatch {1} {0} -N 1 -n {2}".format(filename, queuetag, args["threads"])
        header.logger.debug(cmd)
        output = util.getOutputCall(cmd.split())
        jobid = output.strip().split()[-1]
        return jobid, queue


    def _write_SLURM(self, dictData, filename = None):
        """ Writes a unique SLURM file 
        which instructs the SLURM job to run with the appropriate args
        """
        if not filename:
            filename = util.unique_filename()
        with open(filename, 'w') as f:
            slurmfile  =self.templ.format(**dictData)
            f.write(slurmfile)
        return filename

    def _write_SLURM_production(self, dictData, filename = None):
        """ Writes a unique SLURM file 
        which instructs the SLURM job to run with the appropriate args
        """
        if not filename:
            filename = util.unique_filename()
        with open(filename, 'w') as f:
            slurmfile  =self.prodtempl.format(**dictData)
            f.write(slurmfile)
        return filename


    def run_wrap_warmup(self, test = None, expandedCard = None):
        """ Wrapper function. It assumes the initialisation stage has already happend
            Writes sbatch file with the appropiate information and send one single job
            (or n_sockets jobs) to the queue

        NOT YET SOCKET COMPATIBLE - MAY NOT HAVE TMUX :(
            ExpandedCard is an override for util.expandCard for use in auto-resubmission
        """
        # runcard names (of the form foo.run)
        # dCards, dictionary of { 'runcard' : 'name' }, can also include extra info
        if expandedCard is None:
            rncards, dCards = util.expandCard()
        else:
            rncards, dCards = expandedCard
        if test:
            from src.header import test_queue as queue
        else:
            from src.header import warmup_queue as queue
        
        if header.sockets_active > 1:
            sockets = True
            n_sockets = header.sockets_active
            if "openmp7.q" not in queue:
                header.logger.info("Current submission computing queue: {0}".format(queue))
                header.logger.critical("Can't submit socketed warmups to locations other than openmp7.q")
        else:
            sockets = False
            n_sockets = 1
            if test:
                job_type = "Warmup Test"
            else:
                job_type = "Warmup"

        self.runfolder = header.runcardDir
        from src.header import warmupthr, jobName
        # loop over al .run files defined in runcard.py

        print("Runcards selected: {0}".format(" ".join(r for r in rncards)))
        port = header.port
        for r in rncards:
            if n_sockets > 1:
                # Automagically activates the socket and finds the best port for it!
                port = sapi.fire_up_socket_server(header.server_host, port, n_sockets, 
                                                  None, header.socket_exe,
                                                  tag="{0}-{1}".format(r,dCards[r]),
                                                  tmuxloc=header.tmux_location)
                job_type = "Socket={}".format(port)
            # TODO check if warmup exists? nah

            # Generate the SLURM file
            if n_sockets >1:
                array = True
            else:
                array=False
            arguments = self._get_warmup_args(r, dCards[r], n_sockets = n_sockets,
                                              threads=warmupthr,
                                              sockets=sockets, port=port, array=array)
            slurmfile = self._write_SLURM(arguments)
            print(" > Path of slurm file: {0}".format(slurmfile))
            jobids = []
            # for i_socket in range(n_sockets):
            #     # Run the file
            jobid, queue = self._run_SLURM(slurmfile, arguments, queue, test=test)
            jobids.append(jobid)
            # Create database entry
            dataDict = {'jobid'     : ' '.join(jobids),
                        'no_runs'   : str(n_sockets),
                        'date'      : str(datetime.now()),
                        'pathfolder': arguments["runcard_dir"],
                        'runcard'   : r,
                        'runfolder' : dCards[r],
                        'jobtype'   : job_type,
                        'queue'     : queue,
                        'status'    : "active",}
            self.dbase.insert_data(self.table, dataDict)


    def run_wrap_production(self, test = None):
        """ Wrapper function. It assumes the initialisation stage has already happend
            Writes sbatch file with the appropiate information and sends producrun #
            of jobs to the queue
        """
        # runcard names (of the form foo.run)
        # dCards, dictionary of { 'runcard' : 'name' }, can also include extra info
        rncards, dCards = util.expandCard()
        if test:
            from src.header import test_queue as queue
        else:
            from src.header import production_queue as queue
        job_type="Production"
        self.runfolder = header.runcardDir
        from src.header import producRun, jobName, baseSeed, production_threads
        # loop over all .run files defined in runcard.py

        print("Runcards selected: {0}".format(" ".join(r for r in rncards)))
        for r in rncards:
            self._checkfor_existing_output_local(r, dCards[r], baseSeed, producRun)
            
            # Generate the SLURM file
            arguments = self._get_production_args(r, dCards[r], baseSeed, producRun,
                                                  production_threads, array=True)
            slurmfile = self._write_SLURM_production(arguments)
            print(" > Path of slurm file: {0}".format(slurmfile))
            jobids = []
            jobid, queue = self._run_SLURM(slurmfile, arguments, queue, test=test)
            jobids.append(jobid)
            # Create database entry
            dataDict = {'jobid'     : ' '.join(jobids),
                        'date'      : str(datetime.now()),
                        'pathfolder': arguments["runcard_dir"],
                        'runcard'   : r,
                        'runfolder' : dCards[r],
                        'jobtype'   : job_type,
                        'queue'     : queue,
                        'iseed'     : str(baseSeed),
                        'no_runs'   : str(producRun),
                        'status'    : "active",}
            self.dbase.insert_data(self.table, dataDict)



def runWrapper(runcard, test = None, expandedCard = None):
    header.logger.info("Running SLURM job for {0}".format(runcard))
    slurm = RunSlurm()
    slurm.run_wrap_warmup(test,expandedCard)

def runWrapperProduction(runcard, test = None, expandedCard = None):
    header.logger.info("Running SLURM production job for {0}".format(runcard))
    slurm = RunSlurm()
    slurm.run_wrap_production(test)

def iniWrapper(runcard, warmup=None):
    header.logger.info("Initialising SLURM for {0}".format(runcard))
    slurm = RunSlurm()
    slurm.init_warmup(warmup)
