from src.Backend import Backend
import src.header as header
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
        self.runfolder = header.runcardDir
        self.tarw      = util.TarWrap()

    def _get_warmup_args(self, runcard, tag, threads=1,
                         sockets=None, port=header.port):
        return {"runcard":runcard, "runcard_dir":self.get_local_dir_name(runcard, tag),
                "threads":threads, "sockets":sockets, "port":port}


    def _run_SLURM(self, filename, args, test=False):
        if test:
            from src.header import test_queue as queue
        else:
            from src.header import warmup_queue as queue
        if queue is not None:
            queuetag = "-p {0}".format(queue)
        else:
            queuetag = ""
        cmd = "sbatch {1} {0} -N 1 -n {2}".format(filename, queuetag, args["threads"])
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
                header.logger.info("Current submission computing queue: {0}".format(ce))
                header.logger.critical("Can't submit socketed warmups to locations other than openmp7.q")
        else:
            sockets = False
            n_sockets = 1
            if test:
                job_type = "Warmup Test"
            else:
                job_type = "Warmup"

        self.runfolder = header.runcardDir
        from src.header import warmupthr, jobName, warmup_base_dir
        # loop over al .run files defined in runcard.py

        print("Runcards selected: {0}".format(" ".join(r for r in rncards)))
        port = header.port
        for r in rncards:
            if n_sockets > 1:
                # Automagically activates the socket and finds the best port for it!
                port = sapi.fire_up_socket_server(header.server_host, port, n_sockets, 
                                                  header.wait_time, header.socket_exe,
                                                  tag="{0}-{1}".format(r,dCards[r]))
                job_type = "Socket={}".format(port)
            # TODO check if warmup exists?

            # Generate the SLURM file
            arguments = self._get_warmup_args(r, dCards[r], threads=warmupthr,
                                              sockets=sockets, port=port)
            slurmfile = self._write_SLURM(arguments)
            print(" > Path of slurm file: {0}".format(slurmfile))
            jobids = []
            for i_socket in range(n_sockets):
                # Run the file
                jobid, queue = self._run_SLURM(slurmfile, arguments, test=test)
                jobids.append(jobid)
            # Create database entry
            dataDict = {'jobid'     : ' '.join(jobids),
                        'date'      : str(datetime.now()),
                        'pathfolder': arguments["runcard_dir"],
                        'runcard'   : r,
                        'runfolder' : dCards[r],
                        'jobtype'   : job_type,
                        'queue'     : queue,
                        'status'    : "active",}
            self.dbase.insert_data(self.table, dataDict)


def runWrapper(runcard, test = None, expandedCard = None):
    print("Running SLURM job for {0}".format(runcard))
    slurm = RunSlurm()
    slurm.run_wrap_warmup(test,expandedCard)

def iniWrapper(runcard, warmup=None):
    print("Initialising SLURM for {0}".format(runcard))
    slurm = RunSlurm()
    slurm.init_warmup(warmup)
