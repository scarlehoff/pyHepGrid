from pyHepGrid.src.Backend import Backend
from datetime import datetime
import pyHepGrid.src.utilities as util
import pyHepGrid.src.header as header


class RunDirac(Backend):
    """
    Subclass of Backend for Dirac submission.

    Attributes:
        table: name of Dirac jobs table in local jobs database
        templ: list of lines to be written to Dirac JDL file
        runfolder: location of runcard (passed from header)
        gridw: initialised instance of grid function wrapper class
        tarw: initialised instance of tarfile wrapper class

    """
    def __init__(self, **kwargs):
        super(RunDirac, self).__init__(**kwargs)
        self.table = header.diractable
        self.templ = header.DIRACSCRIPTDEFAULT
        self.runfolder = None
        self.gridw = util.GridWrap()
        self.tarw = util.TarWrap()

    #
    # XRSL file utilities
    #
    def _format_args(self, input_args):
        if isinstance(input_args, dict):
            string_arg = ""
            for key in input_args.keys():
                arg_value = input_args[key]
                if arg_value is not None:
                    string_arg += " --{0} {1}".format(key, arg_value)
                else:
                    string_arg += " --{0} ".format(key)
            return string_arg
        elif isinstance(input_args, str):
            return " {}".format(input_args)
        elif isinstance(input_args, list):
            return " {}".format(" ".join(input_args))
        else:
            header.logger.warning("Arguments: {}".format(input_args))
            raise Exception(F"Type of input arguments: {type(input_args)} "
                            "not recognised in DIRAC ._format_args")

    def _write_JDL(self, argument_string, start_seed, no_runs, filename=None):
        """ Writes a unique JDL file
        which instructs the dirac job to run
        """
        if not filename:
            filename = util.unique_filename()
        with open(filename, 'w') as f:
            for i in self.templ:
                f.write(i)
                f.write("\n")
            f.write("Arguments = \"{}\";\n".format(argument_string))
            f.write("Parameters = {0};\n".format(no_runs))
            f.write("ParameterStart = {0};\n".format(start_seed))
            f.write("ParameterStep = 1;\n")
            f.write("ParameterFactor = 1;\n")
        return filename

    def _run_JDL(self, filename):
        """ Sends JDL file to the dirac
        management system
        """
        cmd = "dirac-wms-job-submit {}".format(filename)
        output = util.getOutputCall(cmd.split(), include_return_code=False)
        jobids = output.rstrip().strip().split("]")[0].split("[")[-1]
        jobids = jobids.split(", ")
        return jobids

    # Run for DIRAC
    def run_wrap_production(self):
        """
        Wrapper function. It assumes the initialisation stage has already
        happened Writes JDL file with the appropiate information and send
        procrun number of jobs to the diract management system
        """
        rncards, dCards = util.expandCard()
        header.logger.info("Runcards selected: {0}".format(
            " ".join(r for r in rncards)))
        self.runfolder = header.runcardDir
        from pyHepGrid.src.header import baseSeed, producRun

        increment = 750
        for r in rncards:
            header.logger.info(
                "> Submitting {0} job(s) for {1} to Dirac".format(producRun, r))
            header.logger.info(
                "> Beginning at seed {0} in batches of {1}.".format(
                    baseSeed, increment))
            self.check_for_existing_output(r, dCards[r])
            jdlfile = None
            args = self._get_prod_args(r, dCards[r], "%s")
            joblist, remaining_seeds, seed_start = [], producRun, baseSeed
            while remaining_seeds > 0:
                no_seeds = min(increment, remaining_seeds)
                jdlfile = self._write_JDL(args, seed_start, no_seeds)
                max_seed = seed_start+no_seeds-1
                header.logger.info(
                    " > jdl file path for seeds {0}-{1}: {2}".format(
                        seed_start, max_seed, jdlfile))
                joblist += self._run_JDL(jdlfile)
                remaining_seeds = remaining_seeds - no_seeds
                seed_start = seed_start + no_seeds
            # Create daily path
            pathfolder = util.generatePath(False)
            # Create database entr
            jobStr = ' '.join(joblist)
            dataDict = {'jobid': jobStr,
                        'date': str(datetime.now()),
                        'pathfolder': pathfolder,
                        'runcard': r,
                        'runfolder': dCards[r],
                        'iseed': str(baseSeed),
                        'no_runs': str(producRun),
                        'jobtype': "Production",
                        'status': "active", }
            self.dbase.insert_data(self.table, dataDict)


def runWrapper(runcard, test=None):
    header.logger.info("Running dirac job for {0}".format(runcard))
    if test:
        header.logger.critical(
            "--test flag disallowed for Dirac as there is no test queue.")
    dirac = RunDirac()
    dirac.run_wrap_production()


def testWrapper(r, dCards):
    header.logger.info("Running dirac job for {0}".format(r))
    dirac = RunDirac()
    return dirac._get_prod_args(r, dCards[r], 1)

# code graveyard


def iniWrapper(runcard, warmupProvided=None):
    header.logger.info("Initialising dirac for {0}".format(runcard))
    dirac = RunDirac()
    dirac.init_production(warmupProvided)
