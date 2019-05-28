import sys
import os
############
# Fortran runcards can be partly line-fixed
# In order to read new stuff from the fixed part just add here
runcard_linecode = {
        1 : "id",
        2 : "proc",
        3 : "events",
        4 : "iterations",
        5 : "seed",
        6 : "warmup",
        7 : "production",
        8 : "pdf_name",
        9 : "pdf_member",
        10 : "jet_algorithm",
        11 : "r_cut",
        12 : "exclusive",
        13 : "decay_type",
        14 : "tc", # Technical cut
        17 : "region", # a, b, all
    }

valid_channels = ["rr","rv","vv","r","v","lo","rra","rrb"]

numeric_ids = [3,4,5,9,13]

class PROGRAMruncard:
    """
    Reads a PROGRAM runcard into a class containing
    all the different parameters


    NOTE: FOR LOGGING PURPOSES, USE self.print, self.info etc rather than just print
    If logger is initialised, these will use the logger, otherwise they'll default to
    the inbuilt print function [set up controlled by _setup_logging()]
    """

    def __init__(self, runcard_file = None, runcard_class = None, blocks = ["channels"],
                 logger=None, grid_run = True):

        self._setup_logging(logger)
        self.runcard_dict = {}
        self.runcard_dict_case_preserving = {}
        if runcard_class and isinstance(runcard_class, type(self)):
            raise Exception("Not implemented yet")
        elif runcard_file:
            pass
            # Preprocessing
            self.blocks_to_read = []
            for i in blocks:
                self.blocks_to_read.append(i.lower())

        #     # Read the runcard into runcard_dict
            self._parse_runcard_from_file(runcard_file)

        #     # Safety Checks
        #     # Check channels
            self.print("Checking channel block in {0}".format(runcard_file))
            for i in self.runcard_dict["channels"]:
                self._check_channel(i)
        self._check_numeric()
        self._check_pdf(grid_run)


    def __check_local_pdf(self):
        from subprocess import Popen, PIPE
        pdf = self.runcard_dict_case_preserving["pdf_name"]
        cmd = ["lhapdf","ls","--installed"]
        outbyt = Popen(cmd, stdout = PIPE).communicate()[0]
        pdfs = [i for i in outbyt.decode("utf-8").split("\n") if i != ""]
        try:
            assert pdf in pdfs
        except AssertionError as e:
            self.critical("PDF set {0} is not installed in local version of LHAPDF".format(pdf))


    def __check_grid_pdf(self):
        import json
        infofile = os.path.join(os.path.dirname(os.path.realpath(__file__)),".pdfinfo")
        try:
            with open(infofile,"r") as f:
                data = json.load(f)
                pdf = self.runcard_dict_case_preserving["pdf_name"]
                member = self.runcard_dict_case_preserving["pdf_member"]
                try:
                    members = data[pdf]
                except KeyError as e:
                    self.critical("PDF set {0} is not included in currently initialised version of LHAPDF".format(pdf))
                try:
                    assert int(member) in members
                except AssertionError as e:
                    self.critical("PDF member {1} for PDF set {0} is not included in currently initialised version of LHAPDF".format(pdf, member))
        except FileNotFoundError as e:
            self.warning("No PDF info file found. Skipping check.")

    def _check_pdf(self, grid_run):
        self.print("Checking PDF set validity")
        if grid_run:
            self.__check_grid_pdf()
        else:
            self.__check_local_pdf()

    def _check_numeric(self):
        """ Asserts that runcard elements that need to be numeric indeed are """
        for i in numeric_ids:
            try:
                float(self.runcard_dict[runcard_linecode[i]])
            except:
                self.critical("Line {0} [{1}] should be numeric type. Value is instead {2}.".format(i,runcard_linecode[i], self.runcard_dict[runcard_linecode[i]]))
                print(self.logger)

    # Safety check functions
    def _check_channel(self, chan):
        chan = chan.strip()
        if " " in chan:
            channels = [i for i in chan.split() if i != ""] # Split line in case it's a list of channels e.g 1 2 3
        else:
#            channels = [i for i in chan if i != ""]
            channels = [chan]
        for element in channels:
            try:
                int(element) # numeric channel
            except ValueError as e:
                if not element in valid_channels:
                    self.error("{0} is not a valid channel in your PROGRAM runcard.".format(element.upper()))
                    sys.exit(-1)

    # Parsing routines
    def _parse_fixed(self):
        """
        Parse the fixed part of the PROGRAM runcard
        """
        for line_key in runcard_linecode:
            line = self.runcard_list[line_key]
            self.runcard_dict[runcard_linecode[line_key]] = line
            line = self.runcard_list_case_preserving[line_key]
            self.runcard_dict_case_preserving[runcard_linecode[line_key]] = line
            self.debug("{0:<15}: {1:<20} {2}".format(runcard_linecode[line_key],
                                                      line, os.path.basename(self.runcard_file)))

    def _parse_block(self, block_name):
        """
        Parse PROGRAM blocks
        """
        block_start = block_name
        block_end = "end_{0}".format(block_name)

        reading = False
        block_content = []
        for line in self.runcard_list:
            if line == block_end:
                break
            if reading:
                block_content.append(line)
            if line == block_start:
                reading = True
        self.runcard_dict[block_name] = block_content
        self.debug("{0:<15}: {1:<20} {2}".format(block_name, " ".join(block_content),
                                                 os.path.basename(self.runcard_file)))

    def _parse_runcard_from_file(self, filename):
        f = open(filename, 'r', encoding="utf-8")
        self.name = filename.split("/")[-1]
        self.runcard_file = filename
        # Read entire runcard removing comments
        self.runcard_list = []
        self.runcard_list_case_preserving = []
        for line_raw in f:
            line = line_raw.strip().split("!")[0]
            if line:
                self.runcard_list.append(line.strip().lower())
                self.runcard_list_case_preserving.append(line.strip())
        f.close()

        # Step 1, save everything from the fixed part of the runcard in the dictionary
        self._parse_fixed()

        # Step 2, parse blocks into the dictionary
        for block in self.blocks_to_read:
            self._parse_block(block)

    # Internal functions for external API
    def _is_mode(self, mode, accepted = None):
        """
        Checks whether the mode is set to true from a set of
        predefined values.
        If accepted = [list of accepted values]  is provided
        the check is done against this list
        """
        mode = self.runcard_dict[mode]
        if accepted:
            if mode in accepted:
                return True
            else:
                return False
        if mode in [".false.", "0"]:
            return False
        elif mode in [".true.", "2", "1"]:
            return True

    # External API
    def is_continuation(self):
        return self._is_mode("warmup", accepted = ["2"])

    def is_warmup(self):
        return self._is_mode("warmup")

    def is_production(self):
        return self._is_mode("production")

    def warmup_filename(self):
        """
        If the runcard is set with one and only one of the generic channel names (LO, R, V...)
        returns the corresponding warmup for the process. Otherwise don't fill suffix field
        """
        born = 'vb'
        real = 'vr'
        dreal = 'rr'
        channels_raw = " ".join(self.runcard_dict["channels"])
        channels = channels_raw.strip()
        if channels in ['b', 'lo', 'v', 'vv']:
            warmup_suffix = born + self.runcard_dict["region"]
        elif channels in ['rv', 'r']:
            warmup_suffix = real + self.runcard_dict["region"]
        elif channels in ['rr']:
            warmup_suffix = dreal + self.runcard_dict["region"]
        else:
            warmup_suffix = ''
        process_name = self.runcard_dict["proc"]
        runname = self.runcard_dict["id"]
        tech_cut = self.runcard_dict["tc"]
        warmup_name = "{0}.{1}.y{2}.{3}".format(process_name, runname, tech_cut, warmup_suffix)

        return warmup_name

    def _setup_logging(self,logger):
        if logger is not None:
            self.print = logger.info
            self.info = logger.info
            self.critical = logger.critical
            self.warning = logger.warning
            self.debug = logger.debug
            self.error = logger.error
        else:
            self.print = print
            self.info = print
            self.critical = print
            self.warning = print
            self.debug = print
            self.error = print
