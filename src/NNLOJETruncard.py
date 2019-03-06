import sys
import os
############
# NNLOJET runcard are partly line-fixed
# In order to read new stuff from the fixed part just add here
valid_channels = ["rr","rv","vv","r","v","lo","rr"]

class NNLOJETruncard:
    """
    Reads a NNLOJET runcard into a class containing
    all the different parameters


    NOTE: FOR LOGGING PURPOSES, USE self.print, self.info etc rather than just print
    If logger is initialised, these will use the logger, otherwise they'll default to
    the inbuilt print function [set up controlled by _setup_logging()]
    """

    def __init__(self, runcard_file = None, runcard_class = None, 
                 blocks = {"channels":[], "process":{}, "run":{}, "misc":{}},
                 logger=None, grid_run = True):

        self._setup_logging(logger)
        self.runcard_dict = {}
        self.blocks = blocks
        if runcard_class and isinstance(runcard_class, type(self)):
            raise Exception("Not implemented yet")
        elif runcard_file:
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
                self._check_channel(i.lower())
        self._check_pdf(grid_run)


    def __repr__(self):
        return str(self.runcard_dict)
        

    def parse_pdf_entry(self):
        """TODO rewrite with regexp for niceness"""
        pdf_tag = self.runcard_dict["run"]["pdf"]
        pdf = pdf_tag.split("[")[0]
        member = pdf_tag.split("[")[-1].split("]")[0]
        return pdf, member


    def __check_local_pdf(self):
        from subprocess import Popen, PIPE
        pdf, member = self.parse_pdf_entry()
        cmd = ["lhapdf","ls","--installed"]
        outbyt = Popen(cmd, stdout = PIPE).communicate()[0]
        pdfs = [i for i in outbyt.decode("utf-8").split("\n") if i != ""]
        try:
            assert pdf in pdfs
            self.print("PDF set found")
        except AssertionError as e:
            self.critical("PDF set {0} is not installed in local version of LHAPDF".format(pdf))


    def __check_grid_pdf(self):
        import json
        infofile = os.path.join(os.path.dirname(os.path.realpath(__file__)),".pdfinfo") 
        try:
            with open(infofile,"r") as f:
                data = json.load(f)
                pdf, member = self.parse_pdf_entry()
                try:
                    members = data[pdf]
                    self.print("PDF set found")
                except KeyError as e:
                    self.critical("PDF set {0} is not included in currently initialised version of LHAPDF".format(pdf))
                try:
                    assert int(member) in members
                    self.print("PDF member found")
                except AssertionError as e:
                    self.critical("PDF member {1} for PDF set {0} is not included in currently initialised version of LHAPDF".format(pdf, member))
        except FileNotFoundError as e:
            self.warning("No PDF info file found. Skipping check.")

    def _check_pdf(self, grid_run):
        self.print("Checking PDF set validity...")
        if grid_run:
            self.__check_grid_pdf()
        else:
            self.__check_local_pdf()

    def _check_numeric(self):
        """ Asserts that runcard elements that need to be numeric indeed are """
        for i in numeric_ids:
            try:
                float(self.runcard_dict[nnlojet_linecode[i]])
            except:
                self.critical("Line {0} [{1}] should be numeric type. Value is instead {2}.".format(i,nnlojet_linecode[i], self.runcard_dict[nnlojet_linecode[i]])) 
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
                    self.error("{0} is not a valid channel in your NNLOJET runcard.".format(element.upper()))
                    sys.exit(-1)

    def _parse_block(self, block_name, blocks):
        """
        Parse NNLOJET blocks
        """
        block_start = block_name
        block_end = "end_{0}".format(block_name)

        if isinstance(blocks[block_name], dict):
            option_block = True
        elif isinstance(blocks[block_name], list):
            option_block = False

        rc_list = self.runcard_list
        rc_dict = self.runcard_dict

        reading = False
        block_content = blocks[block_name]
        for line in rc_list:
            if line.lower() == block_end:
                break
            if reading:
                if option_block:
                    option = [i.strip() for i in line.strip().split("=")]
                    block_content[option[0].lower()] = option[1]
                else:
                    block_content.append(line)
            splitline = [i.strip() for i in line.strip().split()]
            if splitline[0].lower() == block_start.lower():
                reading = True
                if len(splitline) > 1:
                    if option_block:
                        block_content[block_name] = " ".join(splitline[1:])
                    elif "=" in splitline[1:]:
                        blocks["misc"][splitline[1].lower()] = splitline[3]
        rc_dict[block_name] = block_content
        self.debug("{0:<15}: {1:<20} {2}".format(block_name, str(rc_dict[block_name]), 
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
                self.runcard_list.append(line.strip())
        f.close()

        # Step 1, parse blocks into the dictionary
        for block in self.blocks_to_read:
            self._parse_block(block, self.blocks)

    # Internal functions for external API
    def _is_mode(self, mode, accepted = None):
        """
        Checks whether the mode is set to true from a set of 
        predefined values. 
        If accepted = [list of accepted values]  is provided 
        the check is done against this list
        """
        runblock = self.runcard_dict["run"]
        if mode in runblock.keys():
            return True
        else:
            return False

    # External API
    def is_continuation(self): # Legacy support. continuation no longer requires separate flag
        return self._is_mode("warmup")

    def is_warmup(self):
        return self._is_mode("warmup")
    
    def is_production(self):
        return self._is_mode("production")

    def warmup_filename(self):
        """
        If the runcard is set with one and only one of the generic channel names (LO, R, V...)
        returns the corresponding warmup for the process. Otherwise don't fill suffix field
        """
        # Currently breaks for VJJ type processes with Ra/b contributions

        channels_raw = " ".join(self.runcard_dict["channels"])
        channels = channels_raw.strip().lower()
        if channels in ['b', 'lo', 'v', 'vv']:
            warmup_suffix = channels.upper()
        elif channels in ['rv', 'r']:
            warmup_suffix = channels.upper()
        elif channels in ['rr']:
            warmup_suffix = channels.upper() + self.runcard_dict["misc"]["region"]
        else:
            warmup_suffix = ''
        process_name = self.runcard_dict["process"]["process"]
        runname = self.runcard_dict["run"]["run"]
        tech_cut = '{0:.2E}'.format(float(self.runcard_dict["run"]["tcut"].replace("d","E")))
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


