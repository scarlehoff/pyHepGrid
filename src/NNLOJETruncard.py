############
# NNLOJET runcard are partly line-fixed
# In order to read new stuff from the fixed part just add here
nnlojet_linecode = {
        1 : "id",
        2 : "proc",
        6 : "warmup",
        7 : "production",
        14 : "tc", # Technical cut
        17 : "region", # a, b, all
    }

class NNLOJETruncard:
    """
    Reads a NNLOJET runcard into a class containing
    all the different parameters
    """

    def __init__(self, runcard_file = None, runcard_class = None, blocks = ["channels"]):
        self.runcard_dict = {}
        if runcard_class and isinstance(runcard_class, type(self)):
            raise Exception("Not implemented yet")
        elif runcard_file:
            # Preprocessing
            self.blocks_to_read = []
            for i in blocks:
                self.blocks_to_read.append(i.lower())

            # Read the runcard into runcard_dict
            self._parse_runcard_from_file(runcard_file)

            # Safety Checks

    # Parsing routines
    def _parse_fixed(self):
        """
        Parse the fixed part of the NNLOJET runcard
        """
        for line_key in nnlojet_linecode:
            line = self.runcard_list[line_key]
            self.runcard_dict[nnlojet_linecode[line_key]] = line

    def _parse_block(self, block_name):
        """
        Parse NNLOJET blocks
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

    def _parse_runcard_from_file(self, filename):
        f = open(filename, 'r')
        self.name = filename.split("/")[-1]
        self.runcard_file = filename
        # Read entire runcard removing comments
        self.runcard_list = []
        for line_raw in f:
            line = line_raw.split("!")[0]
            if line:
                self.runcard_list.append(line.strip().lower())
        f.close()

        # Step 1, save everything from the fixed part of the runcard in the dictionary
        self._parse_fixed()

        # Step 2, parse blocks into the dictionary
        for block in self.blocks_to_read:
            self._parse_block(block)

    # External API
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
        
        

