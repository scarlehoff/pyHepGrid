class OptionalArgument():
    """ Wrapper class to add optional arguments to the argparse parser.

    All optional arguments are included in the main programme if added to the
    optional_arguments list below.

    Extra optional keywords:
    ignore_list_output="False" [suppresses list channels output if True]
    chanattr="<attributename>" [sets Channel class variable attributename to
                                the argparse argument value if declared.]
    """

    def __init__(self, *inlist, **kwargs):
        self.names = inlist
        self.kwargs = kwargs
        self.ID = inlist[0].replace("-", "")
        self.__add_attribute("chanattr", default=None)
        self.__add_attribute('ignore_list_output', attrname="listchans",
                             default=False)

    def __add_attribute(self, kwargname, attrname=None, default=None):
        if attrname == None:
            attrname = kwargname
        if kwargname in self.kwargs:
            setattr(self, attrname, self.kwargs[kwargname])
        else:
            setattr(self, attrname, default)
        self.kwargs.pop(kwargname, None)


optional_arguments = [
    OptionalArgument("-rc", "-w",
                     help="write out list of channels for runcards.",
                     action="store_true",
                     ignore_list_output=True),
    OptionalArgument("--sort", "-s", default="IP",
                     help="sorts output by specified attribute",
                     type=str),
    OptionalArgument("--remove_normalisation", "-rmfac", default=False,
                     help="removes factors of facXX from fac output",
                     action="store_true",
                     chanattr="rmfac"),
    OptionalArgument("-Full", "-full", "-fme", "-FME",
                     help="""Give the sum of channels for use in
                     generating full matrix elements.""",
                     action="store_true",
                     ignore_list_output=True),
    OptionalArgument("--fac", "-FAC", "-f",
                     help="""display factors if in
                     verbose mode""",
                     action="store_true",
                     chanattr="print_fac"),
    OptionalArgument("--fullfac", "-FULLFAC", "-ff",
                     help="display factors if in verbose mode",
                     action="store_true",
                     chanattr="print_fullfac"),
    OptionalArgument("--FLAV", "-flav", "-fl",
                     help="display any flavour information available",
                     action="store_true",
                     chanattr="print_flavour_info"),
    OptionalArgument("--PDFS", "-p",
                     help="display PDFs",
                     action="store_true",
                     chanattr="print_PDFS"),
    OptionalArgument("--debug", "-d", "-v",
                     help="""debug/verbose mode. Defaults to true if
                     no other output specified, such as runcard or
                     full ME output.""",
                     action="store_true",
                     ignore_list_output=True),
    OptionalArgument("--table", "-t", "--org",
                     help="writes a table of output for Emacs Org Mode.",
                     action="store_true",
                     ignore_list_output=True),
    OptionalArgument('--links', "-lk",
                     help="""add a column for driver links into table
                     output. No effect if table output not specified""",
                     action="store_true"),
    OptionalArgument('--tested', "-ts",
                     help="""add a column for tests into table output.
                     No effect if table output not specified""",
                     action="store_true"),
    OptionalArgument('--unique_initial_states', "-unique",
                     help="""print a list of unique initial state
                     parton combinations""",
                     action="store_true",
                     ignore_list_output=True),
    OptionalArgument('--unique_PDFs', "-unique-pdf",
                     help="""print a list of unique initial state
                     PDF combinations""",
                     action="store_true",
                     ignore_list_output=True),
    OptionalArgument('--LOtest', "-LO",
                     help="""write out full LO ME into test file, name
                     of which is an argument here.""",
                     nargs=None),
    OptionalArgument('--LOtestfuncname', "-LOfn",
                     help="""optional name for the function written by LOtest.
                     """,
                     nargs=None),
    OptionalArgument('--printfullcalls', "-pfc",
                     help="""include all colour orderings when printing ME arguments.
                     """,
                     action="store_true",
                     chanattr="print_all_ME_calls"),
    OptionalArgument('--print_NC', "-pNC", "-pnc",
                     help="""include NC in print output.
                     """,
                     action="store_true",
                     chanattr="print_NC"),
    OptionalArgument('--print_NF', "-pNF", "-pnf",
                     help="""include nf in print output.
                     """,
                     action="store_true",
                     chanattr="print_NF"),
]


def get_optional_arguments():
    return optional_arguments


def get_opt_args_no_list_output():
    return [x for x in optional_arguments
            if x.listchans is not False]
