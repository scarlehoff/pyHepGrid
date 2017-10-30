import copy


class Selector():
    """ Wrapper class to add selectors and rejectors to the argparse parser.

    All selectors (and associated rejectors) are included in the main
    programme if added to the selectors list below.

    Extra optional keywords:
    strict=False [activates strict argument checking - attributes must EXACTLY
                  match to be selected/rejected]
    """

    def __init__(self, inlist, help="", nargs="*", strict=False):
        self.names = inlist
        self.help = help
        self.nargs = nargs
        self.ID = inlist[0].replace("-", "")
        self.strict = strict

    def create_rejector(self):
        rejector = copy.deepcopy(self)
        rejector.help = rejector.help.replace("selec", "rejec")
        rejector.names = [x.replace("--", "--r").replace("-", "-r")
                          for x in rejector.names]
        rejector.ID = rejector.names[0].replace("-", "")
        return rejector


# Would be nice to store this in a config file of some sort.
selectors = [
    Selector(["-ME", "-me"],
             help="selects by ME"),
    Selector(["-MSUB", "-msub"],
             help="""selects  by subtraction term & matrix element,
             where subtraction terms take priority unless LO.""",),
    Selector(["-SUB", "-sub"],
             help="selects by subtraction term"),
    Selector(["-IP", "-ip"],
             help="selects by IP (channel number)"),
    Selector(["-IP1", "-ip1"],
             help="selects by IP1",
             strict=True),
    Selector(["-IP2", "-ip2"],
             help="selects by IP2",
             strict=True),
    Selector(["-PDF1", "-pdf1"],
             help="selects by PDF1"),
    Selector(["-PDF2", "-pdf2"],
             help="selects by PDF2"),
    Selector(["-O", "-o", "-order"],
             help="selects by order: LO,R,V,RR,RV,VV"),
    Selector(["-B1", "-nfB1", "-nfb1"],
             help="selects by B1"),
    Selector(["-C1", "-nfC1", "-nfc1"],
             help="selects by C1"),
    Selector(["-C2", "-nfC2", "-nfc2"],
             help="selects by C2"),
    Selector(["-PO", "-po", "-pertorder"],
             help="selects by perturbative order: LO, NLO, NNLO"),
    Selector(["-NC"],
             help="selects by power of NC"),
    Selector(["-NF"],
             help="selects by power of NF")
]

rejectors = [i.create_rejector() for i in selectors]

selector_IDs = [x.ID for x in selectors]


def get_selectors():
    return selectors


def get_rejectors():
    return rejectors


def get_selectors_rejectors():
    return selectors+rejectors


def get_selector_IDs():
    return selector_IDs
