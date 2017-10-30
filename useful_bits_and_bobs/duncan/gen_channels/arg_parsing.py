import argparse as ap
import channel as ch
import fnmatch
import optional_arguments as optargs
import os
import selectors_rejectors as selrej


def parse_cmd_input(schans=None, qcdnorms=None):
    descriptiontxt = get_description_text()
    epiloguetxt = "Available processes are: " + available_procs(schans)

    parser = ap.ArgumentParser(
        description=descriptiontxt, epilog=epiloguetxt,
        formatter_class=ap.RawTextHelpFormatter)

    parser.add_argument(
        "proc", help="process name to parse for input.", nargs="+")
    add_selectors_rejectors(parser)
    optional_arguments = add_optional_arguments(parser)

    args = parser.parse_args()

    check_list_output_suppression(args)
    add_Channel_attribute_correspondence(optional_arguments, args)

    return args


def add_Channel_attribute_correspondence(optional_arguments, args):
    """ Set Channel class attributes to the values coming from optional
    arguments if an optional argument has kwarg chanattr declared in
    optional_argument.py. Equivalent to:

    Channel.(OptionalArgument.chanattr) = OptionalArgumentValue

    where chanattr is not None/not explicitly declared
    """
    for argument in optional_arguments:
        if argument.chanattr is not None:
            setattr(ch.Channel, argument.chanattr, getattr(args, argument.ID))


def add_selectors_rejectors(parser):
    selector_rejector_list = selrej.get_selectors_rejectors()
    for sel in selector_rejector_list:
        parser.add_argument(*sel.names, help=sel.help, nargs=sel.nargs)


def add_optional_arguments(parser):
    opt_argument_list = optargs.get_optional_arguments()
    for arg in opt_argument_list:
        parser.add_argument(*arg.names, **arg.kwargs)
    return opt_argument_list


def available_procs(selchans):
    procs = sorted([i.proc for i in selchans])
    retstr = " ".join(str(i) for i in procs)
    return retstr


def get_matches(channels, args, matchname):
    retlist = []
    for match in getattr(args, matchname.ID):
        for chan in channels:
            attribute = getattr(chan, matchname.ID)

            # SPECIAL CASES
            if "PDF" in matchname.ID:
                if match[0] != "+" and match[0] != "-":
                    match = "+"+match

            if "NC" in matchname.ID or "NF" in matchname.ID:
                for x in attribute:
                    if fnmatch.fnmatch(x, str(match)):
                        retlist.append(chan)
                continue

            # ATTRIBUTE MATCHING
            if fnmatch.fnmatch(attribute, match):
                if matchname.strict:
                    if match == attribute:
                        retlist.append(chan)
                else:
                    retlist.append(chan)
    return retlist


def parse_selector(args, channels, selector):
    retchans = []
    if getattr(args, selector.ID) is not None:
        retchans = get_matches(channels, args, selector)
        return retchans
    else:
        return channels


def parse_rejector(args, channels, rejector):
    if getattr(args, rejector.ID) is not None:
        rejectlist = get_matches(channels, args, rejector)
        rejectlist = list(set(rejectlist))
        retchans = [i for i in channels if i not in rejectlist]
        return retchans
    else:
        return channels


def run_selectors_rejectors(allchans, args):
    for selector in selrej.get_selectors():
        allchans = parse_selector(args, allchans, selector)
    for rejector in selrej.get_rejectors():
        allchans = parse_rejector(args, allchans, rejector)
    allchans = list(set(allchans))
    return allchans


def check_list_output_suppression(args):
    """Suppresses channel list if one of the optional arguments with
    ignore_list_output is called"""
    no_list_output_opt_args = optargs.get_opt_args_no_list_output()
    no_list_input = [getattr(args, i.ID) for i in no_list_output_opt_args]
    if all(not x for x in no_list_input):
        args.debug = True


def get_description_text():
    dir_path = os.path.dirname(os.path.realpath(__file__))
    with open(os.path.join(dir_path, "description.txt")) as infile:
        description = infile.read()
    return description
