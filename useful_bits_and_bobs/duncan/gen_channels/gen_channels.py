#!/usr/bin/env python2
#############################################
#                   TODO                    #
#############################################
# Fix FullFac / Factor generation
import os
import sys
import config as cf
import file_parsing as fp
import arg_parsing as apg
import gen_output as go

##############################################
#               READ CONFIG                  #
##############################################

NNLOJET_LOC = os.path.expanduser(cf.NNLOJET_LOC)

##############################################
#               FILE CLASSES                 #
##############################################


class NNLOJETFile():
    def __init__(self, location, name):
        self.fullpath = location
        self.proc = location.split(name)[-1].split(".")[0]


class SelectChannelFile(NNLOJETFile):
    """Wrapper class for input selectchannel files"""


class SigFile(NNLOJETFile):
    """Wrapper class for input sig files"""

    def __init__(self, location, name):
        self.fullpath = location
        self.order = name
        if "NLO" in os.path.basename(location):
            self.order += "NLO"
        self.proc = location.split(name)[-1].split(".")[0].replace("NLO", "")


class QCDNormFile(NNLOJETFile):
    """Wrapper class for input selectchannel files"""


##############################################
#                  FUNCTIONS                 #
##############################################


def get_qcdnorms(location):
    """Recursively searches location to find qcdnorm* type files
    """
    return get_files(location, "qcdnorm", QCDNormFile, ".f")


def get_selchans(location):
    """Recursively searches location to find selectchannel* type files
    """
    return get_files(location, "selectchannel", SelectChannelFile, ".f")


def get_subfiles(location, proc):
    sub_files = ["sigB", "sigT", "sigU", "sigS"]
    files = []
    for string in sub_files:
        files += get_files(location, string, SigFile, ".f")
    files = search_sigs(proc, files)
    return files


def get_MEfiles(location, proc):
    sub_files = ["sigB", "sigR", "sigRR", "sigRV"]
    files = []
    for string in sub_files:
        files += get_files(location, string, SigFile, ".f")
    files = search_sigs(proc, files)
    return files


def get_files(location, name, inclass, ext):
    """Recursively searches location to find sig* type files
    """
    def setup(location):
        get_files.matches = []
        for root, dirnames, filenames in os.walk(location):
            for filename in filenames:
                get_files.matches.append(os.path.join(root, filename))

    def append_files(name, ext):
        files = []
        for match in get_files.matches:
            if name in match and "~" not in match\
               and "#" not in match and match.endswith(ext):
                files.append(inclass(match, name))
        return files

    try:
        return append_files(name, ext)
    except AttributeError as e:
        setup(location)
        return append_files(name, ext)


def search_qcdnorms(proc, qcdnorms):
    for i in qcdnorms:
        if proc == i.proc:
            return i.fullpath
    exceptiontxt = "Process id " + proc + " not found for qcdnorm. "
    exceptiontxt += "Available processes are: " + \
        "\n" + apg.available_procs(qcdnorms)
    print exceptiontxt
    sys.exit()


def search_sigs(proc, subfiles):
    subs = []
    for i in subfiles:
        if proc == i.proc:
            subs.append(i)
    if len(subs) > 0:
        return subs
    exceptiontxt = "Process " + proc + " sigfiles not found. "
    print exceptiontxt
    sys.exit()


def search_selchans(proc, selchans):
    for i in selchans:
        if proc == i.proc:
            return i.fullpath
    exceptiontxt = "Process id " + proc + " not found. "
    exceptiontxt += "Available processes are: " + \
        "\n" + apg.available_procs(selchans)
    print exceptiontxt
    sys.exit()


def get_channel_info(proc, channel, loc):
    import argparse as ap
    # Assumes hardcoded repo
    infiles = get_selchans(loc)
    qcdfiles = get_qcdnorms(loc)
    args = ap.Namespace
    args.proc = proc
    scfile = search_selchans(args, infiles)
    qcdfile = search_qcdnorms(args, qcdfiles)
    allchans = fp.parse_infile(scfile)
    fp.parse_qcdnorm(qcdfile, allchans)
    for chan in allchans:
        if chan.IP == channel:
            return chan


def get_potential_files(NNLOJET_LOC):
    infiles = get_selchans(NNLOJET_LOC)
    qcdfiles = get_qcdnorms(NNLOJET_LOC)
    return infiles, qcdfiles


def search_potential_files(proc, infiles, qcdfiles):
    scfile = search_selchans(proc, infiles)
    qcdfile = search_qcdnorms(proc, qcdfiles)
    return scfile, qcdfile

##############################################
#                    MAIN                    #
##############################################


if __name__ == "__main__":
    # Initial parse to set up helptext for argparser
    infiles, qcdfiles = get_potential_files(NNLOJET_LOC)

    # Parse command line input
    args = apg.parse_cmd_input(
        schans=infiles, qcdnorms=qcdfiles)

    for proc in args.proc:
        if len(args.proc) > 1:
            print(proc+":  ")
        # Get appropriate input files
        sigfiles = get_subfiles(NNLOJET_LOC, proc)
        MEsigfiles = get_MEfiles(NNLOJET_LOC, proc)
        scfile, qcdfile = search_potential_files(
            proc, infiles, qcdfiles)

        # Take channel information from the files
        allchans = fp.parse_files(scfile, qcdfile, sigfiles, MEsigfiles)
        # Apply selectors/rejectors
        allchans = apg.run_selectors_rejectors(allchans, args)

        # Option specific output
        if args.debug:
            go.write_channels(allchans, key=args.sort)
        if args.rc:
            go.write_output_ip(allchans)
        if args.Full:
            go.write_full_ME(allchans)
        if args.table:
            go.write_table(allchans, args)
        if args.unique_initial_states:
            go.write_unique_partons(allchans)
        if args.unique_PDFs:
            go.write_unique_PDFs(allchans)
        if args.LOtest is not None:
            go.write_LO_test(allchans, args)
