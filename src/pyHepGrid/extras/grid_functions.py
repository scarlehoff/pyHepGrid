#!/usr/bin/env python2
"""
This is a selection of common functions, that can be used on the grid. The file
will be copied to each grid node.

Example usage inside your run script:
```
import grid_functions

grid_functions.print_flush("Hallo world")
```
"""
from __future__ import print_function
from time import sleep
import datetime
import os
import subprocess
import sys

MAX_COPY_TRIES = 15
GFAL_TIMEOUT = 300
PROTOCOLS = ["xroot", "gsiftp", "dav"]
LOG_FILE = "output.log"
COPY_LOG = "copies.log"
DEBUG_LEVEL = 0


def print_flush(string):
    """
    Override print with custom version that always flushes to stdout so we have
    up-to-date logs
    """
    print(string)
    sys.stdout.flush()


def print_file(string, logfile=LOG_FILE):
    """ print string to file """
    with open(logfile, "a") as f:
        f.write(string+"\n")


def do_shell(*args):
    """
    Custom wrapper around os.system that auto sets debug level on failure
    """
    global DEBUG_LEVEL
    retval = os.system(*args)
    if retval != 0:
        DEBUG_LEVEL = 9999
        print_flush("Error in {0}. Raising debug level to 9999".format(*args))
    # All non zero error codes will be +ve - can add all to determine whether
    # job is ok
    return abs(retval)


def parse_arguments(parser=None):

    """
    Initialise default runtime arguments

    Args:
        parser (OptionParser, optional): Predefined parser for customisation

    Returns:
        dict: Arguments
    """
    from getpass import getuser

    if parser is None:
        from optparse import OptionParser
        parser = OptionParser(usage="usage: %prog [options]")

    default_user_gfal = ("gsiftp://se01.dur.scotgrid.ac.uk/dpm/" +
                         "dur.scotgrid.ac.uk/home/pheno/{0}").format(getuser())

    parser.add_option("-r", "--runcard", help="Runcard to be run")
    parser.add_option("-j", "--runname", help="Runname")

    # Run options
    parser.add_option("-t", "--threads",
                      help="Number of thread for OMP", default="1")
    parser.add_option("-e", "--executable",
                      help="Executable to be run", default="HEJ")
    parser.add_option("-d", "--debug", help="Debug level", default="0")
    parser.add_option("--copy_log", help="Write copy log file.", default=False)
    parser.add_option("-s", "--seed", help="Run seed", default="1")
    parser.add_option("-E", "--events", help="Number of events", default="-1")

    # Grid configuration options
    parser.add_option(
        "-i", "--input_folder",
        help="gfal input folder, relative to gfaldir", default="input")
    parser.add_option(
        "-w", "--warmup_folder",
        help="gfal file (not just the folder!) where HEJ is stored, relative "
        "to gfaldir",
        default="warmup")
    parser.add_option(
        "-o", "--output_folder",
        help="gfal output folder, relative to gfaldir", default="output")
    parser.add_option("-g", "--gfaldir", help="gfaldir",
                      default=default_user_gfal)
    parser.add_option(
        "--gfal_location", default="",
        help="Provide a specific location for gfal executables [intended for "
        "cvmfs locations]. Default is the environment gfal.")

    # LHAPDF options
    parser.add_option("--use_cvmfs_lhapdf", default=True)
    parser.add_option(
        "--cvmfs_lhapdf_location", default="",
        help="Provide a cvmfs location for LHAPDF.")
    parser.add_option(
        "--lhapdf_grid", help="absolute value of lhapdf location or relative to"
        " gfaldir",
        default="util/lhapdf.tar.gz")
    parser.add_option(
        "--lhapdf_local", help="name of LHAPDF folder local to the sandbox",
        default="lhapdf")

    # Rivet options
    parser.add_option("--use_custom_rivet", default=False)
    parser.add_option("--rivet_folder", default="Wjets/Rivet/Rivet.tgz",
                      help="Provide the location of RivetAnalyses tarball.")

    # Socket options
    parser.add_option("-S", "--Sockets", help="Activate socketed run",
                      action="store_true", default=False)
    parser.add_option(
        "-p", "--port", help="Port to connect the sockets to", default="8888")
    parser.add_option("-H", "--Host", help="Host to connect the sockets to",
                      default="gridui1.dur.scotgrid.ac.uk")

    # Mark the run as production or warmup
    parser.add_option("-P", "--Production", help="Production run",
                      action="store_true", default=False)
    parser.add_option("-W", "--Warmup", help="Warmup run",
                      action="store_true", default=False)

    parser.add_option("--pedantic", help="Enable various checks",
                      action="store_true", default=False)

    (options, positional) = parser.parse_args()

    if positional:
        parser.error(
            "Positional arguments are not allowed, found {0}".format(positional))

    if not options.runcard or not options.runname:
        parser.error("Runcard and runname must be provided")

    if options.Production == options.Warmup:
        parser.error(
            "You need to enable one and only one of production and warmup")

    # Pedantic checks
    if options.Production:
        if options.pedantic:
            if int(options.threads) > 1:
                parser.error("Can't run production on more than one core")
            if int(options.threads) > 16:
                parser.error("No node can run more than 16 threads at a time!")
        if options.Sockets:
            parser.error("Probably a bad idea to run sockets in production")

    print_flush("Arguments: {0}".format(options))

    # global options
    global DEBUG_LEVEL
    DEBUG_LEVEL = int(options.debug)

    return options


# ------------------------- SYSTEM UTILITIES -------------------------
def set_default_environment(args):
    """
    Initialise default environment, such that gfal works
    """
    do_shell(
        "export PYTHONPATH=${PYTHONPATH}:${DIRAC}/Linux_x86_64_glibc-2.12/lib"
        "/python2.6/site-packages")
    try:
        import gfal2_util.shell
        print_flush("Using default gfal at {0}".format(
            gfal2_util.shell.__file__))
    except KeyError:
        pass
    except ImportError:
        # If gfal can't be imported then the site packages need to be added to
        # the python path because ? :(
        os.environ["PYTHONPATH"] = os.environ.get("PYTHONPATH", "")\
            + ":" + \
            args.gfal_location.replace("/bin/", "/lib/python2.7/site-packages/")
        os.environ["LD_LIBRARY_PATH"] = os.environ.get("LD_LIBRARY_PATH", "")\
            + ":"+args.gfal_location.replace("/bin/", "/lib/")
    return 0


def run_command(command):
    """run command & catch output in LOG_FILE"""
    # Avoid overwriting of the status code for piping to tee
    command = 'bash  -o pipefail -c "{0}  2>&1 | tee -a {1}"'.format(
        command, LOG_FILE)
    print_flush(" > Executed command: {0} ({1})".format(
        command, datetime.datetime.now()))
    return do_shell(command)


# ------------------------- TAR UTILITIES -------------------------
def untar_file(local_file):
    if DEBUG_LEVEL > 16:
        cmd = "tar zxfv {0}".format(local_file)
    else:
        cmd = "tar zxf " + local_file
    return do_shell(cmd)


def tar_this(tarfile, sourcefiles):
    cmd = "tar -cvzf " + tarfile + " " + sourcefiles
    stat = do_shell(cmd)
    do_shell("ls")
    return stat


# ------------------------- COPY UTILITIES -------------------------
def copy_from_grid(grid_file, local_file, args, maxrange=MAX_COPY_TRIES):
    """Downloads a file from gfal

    Args:
        grid_file (string): source file on gfal, relative to args.gfaldir
        local_file (string): target file name
        args (dict): general configuration, i.e. from parse_arguments
        maxrange (int, optional): maximal number of copy attempts

    Returns:
        int: Return value, 0 for successful download
    """
    filein = os.path.join(args.gfaldir, grid_file)
    fileout = "file://$PWD/{0}".format(local_file)
    return _grid_copy(filein, fileout, args, maxrange=maxrange)


def copy_to_grid(local_file, grid_file, args, maxrange=MAX_COPY_TRIES):
    """Upload a file to gfal

    Args:
        local_file (string): source file name
        grid_file (string): target file on gfal, relative to args.gfaldir
        args (dict): general configuration, i.e. from parse_arguments
        maxrange (int, optional): maximal number of copy attempts

    Returns:
        int: Return value, 0 for successful upload
    """
    filein = "file://$PWD/{0}".format(local_file)
    fileout = os.path.join(args.gfaldir, grid_file)
    return _grid_copy(filein, fileout, args, maxrange=maxrange)


def _remove_file(filepath, args, tries=5, protocol=None):
    if protocol:
        prot = args.gfaldir.split(":")[0]
        filepath = filepath.replace(prot, protocol, 1)
    rmcmd = "{gfal_loc}gfal-rm {f}".format(f=filepath,
                                           gfal_loc=args.gfal_location)

    file_present = _test_file_presence(filepath, args)
    tried = 0
    try:
        while file_present and tried < tries:
            print_flush("Removing file {of}.".format(of=filepath))
            retval = os.system(rmcmd)
            if DEBUG_LEVEL > 1:
                print_flush(rmcmd)
                print_flush("returned error code: {ec}".format(ec=retval))
            file_present = _test_file_presence(filepath, args)
            if DEBUG_LEVEL > 1:
                print_flush("File still present? {TF}".format(TF=file_present))
            tried += 1
            sleep(tried)

    # don't crash if gfal-rm throws an error
    except subprocess.CalledProcessError as e:
        if args.copy_log:
            print_file(
                "Gfal-rm failed at {t}.".format(t=datetime.datetime.now()),
                logfile=COPY_LOG)
            print_file("   > Command issued: {cmd}".format(
                cmd=rmcmd), logfile=COPY_LOG)
        if DEBUG_LEVEL > 1:
            print_flush(e)
        return 1

    return 0


def _test_file_presence(filepath_in, args, protocol=None):
    if protocol:
        all_protocols = [protocol] + list(set(PROTOCOLS) - {protocol})
    else:
        all_protocols = [None]
    filepath = filepath_in

    for loop_prot in all_protocols:
        if loop_prot:
            prot = args.gfaldir.split(":")[0]
            filepath = filepath_in.replace(prot, loop_prot, 1)

        filename = os.path.basename(filepath)
        lscmd = "{gfal_loc}gfal-ls -t {timeout} {file}".format(
            gfal_loc=args.gfal_location, file=filepath, timeout=GFAL_TIMEOUT)
        if DEBUG_LEVEL > 1:
            print_flush(lscmd)
        try:
            # In principle, empty if file doesn't exist, so unnecessary to check
            # contents. Test to be robust against unexpected output.
            filelist = subprocess.check_output(
                lscmd, shell=True, universal_newlines=True).splitlines()[0]
            return (filename in filelist)
        except subprocess.CalledProcessError as e:
            if args.copy_log:
                print_file(
                    "Gfal-ls failed at {t}.".format(t=datetime.datetime.now()),
                    logfile=COPY_LOG)
                print_file("   > Command issued: {cmd}".format(
                    cmd=lscmd), logfile=COPY_LOG)
            if DEBUG_LEVEL > 1:
                print_flush(e)

    if DEBUG_LEVEL > 1:
        print_file("Gfal-ls failed for all protocols.")
    return False


def _get_hash(filepath, args, algo="MD5", protocol=None):
    if protocol:
        prot = args.gfaldir.split(":")[0]
        filepath = filepath.replace(prot, protocol, 1)
    hashcmd = "{gfal_loc}gfal-sum -t {timeout} {file} {checksum}".format(
        gfal_loc=args.gfal_location, file=filepath, checksum=algo,
        timeout=GFAL_TIMEOUT)
    if DEBUG_LEVEL > 1:
        print_flush(hashcmd)
    try:
        hash = subprocess.check_output(
            hashcmd, shell=True, universal_newlines=True).split()[1]
    except subprocess.CalledProcessError as e:
        if args.copy_log:
            print_file(
                "Gfal-sum failed at {t}.".format(t=datetime.datetime.now()),
                logfile=COPY_LOG)
            print_file("   > Command issued: {cmd}".format(
                cmd=hashcmd), logfile=COPY_LOG)
        if DEBUG_LEVEL > 1:
            print_flush(e)
        # try again when gsiftp is down (nothing to lose)
        if protocol == "gsiftp":
            return _get_hash(filepath, args, algo=algo, protocol="dav")
        return None
    return hash


def _grid_copy(infile, outfile, args, maxrange=MAX_COPY_TRIES):
    protoc = args.gfaldir.split(":")[0]
    # gfal-sum only returns expected hash for subset of protocols (gsiftp, srm,
    # dav, davs). Hardcode gsiftp for now.
    infile_hash = _get_hash(infile, args, protocol="gsiftp")

    print_flush("Copying {0} to {1}".format(infile, outfile))
    for i in range(maxrange):
        print_flush("Attempting copy try {0}".format(i+1))

        # cycle through available protocols until one works.
        for j, protocol in enumerate(PROTOCOLS):
            infile_tmp = infile.replace(protoc, protocol, 1)
            outfile_tmp = outfile.replace(protoc, protocol, 1)

            print_flush("Attempting Protocol {0}".format(protocol))

            cmd = "{2}gfal-copy -f -p {0} {1}".format(
                infile_tmp, outfile_tmp, args.gfal_location)
            if DEBUG_LEVEL > 1:
                print_flush(cmd)
            retval = os.system(cmd)
            file_present = _test_file_presence(outfile, args, protocol="gsiftp")
            # if compatibiility with python versions < 2.7 is still required,
            # need something like the following instead
            # p = subprocess.Popen(cmd2, stdout=subprocess.PIPE,
            #     stderr=subprocess.PIPE, shell=True)
            # out, err = p.communicate()
            if retval == 0 and file_present:
                return 0
            elif retval == 0 and not file_present:
                print_flush("Copy command succeeded, but failed to copy file. "
                            "Retrying.")
            elif retval != 0 and file_present:
                if not infile_hash:
                    print_flush("Copy reported error, but file present & can "
                                "not compute original file hash. Proceeding.")
                    return 0

                outfile_hash = _get_hash(outfile, args, protocol="gsiftp")
                if not outfile_hash:
                    print_flush("Copy reported error, but file present & can "
                                "not compute copied file hash. Proceeding.")
                    return 0
                elif infile_hash == outfile_hash:
                    print_flush("Copy command reported errors, but file was "
                                "copied and checksums match. Proceeding.")
                    return 0
                else:
                    print_flush("Copy command reported errors and the "
                                "transferred file was corrupted. Retrying.")
            else:
                print_flush("Copy command failed. Retrying.")
            if args.copy_log:
                print_file("Copy failed at {t}.".format(
                    t=datetime.datetime.now()), logfile=COPY_LOG)
                print_file("   > Command issued: {cmd}".format(
                    cmd=cmd), logfile=COPY_LOG)
                print_file("   > Returned error code: {ec}".format(
                    ec=retval), logfile=COPY_LOG)
                print_file("   > File now present: {fp}".format(
                    fp=file_present), logfile=COPY_LOG)
            # sleep time scales steeply with failed attempts (min wait 1s, max
            # wait ~2 mins)
            sleep((i+1)*(j+1)**2)

    # Copy failed to complete successfully; attemt to clean up corrupted files
    # if present. Only make it this far if file absent, or present and
    # corrupted.
    for protocol in PROTOCOLS:
        if _remove_file(outfile, args, protocol=protocol) == 0:
            break

    return 9999


# ------------------------- Misc -------------------------
def print_node_info(outputfile):
    """Save information about this node to outputfile"""
    do_shell("hostname >> {0}".format(outputfile))
    do_shell("gcc --version >> {0}".format(outputfile))
    do_shell("python --version >> {0}".format(outputfile))
    do_shell(
        "(python3 --version || echo no python3) >> {0}".format(outputfile))
    do_shell("gfal-copy --version >> {0}".format(outputfile))
    do_shell("cat {0}".format(outputfile))  # print to log


def end_program(status):
    """Exit program and print general debug informations

    Args:
        status (int): return code, 0 for success
    """
    # print debug infos here if status!=0
    if status != 0 or DEBUG_LEVEL > 8:
        do_shell("cat "+LOG_FILE)
        do_shell("ls")
    end_time = datetime.datetime.now()
    print_flush("End time: {0}".format(end_time.strftime("%d-%m-%Y %H:%M:%S")))
    print_flush("Final return Code: {0}".format(status))
    # make sure we return a valid return code
    if status != 0:
        sys.exit(1)
    sys.exit(0)
