#!/usr/bin/env python
from __future__ import print_function
import os
import sys
import subprocess
import datetime
from time import sleep
import socket
from optparse import OptionParser
from getpass import getuser

# NOTE: Try to keep this all python2.4 compatible. It may fail at some nodes
# otherwise :( Hopefully after the shutdown we can rely on python 2.6/7 but that
# is TBC

RUN_CMD = "OMP_NUM_THREADS={0} ./{1} -run {2}"
MAX_COPY_TRIES = 15
GFAL_TIMEOUT = 300
PROTOCOLS = ["xroot", "gsiftp", "dav"]
LOG_FILE = "run.log"
COPY_LOG = "copies.log"

####### MISC ABUSIVE SETUP #######
def print_flush(string):
    """
    Override print with custom version that always flushes to stdout so we have
    up-to-date logs
    """
    print(string)
    sys.stdout.flush()


def print_file(string, logfile=LOG_FILE):
    with open(logfile, "a") as f:
        f.write(string+"\n")


####### FILE NAME HELPERS #######
def warmup_name(runcard, rname):
    # This function must always be the same as the one in Backend.py
    out = "output{0}-warm-{1}.tar.gz".format(runcard, rname)
    return out


def warmup_name_ns(runcard, rname, socket_no):
    # Save socketed run output with as well just in case one fails
    out = "output{0}-warm-socket_{2}-{1}.tar.gz".format(
        runcard, rname, socket_no)
    return out


def output_name(runcard, rname, seed):
    # This function must always be the same as the one in Backend.py
    out = "output{0}-{1}-{2}.tar.gz".format(runcard, rname, seed)
    return out
####### END FILE NAME HELPERS #######


# Override os.system with custom version that auto sets debug level on failure
# Abusive...
syscall = os.system


def do_shell(*args):
    global debug_level
    retval = syscall(*args)
    if retval != 0:
        debug_level = 9999
        print_flush("Error in {0}. Raising debug level to 9999".format(*args))
    # All non zero error codes will be +ve - can add all to determine whether
    # job is ok (but with caution: in bash they're mod 256.)
    return abs(retval)


os.system = do_shell
####### END MISC ABUSIVE SETUP #######


####### SETUP/TEARDOWN FUNCTIONS #######
def setup():
    start_time = datetime.datetime.now()
    print_flush("Start time: {0}".format(
        start_time.strftime("%d-%m-%Y %H:%M:%S")))
    args = parse_arguments()
    debug_level = int(args.debug)
    copy_log = args.copy_log
    setup_environment(args.lhapdf_local, args)
    socket_config = None

    if debug_level > 1:
        # Architecture info
        print_flush("Python version: {0}".format(sys.version))
        print_node_info("node_info.log")
        syscall("lsb_release -a")
        os.system("env")
        os.system("voms-proxy-info --all")

    if copy_log:
        # initialise with node name
        os.system("hostname >> {0}".format(COPY_LOG))

    return args, debug_level, socket_config


def setup_sockets(args, nnlojet_command, bring_status):
    host = args.Host
    port = args.port
    if bring_status != 0:
        print_flush("Not able to bring data from gfal, "
                    "removing myself from the pool")
        socket_sync_str(host, port, handshake="oupsities")
        sys.exit(-95)
    print_flush(
        "Sockets are active, trying to connect to {0}:{1}".format(host, port))
    socket_config = socket_sync_str(host, port)
    if "die" in socket_config:
        print_flush("Timeout'd by socket server")
        sys.exit(0)
    print_flush("Connected to socket server")
    nnlojet_command += " -port {0} -host {1} {2}".format(
        port, host, socket_config)
    return nnlojet_command, socket_config


def setup_environment(lhapdf_dir, options):
    # GCC
    cvmfs_gcc_dir = '/cvmfs/pheno.egi.eu/compilers/GCC/5.2.0/'
    gcc_libpath = os.path.join(cvmfs_gcc_dir, "lib")
    gcc_lib64path = os.path.join(cvmfs_gcc_dir, "lib64")
    gcc_PATH = os.path.join(cvmfs_gcc_dir, "bin")
    # GLIBC
    cvmfs_glibc = ""
    # /cvmfs/dirac.egi.eu/dirac/v6r21p4/Linux_x86_64_glibc-2.17/lib"
    cvmfs_glibc64 = ""
    # /cvmfs/dirac.egi.eu/dirac/v6r21p4/Linux_x86_64_glibc-2.17/lib64"
    # LHAPDF
    lha_PATH = lhapdf_dir + "/bin"
    lhapdf_lib = lhapdf_dir + "/lib"
    lhapdf_share = lhapdf_dir + "/share/LHAPDF"

    old_PATH = os.environ["PATH"]
    os.environ["PATH"] = "%s:%s:%s" % (gcc_PATH, lha_PATH, old_PATH)
    old_ldpath = os.environ["LD_LIBRARY_PATH"]
    os.environ["LD_LIBRARY_PATH"] = "%s:%s:%s:%s:%s:%s" % (
        gcc_libpath, gcc_lib64path, lhapdf_lib, cvmfs_glibc, cvmfs_glibc64,
        old_ldpath)
    os.environ["LFC_HOST"] = "lfc01.dur.scotgrid.ac.uk"
    os.environ["LCG_CATALOG_TYPE"] = "lfc"
    os.environ["LCG_GFAL_INFOSYS"] = "lcgbdii.gridpp.rl.ac.uk:2170"
    os.environ['OMP_STACKSIZE'] = "999999"
    os.environ['LHAPATH'] = lhapdf_share
    os.environ['LHA_DATA_PATH'] = lhapdf_share
    try:
        import gfal2_util.shell
    except KeyError as e:
        pass
    except ImportError as e:
        # If gfal can't be imported then the site packages need to be added to
        # the python path because ? :(
        try:
            os.environ["PYTHONPATH"] = os.environ["PYTHONPATH"] + ":" + \
                options.gfal_location.replace(
                    "/bin/", "/lib/python2.6/site-packages/")
        except KeyError:
            os.environ["PYTHONPATH"] = options.gfal_location.replace(
                "/bin/", "/lib/python2.6/site-packages/")
        os.environ["LD_LIBRARY_PATH"] = os.environ["LD_LIBRARY_PATH"] + \
            ":"+options.gfal_location.replace("/bin/", "/lib/")
    return 0


def teardown(*statuses):
    end_time = datetime.datetime.now()
    print_flush("End time: {0}".format(end_time.strftime("%d-%m-%Y %H:%M:%S")))

    ec = int(any(status != 0 for status in statuses))
    print_flush("Final Error Code: {0}".format(ec))
    # fail if any status is non zero
    sys.exit(ec)
####### END SETUP/TEARDOWN FUNCTIONS #######


####### ARGUMENT PARSING #######
def parse_arguments():
    default_user_gfal = "xroot://se01.dur.scotgrid.ac.uk/dpm/dur.scotgrid.ac.uk/home/pheno/{0}".format(
        getuser())
    parser = OptionParser(usage="usage: %prog [options]")

    parser.add_option("-r", "--runcard", help="Runcard to be run")
    parser.add_option("-j", "--runname", help="Runname")

    # Run options
    parser.add_option("-t", "--threads",
                      help="Number of thread for OMP", default="1")
    parser.add_option("-e", "--executable",
                      help="Executable to be run", default="NNLOJET")
    parser.add_option("-d", "--debug", help="Debug level", default="0")
    parser.add_option("--copy_log", help="Write copy log file.",
                      action="store_true", default=False)
    parser.add_option("-s", "--seed", help="Run seed for NNLOJET", default="1")

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
    parser.add_option("--use_cvmfs_lhapdf", action="store_true", default=True)
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

    # Rivet options (not used)
    parser.add_option("--use_custom_rivet", action="store_true", default=False)
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

    parser.add_option("--pedantic",
                      help="Enable various checks", action="store_true",
                      default=False)
    parser.add_option("--events", default="0")

    (options, positional) = parser.parse_args()

    # Post-parsing setup/checks

    print_flush("Using GFAL for storage")
    if os.path.exists(options.gfal_location) and options.gfal_location != "":
        print_flush("GFAL location found: {0}".format(options.gfal_location))
    elif options.gfal_location == "":
        print_flush("Using environment gfal. Good luck!")
    else:
        print_flush("GFAL location not found!")
        print_flush("Reverting to environment gfal commands")
        options.gfal_location = ""

    if options.use_cvmfs_lhapdf:
        print_flush("Using cvmfs LHAPDF at {0}".format(
            options.cvmfs_lhapdf_location))
        options.lhapdf_local = options.cvmfs_lhapdf_location

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
    return options
####### END ARGUMENT PARSING #######


####### TAR UTILITIES #######
def untar_file(local_file, debug_level):
    if debug_level > 2:
        cmd = "tar zxfv {0}".format(local_file)
    else:
        cmd = "tar zxf {0}".format(local_file)
    return os.system(cmd)


def tar_this(tarfile, sourcefiles):
    cmd = "tar -czf {0} {1}".format(tarfile, sourcefiles)
    stat = os.system(cmd)
    return stat
####### END TAR UTILITIES #######


####### SOCKET HELPERS #######
def socket_sync_str(host, port, handshake="greetings"):
    # Blocking call, it will receive a str of the form
    # -sockets {0} -ns {1}
    sid = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sid.connect((host, int(port)))
    sid.send(handshake)
    return sid.recv(32)
####### END SOCKET HELPERS #######


####### COPY WRAPPER FUNCTIONS #######
def bring_files(args):
    bring_status = 0
    if not args.use_cvmfs_lhapdf:
        print_flush("Using own version of LHAPDF")
        bring_status += bring_lhapdf(args.lhapdf_grid, debug_level)
    bring_status += bring_nnlojet(args.input_folder, args.runcard,
                                  args.runname, debug_level)
    os.system("chmod +x {0}".format(args.executable))
    if bring_status != 0:
        print_flush("Not able to bring data from storage. Exiting now.")
        sys.exit(-95)
    return bring_status


def bring_lhapdf(lhapdf_grid, debug):
    tmp_tar = "lhapdf.tar.gz"
    stat = copy_from_grid(lhapdf_grid, tmp_tar, args)
    print_flush("LHAPDF copy from GRID status: {0}".format(stat))
    stat += untar_file(tmp_tar, debug)
    return os.system("rm {0}".format(tmp_tar))+stat


def bring_nnlojet(input_grid, runcard, runname, debug):
    # Todo: this is not very general, is it?
    tmp_tar = "nnlojet.tar.gz"
    input_name = "{0}/{1}{2}.tar.gz".format(input_grid, runcard, runname)
    stat = copy_from_grid(input_name, tmp_tar, args)
    stat += untar_file(tmp_tar, debug)
    stat += os.system("rm {0}".format(tmp_tar))
    stat += os.system("ls")
    return stat


def store_output(args, socketed=False, socket_config=""):
    # Copy stuff to grid storage, remove executable and lhapdf folder
    # Core files can be upwards of 6G - make sure they're deleted!
    syscall("rm core*")

    if not args.use_cvmfs_lhapdf:
        os.system("rm -rf {0} {1}".format(args.executable, args.lhapdf_local))
    if args.Production:
        local_out = output_name(args.runcard, args.runname, args.seed)
        output_file = os.path.join(args.output_folder, args.runname, local_out)
    elif args.Warmup:
        local_out = warmup_name(args.runcard, args.runname)
        output_file = os.path.join(args.warmup_folder, local_out)

    tar_status = tar_this(local_out, "*")
    if debug_level > 1:
        os.system("ls")

    if socketed:
        # Avoid conflicting simultaneous copies and resulting error messages:
        # - first node attempts copy to warmup_name(...);
        # - other nodes attempt copy to warmup_name_ns(...) as backup.
        # Still results in large redundancy.

        socket_no = int(socket_config.split()[-1].strip())
        if socket_no == 1:
            status_copy = copy_to_grid(local_out, output_file, args)
        else:
            subfolder = os.path.splitext(os.path.basename(output_file))[
                0].replace(".tar", "")
            backup_name = warmup_name_ns(args.runcard, args.runname, socket_no)
            backup_fullpath = os.path.join(
                args.warmup_folder, subfolder, backup_name)
            status_copy = copy_to_grid(local_out, backup_fullpath, args)
    else:
        status_copy = copy_to_grid(local_out, output_file, args)
    return status_copy, tar_status
####### END COPY WRAPPER FUNCTIONS #######


####### COPY UTILITIES #######
def copy_from_grid(grid_file, local_file, args, maxrange=MAX_COPY_TRIES):
    filein = os.path.join(args.gfaldir, grid_file)
    fileout = "file://$PWD/{0}".format(local_file)
    return grid_copy(filein, fileout, args, maxrange=maxrange)


def copy_to_grid(local_file, grid_file, args, maxrange=MAX_COPY_TRIES):
    filein = "file://$PWD/{0}".format(local_file)
    fileout = os.path.join(args.gfaldir, grid_file)
    return grid_copy(filein, fileout, args, maxrange=maxrange)


def remove_file(filepath, args, tries=5, protocol=None):
    if protocol:
        prot = args.gfaldir.split(":")[0]
        filepath = filepath.replace(prot, protocol, 1)
    rmcmd = "{gfal_loc}gfal-rm {f}".format(f=filepath,
                                           gfal_loc=args.gfal_location)

    file_present = test_file_presence(filepath, args)
    tried = 0
    try:
        while file_present and tried < tries:
            print_flush("Removing file {of}.".format(of=filepath))
            retval = syscall(rmcmd)
            if debug_level > 1:
                print_flush(rmcmd)
                print_flush("returned error code: {ec}".format(ec=retval))
            file_present = test_file_presence(filepath, args)
            if debug_level > 1:
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
        if debug_level > 1:
            if hasattr(e, 'message'):
                print_flush(e.message)
            else:
                print_flush(e)
        return 1

    return 0


def test_file_presence(filepath_in, args, protocol=None):
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
        if debug_level > 1:
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
            if debug_level > 1:
                if hasattr(e, 'message'):
                    print_flush(e.message)
                else:
                    print_flush(e)

    if debug_level > 1:
        print_file("Gfal-ls failed for all protocols.")
    return False


def get_hash(filepath, args, algo="MD5", protocol=None):
    if protocol:
        prot = args.gfaldir.split(":")[0]
        filepath = filepath.replace(prot, protocol, 1)
    hashcmd = "{gfal_loc}gfal-sum -t {timeout} {file} {checksum}".format(
        gfal_loc=args.gfal_location, file=filepath, checksum=algo,
        timeout=GFAL_TIMEOUT)
    if debug_level > 1:
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
        if debug_level > 1:
            if hasattr(e, 'message'):
                print_flush(e.message)
            else:
                print_flush(e)
        if protocol == "gsiftp":  # try again when gsiftp is down
            return get_hash(filepath, args, algo=algo, protocol="dav")
        return None
    return hash


def grid_copy(infile, outfile, args, maxrange=MAX_COPY_TRIES):
    protoc = args.gfaldir.split(":")[0]
    # gfal-sum only returns expected hash for subset of protocols (gsiftp, srm,
    # dav, davs). Hardcode gsiftp for now.
    infile_hash = get_hash(infile, args, protocol="gsiftp")

    print_flush("Copying {0} to {1}".format(infile, outfile))
    for i in range(maxrange):
        print_flush("Attempting copy try {0}".format(i+1))

        # cycle through available protocols until one works.
        for j, protocol in enumerate(PROTOCOLS):
            infile_tmp = infile.replace(protoc, protocol, 1)
            outfile_tmp = outfile.replace(protoc, protocol, 1)

            print_flush("Attempting Protocol {0}".format(protocol))
            outfile_dir = os.path.dirname(outfile_tmp)
            outfile_fn = os.path.basename(outfile_tmp)

            cmd = "{2}gfal-copy -f -p {0} {1}".format(
                infile_tmp, outfile_tmp, args.gfal_location)
            if debug_level > 1:
                print_flush(cmd)
            retval = syscall(cmd)
            file_present = test_file_presence(outfile, args, protocol="gsiftp")
            # if compatibiility with python versions < 2.7 is still required,
            # need something like the following instead
            # p = subprocess.Popen(cmd2, stdout=subprocess.PIPE,
            #     stderr=subprocess.PIPE, shell=True)
            # out, err = p.communicate()
            if retval == 0 and file_present:
                return retval
            elif retval == 0 and not file_present:
                print_flush(
                    "Copy command succeeded, but failed to copy file. "
                    "Retrying.")
            elif retval != 0 and file_present:
                if not infile_hash:
                    print_flush("Copy reported error, but file present & can "
                                "not compute original file hash. Proceeding.")
                    return 0

                outfile_hash = get_hash(outfile, args, protocol="gsiftp")
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
        if remove_file(outfile, args, protocol=protocol) == 0:
            break

    return 9999
####### END COPY UTILITIES #######


####### RUN FUNCTIONS #######
def run_executable(nnlojet_command):
    print_flush(" > Executing command: {0}".format(nnlojet_command))
    # Run command
    status_nnlojet = os.system(nnlojet_command)
    if status_nnlojet == 0:
        print_flush("Command successfully executed")
    else:
        print_flush("Something went wrong")
        debug_level = 9999
    return status_nnlojet
####### END RUN FUNCTIONS #######


####### PRINT FUNCTIONS #######
def print_copy_status(args, status_copy):
    if status_copy == 0:
        print_flush("Copied over to grid storage!")
    elif args.Sockets:
        print_flush(
            "This was a socketed run so we are copying the grid to stderr just "
            "in case")
        os.system("cat $(ls *.y* | grep -v .txt) 1>&2")
        status_copy = 0
    elif args.Warmup:
        print_flush("Failure! Outputing vegas warmup to stdout")
        os.system("cat $(ls *.y* | grep -v .txt)")


def print_node_info(outputfile):
    os.system("hostname >> {0}".format(outputfile))
    os.system("gcc --version >> {0}".format(outputfile))
    os.system("python --version >> {0}".format(outputfile))
    os.system(
        "(python3 --version || echo no python3) >> {0}".format(outputfile))
    os.system("gfal-copy --version >> {0}".format(outputfile))
####### END PRINT FUNCTIONS #######


####################
###     MAIN     ###
####################
if __name__ == "__main__":
    args, debug_level, socket_config = setup()
    bring_status = bring_files(args)

    nnlojet_command = RUN_CMD.format(
        args.threads, args.executable, args.runcard)

    if args.Sockets:
        nnlojet_command, socket_config = setup_sockets(
            args, nnlojet_command, bring_status)
    if args.Production:  # Assume sockets does not work with production
        nnlojet_command += " -iseed {0}".format(args.seed)

    if debug_level > 1:
        os.system("ls")
        os.system("ldd -v {0}".format(args.executable))

    # Run executable
    status_nnlojet = run_executable(nnlojet_command)

    # Store output
    status_copy, status_tar = store_output(
        args, socketed=args.Sockets, socket_config=socket_config)
    print_copy_status(args, status_copy)

    if args.Sockets:
        try:  # only the first one arriving will go through!
            print_flush("Close Socket connection")
            _ = socket_sync_str(args.Host, args.port, "bye!")  # Be polite
        except socket.error as e:
            pass

    teardown(status_nnlojet, status_copy, status_tar, bring_status)
