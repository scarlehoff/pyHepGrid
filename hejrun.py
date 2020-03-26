#!/usr/bin/env python
from __future__ import print_function
import os
import sys
import subprocess
import datetime
from time import sleep

try:
    dirac = os.environ["DIRAC"]
    sys.path.append(
        "{0}/Linux_x86_64_glibc-2.12/lib/python2.6/site-packages".format(dirac)
    )
except KeyError:
    pass

MAX_COPY_TRIES = 15
GFAL_TIMEOUT = 300
PROTOCOLS = ["xroot", "gsiftp", "dav"]
LHE_FILE = "SherpaLHE.lhe"
LOG_FILE = "output.log"
COPY_LOG = "copies.log"


def print_flush(string):
    """
    Override print with custom version that always flushes to stdout so we have
    up-to-date logs
    """
    print(string)
    sys.stdout.flush()


def print_file(string, logfile=LOG_FILE):
    with open(logfile, "a") as f:
        f.write(string + "\n")


# This function must always be the same as the one in program.py
def warmup_name(runcard, rname):
    out = runcard + "+" + rname + ".tar.gz"
    return out


# This function must always be the same as the one in program.py
def output_name(runcard, rname, seed):
    out = "output-" + runcard + "-" + rname + "-" + seed + ".tar.gz"
    return out


def yoda_name(seed):
    return "HEJ_{0}".format(seed)


def config_name(rname):
    return "{0}.yml".format(rname)


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
    # job is ok
    return abs(retval)


os.system = do_shell


def parse_arguments():
    from optparse import OptionParser
    from getpass import getuser

    default_user_gfal = (
        "gsiftp://se01.dur.scotgrid.ac.uk/dpm/" + "dur.scotgrid.ac.uk/home/pheno/{0}"
    ).format(getuser())
    parser = OptionParser(usage="usage: %prog [options]")

    parser.add_option("-r", "--runcard", help="Runcard to be run")
    parser.add_option("-j", "--runname", help="Runname")

    # Run options
    parser.add_option("-t", "--threads", help="Number of thread for OMP", default="1")
    parser.add_option("-e", "--executable", help="Executable to be run", default="HEJ")
    parser.add_option("-d", "--debug", help="Debug level", default="0")
    parser.add_option(
        "--copy_log", help="Write copy log file.", action="store_true", default=False
    )
    parser.add_option("-s", "--seed", help="Run seed", default="1")
    parser.add_option("-E", "--events", help="Number of events", default="-1")

    # Grid configuration options
    parser.add_option(
        "-i",
        "--input_folder",
        help="gfal input folder, relative to gfaldir",
        default="input",
    )
    parser.add_option(
        "-w",
        "--warmup_folder",
        help="gfal file (not just the folder!) where HEJ is stored, relative "
        "to gfaldir",
        default="warmup",
    )
    parser.add_option(
        "-o",
        "--output_folder",
        help="gfal output folder, relative to gfaldir",
        default="output",
    )
    parser.add_option("-g", "--gfaldir", help="gfaldir", default=default_user_gfal)
    parser.add_option(
        "--gfal_location",
        default="",
        help="Provide a specific location for gfal executables [intended for "
        "cvmfs locations]. Default is the environment gfal.",
    )

    # LHAPDF options
    parser.add_option("--use_cvmfs_lhapdf", action="store_true", default=True)
    parser.add_option(
        "--cvmfs_lhapdf_location",
        default="",
        help="Provide a cvmfs location for LHAPDF.",
    )
    parser.add_option(
        "--lhapdf_grid",
        help="absolute value of lhapdf location or relative to" " gfaldir",
        default="util/lhapdf.tar.gz",
    )
    parser.add_option(
        "--lhapdf_local",
        help="name of LHAPDF folder local to the sandbox",
        default="lhapdf",
    )

    # Rivet options
    parser.add_option("--use_custom_rivet", action="store_true", default=False)
    parser.add_option(
        "--rivet_folder",
        default="Wjets/Rivet/Rivet.tgz",
        help="Provide the location of RivetAnalyses tarball.",
    )

    # Socket options
    parser.add_option(
        "-S",
        "--Sockets",
        help="Activate socketed run",
        action="store_true",
        default=False,
    )
    parser.add_option(
        "-p", "--port", help="Port to connect the sockets to", default="8888"
    )
    parser.add_option(
        "-H",
        "--Host",
        help="Host to connect the sockets to",
        default="gridui1.dur.scotgrid.ac.uk",
    )

    # Mark the run as production or warmup
    parser.add_option(
        "-P", "--Production", help="Production run", action="store_true", default=False
    )
    parser.add_option(
        "-W", "--Warmup", help="Warmup run", action="store_true", default=False
    )

    parser.add_option(
        "--pedantic", help="Enable various checks", action="store_true", default=False
    )

    (options, positional) = parser.parse_args()

    if not options.runcard or not options.runname:
        parser.error("Runcard and runname must be provided")

    if options.Production == options.Warmup:
        parser.error("You need to enable one and only one of production and warmup")

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


def set_environment(lhapdf_dir):
    os.system(
        "export PYTHONPATH=${PYTHONPATH}:${DIRAC}/Linux_x86_64_glibc-2.12/lib"
        "/python2.6/site-packages"
    )
    os.environ["LFC_HOST"] = "lfc01.dur.scotgrid.ac.uk"
    os.environ["LCG_CATALOG_TYPE"] = "lfc"
    os.environ["LCG_GFAL_INFOSYS"] = "lcgbdii.gridpp.rl.ac.uk:2170"
    os.environ["OMP_STACKSIZE"] = "999999"
    try:
        import gfal2_util.shell

        print_flush("Using default gfal at {0}".format(gfal2_util.shell.__file__))
    except KeyError:
        pass
    except ImportError:
        # If gfal can't be imported then the site packages need to be added to
        # the python path because ? :(
        os.environ["PYTHONPATH"] = (
            os.environ["PYTHONPATH"]
            + ":"
            + args.gfal_location.replace("/bin/", "/lib/python2.7/site-packages/")
        )
        os.environ["LD_LIBRARY_PATH"] = (
            os.environ["LD_LIBRARY_PATH"]
            + ":"
            + args.gfal_location.replace("/bin/", "/lib/")
        )
    # HEJ environment
    os.environ["LD_LIBRARY_PATH"] = "./HEJ/lib" + ":" + os.environ["LD_LIBRARY_PATH"]
    # LHAPDF
    os.environ["LHAPDF_DATA_PATH"] = lhapdf_dir
    return 0


# Define some utilites
def run_command(command):
    """run command & catch output in LOG_FILE"""
    # Avoid overwriting of the status code for piping to tee
    command = 'bash  -o pipefail -c "{0}  2>&1 | tee -a {1}"'.format(command, LOG_FILE)
    print_flush(
        " > Executed command: {0} ({1})".format(command, datetime.datetime.now())
    )
    return os.system(command)


# ------------------------- TAR UTILITIES -------------------------
def untar_file(local_file, debug_level):
    if debug_level > 16:
        cmd = "tar zxfv {0}".format(local_file)
    else:
        cmd = "tar zxf " + local_file
    return os.system(cmd)


def tar_this(tarfile, sourcefiles):
    cmd = "tar -cvzf " + tarfile + " " + sourcefiles
    stat = os.system(cmd)
    os.system("ls")
    return stat


# ------------------------- COPY UTILITIES -------------------------
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
    rmcmd = "{gfal_loc}gfal-rm {f}".format(f=filepath, gfal_loc=args.gfal_location)

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
                logfile=COPY_LOG,
            )
            print_file("   > Command issued: {cmd}".format(cmd=rmcmd), logfile=COPY_LOG)
        if debug_level > 1:
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
            gfal_loc=args.gfal_location, file=filepath, timeout=GFAL_TIMEOUT
        )
        if debug_level > 1:
            print_flush(lscmd)
        try:
            # In principle, empty if file doesn't exist, so unnecessary to check
            # contents. Test to be robust against unexpected output.
            filelist = subprocess.check_output(
                lscmd, shell=True, universal_newlines=True
            ).splitlines()[0]
            return filename in filelist
        except subprocess.CalledProcessError as e:
            if args.copy_log:
                print_file(
                    "Gfal-ls failed at {t}.".format(t=datetime.datetime.now()),
                    logfile=COPY_LOG,
                )
                print_file(
                    "   > Command issued: {cmd}".format(cmd=lscmd), logfile=COPY_LOG
                )
            if debug_level > 1:
                print_flush(e)

    if debug_level > 1:
        print_file("Gfal-ls failed for all protocols.")
    return False


def get_hash(filepath, args, algo="MD5", protocol=None):
    if protocol:
        prot = args.gfaldir.split(":")[0]
        filepath = filepath.replace(prot, protocol, 1)
    hashcmd = "{gfal_loc}gfal-sum -t {timeout} {file} {checksum}".format(
        gfal_loc=args.gfal_location, file=filepath, checksum=algo, timeout=GFAL_TIMEOUT
    )
    if debug_level > 1:
        print_flush(hashcmd)
    try:
        hash = subprocess.check_output(
            hashcmd, shell=True, universal_newlines=True
        ).split()[1]
    except subprocess.CalledProcessError as e:
        if args.copy_log:
            print_file(
                "Gfal-sum failed at {t}.".format(t=datetime.datetime.now()),
                logfile=COPY_LOG,
            )
            print_file(
                "   > Command issued: {cmd}".format(cmd=hashcmd), logfile=COPY_LOG
            )
        if debug_level > 1:
            print_flush(e)
        # try again when gsiftp is down (nothing to lose)
        if protocol == "gsiftp":
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
        print_flush("Attempting copy try {0}".format(i + 1))

        # cycle through available protocols until one works.
        for j, protocol in enumerate(PROTOCOLS):
            infile_tmp = infile.replace(protoc, protocol, 1)
            outfile_tmp = outfile.replace(protoc, protocol, 1)

            print_flush("Attempting Protocol {0}".format(protocol))

            cmd = "{2}gfal-copy -f -p {0} {1}".format(
                infile_tmp, outfile_tmp, args.gfal_location
            )
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
                    "Copy command succeeded, but failed to copy file. " "Retrying."
                )
            elif retval != 0 and file_present:
                if not infile_hash:
                    print_flush(
                        "Copy reported error, but file present & can "
                        "not compute original file hash. Proceeding."
                    )
                    return 0

                outfile_hash = get_hash(outfile, args, protocol="gsiftp")
                if not outfile_hash:
                    print_flush(
                        "Copy reported error, but file present & can "
                        "not compute copied file hash. Proceeding."
                    )
                    return 0
                elif infile_hash == outfile_hash:
                    print_flush(
                        "Copy command reported errors, but file was "
                        "copied and checksums match. Proceeding."
                    )
                    return 0
                else:
                    print_flush(
                        "Copy command reported errors and the "
                        "transferred file was corrupted. Retrying."
                    )
            else:
                print_flush("Copy command failed. Retrying.")
            if args.copy_log:
                print_file(
                    "Copy failed at {t}.".format(t=datetime.datetime.now()),
                    logfile=COPY_LOG,
                )
                print_file(
                    "   > Command issued: {cmd}".format(cmd=cmd), logfile=COPY_LOG
                )
                print_file(
                    "   > Returned error code: {ec}".format(ec=retval), logfile=COPY_LOG
                )
                print_file(
                    "   > File now present: {fp}".format(fp=file_present),
                    logfile=COPY_LOG,
                )
            # sleep time scales steeply with failed attempts (min wait 1s, max
            # wait ~2 mins)
            sleep((i + 1) * (j + 1) ** 2)

    # Copy failed to complete successfully; attemt to clean up corrupted files
    # if present. Only make it this far if file absent, or present and
    # corrupted.
    for protocol in PROTOCOLS:
        if remove_file(outfile, args, protocol=protocol) == 0:
            break

    return 9999


# ------------------------- Download executable -------------------------
def download_program(source, debug_level):
    tar_name = os.path.basename(source)
    if not tar_name.endswith("tar.gz"):
        print_flush("{0} is not a valid path to download HEJ".format(source))
        return 1
    stat = copy_from_grid(source, tar_name, args)
    stat += untar_file(tar_name, debug_level)
    stat += os.system("rm {0}".format(tar_name))
    if debug_level > 2:
        os.system("ls -l HEJ")
    return stat


def download_runcard(input_folder, runcard, runname, debug_level):
    tar = warmup_name(runcard, runname)
    print_flush("downloading " + input_folder + "/" + tar)
    stat = copy_from_grid(input_folder + "/" + tar, tar, args)
    stat += untar_file(tar, debug_level)

    # TODO download:
    #   Scale setters
    return os.system("rm {0}".format(tar)) + stat


def download_rivet(rivet_folder, debug_level):
    tar = os.path.basename(rivet_folder)
    print_flush("downloading " + rivet_folder)
    stat = copy_from_grid(rivet_folder, "", args)
    stat += untar_file(tar, debug_level)
    rivet_dir = os.path.basename(os.path.splitext(rivet_folder)[0])
    os.environ["RIVET_ANALYSIS_PATH"] = os.getcwd() + "/" + rivet_dir
    return os.system("rm {0}".format(tar)) + stat


# ------------------------- Misc -------------------------
def print_node_info(outputfile):
    os.system("hostname >> {0}".format(outputfile))
    os.system("gcc --version >> {0}".format(outputfile))
    os.system("python --version >> {0}".format(outputfile))
    os.system("(python3 --version || echo no python3) >> {0}".format(outputfile))
    os.system("gfal-copy --version >> {0}".format(outputfile))
    os.system("cat {0}".format(outputfile))  # print to log


def end_program(status, debug_level):
    # print debug infos here if status!=0
    if status != 0 or debug_level > 8:
        os.system("cat " + LOG_FILE)
        os.system("ls")
    end_time = datetime.datetime.now()
    print_flush("End time: {0}".format(end_time.strftime("%d-%m-%Y %H:%M:%S")))
    print_flush("Final return Code: {0}".format(status))
    # make sure we return a valid return code
    if status != 0:
        sys.exit(1)
    sys.exit(0)


# ------------------------- Actual run commands -------------------------
def run_sherpa(args):
    command = "Sherpa RSEED:={0} ANALYSIS_OUTPUT=Sherpa_{0}".format(args.seed)
    if int(args.events) > 0:
        command += " -e {0} ".format(args.events)
    status = run_command(command)
    # TODO run:
    #   unweighter (maybe)
    return status


def run_HEJFOG(args):
    print_flush("TODO HEJFOG not implemented yet")
    command = "HEJ/bin/HEJFOG"
    os.system("chmod +x {0}".format(command))
    # TODO:
    #   parse runcard
    #   run HEJ-FOG (with chmod)
    return 1


def run_HEJ(args):
    config = config_name(args.runname)
    seed = args.seed
    status = os.system('sed -i -e "s/seed:.1/seed: {0}/g" {1}'.format(seed, config))
    status += os.system(
        'sed -i -e "s/output:.HEJ/output: {0}/g" {1}'.format(yoda_name(seed), config)
    )
    status += os.system("chmod +x {0}".format(args.executable))
    if status == 0:
        status += run_command("{0} {1} {2}".format(args.executable, config, LHE_FILE))
    return status


# ------------------------- MAIN -------------------------
if __name__ == "__main__":

    if sys.argv[0] and "ENVSET" not in os.environ:
        print_flush("Setting environment")
        os.environ["ENVSET"] = "ENVSET"
        env = "/cvmfs/pheno.egi.eu/HEJ/HEJ_env.sh"
        os.execvp(
            "bash",
            [
                "bash",
                "-c",
                "source " + env + " && exec python " + sys.argv[0] + ' "${@}"',
                "--",
            ]
            + sys.argv[1:],
        )

    start_time = datetime.datetime.now()
    print_flush("Start time: {0}".format(start_time.strftime("%d-%m-%Y %H:%M:%S")))

    args = parse_arguments()
    debug_level = int(args.debug)
    copy_log = args.copy_log

    lhapdf_local = ""
    if args.use_cvmfs_lhapdf:
        lhapdf_local = args.cvmfs_lhapdf_location
    set_environment(lhapdf_local)

    if debug_level > -1:
        # Architecture info
        print_flush("Python version: {0}".format(sys.version))
        print_node_info("node_info.log")

    if copy_log:
        # initialise with node name
        os.system("hostname >> {0}".format(COPY_LOG))

    # Debug info
    if debug_level > 16:
        os.system("env")
        os.system("voms-proxy-info --all")

    setup_time = datetime.datetime.now()
    # Download components
    # we are abusing "args.warmup_folder", which is otherwise not needed for HEJ
    status = download_program(args.warmup_folder, debug_level)

    os.system("chmod +x {0}".format(args.executable))

    if debug_level > 8:
        os.system("ldd {0}".format(args.executable))

    status += download_runcard(
        args.input_folder, args.runcard, args.runname, debug_level
    )

    if args.use_custom_rivet:
        status += download_rivet(args.rivet_folder, debug_level)

    if status != 0:
        print_flush("download failed")
        end_program(status, debug_level)

    download_time = datetime.datetime.now()

    if "HEJFOG" in args.runname:
        status += run_HEJFOG(args)
    else:
        status += run_sherpa(args)

    if status != 0:
        print_flush("FOG failed")
        end_program(status, debug_level)

    fixedorder_time = datetime.datetime.now()

    status += run_HEJ(args)
    if status != 0:
        print_flush("HEJ failed")
        end_program(status, debug_level)

    HEJ_time = datetime.datetime.now()

    local_out = output_name(args.runcard, args.runname, args.seed)
    output_file = args.output_folder + "/" + local_out

    print_file("setup time:       " + str(setup_time - start_time))
    print_file("download time:    " + str(download_time - setup_time))
    print_file("fixed order time: " + str(fixedorder_time - download_time))
    print_file("HEJ time:         " + str(HEJ_time - fixedorder_time))
    print_file("total runtime:    " + str(HEJ_time - download_time))

    status += tar_this(local_out, "*.yoda *.log *.yml Run.dat")

    status += copy_to_grid(local_out, output_file, args)

    if debug_level > 1:
        os.system("ls")

    if status == 0:
        print_flush("Copied over to grid storage!")

    tarcopy_time = datetime.datetime.now()
    print_file("tar&copy time:    " + str(tarcopy_time - HEJ_time))
    print_file("total time:       " + str(tarcopy_time - setup_time))

    end_program(status, debug_level)
