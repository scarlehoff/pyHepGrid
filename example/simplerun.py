#!/usr/bin/env python
import datetime
import os
import sys

import grid_functions as gf

try:
    dirac = os.environ["DIRAC"]
    sys.path.append(
        "{0}/Linux_x86_64_glibc-2.12/lib/python2.6/site-packages".format(dirac))
except KeyError:
    pass


# This function must always be the same as the one in program.py
def warmup_name(runcard, rname):
    out = runcard + "+" + rname + ".tar.gz"
    return out


# This function must always be the same as the one in program.py
def output_name(runcard, rname, seed):
    out = "output-" + runcard + "-" + rname + "-" + seed + ".tar.gz"
    return out


def parse_arguments():
    from optparse import OptionParser

    parser = OptionParser(usage="usage: %prog [options]")
    # example of a custom simplerun.py variable
    parser.add_option(
        "--executable_location", default="",
        help="GFAL path to executable tarball, relative to gfaldir.")
    return gf.parse_arguments(parser)


def set_environment(lhapdf_dir):
    gf.set_default_environment(args, lhapdf_dir)
    # LHAPDF
    os.environ['LHAPDF_DATA_PATH'] = lhapdf_dir
    return 0


# ------------------------- Download executable -------------------------
def download_program(source):
    tar_name = os.path.basename(source)
    if not tar_name.endswith("tar.gz"):
        gf.print_flush("{0} is not a valid path to download".format(source))
        return 1
    stat = gf.copy_from_grid(source, tar_name, args)
    stat += gf.untar_file(tar_name)
    stat += gf.do_shell("rm {0}".format(tar_name))
    if gf.DEBUG_LEVEL > 2:
        gf.do_shell("ls -l")
    return stat


def download_runcard(input_folder, runcard, runname):
    tar = warmup_name(runcard, runname)
    gf.print_flush("downloading "+input_folder+"/"+tar)
    stat = gf.copy_from_grid(input_folder+"/"+tar, tar, args)
    stat += gf.untar_file(tar)
    return gf.do_shell("rm {0}".format(tar))+stat


def download_rivet(rivet_folder):
    tar = os.path.basename(rivet_folder)
    gf.print_flush("downloading "+rivet_folder)
    stat = gf.copy_from_grid(rivet_folder, "", args)
    stat += gf.untar_file(tar)
    rivet_dir = os.path.basename(os.path.splitext(rivet_folder)[0])
    os.environ['RIVET_ANALYSIS_PATH'] = os.getcwd()+"/"+rivet_dir
    return gf.do_shell("rm {0}".format(tar))+stat


# ------------------------- Actual run commands -------------------------
def run_example(args):
    status = gf.do_shell("chmod +x {0}".format(args.executable))
    if status == 0:
        status += gf.run_command("./{executable} {runcard} {outfile}".format(
            executable=args.executable,
            runcard=args.runname,
            outfile="{0}.out".format(args.seed)))
    return status


# ------------------------- MAIN -------------------------
if __name__ == "__main__":

    # HEJ environment not needed for example;
    # provides template for environment variable sourcing.
    if sys.argv[0] and "ENVSET" not in os.environ:
        gf.print_flush("Setting environment")
        os.environ["ENVSET"] = "ENVSET"
        env = "/cvmfs/pheno.egi.eu/HEJ/HEJ_env.sh"

        # os.execvp *replaces* the current process with that initiated:
        # in this case, a new python instance running simplerun.py, but
        # with the specified BASH environment sourced.
        os.execvp("bash", ["bash", "-c",
                           "source " + env + " && exec python " +
                           sys.argv[0] + ' "${@}"',
                           "--"] + sys.argv[1:])

    # Generic startup:
    start_time = datetime.datetime.now()
    gf.print_flush("Start time: {0}".format(
        start_time.strftime("%d-%m-%Y %H:%M:%S")))

    args = parse_arguments()

    # lhapdf_local = ""
    # if args.use_cvmfs_lhapdf:
    #     lhapdf_local = args.cvmfs_lhapdf_location
    # set_environment(lhapdf_local)

    if gf.DEBUG_LEVEL > -1:
        # Architecture info
        gf.print_flush("Python version: {0}".format(sys.version))
        gf.print_node_info("node_info.log")

    if args.copy_log:
        # initialise with node name
        gf.do_shell("hostname >> {0}".format(gf.COPY_LOG))

    # Debug info
    if gf.DEBUG_LEVEL > 16:
        gf.do_shell("env")
        gf.do_shell("voms-proxy-info --all")

    setup_time = datetime.datetime.now()

    # Download executable:
    if not args.executable_location:
        # if path to executable not provided, exit with error.
        gf.print_flush("Executable location not specified")
        gf.DEBUG_LEVEL = 99999
        gf.end_program(status=1)

    status = download_program(args.executable_location)
    status += download_runcard(args.input_folder, args.runcard, args.runname)

    if status != 0:
        gf.print_flush("download failed")
        gf.end_program(status)

    download_time = datetime.datetime.now()

    status += run_example(args)

    if status != 0:
        gf.print_flush("Executable failed")
        gf.end_program(status)

    run_time = datetime.datetime.now()

    local_out = output_name(args.runcard, args.runname, args.seed)
    output_file = args.output_folder + "/" + local_out

    gf.print_file("setup time:       "+str(setup_time-start_time))
    gf.print_file("download time:    "+str(download_time-setup_time))
    gf.print_file("total runtime:    "+str(run_time-download_time))

    status += gf.tar_this(local_out, "*.log *.out {rc}".format(rc=args.runname))

    status += gf.copy_to_grid(local_out, output_file, args)

    if gf.DEBUG_LEVEL > 1:
        gf.do_shell("ls")

    if status == 0:
        gf.print_flush("Copied over to grid storage!")

    tarcopy_time = datetime.datetime.now()
    gf.print_file("tar&copy time:    "+str(tarcopy_time-run_time))
    gf.print_file("total time:       "+str(tarcopy_time-setup_time))

    gf.end_program(status)
