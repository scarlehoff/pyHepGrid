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


def config_name(rname):
    return "{0}.dat".format(rname)


def set_environment(args, lhapdf_dir):
    gf.set_default_environment(args, lhapdf_dir)
    # LHAPDF
    os.environ['LHAPDF_DATA_PATH'] = lhapdf_dir
    return 0


# ------------------------- Download executable -------------------------
def download_program():
    # TODO read tar and source name from header
    gf.print_flush("using cvmfs Sherpa")
    return 0
    # tar_name = "Sherpa.tar.gz"
    # source = "Sherpa/{0}".format(tar_name)
    # stat = gf.copy_from_grid(source, tar_name, args)
    # stat += gf.untar_file(tar_name, debug)
    # stat += gf.do_shell("rm {0}".format(tar_name))
    # if gf.DEBUG_LEVEL > 2:
    #     gf.do_shell("ls -l Sherpa")
    # return stat


def download_runcard(input_folder, runcard, runname):
    tar = warmup_name(runcard, runname)
    gf.print_flush("downloading "+input_folder+"/"+tar)
    stat = gf.copy_from_grid(input_folder+"/"+tar, tar, args)
    stat += gf.untar_file(tar)
    # TODO download:
    #   Scale setters
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
def run_sherpa(args):
    command = "Sherpa RSEED:={0} ANALYSIS_OUTPUT=Sherpa_{0} -f {1}".format(
        args.seed, config_name(args.runname))
    if int(args.events) > 0:
        command += " -e {0} ".format(args.events)
    status = gf.run_command(command)
    return status


# ------------------------- MAIN -------------------------
if __name__ == "__main__":

    if sys.argv[0] and "ENVSET" not in os.environ:
        gf.print_flush("Setting environment")
        os.environ["ENVSET"] = "ENVSET"
        env = "/cvmfs/pheno.egi.eu/HEJ/HEJ_env.sh"
        os.execvp("bash", ["bash", "-c",
                           "source " + env + " && exec python " +
                           sys.argv[0] + ' "${@}"',
                           "--"] + sys.argv[1:])

    start_time = datetime.datetime.now()
    gf.print_flush("Start time: {0}".format(
        start_time.strftime("%d-%m-%Y %H:%M:%S")))

    args = gf.parse_arguments()

    lhapdf_local = ""
    if args.use_cvmfs_lhapdf:
        lhapdf_local = args.cvmfs_lhapdf_location
    set_environment(args, lhapdf_local)

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
    # Download components
    status = download_program()

    # uncomment for downloaded exe
    # gf.do_shell("chmod +x {0}".format(args.executable))

    # uncomment for downloaded exe
    # if gf.DEBUG_LEVEL > 8:
    #     gf.do_shell("ldd {0}".format(args.executable))

    status += download_runcard(args.input_folder,
                               args.runcard, args.runname)

    if args.use_custom_rivet:
        status += download_rivet(args.rivet_folder)

    if status != 0:
        gf.print_flush("download failed")
        gf.end_program(status)

    download_time = datetime.datetime.now()

    status += run_sherpa(args)

    if status != 0:
        gf.print_flush("Sherpa failed")
        gf.end_program(status)

    run_time = datetime.datetime.now()

    gf.print_file("setup time:    "+str(setup_time-start_time))
    gf.print_file("download time: "+str(download_time-setup_time))
    gf.print_file("Sherpa time:   "+str(run_time-download_time))

    local_out = output_name(args.runcard, args.runname, args.seed)
    status += gf.tar_this(local_out, "*.yoda *.log *.dat")

    output_file = args.output_folder + "/" + local_out
    status += gf.copy_to_grid(local_out, output_file, args)

    if gf.DEBUG_LEVEL > 1:
        gf.do_shell("ls")

    if status == 0:
        gf.print_flush("Copied over to grid storage!")

    tarcopy_time = datetime.datetime.now()
    gf.print_file("tar&copy time: "+str(tarcopy_time-run_time))
    gf.print_file("total time:    "+str(tarcopy_time-setup_time))

    gf.end_program(status)
