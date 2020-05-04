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


LHE_FILE = "SherpaLHE.lhe"


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


def set_environment(args, lhapdf_dir):
    gf.set_default_environment(args)
    # LHAPDF
    os.environ['LHAPDF_DATA_PATH'] = lhapdf_dir
    # HEJ environment
    os.environ['LD_LIBRARY_PATH'] = "./HEJ/lib" + \
        ":"+os.environ["LD_LIBRARY_PATH"]
    return 0


# ------------------------- Download executable -------------------------
def download_program(source):
    tar_name = os.path.basename(source)
    if not tar_name.endswith("tar.gz"):
        gf.print_flush("{0} is not a valid path to download HEJ".format(source))
        return 1
    stat = gf.copy_from_grid(source, tar_name, args)
    stat += gf.untar_file(tar_name)
    stat += gf.do_shell("rm {0}".format(tar_name))
    if gf.DEBUG_LEVEL > 2:
        gf.do_shell("ls -l HEJ")
    return stat


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
    command = "Sherpa RSEED:={0} ANALYSIS_OUTPUT=Sherpa_{0}".format(args.seed)
    if int(args.events) > 0:
        command += " -e {0} ".format(args.events)
    status = gf.run_command(command)
    # TODO run:
    #   unweighter (maybe)
    return status


def run_HEJFOG(args):
    gf.print_flush("TODO HEJFOG not implemented yet")
    command = "HEJ/bin/HEJFOG"
    gf.do_shell("chmod +x {0}".format(command))
    # TODO:
    #   parse runcard
    #   run HEJ-FOG (with chmod)
    return 1


def run_HEJ(args):
    config = config_name(args.runname)
    seed = args.seed
    status = gf.do_shell(
        'sed -i -e "s/seed:.1/seed: {0}/g" {1}'.format(seed, config))
    status += gf.do_shell(
        'sed -i -e "s/output:.HEJ/output: {0}/g" {1}'.format(
            yoda_name(seed), config))
    status += gf.do_shell("chmod +x {0}".format(args.executable))
    if status == 0:
        status += gf.run_command(
            "{0} {1} {2}".format(args.executable, config, LHE_FILE))
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
    # we are abusing "args.warmup_folder", which is otherwise not needed for HEJ
    status = download_program(args.warmup_folder)

    gf.do_shell("chmod +x {0}".format(args.executable))

    if gf.DEBUG_LEVEL > 8:
        gf.do_shell("ldd {0}".format(args.executable))

    status += download_runcard(args.input_folder, args.runcard, args.runname)

    if args.use_custom_rivet:
        status += download_rivet(args.rivet_folder)

    if status != 0:
        gf.print_flush("download failed")
        gf.end_program(status)

    download_time = datetime.datetime.now()

    if "HEJFOG" in args.runname:
        status += run_HEJFOG(args)
    else:
        status += run_sherpa(args)

    if status != 0:
        gf.print_flush("FOG failed")
        gf.end_program(status)

    fixedorder_time = datetime.datetime.now()

    status += run_HEJ(args)
    if status != 0:
        gf.print_flush("HEJ failed")
        gf.end_program(status)

    HEJ_time = datetime.datetime.now()

    local_out = output_name(args.runcard, args.runname, args.seed)
    output_file = args.output_folder + "/" + local_out

    gf.print_file("setup time:       "+str(setup_time-start_time))
    gf.print_file("download time:    "+str(download_time-setup_time))
    gf.print_file("fixed order time: "+str(fixedorder_time-download_time))
    gf.print_file("HEJ time:         "+str(HEJ_time-fixedorder_time))
    gf.print_file("total runtime:    "+str(HEJ_time-download_time))

    status += gf.tar_this(local_out, "*.yoda *.log *.yml Run.dat")

    status += gf.copy_to_grid(local_out, output_file, args)

    if gf.DEBUG_LEVEL > 1:
        gf.do_shell("ls")

    if status == 0:
        gf.print_flush("Copied over to grid storage!")

    tarcopy_time = datetime.datetime.now()
    gf.print_file("tar&copy time:    "+str(tarcopy_time-HEJ_time))
    gf.print_file("total time:       "+str(tarcopy_time-setup_time))

    gf.end_program(status)
