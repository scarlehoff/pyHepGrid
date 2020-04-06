#!/usr/bin/env python
import datetime
import os
import socket
import sys

import grid_functions as gf

# NOTE: Try to keep this all python2.4 compatible. It may fail at some nodes
# otherwise :( Hopefully after the shutdown we can rely on python 2.6/7 but that
# is TBC

RUN_CMD = "OMP_NUM_THREADS={0} ./{1} -run {2}"


# ------------------------- FILE NAME HELPERS -------------------------
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


# ------------------------- SETUP/TEARDOWN FUNCTIONS -------------------------
def setup():
    start_time = datetime.datetime.now()
    gf.print_flush("Start time: {0}".format(
        start_time.strftime("%d-%m-%Y %H:%M:%S")))
    args = gf.parse_arguments()
    setup_environment(args, args.lhapdf_local)
    socket_config = None

    if debug_level > 1:
        # Architecture info
        gf.print_flush("Python version: {0}".format(sys.version))
        gf.print_node_info("node_info.log")
        os.system("lsb_release -a")
        gf.do_shell("env")
        gf.do_shell("voms-proxy-info --all")

    if args.copy_log:
        # initialise with node name
        gf.do_shell("hostname >> {0}".format(gf.COPY_LOG))

    return args, debug_level, socket_config


def setup_sockets(args, nnlojet_command, bring_status):
    host = args.Host
    port = args.port
    if bring_status != 0:
        gf.print_flush("Not able to bring data from gfal, "
                       "removing myself from the pool")
        socket_sync_str(host, port, handshake="oupsities")
        sys.exit(-95)
    gf.print_flush(
        "Sockets are active, trying to connect to {0}:{1}".format(host, port))
    socket_config = socket_sync_str(host, port)
    if "die" in socket_config:
        gf.print_flush("Timeout'd by socket server")
        sys.exit(0)
    gf.print_flush("Connected to socket server")
    nnlojet_command += " -port {0} -host {1} {2}".format(
        port, host, socket_config)
    return nnlojet_command, socket_config


def setup_environment(args, lhapdf_dir):
    gf.set_default_environment(args, lhapdf_dir)
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
    os.environ['LHA_DATA_PATH'] = lhapdf_share

    old_PATH = os.environ["PATH"]
    os.environ["PATH"] = "%s:%s:%s" % (gcc_PATH, lha_PATH, old_PATH)
    old_ldpath = os.environ["LD_LIBRARY_PATH"]
    os.environ["LD_LIBRARY_PATH"] = "%s:%s:%s:%s:%s:%s" % (
        gcc_libpath, gcc_lib64path, lhapdf_lib, cvmfs_glibc, cvmfs_glibc64,
        old_ldpath)
    return 0


# ------------------------- SOCKET HELPERS -------------------------
def socket_sync_str(host, port, handshake="greetings"):
    # Blocking call, it will receive a str of the form
    # -sockets {0} -ns {1}
    sid = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sid.connect((host, int(port)))
    sid.send(handshake)
    return sid.recv(32)


# ------------------------- COPY WRAPPER FUNCTIONS -------------------------
def bring_files(args):
    bring_status = 0
    if not args.use_cvmfs_lhapdf:
        gf.print_flush("Using own version of LHAPDF")
        bring_status += bring_lhapdf(args.lhapdf_grid, debug_level)
    bring_status += bring_nnlojet(args.input_folder, args.runcard,
                                  args.runname, debug_level)
    gf.do_shell("chmod +x {0}".format(args.executable))
    if bring_status != 0:
        gf.print_flush("Not able to bring data from storage. Exiting now.")
        sys.exit(-95)
    return bring_status


def bring_lhapdf(lhapdf_grid, debug):
    tmp_tar = "lhapdf.tar.gz"
    stat = gf.copy_from_grid(lhapdf_grid, tmp_tar, args)
    gf.print_flush("LHAPDF copy from GRID status: {0}".format(stat))
    stat += gf.untar_file(tmp_tar, debug)
    return gf.do_shell("rm {0}".format(tmp_tar))+stat


def bring_nnlojet(input_grid, runcard, runname, debug):
    # Todo: this is not very general, is it?
    tmp_tar = "nnlojet.tar.gz"
    input_name = "{0}/{1}{2}.tar.gz".format(input_grid, runcard, runname)
    stat = gf.copy_from_grid(input_name, tmp_tar, args)
    stat += gf.untar_file(tmp_tar, debug)
    stat += gf.do_shell("rm {0}".format(tmp_tar))
    stat += gf.do_shell("ls")
    return stat


def store_output(args, socketed=False, socket_config=""):
    # Copy stuff to grid storage, remove executable and lhapdf folder
    # Core files can be upwards of 6G - make sure they're deleted!
    os.system("rm core*")

    if not args.use_cvmfs_lhapdf:
        gf.do_shell("rm -rf {0} {1}".format(args.executable, args.lhapdf_local))
    if args.Production:
        local_out = output_name(args.runcard, args.runname, args.seed)
        output_file = os.path.join(args.output_folder, args.runname, local_out)
    elif args.Warmup:
        local_out = warmup_name(args.runcard, args.runname)
        output_file = os.path.join(args.warmup_folder, local_out)

    tar_status = gf.tar_this(local_out, "*")
    if debug_level > 1:
        gf.do_shell("ls")

    if socketed:
        # Avoid conflicting simultaneous copies and resulting error messages:
        # - first node attempts copy to warmup_name(...);
        # - other nodes attempt copy to warmup_name_ns(...) as backup.
        # Still results in large redundancy.

        socket_no = int(socket_config.split()[-1].strip())
        if socket_no == 1:
            status_copy = gf.copy_to_grid(local_out, output_file, args)
        else:
            subfolder = os.path.splitext(os.path.basename(output_file))[
                0].replace(".tar", "")
            backup_name = warmup_name_ns(args.runcard, args.runname, socket_no)
            backup_fullpath = os.path.join(
                args.warmup_folder, subfolder, backup_name)
            status_copy = gf.copy_to_grid(local_out, backup_fullpath, args)
    else:
        status_copy = gf.copy_to_grid(local_out, output_file, args)
    return status_copy, tar_status


# ------------------------- RUN FUNCTIONS-------------------------
def run_executable(nnlojet_command):
    # TODO replace by gf.run_command
    gf.print_flush(" > Executing command: {0}".format(nnlojet_command))
    # Run command
    status_nnlojet = gf.do_shell(nnlojet_command)
    if status_nnlojet == 0:
        gf.print_flush("Command successfully executed")
    else:
        gf.print_flush("Something went wrong")
        gf.DEBUG_LEVEL = 9999
    return status_nnlojet


# ------------------------- PRINT FUNCTIONS -------------------------
def print_copy_status(args, status_copy):
    if status_copy == 0:
        gf.print_flush("Copied over to grid storage!")
    elif args.Sockets:
        gf.print_flush(
            "This was a socketed run so we are copying the grid to stderr just "
            "in case")
        gf.do_shell("cat $(ls *.y* | grep -v .txt) 1>&2")
        status_copy = 0
    elif args.Warmup:
        gf.print_flush("Failure! Outputing vegas warmup to stdout")
        gf.do_shell("cat $(ls *.y* | grep -v .txt)")


# ------------------------- MAIN -------------------------
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
        gf.do_shell("ls")
        gf.do_shell("ldd -v {0}".format(args.executable))

    # Run executable
    status_nnlojet = run_executable(nnlojet_command)

    # Store output
    status_copy, status_tar = store_output(
        args, socketed=args.Sockets, socket_config=socket_config)
    print_copy_status(args, status_copy)

    if args.Sockets:
        try:  # only the first one arriving will go through!
            gf.print_flush("Close Socket connection")
            _ = socket_sync_str(args.Host, args.port, "bye!")  # Be polite
        except socket.error:
            pass

    gf.end_program(status_nnlojet, status_copy, status_tar, bring_status)
