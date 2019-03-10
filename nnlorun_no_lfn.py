#!/usr/bin/env python
import os
import sys
import datetime
import socket
from optparse import OptionParser
from getpass import getuser

RUN_CMD = "OMP_NUM_THREADS={0} ./{1} -run {2}"
protocols = ["xroot", "srm", "gsiftp", "root", "xrootd"]
gsiftp = "{0}://se01.dur.scotgrid.ac.uk/dpm/dur.scotgrid.ac.uk/home/pheno/dwalker/".format(protocols[0])
lcg_cp = "lcg-cp"
lcg_cr = "lcg-cr --vo pheno -l"


#### Override print with custom version that always flushes to stdout so we have up-to-date logs
def print_flush(string):
    print string
    sys.stdout.flush()
    
####

# Try to keep this all python2.4 compatible. It may fail at some nodes otherwise :(
def warmup_name(runcard, rname):
    # This function must always be the same as the one in Backend.py
    out = "output{0}-warm-{1}.tar.gz".format(runcard, rname)
    return out

def warmup_name_ns(runcard, rname, socket_no):
    # Save socketed run output with as well just in case one fails
    out = "output{0}-warm-{1}-socket_{2}.tar.gz".format(runcard, rname, socket_no)
    return out


def output_name(runcard, rname, seed):
    # This function must always be the same as the one in Backend.py
    out = "output{0}-{1}-{2}.tar.gz".format(runcard, rname, seed)
    return out

#### Override os.system with custom version that auto sets debug level on failure
# Abusive...
syscall = os.system
def do_shell(*args):
    global debug_level
    retval = syscall(*args)
    if retval != 0:
        debug_level = 9999
        print_flush("Error in {0}. Raising debug level to 9999".format(*args))
    return abs(retval) # All non zero error codes will be +ve - can add all to determine whether job is ok
os.system = do_shell


def parse_arguments():
    default_user_gfal = "xroot://se01.dur.scotgrid.ac.uk/dpm/dur.scotgrid.ac.uk/home/pheno/{0}".format(getuser())  
    default_user_lfn = ""
    parser = OptionParser(usage = "usage: %prog [options]")

    parser.add_option("-r","--runcard", help = "Runcard to be run")
    parser.add_option("-j", "--runname", help = "Runname")

    # Run options
    parser.add_option("-t", "--threads", help = "Number of thread for OMP", default = "1")
    parser.add_option("-e", "--executable", help = "Executable to be run", default = "NNLOJET")
    parser.add_option("-d", "--debug", help = "Debug level", default="0")
    parser.add_option("-s", "--seed", help = "Run seed for NNLOJET", default="1")

    # Grid configuration options
    parser.add_option("-l", "--lfndir", help = "LFNDIR", default = default_user_lfn)
    parser.add_option("-i", "--input_folder",
                      help = "storage  input folder, relative to --lfndir or gfaldir depending on which mode is used",
                      default = "input")
    parser.add_option("-w", "--warmup_folder",
                      help = "storage  warmup folder, relative to --lfndir or gfaldir depending on which mode is used",
                      default = "warmup")
    parser.add_option("-o", "--output_folder", 
                      help = "storage  output folder, relative to --lfndir or gfaldir depending on which mode is used", 
                      default = "output")
    parser.add_option("-g", "--gfaldir", help = "gfaldir", default = default_user_gfal)
    parser.add_option("--use_gfal", default="False", 
                      help = "Use gfal for file transfer and storage rather than the LFN")
    parser.add_option("--gfal_location", default="", 
                      help = "Provide a specific location for gfal executables [intended for cvmfs locations]. Default is the environment gfal.")

    # LHAPDF options
    parser.add_option("--lhapdf_grid", help = "absolute value of lhapdf location or relative to lfndir/gfaldir", 
                      default = "util/lhapdf.tar.gz")
    parser.add_option("--lhapdf_local", help = "name of LHAPDF folder local to the sandbox", default = "lhapdf")
    parser.add_option("--use_cvmfs_lhapdf", action = "store_true", default = False)
    parser.add_option("--cvmfs_lhapdf_location", default="", 
                      help = "Provide a cvmfs location for LHAPDF.")

    # Socket options
    parser.add_option("-S", "--Sockets", help = "Activate socketed run", action = "store_true", default = False)
    parser.add_option("-p", "--port", help = "Port to connect the sockets to", default = "8888")
    parser.add_option("-H", "--Host", help = "Host to connect the sockets to",
                      default = "gridui1.dur.scotgrid.ac.uk")

    # Mark the run as production or warmup
    parser.add_option("-P", "--Production", help = "Production run", action = "store_true", default = False)
    parser.add_option("-W", "--Warmup", help = "Warmup run", action = "store_true", default = False)

    parser.add_option("--pedantic", help = "Enable various checks", action = "store_true", default = False)

    (options, positional) = parser.parse_args()

    # Post-parsing setup/checks
    if options.use_gfal.lower() == "true":
        options.use_gfal = True
        print("Using GFAL for storage")
        if os.path.exists(options.gfal_location) and options.gfal_location != "":
            print("GFAL location found: {0}".format(options.gfal_location))
        elif options.gfal_location == "":
            print("Using environment gfal. Good luck!")
        else:
            print("GFAL location not found!")
            print("Reverting to environment gfal commands")
            options.gfal_location = ""
    else:
        options.use_gfal = False

    if options.use_cvmfs_lhapdf:
        print("Using cvmfs LHAPDF at {0}".format(options.cvmfs_lhapdf_location))
        options.lhapdf_local = options.cvmfs_lhapdf_location

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

def set_environment(lfndir, lhapdf_dir, options):
    # GCC 
    cvmfs_gcc_dir = '/cvmfs/pheno.egi.eu/compilers/GCC/5.2.0/'
    gcc_libpath = os.path.join(cvmfs_gcc_dir, "lib")
    gcc_lib64path = os.path.join(cvmfs_gcc_dir, "lib64")
    gcc_PATH = os.path.join(cvmfs_gcc_dir, "bin")
    # LHAPDF
    lha_PATH = lhapdf_dir + "/bin"
    lhapdf_lib = lhapdf_dir + "/lib"
    lhapdf_share = lhapdf_dir + "/share/LHAPDF"
    
    old_PATH = os.environ["PATH"]
    os.environ["PATH"] = "%s:%s:%s" % (gcc_PATH,lha_PATH,old_PATH)
    old_ldpath                    = os.environ["LD_LIBRARY_PATH"]
    os.environ["LD_LIBRARY_PATH"] = "%s:%s:%s:%s" % (gcc_libpath, gcc_lib64path, lhapdf_lib, old_ldpath)
    os.environ["LFC_HOST"]         = "lfc01.dur.scotgrid.ac.uk"
    os.environ["LCG_CATALOG_TYPE"] = "lfc"
    os.environ["LFC_HOME"]         = lfndir
    os.environ["LCG_GFAL_INFOSYS"] = "lcgbdii.gridpp.rl.ac.uk:2170"
    os.environ['OMP_STACKSIZE']    = "999999"
    os.environ['LHAPATH']          = lhapdf_share
    os.environ['LHA_DATA_PATH']    = lhapdf_share
    try:
        import gfal2_util.shell
    except KeyError as e:
        pass
    except ImportError as e:
        # If gfal can't be imported then the site packages need to be added to the python path because ? :(
        os.environ["PYTHONPATH"] = os.environ["PYTHONPATH"] +":"+options.gfal_location.replace("/bin/","/lib/python2.6/site-packages/")
        os.environ["LD_LIBRARY_PATH"] = os.environ["LD_LIBRARY_PATH"] +":"+options.gfal_location.replace("/bin/","/lib/")
    return 0

# Define some utilites
def copy_from_grid(grid_file, local_file, args, maxrange=10):
    filein = os.path.join(args.gfaldir, grid_file)
    fileout = "file://$PWD/{0}".format(local_file)
    return grid_copy(filein, fileout, args, maxrange=maxrange)

def copy_to_grid(local_file, grid_file, args, maxrange = 10):
    filein = "file://$PWD/{0}".format(local_file)
    fileout = os.path.join(args.gfaldir, grid_file)
    return grid_copy(filein, fileout, args, maxrange=maxrange)

def grid_copy(infile, outfile, args, maxrange=10):
    print_flush("Copying {0} to {1}".format(infile, outfile))
    protoc = args.gfaldir.split(":")[0]
    for protocol in protocols: # cycle through available protocols until one works.
        infile_tmp = infile.replace(protoc, protocol)
        outfile_tmp = outfile.replace(protoc, protocol)
        print("Attempting Protocol {0}".format(protocol))
        for i in range(maxrange): # try max 10 times for now ;)
            cmd = "{2}gfal-copy {0} {1}".format(infile_tmp, outfile_tmp, args.gfal_location)
            if args.debug > 1:
                print(cmd)
                os.system("ls")
            retval = syscall(cmd)
            if retval == 0:
                return retval
    return 9999999

def untar_file(local_file, debug_level):
    if debug_level > 2:
        cmd = "tar zxfv {0}".format(local_file)
    else:
        cmd = "tar zxf {0}".format(local_file)
    return os.system(cmd)

def tar_this(tarfile, sourcefiles):
    cmd = "tar -czf {0} {1}".format(tarfile, sourcefiles)
    stat = os.system(cmd)
    os.system("ls")
    return stat

def socket_sync_str(host, port, handshake = "greetings"):
    # Blocking call, it will receive a str of the form
    # -sockets {0} -ns {1}
    sid = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sid.connect((host, int(port)))
    sid.send(handshake)
    return sid.recv(32)

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

def print_node_info(outputfile):
    os.system("hostname >> {0}".format(outputfile))
    os.system("gcc --version >> {0}".format(outputfile))
    os.system("python --version >> {0}".format(outputfile))    


def setup_sockets(args, nnlojet_command, bring_status):
    host = args.Host
    port = args.port
    if bring_status != 0:
        print_flush("Not able to bring data from LFN, removing myself from the pool")
        socket_sync_str(host, port, handshake = "oupsities")
        sys.exit(-95)
    print_flush("Sockets are active, trying to connect to {0}:{1}".format(host,port))
    socket_config = socket_sync_str(host, port)
    if "die" in socket_config:
        print_flush("Timeout'd by socket server")
        sys.exit(0)
    print_flush("Connected to socket server")
    nnlojet_command += " -port {0} -host {1} {2}".format(port, host,  socket_config)
    return nnlojet_command, socket_config

def setup():
    start_time = datetime.datetime.now()
    print_flush("Start time: {0}".format(start_time.strftime("%d-%m-%Y %H:%M:%S")))
    args = parse_arguments()
    debug_level = int(args.debug)
    set_environment(args.lfndir, args.lhapdf_local, args)
    return args, debug_level

def teardown(*statuses):
    end_time = datetime.datetime.now()
    print_flush("End time: {0}".format(end_time.strftime("%d-%m-%Y %H:%M:%S")))

    final_state = sum(abs(i) for i in statuses)
    print_flush("Final Error Code: {0}".format(final_state))
    sys.exit(final_state)

def store_output(args, socketed=False):
    # Copy stuff to grid storage, remove executable and lhapdf folder
    if not args.use_cvmfs_lhapdf:
        os.system("rm -rf {0} {1}".format(args.executable, args.lhapdf_local))
    if args.Production:
        local_out = output_name(args.runcard, args.runname, args.seed)
        output_file = os.path.join(args.output_folder, local_out)
    elif args.Warmup:
        local_out = warmup_name(args.runcard, args.runname)
        output_file = os.path.join(args.warmup_folder, local_out)

    tar_status = tar_this(local_out, "*")
    if debug_level > 1:
        os.system("ls")

    if socketed:
        status_copy = copy_to_grid(local_out, output_file, args, maxrange = 1)
        # copy a second time with socket # as a backup in case of main failure
        backup_name = warmup_name_ns(runcard, rname, socket_config.split()[-1].strip())
        status_copy += copy_to_grid(local_out, backup_name, args)
    else:
        status_copy = copy_to_grid(local_out, output_file, args)
    return status_copy, tar_status

def bring_files(args):
    bring_status = 0
    if not args.use_cvmfs_lhapdf:
        print_flush("Using own version of LHAPDF")
        bring_status += bring_lhapdf(args.lhapdf_grid, debug_level)
    bring_status += bring_nnlojet(args.input_folder, args.runcard, args.runname, debug_level)
    if bring_status != 0:
        print_flush("Not able to bring data from storage. Exiting now.")
        sys.exit(-95)
    return bring_status


def run_executable(nnlojet_command):
    # Run command
    status_nnlojet = os.system(nnlojet_command)
    if status_nnlojet == 0:
        print_flush("Command successfully executed")
    else:
        print_flush("Something went wrong")
        os.system("cat outfile.out")
        debug_level = 9999
    return status_nnlojet

def print_copy_status(args, status_copy):
    if status_copy == 0:
        print_flush("Copied over to grid storage!")
    elif args.Sockets:
        print_flush("This was a socketed run so we are copying the grid to stderr just in case")
        os.system("cat $(ls *.y* | grep -v .txt) 1>&2")
        status_copy = 0
    elif args.Warmup:
        print_flush("Failure! Outputing vegas warmup to stdout")
        os.system("cat $(ls *.y* | grep -v .txt)")

#################################################################################
#################################################################################

if __name__ == "__main__":
    args, debug_level = setup()

    if debug_level > 1:
        # Architecture info
        print_flush("Python version: {0}".format(sys.version))
        print_node_info("node_info.log")
        syscall("lsb_release -a")
        os.system("env")
        os.system("voms-proxy-info --all")

    bring_status = bring_files(args)

    nnlojet_command = RUN_CMD.format(args.threads, args.executable, args.runcard)

    if args.Sockets:
        nnlojet_command, socket_config = setup_sockets(args, nnlojet_command, bring_status)
    elif args.Production: # Assume sockets does not work with production
        nnlojet_command += " -iseed {0}".format(args.seed)

    if debug_level > 1:
        os.system("ls")
        os.system("ldd -v {0}".format(args.executable))
        
    os.system("chmod +x {0}".format(args.executable))
    nnlojet_command +=" 2>&1 outfile.out"
    print_flush(" > Executed command: {0}".format(nnlojet_command))

    # Run executable
    status_nnlojet = run_executable(nnlojet_command)
    # Store output
    status_copy, status_tar = store_output(args, socketed=args.Sockets)
    print_copy_status(args, status_copy)

    if args.Sockets:
        try: # only the first one arriving will go through!
            print_flush("Close Socket connection") 
            _ = socket_sync_str(host, port, "bye!") # Be polite
        except:
            pass

    teardown(status_nnlojet,status_copy,status_tar,bring_status)
