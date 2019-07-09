#!/usr/bin/env python
import os, sys, datetime
try:
    dirac = os.environ["DIRAC"]
    sys.path.append("{0}/Linux_x86_64_glibc-2.12/lib/python2.6/site-packages".format(dirac))
except KeyError as e:
    pass

MAX_COPY_TRIES = 15
PROTOCOLS = ["srm", "gsiftp", "root", "xroot", "xrootd"]
LHE_FILE="SherpaLHE_fixed.lhe"
LOG_FILE="output.log"

#### Override print with custom version that always flushes to stdout so we have up-to-date logs
def print_flush(string):
    print string
    sys.stdout.flush()

#####################################################################################
#                                                                                   #
# Try to keep this all python2.4 compatible. It may fail at some nodes otherwise :( #
#                                                                                   #
#####################################################################################

# This function must always be the same as the one in Backend.py
def warmup_name(runcard, rname):
    out = runcard + "+" + rname + ".tar.gz"
    return out

# This function must always be the same as the one in Backend.py
def output_name(runcard, rname, seed):
    out = "output-" + runcard + "-" + rname + "-" + seed + ".tar.gz"
    return out

def yoda_name(seed):
    return "HEJ_{0}".format(seed)

def config_name(rname):
    return "{0}.yml".format(rname)

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
    from optparse import OptionParser
    from getpass import getuser

    default_user_lfn = "/grid/pheno/{0}".format(getuser())
    default_user_gfal = "gsiftp://se01.dur.scotgrid.ac.uk/dpm/dur.scotgrid.ac.uk/home/pheno/{0}".format(getuser())
    parser = OptionParser(usage = "usage: %prog [options]")

    parser.add_option("-r","--runcard", help = "Runcard to be run")
    parser.add_option("-j", "--runname", help = "Runname")

    # Run options
    parser.add_option("-t", "--threads", help = "Number of thread for OMP", default = "1")
    parser.add_option("-e", "--executable", help = "Executable to be run", default = "HEJ")
    parser.add_option("-d", "--debug", help = "Debug level", default="0")
    parser.add_option("-s", "--seed", help = "Run seed", default="1")
    parser.add_option("-E", "--events", help = "Number of events", default="100")

    # Grid configuration options
    parser.add_option("-l", "--lfndir", help = "LFNDIR", default = default_user_lfn)
    parser.add_option("-i", "--input_folder",
                      help = "lfn input folder, relative to --lfndir or gfaldir depending on which mode is used",
                      default = "input")
    parser.add_option("-w", "--warmup_folder",
                      help = "lfn warmup folder, relative to --lfndir or gfaldir depending on which mode is used",
                      default = "warmup")
    parser.add_option("-o", "--output_folder",
                      help = "lfn output folder, relative to --lfndir or gfaldir depending on which mode is used",
                      default = "output")
    parser.add_option("-g", "--gfaldir", help = "gfaldir", default = default_user_gfal)
    parser.add_option("--use_gfal", default="False",
                      help = "Use gfal for file transfer and storage rather than the LFN")
    parser.add_option("--gfal_location", default="",
                      help = "Provide a specific location for gfal executables [intended for cvmfs locations]. Default is the environment gfal.")

    # LHAPDF options
    parser.add_option("--use_cvmfs_lhapdf", action = "store_true", default = True)
    parser.add_option("--cvmfs_lhapdf_location", default="",
                      help = "Provide a cvmfs location for LHAPDF.")
    parser.add_option("--lhapdf_grid", help = "absolute value of lhapdf location or relative to lfndir",
                      default = "util/lhapdf.tar.gz")
    parser.add_option("--lhapdf_local", help = "name of LHAPDF folder local to the sandbox", default = "lhapdf")

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


    if options.use_gfal.lower() == "true":
        options.use_gfal = True
        print("Using GFAL for storage")
    else:
        options.use_gfal = False

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

def set_environment(lfndir, lhapdf_dir):
    os.system("export PYTHONPATH=${PYTHONPATH}:${DIRAC}/Linux_x86_64_glibc-2.12/lib/python2.6/site-packages")
    os.environ["LFC_HOST"]         = "lfc01.dur.scotgrid.ac.uk"
    os.environ["LCG_CATALOG_TYPE"] = "lfc"
    os.environ["LFC_HOME"]         = lfndir
    os.environ["LCG_GFAL_INFOSYS"] = "lcgbdii.gridpp.rl.ac.uk:2170"
    os.environ['OMP_STACKSIZE']    = "999999"
    # os.environ['PYTHONPATH']       = os.environ["PYTHONPATH"]+":"+os.environ["DIRAC"]+ \
    #     "/linux_x86_64_glibc-2.12/lib/python2.6/site-packages/"
    try:
        import gfal2_util.shell
    except KeyError as e:
        pass
    except ImportError as e:
        # If gfal can't be imported then the site packages need to be added to the python path because ? :(
        os.environ["PYTHONPATH"] = os.environ["PYTHONPATH"]\
            +":"+args.gfal_location.replace("/bin/","/lib/python2.7/site-packages/")
        os.environ["LD_LIBRARY_PATH"] = os.environ["LD_LIBRARY_PATH"]\
            +":"+args.gfal_location.replace("/bin/","/lib/")
    # HEJ environment
    os.environ['LD_LIBRARY_PATH'] = "./HEJ/lib"+":"+os.environ["LD_LIBRARY_PATH"]
    return 0
# export PYTHONPATH=$PYTHONPATH:$DIRAC/Linux_x86_64_glibc-2.12/lib/python2.6/site-packages


gsiftp = "gsiftp://se01.dur.scotgrid.ac.uk/dpm/dur.scotgrid.ac.uk/home/pheno/mheil/"
lcg_cp = "lcg-cp"
lcg_cr = "lcg-cr --vo pheno -l"
lfn    = "lfn:"

# Define some utilites


def run_command(command):
    command += " 1>> {0} 2>&1".format(LOG_FILE)
    print_flush(" > Executed command: {0}".format(command))
    return os.system(command)

#### COPYING ####

def copy_from_grid(grid_file, local_file, args, maxrange=MAX_COPY_TRIES):
    if args.use_gfal:
        # filein = "file://$PWD/" + local_file
        # cmd = "gfal-copy {2}/{0} {1}".format(grid_file, filein, args.gfaldir)
        # return os.system(cmd)
        filein = os.path.join(args.gfaldir, grid_file)
        fileout = "file://$PWD/{0}".format(local_file)
        return gfal_copy(filein, fileout, args, maxrange=maxrange)

    else:
        cmd = lcg_cp + " " + lfn
        cmd += grid_file + " " + local_file
        return os.system(cmd)

def copy_to_grid(local_file, grid_file, args, maxrange = 10):
    print_flush("Copying " + local_file + " to " + grid_file)
    fileout = lfn + grid_file
    if args.use_gfal:
        filein = "file://$PWD/{0}".format(local_file)
        fileout = os.path.join(args.gfaldir, grid_file)
        return gfal_copy(filein, fileout, args, maxrange=maxrange)

        # filein = "f# ile://$PWD/" + local_file
        # cmd = "gfal-copy {0} {2}/{1}".format(filein, grid_file, args.gfaldir)
        # os.system(cmd)
        # return 0
    else:
        filein = "file:$PWD/" + local_file
        cmd = lcg_cr + " " + fileout + " " + filein
    print_flush("> Sandbox -> LFN copy command: {0}".format(cmd))
    dirname = os.path.dirname(grid_file)
    for i in range(maxrange): # try max 10 times for now ;)
        exit_code=1
        output = os.popen(cmd).read().strip()
        print_flush(output)
        if "guid" in output: # Don't want to lfc-ls in case it's not present...
            exit_code = 0
            break
        elif i != maxrange-1:
            print_flush("Copy failure. Trying again. [Attempt "+str(i+1)+"]")
        else:
            print_flush("Copy failed after "+str(i+1)+" attempts.")
            print_flush("Giving up now.")
            exit_code=500
    return exit_code

def gfal_copy(infile, outfile, args, maxrange=MAX_COPY_TRIES):
    print_flush("Copying {0} to {1}".format(infile, outfile))
    protoc = args.gfaldir.split(":")[0]
    for protocol in PROTOCOLS: # cycle through available protocols until one works.
        infile_tmp = infile.replace(protoc, protocol)
        outfile_tmp = outfile.replace(protoc, protocol)
        print_flush("Attempting Protocol {0}".format(protocol))
        for i in range(maxrange): # try max 10 times for now ;)
            cmd = "{2}gfal-copy --checksum-mode both --abort-on-failure -f {0} {1}".format(
                infile_tmp, outfile_tmp, args.gfal_location)
            if args.debug > 1:
                print_flush(cmd)
            retval = syscall(cmd)
            if retval == 0:
                return retval
            # if copying to the grid and it has failed, remove before trying again
            if retval != 0 and "file" not in outfile and not args.Sockets:
                os.system("gfal-rm {0}".format(outfile_tmp))
    return 9999999

#### TAR ####

def untar_file(local_file, debug):
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

### Download executable ###

def download_program(debug):
    # TODO read tar and source name from header
    tar_name = "HEJ.tar.gz"
    source = "HEJ/{0}".format(tar_name)
    stat = copy_from_grid(source, tar_name, args)
    stat += untar_file(tar_name, debug)
    stat += os.system("rm {0}".format(tar_name))
    if debug_level > 2:
        os.system("ls -l HEJ")
    return stat

def download_runcard(input_folder, runcard, runname, debug_level):
    tar = warmup_name(runcard,runname)
    print_flush("downloading "+input_folder+"/"+tar)
    stat = copy_from_grid(input_folder+"/"+tar, tar, args)
    stat += untar_file(tar, debug_level)
    # TODO download:
    #   rivet analysis
    #   Scale setters
    return os.system("rm {0}".format(tar))+stat

### Misc ###

def print_node_info(outputfile):
    os.system("hostname >> {0}".format(outputfile))
    # os.system("cat /proc/cpuinfo >> {0}".format(outputfile))
    os.system("gcc --version >> {0}".format(outputfile))
    os.system("python --version >> {0}".format(outputfile))

def end_program(status, debug_level):
    # TODO print debug infos here if status!=0
    if status != 0 or debug_level > 8:
        os.system("cat "+LOG_FILE)
        os.system("ls")
    end_time = datetime.datetime.now()
    print_flush("End time: {0}".format(end_time.strftime("%d-%m-%Y %H:%M:%S")))
    print_flush("Final Error Code: {0}".format(status))
    sys.exit(status)


########################## Actual run commands ##########################

def run_sherpa(args):
    status = run_command("Sherpa RSEED:={0} -e {1} ANALYSIS_OUTPUT=Sherpa_{0}"
        .format(args.seed, args.events))
    status += run_command("SherpaLHEF SherpaLHE.lhe {0}".format(LHE_FILE))
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
    status = os.system(
        'sed -i -e "s/seed:.1/seed: {0}/g" {1}'.format(seed, config) )
    status += os.system(
        'sed -i -e "s/output:.HEJ/output: {0}/g" {1}'.format(
            yoda_name(seed), config) )
    status += os.system("chmod +x {0}".format(args.executable))
    if status == 0:
        status += run_command(
            "{0} {1} {2}".format(args.executable, config, LHE_FILE) )
    return status

########################## main ##########################

## test
# ./main.py test runcards/hej_runcard.py -B
# -B = arc production
## run on arc
# ./main.py run runcards/hej_runcard.py -B
## send test job to arc
# ./main.py run runcards/hej_runcard.py -B --test

## init (uploads runcards & Sherpa to gfal)
# ./main.py ini runcards/hej_runcard.py -B

## get job info
# ./main.py man runcards/hej_runcard.py -B
# -s for stats
# -p for print
# ask the help for more

## getting (known) queues
# ./get_site_info.py

if __name__ == "__main__":

    if sys.argv[0] and not "ENVSET" in os.environ:
        print_flush("Setting environment")
        os.environ["ENVSET"]="ENVSET"
        env = "/cvmfs/pheno.egi.eu/HEJ/HEJ_env.sh"
        os.execvp("bash", ["bash", "-c",
            "source " + env + " && exec python " + sys.argv[0] + ' "${@}"',
            "--"] + sys.argv[1:])

    start_time = datetime.datetime.now()
    print_flush("Start time: {0}".format(start_time.strftime("%d-%m-%Y %H:%M:%S")))

    args = parse_arguments()
    debug_level = int(args.debug)

    set_environment(args.lfndir, args.lhapdf_local)

    if debug_level > -1:
        # Architecture info
        print_flush("Python version: {0}".format(sys.version))
        print_node_info("node_info.log")

    # Debug info
    if debug_level > 16:
        os.system("env")
        os.system("voms-proxy-info --all")

    # Download components
    status = download_program(debug_level)

    os.system("chmod +x {0}".format(args.executable))

    if debug_level > 8:
        os.system("ldd {0}".format(args.executable))

    status += download_runcard(args.input_folder, args.runcard, args.runname, debug_level)

    if status != 0:
        print_flush("download failed")
        end_program(status, debug_level)

    if "HEJFOG" in args.runname:
        status += run_HEJFOG(args)
    else:
        status += run_sherpa(args)

    if status != 0:
        print_flush("FOG failed")
        end_program(status, debug_level)

    status += run_HEJ(args)
    if status != 0:
        print_flush("HEJ failed")
        end_program(status, debug_level)

    local_out = output_name(args.runcard, args.runname, args.seed)
    output_file = args.output_folder + "/" + local_out

    status += tar_this(local_out, "*.yoda *.log *.yml Run.dat")

    status += copy_to_grid(local_out, output_file, args)

    if debug_level > 1:
        os.system("ls")

    if status == 0:
        print_flush("Copied over to grid storage!")

    end_program(status, debug_level)
