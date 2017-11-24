#!/usr/bin/env python

import os

def warmup_name(runcard, rname):
    # This function must always be the same as the one in Backend.py
    out = "output" + runcard + "-warm-" + rname + ".tar.gz"
    return out

def output_name(runcard, rname, seed):
    # This function must always be the same as the one in Backend.py
    out = "output" + runcard + "-" + rname + "-" + seed + ".tar.gz"
    return out

def parse_arguments():
    from optparse import OptionParser
    from getpass import getuser

    default_user_lfn = "/grid/pheno/{0}".format(getuser())  
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
    parser.add_option("-i", "--input_folder", help = "lfn input folder, relative to --lfndir", default = "input")
    parser.add_option("-w", "--warmup_folder", help = "lfn warmup folder, relative to --lfndir", default = "warmup")
    parser.add_option("-o", "--output_folder", help = "lfn output folder, relative to --lfndir", default = "output")

    # LHAPDF options
    parser.add_option("--lhapdf_grid", help = "absolute value of lhapdf location or relative to lfndir", default = "util/lhapdf.tar.gz")
    parser.add_option("--lhapdf_local", help = "name of LHAPDF folder local to the sandbox", default = "lhapdf")

    # Socket options
    parser.add_option("-S", "--Sockets", help = "Activate socketed run", action = "store_true", default = False)
    parser.add_option("-p", "--port", help = "Port to connect the sockets to", default = "8888")
    parser.add_option("-H", "--Host", help = "Host to connect the sockets to", default = "gridui1.dur.scotgrid.ac.uk")

    # Mark the run as production or warmup
    parser.add_option("-P", "--Production", help = "Production run", action = "store_true", default = False)
    parser.add_option("-W", "--Warmup", help = "Warmup run", action = "store_true", default = False)

    parser.add_option("--pedantic", help = "Enable various checks", action = "store_true", default = False)

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
    
    print("Arguments: {0}".format(options))

    return options

def set_environment(lfndir, lhapdf_dir):
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
    return 0

    

gsiftp = "gsiftp://se01.dur.scotgrid.ac.uk/dpm/dur.scotgrid.ac.uk/home/pheno/generated/"

lcg_cp = "lcg-cp"
lcg_cr = "lcg-cr --vo pheno -l"
lfn    = "lfn:"
gfal   = False

#lcg_cp = "gfal-copy"
#lcg_cr = "gfal-copy"
#lfn = "lfn://grid/pheno/jmartinez/"
#gfal = True

# Define some utilites
def copy_from_grid(grid_file, local_file):
    cmd = lcg_cp + " " + lfn
    cmd += grid_file + " " + local_file
    os.system(cmd)

def untar_file(local_file, debug):
    if debug_level > 2:
        cmd = "tar zxfv {0}".format(local_file)
    else:
        cmd = "tar zxf " + local_file
    os.system(cmd)

def tar_this(tarfile, sourcefiles):
    cmd = "tar -czf " + tarfile + " " + sourcefiles
    os.system(cmd)
    os.system("ls")

def copy_to_grid(local_file, grid_file):
    print("Copying " + local_file + " to " + grid_file)
    filein = "file:$PWD/" + local_file
    fileout = lfn + grid_file
    if gfal: # May need checking if we move to gfal
        from uuid import uuid1 as generateRandom
        today_str = datetime.today().strftime('%Y-%m-%d')
        unique_str = "ffilef" + str(generateRandom())
        file_str = today_str + "/" + unique_str
        midfile = gsiftp + file_str
        cmd = lcg_cr + " " + filein + " " + midfile + " " + fileout
    else:
        cmd = lcg_cr + " " + fileout + " " + filein
    print(cmd)
    fail = os.system(cmd)
    if fail == 0:
    # success!
        return True
    else:
        return False

def socket_sync_str(host, port, handshake = "greetings"):
    # Blocking call, it will receive a str of the form
    # -sockets {0} -ns {1}
    import socket
    sid = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sid.connect((host, int(port)))
    sid.send(handshake)
    return sid.recv(32)

def bring_lhapdf(lhapdf_grid, debug):
    tmp_tar = "lhapdf.tar.gz"
    copy_from_grid(lhapdf_grid, tmp_tar)
    untar_file(tmp_tar, debug)
    os.system("rm {0}".format(tmp_tar))
    return 0

def bring_nnlojet(input_grid, runcard, runname, debug):
    # Todo: this is not very general, is it?
    tmp_tar = "nnlojet.tar.gz"
    input_name = "{0}/{1}{2}.tar.gz".format(input_grid, runcard, runname)
    copy_from_grid(input_name, tmp_tar)
    untar_file(tmp_tar, debug)
    os.system("rm {0}".format(tmp_tar))
    os.system("ls")
    return 0


#################################################################################
#################################################################################

if __name__ == "__main__":
    args = parse_arguments()

    debug_level = int(args.debug)
    
    if debug_level > 1:
        from sys import version
        print("Running Python version {0}".format(version))

    nnlojet_command = "OMP_NUM_THREADS={0} ./{1} -run {2}".format(args.threads, args.executable, args.runcard)

    if args.Sockets:
        host = args.Host
        port = args.port
        print("Sockets are active, trying to connect to {0}:{1}".format(host,port))
        socket_config = socket_sync_str(host, port)
        if "die" in socket_config:
            print("Timeout'd by socket server")
            exit(0)
        socketed = True
        print("Connected to socket server")
        nnlojet_command += " -port {0} -host {1} {2}".format(port, host,  socket_config)

    if args.Production:
        nnlojet_command += " -iseed {0}".format(args.seed)

    set_environment(args.lfndir, args.lhapdf_local)

    if debug_level > 2:
        os.system("env")

    print("Downloading LHAPDF")
    bring_lhapdf(args.lhapdf_grid, debug_level)
    bring_nnlojet(args.input_folder, args.runcard, args.runname, debug_level)

    if debug_level > 1:
        os.system("ls")
        os.system("ldd -v {0}".format(args.executable))
        
    os.system("chmod +x {0}".format(args.executable))
    nnlojet_command +=" 2>&1 outfile.out"
    print(" > Executed command: {0}".format(nnlojet_command))

    # Run command
    status = os.system(nnlojet_command)
    if status == 0:
        print("Command successfully executed")
    else:
        print("Something went wrong")
        os.system("cat outfile.out")
        debug_level = 9999

    # Debug info
    os.system("voms-proxy-info --all")

    # Copy stuff to grid storage, remove executable and lhapdf folder
    os.system("rm -rf {0} {1}".format(args.executable, args.lhapdf_local))
    if args.Production:
        local_out = output_name(args.runcard, args.runname, args.seed)
        output_file = args.output_folder + "/" + local_out
    elif args.Warmup:
        local_out = warmup_name(args.runcard, args.runname)
        output_file = args.warmup_folder + "/" + local_out

    if debug_level > 1:
        os.system("ls")

    tar_this(local_out, "*")

    success = copy_to_grid(local_out, output_file)

    if success:
        print("Copied over to grid storage!")
    elif args.Warmup:
        print("Failure! Outputing vegas warmup to stdout")
        os.system("cat $(ls *.y* | grep -v .txt)")

    if args.Sockets:
        try: # only the first one arriving will go through!
            print("Close Socket connection") 
            _ = socket_sync_str(host, port, "bye!") # Be polite
        except:
            pass
