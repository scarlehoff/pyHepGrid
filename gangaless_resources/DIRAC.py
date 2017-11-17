#!/usr/bin/env python

# TODO:
# Split out separte gcc location rather than use LHAPDF loc in case they live in different places
# and instead pass through via input args for maximum generality.
# Move DEBUG to command line arg passed by runDiracJob.py.
import os
import sys
gsiftp = "gsiftp://se01.dur.scotgrid.ac.uk/dpm/dur.scotgrid.ac.uk/home/pheno/generated/"

lcg_cp = "lcg-cp"
lcg_cr = "lcg-cr --vo pheno -l"
lfn = "lfn:"
gfal = False
DEBUG = False


#lcg_cp = "gfal-copy"
#lcg_cr = "gfal-copy"
#lfn = "lfn://grid/pheno/jmartinez/"
#gfal = True

# Define some utilites
def output_name(runcard, rname, seed):
    # This function must always be the same as the one in Backend.py
    out = "output" + runcard + "-" + rname + "-" + seed + ".tar.gz"
    return out


def copy_from_grid(grid_file, local_file):
    cmd = lcg_cp + " " + lfn
    cmd += grid_file + " " + local_file
    print("Copying {0} to {1} from the grid".format(grid_file, local_file))
    os.system(cmd)


def untar_file(local_file):
    if DEBUG:
        # cmd = "tar zxvf " + local_file
        cmd = "tar zvxf " + local_file
    else:
        cmd = "tar zxf " + local_file
    print("Untarring {0}".format(local_file))
    os.system(cmd)


def tar_this(tarfile, sourcefiles):
    if DEBUG:
        cmd = "tar -cvzf " + tarfile + " " + sourcefiles
    else:
        cmd = "tar -czf " + tarfile + " " + sourcefiles
    print("Tarring {0} as {1}".format(sourcefiles, tarfile))
    os.system(cmd)


def copy_to_grid(local_file, grid_file):
    filein = "file:$PWD/" + local_file
    fileout = lfn + grid_file
    if gfal:
        from uuid import uuid1 as generateRandom
        from header import gsiftp
        today_str = datetime.today().strftime('%Y-%m-%d')
        unique_str = "ffilef" + str(generateRandom())
        file_str = today_str + "/" + unique_str
        midfile = gsiftp + file_str
        cmd = lcg_cr + " " + filein + " " + midfile + " " + fileout
    else:
        cmd = lcg_cr + " " + fileout + " " + filein
    fail = os.system(cmd)
    if fail == 0:
        return True
    else:
        return False

# Runscript for Dirac (modified from Tom's ganga.py)

#
#  Read input arguments
#


#LFNDIR  = "/grid/pheno/dwalker"
RUNCARD = sys.argv[1]
RUNNAME = sys.argv[2]
SEED = sys.argv[3]
lhapdf_grid_loc = sys.argv[4]
LFNDIR = sys.argv[5]
LHAPDF_LOC = sys.argv[6]
EXENAME = sys.argv[7]
OUTDIR = sys.argv[8]

#
# Set environment
#

#os.environ["LFC_HOST"]         = "lfc.grid.sara.nl"
os.environ["LFC_HOST"] = "lfc01.dur.scotgrid.ac.uk"
os.environ["LCG_CATALOG_TYPE"] = "lfc"
os.environ["LFC_HOME"] = LFNDIR
os.environ["LCG_GFAL_INFOSYS"] = "lcgbdii.gridpp.rl.ac.uk:2170"
os.environ['OMP_STACKSIZE'] = "999999"
os.environ['OMP_NUM_THREADS'] = "1"

# ./bin/lhapdf:    configured_datadir = '${prefix}/share/LHAPDF'.replace('${prefix}', '/mt/home/jmartinez/LHAPDF')
PATH = os.environ['PATH']
LHAPDFBIN = os.path.join(os.getcwd(), LHAPDF_LOC, 'bin')
os.environ['PATH'] = LHAPDFBIN + ':' + PATH + ":" 
LHAPDFEXE = os.path.join(LHAPDFBIN, "lhapdf")
lhapdf_path = os.path.join(os.getcwd(), LHAPDF_LOC, "lib")
lhapdf_sharepath = os.path.join(os.getcwd(), LHAPDF_LOC, "share", "LHAPDF")

os.environ['LHAPATH'] = lhapdf_sharepath
os.environ['LHA_DATA_PATH'] = lhapdf_sharepath
os.environ['LHAPDF_DATA_PATH'] = lhapdf_sharepath
os.environ['LHAPDFLIB'] = lhapdf_path


# Check for gcc in cvmfs
cvmfs_gcc_dir = '/cvmfs/pheno.egi.eu/compilers/GCC/5.2.0/'
if os.path.isdir(cvmfs_gcc_dir):
    gcclocal = False
    gcc_libpath = os.path.join(cvmfs_gcc_dir, "lib")
    gcc_lib64path = os.path.join(cvmfs_gcc_dir, "lib64")
    gcc_PATH = os.path.join(cvmfs_gcc_dir, "bin")
else:
    gcclocal = True
    gcc_libpath = os.path.join(os.getcwd(), "gcc", "lib")
    gcc_lib64path = os.path.join(os.getcwd(), "gcc", "lib64")
    gcc_PATH = os.path.join(os.getcwd(), "gcc", "bin")


# Populate LD_LIBRARY_PATH
try:
    old_ldpath = os.environ["LD_LIBRARY_PATH"]
except KeyError as e:
    old_ldpath = ""
os.environ["LD_LIBRARY_PATH"] = "%s:%s:%s:%s" % (
    gcc_libpath, gcc_lib64path, lhapdf_path, old_ldpath)

# Populate PATH
old_PATH = os.environ["PATH"]
os.environ["PATH"] = "%s:%s" % (gcc_PATH, old_PATH)

gcc_libpath = os.path.join(os.getcwd(), "gcc", "lib")
gcc_lib64path = os.path.join(os.getcwd(), "gcc", "lib64")

#
# Bringing files from Grid Storage
#

# Bring LHAPDF from Grid Storage
lhapdf_file = "lhapdf.tar.gz"
#copy_from_grid("input/" + lhapdf_file, lhapdf_file)
copy_from_grid(lhapdf_grid_loc + lhapdf_file, lhapdf_file)
untar_file(lhapdf_file)
os.system("chmod +x " + LHAPDFEXE)
os.system("chmod +x " + LHAPDFEXE+"-config")

# DEBUG

if DEBUG:
    print "****************DEBUG****************"
    print("PATH:")
    os.system('echo $PATH')
    print("LHAPATH:")
    os.system('echo $LHAPATH')
    print("LHA_DATA_PATH:")
    os.system('echo $LHA_DATA_PATH')
    print("LD_LIBRARY_PATH:")
    os.system('echo $LD_LIBRARY_PATH')
    print("LHAPDFLIB:")
    os.system('echo $LHAPDFLIB')
    print("LHAPDF DEBUG:") 
    os.system('which lhapdf')
    os.system("lhapdf -v ls --installed")
    print("LHAPDF-CONFIG")
    os.system('which lhapdf-config')
    print("LIBDIR")
    os.system("lhapdf-config --libdir")
    print("DATADIR")
    os.system("lhapdf-config --datadir")
    print("PREFIX")
    os.system("lhapdf-config --prefix")
    os.system("env | grep -i lha")
    os.system('env')
#    os.system("lhapdf -v ls --outdated")
#    print("FIND OUTPUT")
#    os.system("find "+os.getcwd())

    print "**************END DEBUG**************"


# Bring gcc if needed
if gcclocal:
    print("GCC NOT FOUND")
    gcc_file = "gcc.tar.gz"
#    copy_from_grid("input/" + gcc_file, gcc_file)
    copy_from_grid(lhapdf_grid_loc + gcc_file, gcc_file)
    untar_file(gcc_file)
# Bring NNLOJET and runcards
nnlojet_tar = "NNLOJET.tar.gz"
copy_from_grid("input/" + RUNCARD + RUNNAME + ".tar.gz", nnlojet_tar)
untar_file(nnlojet_tar)

if DEBUG:
    print("LDD")
    os.system("ldd NNLOJET")
    os.system("ls")

#
# Run NNLOJET
#

# Prepare for running
os.system("chmod +x " + EXENAME)
command = "./" + EXENAME + " -run " + RUNCARD + " -iseed " + SEED

# For debugging
command += " 2>&1 outfile.out;echo $LD_LIBRARY_PATH"
print(" > Executed command: {0}".format(command))
print(" > Sys.argv: {0}".format(sys.argv))

# Get installed PDFS
# os.system("lhapdf ls --installed")
# Run command
status = os.system(command)
if status == 0:
    print("Command successfully executed")
else:
    print("Something went wrong")
    os.system("cat outfile.out")

#
# Is cleanup necessary at all? Is this not done by ARC itself
# once we "timeout"?
#
os.system("voms-proxy-info --all")
os.system("rm -rf lhapdf/")
os.system("rm -rf runcards/")
if gcclocal:
    os.system("rm -rf gcc/")
    os.system("rm -rf gcc.tar.gz")
os.system("rm " + lhapdf_file)
os.system('rm *.RRa *.RRb *.vBa *.vRa ' + EXENAME)
os.system("rm " + nnlojet_tar)
os.system("rm TOT.*")
os.system("rm fort*")

# Create warmup name
output = output_name(RUNCARD, RUNNAME, SEED)
# Copy to grid storage
tar_this(output, "*")
success = copy_to_grid(output, OUTDIR + "/" + output)
if success:
    print("All information copied over to grid storage")
else:
    print("Failure copying to grid storage")
    # To do: keep a socket open in gridui which will automatically receive all data if copying fails... mmmm
os.system('ls')

# Bring cross section parser
# TODO:
# Remove this? OR replace with numpy independent equivalent.

# try:
#     copy_from_grid("util/pyCross.py", "pyCross.py")
#     dir = os.listdir('.')
#     print(dir)
#     for i in dir:
#         if "cross" in i:
#             cmd = "python pyCross.py " + i + " " + RUNCARD
#             os.system(cmd)
# except:
#     print("Some problem doing pycross")
