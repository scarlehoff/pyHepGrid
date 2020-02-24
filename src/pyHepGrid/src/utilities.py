#!/usr/bin/env python3
import collections
from datetime import datetime
import json
import os
import pyHepGrid.src.header as header
import re
import shutil
import subprocess
from sys import version_info
import tarfile
from uuid import uuid4
#
# Misc. Utilities
#
#

MAX_COPY_TRIES = 5
PROTOCOLS = ["xroot", "gsiftp", "dav"]

###################################
def pythonVersion():
    try:
        return version_info.major
    except:
        return 2
##################################

#
# Runcard parser
#
def expandCard(dummy=None):
    dictCard = header.dictCard
    rcards = dictCard.keys()
    return rcards, dictCard

#
# Subprocess Wrappers
#
def spCall(cmd, suppress_errors = False, shell=False):
    if shell:
        cmd = [" ".join(cmd)]
    try:
        header.logger.debug(cmd)
        if not suppress_errors:
            return subprocess.call(cmd, shell=shell)
        else:
            return subprocess.call(cmd, stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL, shell=shell)
        return 0
    except:
        raise Exception("Couldn't issue the following command: ", ' '.join(cmd))
        return -1

def getOutputCall(cmd, suppress_errors=False, include_return_code=True):
    try:
        header.logger.debug(cmd)
        if not suppress_errors:
            popen = subprocess.Popen(cmd, stdout=subprocess.PIPE)
        else:
            popen = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        outbyt, syserr = popen.communicate()
        header.logger.debug(outbyt)
        header.logger.debug(syserr)
        outstr = outbyt.decode("utf-8")
        if include_return_code:
            outstr = (outstr, popen.returncode)
        return outstr
    except:
        raise Exception("Something went wrong with Popen: ", ' '.join(cmd))
        return -1

#
# Fileystem Wrappers
#
def unique_filename():
    """ Create a unique filename in /tmp/
    if possible, otherwise create file in current directory
    """
    unique_name = str(uuid4().hex)
    filename = os.path.join("/tmp/", unique_name)
    # better ask for forgiveness than for permission
    try:
        f = open(filename, 'w')
        f.close()
        return filename
    except:
        return unique_name

def checkIfThere(dirPath, filename = ''):
    if not os.path.exists(os.path.join(dirPath, filename)):
        return False
    else:
        return True

def generatePath(warmup):
    # Check whether the folder already exist
    date     = datetime.now()
    month    = date.strftime("%B")
    day      = str(date.day)
    homePath = os.environ['HOME']
    if warmup:
        baseDir = header.warmup_base_dir
    else:
        baseDir = header.production_base_dir
    if baseDir is not None:
        basePath = homePath + baseDir
        monthlyPath = os.path.join(basePath, month)
        dailyPath = os.path.join(monthlyPath, day)
    # Only create the folder structure if we are using the "native" get_data
        if not header.finalisation_script and not os.path.exists(dailyPath):
            header.logger.info("Creating daily path at {0}".format(dailyPath))
            os.makedirs(dailyPath)
        return dailyPath

def sanitiseGeneratedPath(dailyPath, rname):
    i = 0
    finalname = rname + "-n0"
    while checkIfThere(dailyPath, finalname):
        i += 1
        finalname = rname + "-n" + str(i)
    finalPlacement = os.path.join(dailyPath, finalname)
    return finalPlacement

#
# Miscellaneous helpers
#
def batch_gen(data, batch_size):
    for i in range(0, len(data), batch_size):
            yield data[i:i+batch_size]

#
# Library initialisation
#
def lhapdfIni():
    lha_conf = "lhapdf-config"
    if getOutputCall(["which", lha_conf], include_return_code=False) != "":
        header.logger.info("Using lhapdf-config to get lhapdf directory")
        lha_raw = getOutputCall([lha_conf, "--prefix"], include_return_code=False)
        lha_dir = lha_raw.rstrip()
    else:
        lha_dir = header.lhapdf
    lhapdf = header.lhapdf_loc
    header.logger.info("Copying lhapdf from {0} to {1}".format(lha_dir, lhapdf))
    bring_lhapdf_pwd = ["cp", "-LR", lha_dir, lhapdf]
    spCall(bring_lhapdf_pwd)
    # Remove any unwanted directory from lhapdf
    rmdirs = header.lhapdf_ignore_dirs
    if header.lhapdf_ignore_dirs is not None:
        for root, dirs, files in os.walk(lhapdf):
            for directory in dirs:
                directory_path = os.path.join(root, directory)
                for rname in rmdirs:
                    if rname in directory_path:
                        shutil.rmtree(directory_path)
                        break
    pdfs = collections.defaultdict(set)
    if header.lhapdf_central_scale_only:
        header.logger.info("Removing non-central scales from lhapdf")

    for root, dirs, files in os.walk(lhapdf):
        for xfile in files:
            fullpath = os.path.join(root,xfile)
            if "share" and ".dat" in fullpath and "._" not in fullpath:
                if "_0000.dat" not in fullpath and header.lhapdf_central_scale_only:
                    os.remove(fullpath)
                    continue
                elif header.lhapdf_central_scale_only:
                    prettyname = fullpath.split("/")[-1].replace("_0000.dat","")
                    header.logger.info("Including central PDF for {0} from {1}".format(prettyname,root))
                setname = re.sub(r"_([0-9]*).dat","",fullpath.split("/")[-1])
                member = int(re.search(r"_([0-9]*).dat",fullpath.split("/")[-1]
                                       ).group(0).replace("_","").replace(".dat",""))
                pdfs[setname].update(set([member]))


    pdf_file_name = os.path.join(os.path.dirname(os.path.realpath(__file__)),".pdfinfo")
    for key,val in pdfs.items():
        pdfs[key]=list(val)

    pdfs = dict(pdfs)
    header.logger.info("Writing pdf contents to .pdfinfo file")

    with open(pdf_file_name,"w") as pdf_file_obj:
        pdf_file_obj.write(json.dumps(pdfs, sort_keys=True, indent=2, separators=(',', ':')))

    # Tar lhapdf and prepare it to be sent
    lhapdf_remote = header.lhapdf_grid_loc
    lhapdf_griddir = lhapdf_remote.rsplit("/",1)[0]
    lhapdf_gridname = lhapdf_remote.rsplit("/")[-1]
    tar_w = TarWrap()
    grid_w = GridWrap()
    tar_w.tarDir(lhapdf, lhapdf_gridname)
    size = os.path.getsize(lhapdf_gridname)/float(1<<20)
    header.logger.info("> LHAPDF tar size: {0:>6.3f} MB".format(size))
    if grid_w.checkForThis(lhapdf_gridname, lhapdf_griddir):
        header.logger.info("> Removing previous version of lhapdf in the grid")
        grid_w.delete(lhapdf_gridname, lhapdf_griddir)
    header.logger.info("> Sending new lhapdf to grid as {0}".format(lhapdf_gridname))
    grid_w.send(lhapdf_gridname, lhapdf_griddir)
    shutil.rmtree(lhapdf)
    os.remove(lhapdf_gridname)

#
# Tar wrappers
#

class TarWrap:

    def tarDir(self, inputDir, output_name):
        with tarfile.open(output_name, "w:gz") as output_tar:
            output_tar.add(inputDir)

    def tarFiles(self, inputList, output_name):
        with tarfile.open(output_name, "w:gz") as output_tar:
            for infile in inputList:
                output_tar.add(infile)

    def listFilesTar(self, tarred_file):
        with tarfile.open(tarred_file, 'r|gz') as tfile:
            outp = tfile.getnames()
        return outp

    def extractThese(self, tarred_file, listFiles):
        with tarfile.open(tarred_file, 'r|gz') as tfile:
            for t in tfile:
                if t.name in listFiles:
                    tfile.extract(t)

    def extractAll(self, tarred_file):
        with tarfile.open(tarred_file, 'r|gz') as tfile:
            tfile.extractall()

    def extract_extension_to(self, tarred_file, extension_dict):
        """
        extenson_dict is a dictionary {"ext" : "path"} so that
        if the file has extension "ext" it will be extracted to "path"
        """
        # TODO: I don't like this, I'm sure there is a better way...
        with tarfile.open(tarred_file, 'r|gz') as tfile:
            for t in tfile:
                for ext in extension_dict:
                    if t.name.endswith(ext):
                        tfile.extract(t, path = extension_dict[ext])
                        break

    def check_filesizes(self, tarred_file, extensions):
        matches = []
        sizes = []
        tuple_ext = tuple(extensions)
        with tarfile.open(tarred_file, 'r|gz') as tfile:
            for t in tfile:
                if t.name.endswith(tuple_ext):
                    sizes.append(t.size)
                    matches.append(t.name)
        return matches, sizes

    def extract_extensions(self, tarred_file, extensions):
        matches = []
        tuple_ext = tuple(extensions)
        with tarfile.open(tarred_file, 'r|gz') as tfile:
            for t in tfile:
                if t.name.endswith(tuple_ext):
                    tfile.extract(t)
                    matches.append(t.name)
        return matches

#
# GridUtilities
#
class GridWrap:
    # Defaults
    # Need to refactor post dpm gfal

    def send(self, tarfile, whereTo, shell=False):
        gridfile = os.path.join(header.gfaldir, whereTo, tarfile)
        localfile = "file://{0}/{1}".format(os.getcwd(),tarfile)
        count = 1
        while True:
            success = gfal_copy(localfile, gridfile)
            # Check whether we actually sent what we wanted to send
            if self.checkForThis(tarfile, whereTo):
                break
            elif count < 3: # 3 attempts before asking for input...
                header.logger.warning("{0} could not be copied to the grid storage /for some reason/ after {1} attempt(s)".format(tarfile,count))
                header.logger.info("Automatically trying again...")
            else:
                header.logger.warning("{0} could not be copied to the grid storage /for some reason/ after {1} attempt(s)".format(tarfile,count))
                yn = input(" Try again? (y/n) ")
                if not yn.startswith("y"):
                    header.logger.error("{0} was not copied to the grid storage after {1} attempt(s)".format(tarfile,count))
                    break
            count +=1
        return success

    def bring(self, tarfile, whereFrom, whereTo, shell=False, timeout = None, suppress_errors=False, force=False):
        gridname = os.path.join(header.gfaldir, whereFrom, tarfile)
        destpath = "file://$PWD/{0}".format(whereTo)
        success = gfal_copy(gridname, destpath)
        return os.path.isfile(whereTo)

    def delete(self, tarfile, whereFrom):
        gridname = os.path.join(header.gfaldir, whereFrom, tarfile)
        cmd = ["gfal-rm", gridname]
        return spCall(cmd)

    def checkForThis(self, filename, where):
        gridname = os.path.join(header.gfaldir, where)
        cmd = ["gfal-ls", gridname]
        output = getOutputCall(cmd, include_return_code=False)
        filelist = output.split("\n")
        if filename in filelist:
            return True
        else:
            return False

    def get_dir_contents(self, directory):
        gridname = os.path.join(header.gfaldir, directory)
        cmd = ["gfal-ls", gridname]
        output = getOutputCall(cmd, include_return_code=False)
        return output

    def delete_directory(self, directory):
        # Get contents and delete them one by one (there is no recursive for this that I could find)
        files = self.get_dir_contents(directory).split()
        for filename in files:
            self.delete(filename, directory)
        return spCall(self.delete_dir + [directory])


def gfal_copy(infile, outfile, maxrange=MAX_COPY_TRIES, force=False):
    header.logger.info("Copying {0} to {1}".format(infile, outfile))
    protoc = header.gfaldir.split(":")[0]
    if force:
        forcestr = "-f"
    else:
        forcestr = ""
    for i in range(maxrange):
        for protocol in PROTOCOLS: # cycle through available protocols until one works.
            infile_tmp = infile.replace(protoc, protocol)
            outfile_tmp = outfile.replace(protoc, protocol)
            header.logger.debug("Attempting Protocol {0}".format(protocol))
            cmd = "gfal-copy {f} {inf} {outf}".format(f=forcestr,
                                                      inf=infile_tmp,
                                                      outf=outfile_tmp)
            retval = spCall([cmd], shell=True)
            if retval == 0:
                return retval
        # if copying to the grid and it has failed, remove before trying again
            if retval != 0 and "file:" not in outfile:
                os.system("gfal-rm {0}".format(outfile_tmp))
    header.logger.error("Copy failed.")
    return 9999999


if __name__ == '__main__':
    tar  = TarWrap()
    grid = GridWrap()
    print("Test for Utilities.py")
    print("Running with: Python ", version_info.major)
    waitEnter = input
    if version_info.major == 2: waitEnter = raw_input
#    print("This will test access to the GRID and the tar/untar utilities")
#    print("Press [ENTER] after each test")
#    a = "TEST_FILE" ; al = [a]; b = "TEST.TAR.GZ"
#    spCall(["bash", "-c", "echo TEST > " + a])
#    print("Tar " + a + " into " + b)
#    tar.tarFiles(al, b)
#    waitEnter()
#    print("List files inside " + b)
#    print(tar.listFilesTar(b))
#    waitEnter()
#    print("Extract a from b")
#    tar.extractThese(b, al)
#    waitEnter()
#    print("Extract all from b")
#    tar.extractAll(b)
#    waitEnter()
#    spCall(["rm", a, b])
    print("--------------------------")
    print("Runcard parser:")
    runcards, dictCards, runfolder = expandCard()
    print("Runcards: ", runcards)
    print("Dictionary: ", dictCards)
    print("Runfolder: ", runfolder)
    print("Todo: test to the gridwrap")
