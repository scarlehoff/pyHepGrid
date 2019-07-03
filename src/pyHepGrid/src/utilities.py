#!/usr/bin/env python3

#
# Misc. Utilities
#
#


MAX_COPY_TRIES = 10
PROTOCOLS = ["srm", "gsiftp", "root", "xroot", "xrootd"]

###################################
def pythonVersion():
    from sys import version_info
    try:
        return version_info.major
    except:
        return 2
##################################

#
# Runcard parser
#
def expandCard(dummy=None):
    import pyHepGrid.src.header as header
    dictCard = header.dictCard
    rcards = dictCard.keys()
    return rcards, dictCard

#
# Subprocess Wrappers
#
def spCall(cmd, suppress_errors = False, shell=False):
    from subprocess import call, DEVNULL
    from pyHepGrid.src.header import logger
    if shell:
        cmd = [" ".join(cmd)]
    try:
        logger.debug(cmd)
        if not suppress_errors:
            return call(cmd, shell=shell)
        else:
            return call(cmd, stderr=DEVNULL, stdout=DEVNULL, shell=shell)
        return 0
    except:
        raise Exception("Couldn't issue the following command: ", ' '.join(cmd))
        return -1

def getOutputCall(cmd, suppress_errors = False):
    from subprocess import Popen, PIPE, DEVNULL
    from pyHepGrid.src.header import logger
    try:
        logger.debug(cmd)
        if not suppress_errors:
            outbyt = Popen(cmd, stdout = PIPE).communicate()[0]
        else:
            outbyt = Popen(cmd, stdout = PIPE, stderr=DEVNULL).communicate()[0]
        outstr = outbyt.decode("utf-8")
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
    from uuid import uuid4
    unique_name = str(uuid4().hex)
    filename = "/tmp/" + unique_name
    # better ask for forgiveness than for permission
    try:
        f = open(filename, 'w')
        f.close()
        return filename
    except:
        return unique_name


def checkIfThere(dirPath, file):
    from os import path
    if not path.exists(dirPath + "/" + file):
        return False
    else:
        return True

def generatePath(warmup):
    from pyHepGrid.src.header import finalisation_script
    from os import path, makedirs, environ
    from datetime import datetime
    # Check whether the folder already exist
    date     = datetime.now()
    month    = date.strftime("%B")
    day      = str(date.day)
    homePath = environ['HOME']
    if warmup:
        from pyHepGrid.src.header import warmup_base_dir as baseDir
    else:
        from pyHepGrid.src.header import production_base_dir as baseDir
    if baseDir is not None:
        basePath = homePath + baseDir
        monthlyPath = basePath + "/" + month
        dailyPath = monthlyPath + "/" + day
    # Only create the folder structure if we are using the "native" get_data
        from pyHepGrid.src.header import finalisation_script
        if not finalisation_script and not path.exists(dailyPath):
            print("Creating daily path at " + dailyPath)
            makedirs(dailyPath)
        return dailyPath

def sanitiseGeneratedPath(dailyPath, rname):
    i = 0
    finalname = rname + "-n0"
    while checkIfThere(dailyPath, finalname):
        i += 1
        finalname = rname + "-n" + str(i)
    finalPlacement = dailyPath + "/" + finalname
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
    import shutil, os
    import pyHepGrid.src.header as header
    import collections, re
    import json
    from pyHepGrid.src.header import logger
    lha_conf = "lhapdf-config"
    if getOutputCall(["which", lha_conf]) != "":
        logger.info("Using lhapdf-config to get lhapdf directory")
        lha_raw = getOutputCall([lha_conf, "--prefix"])
        lha_dir = lha_raw.rstrip()
    else:
        from pyHepGrid.src.header import lhapdf as lha_dir
    lhapdf = header.lhapdf_loc
    logger.info("Copying lhapdf from {0} to {1}".format(lha_dir, lhapdf))
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
        logger.info("Removing non-central scales from lhapdf")

    for root, dirs, files in os.walk(lhapdf):
        for xfile in files:
            fullpath = os.path.join(root,xfile)
            if "share" and ".dat" in fullpath and "._" not in fullpath:
                if "_0000.dat" not in fullpath and header.lhapdf_central_scale_only:
                    os.remove(fullpath)
                    continue
                elif header.lhapdf_central_scale_only:
                    prettyname = fullpath.split("/")[-1].replace("_0000.dat","")
                    logger.info("Including central PDF for {0} from {1}".format(prettyname,root))
                setname = re.sub(r"_([0-9]*).dat","",fullpath.split("/")[-1])
                member = int(re.search(r"_([0-9]*).dat",fullpath.split("/")[-1]
                                       ).group(0).replace("_","").replace(".dat",""))
                pdfs[setname].update(set([member]))


    pdf_file_name = os.path.join(os.path.dirname(os.path.realpath(__file__)),".pdfinfo")
    for key,val in pdfs.items():
        pdfs[key]=list(val)

    pdfs = dict(pdfs)
    logger.info("Writing pdf contents to .pdfinfo file")

    with open(pdf_file_name,"w") as pdf_file_obj:
        pdf_file_obj.write(json.dumps(pdfs))

    # Tar lhapdf and prepare it to be sent
    lhapdf_remote = header.lhapdf_grid_loc
    lhapdf_griddir = lhapdf_remote.rsplit("/",1)[0]
    lhapdf_gridname = lhapdf_remote.rsplit("/")[-1]
    tar_w = TarWrap()
    grid_w = GridWrap()
    tar_w.tarDir(lhapdf, lhapdf_gridname)
    size = os.path.getsize(lhapdf_gridname)/float(1<<20)
    logger.info("> LHAPDF tar size: {0:>6.3f} MB".format(size))
    if grid_w.checkForThis(lhapdf_gridname, lhapdf_griddir):
        logger.info("> Removing previous version of lhapdf in the grid")
        grid_w.delete(lhapdf_gridname, lhapdf_griddir)
    logger.info("> Sending new lhapdf to grid as {0}".format(lhapdf_gridname))
    grid_w.send(lhapdf_gridname, lhapdf_griddir)
    shutil.rmtree(lhapdf)
    os.remove(lhapdf_gridname)

#
# Tar wrappers
#

import tarfile

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
    from pyHepGrid.src.header import use_gfal, gfaldir
    # Defaults
    # Need to refactor post dpm gfal
    sendto = ["lcg-cr", "--vo", "pheno", "-l"]
    retriv = ["lcg-cp"]
    delcmd = ["lcg-del", "-a"]
    delete_dir = ["lfc-rm", "-r"]
    rename = ["lfc-rename"]
    listfi = ["lfc-ls"]
    lfn = "lfn:"
    gfal = use_gfal


    def init(self, sendto = None, retriv = None, delete = None, lfn = None):
        if sendto: self.sendto = sendto
        if retriv: self.retriv = retriv
        if delete: self.delcmd = delete
        if lfn: self.lfn = lfn

    def send(self, tarfile, whereTo, shell=False):
        import os
        from pyHepGrid.src.header import logger, gfaldir
        if self.gfal:
            what = ["file:///{0}/".format(os.getcwd()) + tarfile]
            from pyHepGrid.src.header import gsiftp
            gridname = os.path.join(gfaldir, whereTo, tarfile)
            cmd = ["gfal-copy", what[0], gridname]
        else:
            wher = [self.lfn + whereTo + "/" + tarfile]
            what = ["file:" + tarfile]
            cmd = self.sendto + wher + what
        count = 1
        while True:
            success = spCall(cmd, shell=shell)
            # Check whether we actually sent what we wanted to send
            if self.checkForThis(tarfile, whereTo):
                break
            elif count < 3: # 3 attempts before asking for input...
                logger.warning("{0} could not be copied to the grid storage /for some reason/ after {1} attempt(s)".format(tarfile,count))
                logger.info("Automatically trying again...")
            else:
                logger.warning("{0} could not be copied to the grid storage /for some reason/ after {1} attempt(s)".format(tarfile,count))
                yn = input(" Try again? (y/n) ")
                if not yn.startswith("y"):
                    logger.error("{0} was not copied to the grid storage after {1} attempt(s)".format(tarfile,count))
                    break
            count +=1
        return success

    def bring(self, tarfile, whereFrom, whereTo, shell=False, timeout = None, suppress_errors=False):
        from os import path
        from pyHepGrid.src.header import gfaldir
        if self.gfal:
            gridname = path.join(gfaldir, whereFrom, tarfile)
            destpath = "file://$PWD/{0}".format(whereTo)
            success = gfal_copy(gridname, destpath)
            # cmd = ["gfal-copy", gridname, whereTo]
            # if timeout:
            #     cmd += ["-t", str(timeout)]
            # success = spCall(cmd, shell=shell, suppress_errors=suppress_errors)
        else:
            args = [self.lfn + whereFrom + "/" + tarfile, whereTo]
            if timeout:
                args += ["--sendreceive-timeout", str(timeout)]
            success = spCall(self.retriv + args, shell=shell, suppress_errors=suppress_errors)
        # lcg-cp returns always 0 even when it fails :___
        return path.isfile(whereTo)

    def delete(self, tarfile, whereFrom):
        from pyHepGrid.src.header import gfaldir
        import os
        if self.gfal:
            gridname = os.path.join(gfaldir, whereFrom, tarfile)
            cmd = ["gfal-rm", gridname]
        else:
            args = [self.lfn + whereFrom + "/" + tarfile]
            cmd = self.delcmd + args
        return spCall(cmd)

    def checkForThis(self, filename, where):
        from pyHepGrid.src.header import gfaldir
        import os
        if self.gfal:
            gridname = os.path.join(gfaldir, where)
            cmd = ["gfal-ls", gridname]
        else:
            args = [where]
            cmd = self.listfi + args
        output = getOutputCall(cmd)
        if filename in output:
            return True
        else:
            return False

    def get_dir_contents(self, directory):
        from pyHepGrid.src.header import gfaldir
        import os
        if self.gfal:
            gridname = os.path.join(gfaldir, directory)
            cmd = ["gfal-ls", gridname]
        else:
            args = [directory]
            cmd = self.listfi + args
        output = getOutputCall(cmd)
        return output

    def delete_directory(self, directory):
        # Get contents and delete them one by one (there is no recursive for this that I could find)
        files = self.get_dir_contents(directory).split()
        for filename in files:
            self.delete(filename, directory)
        return spCall(self.delete_dir + [directory])



def gfal_copy(infile, outfile, maxrange=MAX_COPY_TRIES):
    print("Copying {0} to {1}".format(infile, outfile))
    from pyHepGrid.src.header import gfaldir
    import os
    protoc = gfaldir.split(":")[0]
    for protocol in PROTOCOLS: # cycle through available protocols until one works.
        infile_tmp = infile.replace(protoc, protocol)
        outfile_tmp = outfile.replace(protoc, protocol)
        print("Attempting Protocol {0}".format(protocol))
        for i in range(maxrange): # try max 10 times for now ;)
            cmd = "gfal-copy {0} {1}".format(infile_tmp, outfile_tmp)
            print(cmd)
            retval = os.system(cmd)
            if retval == 0:
                return retval
        # if copying to the grid and it has failed, remove before trying again
            if retval != 0 and "file" not in outfile:
                os.system("gfal-rm {0}".format(outfile_tmp))
    return 9999999



if __name__ == '__main__':
    from sys import version_info
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
