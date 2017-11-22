#!/usr/bin/env python3

#
# Misc. Utilities
#
#

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
def expandCard(runcard, dicRuns = None):
    rcards = []
    if ".py" in runcard:
        vessel = {}
        if pythonVersion() == 3:
            with open(runcard) as source_file:
               exec(source_file.read(), vessel)
        else:
            execfile(runcard, vessel) 
        dictCard = vessel['dictCard']
        for key in dictCard:
            rcards.append(key)
        # If more runcard-only variables are defined, they will be added to dictCard
        if "sockets_active" in vessel: 
            dictCard["sockets_active"] = vessel["sockets_active"]
        if "port" in vessel: 
            dictCard["port"] = vessel["port"]
    else:
        rcards.append(runcard)
        dictCard = {}
    return rcards, dictCard

#
# Subprocess Wrappers
# 
def spCall(cmd):
    from subprocess import call
    try:
        call(cmd)
        return 0
    except:
        raise Exception("Couldn't issue the following command: ", ' '.join(cmd))
        return -1

def getOutputCall(cmd):
    from subprocess import Popen, PIPE
    try:
        outbyt = Popen(cmd, stdout = PIPE).communicate()[0]
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
    from header import finalisation_script
    from os import path, makedirs, environ
    from datetime import datetime
    # Check whether the folder already exist
    date     = datetime.now()
    month    = date.strftime("%B")
    day      = str(date.day)
    homePath = environ['HOME']
    if warmup:
        from header import warmup_base_dir as baseDir
    else:
        from header import production_base_dir as baseDir
    if baseDir is not None:
        basePath = homePath + baseDir
        monthlyPath = basePath + "/" + month
        dailyPath = monthlyPath + "/" + day
    # Only create the folder structure if we are using the "native" get_data
        from header import finalisation_script
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
# Library initialisation
#
def lhapdfIni():
    import shutil, os, header
    lha_conf = "lhapdf-config"
    if getOutputCall(["which", lha_conf]) != "":
        print("Using lhapdf-config to get lhapdf directory")
        lha_raw = getOutputCall([lha_conf, "--prefix"])
        lha_dir = lha_raw.rstrip()
    else:
        from header import lhapdf as lha_dir
    lhapdf = header.lhapdf_loc
    print("Copying lhapdf from {0} to {1}".format(lha_dir, lhapdf))
    bring_lhapdf_pwd = ["cp", "-LR", lha_dir, lhapdf]
    spCall(bring_lhapdf_pwd)
    # Remove any unwatend directory from lhapdf
    rmdirs = header.lhapdf_ignore_dirs
    for root, dirs, files in os.walk(lhapdf):
        for directory in dirs:
            directory_path = os.path.join(root, directory)
            for rname in rmdirs:
                if rname in directory_path:
                    shutil.rmtree(directory_path)
                    break
    # Tar lhapdf and prepare it to be sent
    lhapdf_remote = header.lhapdf_grid_loc
    lhapdf_griddir = lhapdf_remote.rsplit("/",1)[0]
    lhapdf_gridname = lhapdf_remote.rsplit("/")[-1]
    tar_w = TarWrap()
    grid_w = GridWrap()
    tar_w.tarDir(lhapdf, lhapdf_gridname)
    if grid_w.checkForThis(lhapdf_gridname, lhapdf_griddir):
        print("Removing previous version of lhapdf in the grid")
        grid_w.delete(lhapdf_gridname, lhapdf_griddir)
    grid_w.send(lhapdf_gridname, lhapdf_griddir)
    shutil.rmtree(lhapdf)
    os.remove(lhapdf_gridname)

#
# Tar wrappers
#

class TarWrap:
    # Defaults
    cmdbase = ["tar"]
    targz   = "-czf"
    tarlist = "-tvf"
    untar   = "-xzf"
    def init(self, cmdbase = None, targz = None, tarlist = None, untar = None):
        if cmdbase: self.cmdbase = cmdbase
        if targz:   self.targz   = targz
        if tarlist: self.tarlist = tarlist
        if untar:   self.untar   = untar
    
    def tarDir(self, inputDir, output_name):
        args = [self.targz, output_name, inputDir]
        cmd  = self.cmdbase + args
        spCall(cmd)

    def tarFiles(self, inputList, output_name):
        args = [self.targz, output_name] +  inputList
        cmd  = self.cmdbase + args
        spCall(cmd)

    def listFilesTar(self, tarfile):
        args = [self.tarlist, tarfile]
        outp = getOutputCall(self.cmdbase + args).split('\n')
        return outp

    def extractThese(self, tarfile, listFiles):
        args = [self.untar, tarfile] + listFiles
        spCall(self.cmdbase + args)

    def extractAll(self, tarfile):
        args = [self.untar, tarfile]
        spCall(self.cmdbase + args)

#
# GridUtilities
# 
class GridWrap:
    from header import grid_username as username
    # Defaults
    sendto = ["lcg-cr", "--vo", "pheno", "-l"]
    retriv = ["lcg-cp"]
    delcmd = ["lcg-del", "-a"]
    listfi = ["lfc-ls"]
    lfn = "lfn:"
    gfal = False
    # Gfal time
#    gfal = True
#    lfn = "lfn://grid/pheno/{0}/".format(username)
#    sendto = ["gfal-copy", "-p"]
#    retriv = ["gfal-copy"]
#    delcmd = ["gfal-rm"]
#    listfi = ["gfal-ls"]
    def init(self, sendto = None, retriv = None, delete = None, lfn = None):
        if sendto: self.sendto = sendto
        if retriv: self.retriv = retriv
        if delete: self.delcmd = delete
        if lfn: self.lfn = lfn
    
    def send(self, tarfile, whereTo):
        wher = [self.lfn + whereTo + "/" + tarfile]
        what = ["file:" + tarfile]
        if self.gfal:
            from datetime import datetime
            from uuid import uuid1 as generateRandom
            from header import gsiftp
            today_str = datetime.today().strftime('%Y-%m-%d')
            unique_str = "ffilef" + str(generateRandom())
            file_str = today_str + "/" + unique_str
            gsiftp_wher = [gsiftp + file_str]
            cmd = self.sendto + what + gsiftp_wher + wher
        else:
            cmd = self.sendto + wher + what
        spCall(cmd)

    def bring(self, tarfile, whereFrom, whereTo):
        args = [self.lfn + whereFrom + "/" + tarfile, whereTo]
        spCall(self.retriv + args)

    def delete(self, tarfile, whereFrom):
        args = [self.lfn + whereFrom + "/" + tarfile]
        spCall(self.delcmd + args)

    def checkForThis(self, filename, where):
        if self.gfal:
            args = [self.lfn + where]
        else:
            args = [where]
        cmd = self.listfi + args
        output = getOutputCall(cmd)
        if filename in output:
            return True
        else:
            return False

    def get_dir_contents(self, directory):
        if self.gfal:
            args = [self.lfn + directory]
        else:
            args = [directory]
        cmd = self.listfi + args
        output = getOutputCall(cmd)
        return output



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
    runcards, dictCards, runfolder = expandCard("runcard.py")
    print("Runcards: ", runcards)
    print("Dictionary: ", dictCards)
    print("Runfolder: ", runfolder)
    print("Todo: test to the gridwrap")





