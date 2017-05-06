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
        folder   = vessel['runcardDir']
        dictCard = vessel['dictCard']
        for key in dictCard:
            rcards.append(key)
        # If more variables are defined, they will be added to dictCard
        if "NNLOJETdir" in vessel: dictCard["NNLOJETdir"] = vessel["NNLOJETdir"]
    else:
        rcards.append(runcard)
        dictCard = {}
    return rcards, dictCard, folder

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
def checkIfThere(dirPath, file):
    from os import path
    if not path.exists(dirPath + "/" + file):
        return False
    else:
        return True

def generatePath(warmup):
    from os import path, makedirs, environ
    from datetime import datetime
    # Check whether the folder already exist
    date     = datetime.now()
    month    = date.strftime("%B")
    day      = str(date.day)
    homePath = environ['HOME']
    if warmup:
        baseDir = "/WarmupsRunGrids"
    else:
        baseDir = "/ResultsRunGrids"
    basePath = homePath + baseDir
    # And create it if it doesn't
    if not path.exists(basePath):
        print("Creating the basepath at " + basePath)
        makedirs(basePath)
    monthlyPath = basePath + "/" + month
    if not path.exists(monthlyPath):
        print("Creating monthly path at " + monthlyPath)
        makedirs(monthlyPath)
    dailyPath = monthlyPath + "/" + day
    if not path.exists(dailyPath):
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
    lhaConf = "lhapdf-config"
    testBin = ["which", lhaConf]
    tarw    = TarWrap()
    gridw   = GridWrap()
    outputn = "lhapdf.tar.gz"
    if getOutputCall(testBin) != "":
        lhPath = [lhaConf, "--prefix"]
        lhaRaw = getOutputCall(lhPath)
        lhaDir = lhaRaw.rstrip()
    else:
        from header import lhapdf as lhaDir
    # Bring lhapdf and create tar
    lhapdf      = "lhapdf"
    bringLhapdf = ["cp", "-LR", lhaDir, lhapdf]
    spCall(bringLhapdf)
    tarw.tarDir(lhapdf, outputn)
    # Send to grid util
    ginput = "util"
    if gridw.checkForThis(outputn, ginput): gridw.delete(outputn, ginput)
    gridw.send(outputn, ginput)
    # This is better than doing rm -rf and it will be removed in due time anyway
    movetotmp   = ["mv", "-f", lhapdf, "/tmp/"]
    spCall(movetotmp)
    movetotmp   = ["mv", "-f", outputn, "/tmp/"]
    spCall(movetotmp)

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
    
    def tarDir(self, inputDir, outputName):
        args = [self.targz, outputName, inputDir]
        cmd  = self.cmdbase + args
        spCall(cmd)

    def tarFiles(self, inputList, outputName):
        args = [self.targz, outputName] +  inputList
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
    from header import username
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





