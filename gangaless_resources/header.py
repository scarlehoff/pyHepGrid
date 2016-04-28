### User parameters
##
## HARDCODED STUFF IN ARC.PY DIRAC.PY:
##  1) LFN is set to pheno/jmartinez por defecto

gccdir     = "/mt/home/jmartinez/LIBRARIES/gcc-5.2.0"
NNLOJETdir = "/mt/home/jmartinez/NNLOJET/"
runcardDir = "/mt/home/jmartinez/NNLOJET/driver/runcardPS/"#/runcardsGrid/tests"
lfndir     = "/grid/pheno/jmartinez"
warmupthr  = 16
producrun  = 1000
arcbase    = "/mt/home/jmartinez/.arc/jobs.dat" # arc database
dbname     = 'Juan_Jobs.dat' # database used by this program

### NNLOJET Parameters
NNLOJETexecutable = "NNLOJET"
baseseed          = 400

### Helping functions
def spGetOutput(cmdargs):
    import subprocess as sp
    stripp = cmdargs.split(' ', 1)
    cmd    = stripp[0]
    args   = stripp[1]
    ans    = sp.Popen([cmd, args], stdout = sp.PIPE)
    output = ans.communicate()[0]
    return output

def expandCard(runcard, dicruns = None):
    rcards = []
    if ".py" in runcard:
        vessel = {}
        execfile(runcard, vessel)
        dictCard = vessel['dictCard']
        for key in dictCard:
            rcards.append(key)
    else:
        rcards.append(runcard)
        dictCard = {}
    return rcards, dictCard

def tarFiles(inputList, outputName):
    from subprocess import call
    cmdbase = ["tar"]
    args    = ["-czf", outputName]
    args   += inputList
    cmd     = cmdbase + args
    try:
        call(cmd)
        return 0
    except:
        print("Couldn't tar the given files into " + outputName)
        return -1

def sendToGrid(tarfile, whereTo):
    from subprocess import call
    # Send any file to lfn:input/file
    cmdbase = ["lcg-cr"]
    args    = ["--vo", "pheno", "-l", "lfn:" + whereTo + "/" + tarfile, "file:" + tarfile]
    cmd     = cmdbase + args
    try:
        call(cmd)
        return 0
    except:
        raise Exception("Couldn't send %s to Grid Storage" % tarfile)

def bringFromGrid(tarfile, whereFrom, whereTo):
    from subprocess import call
    cmdbase = ["lcg-cp"]
    args    = ["lfn:" + whereFrom + "/" + tarfile, whereTo]
    try:
        call(cmdbase + args)
        return 0
    except:
        raise Exception("Couldn't bring %s from Grid Storage" % tarfile)

def deleteFromGrid(delfile, whereFrom):
    from subprocess import call
    cmdbase = ["lcg-del"]
    args    = ["-a", "lfn:" + whereFrom + "/" + delfile]
    cmd     = cmdbase + args
    try:
        call(cmd)
        return 0
    except:
        raise Exception("Couldn't remove %s from Grid Storage" % delfile)
