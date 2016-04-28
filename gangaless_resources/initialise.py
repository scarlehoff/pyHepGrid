def findLibraries(exe, gccsource):
    import subprocess as sp
    # Given an executable file, returns a list 
    # with all libraries used within the home directory
    # and their relative path inside the gccsource folder
    comm = r"ldd " + exe + " | grep so | sed -e '/^[^\t]/ d' | sed -e 's/\t//' | sed -e 's/.*=..//' | sed -e 's/ (0.*)//' | sort"
    ldd = sp.Popen(comm, stdout = sp.PIPE, shell = True)
    libsString = ldd.communicate()[0]
    libsList   = libsString.split('\n')
    returnList = []
    folderList = []
    for lib in libsList:
        if "home" and gccsource in lib:
            libName = lib.split('/')[-1]
            returnList.append(lib)
            # Relative to gcc source
            # Will not work properly if people is trying to be clever with ln -s
            folder = lib.split(gccsource)[1]
            # Remove the actual file now
            folder = folder.split(libName)[0]
            folderList.append(folder)
    return returnList, folderList


def deleteFromInput(delfile):
    from header import deleteFromGrid
    deleteFromGrid(delfile, "input")

def sendToInput(tarfile):
    from header import sendToGrid
    sendToGrid(tarfile, "input")

def updateLibraries():
    # List all necessary libraries for NNLOJET to run
    # and uploads them to the Grid under lfn:input/local.tar.gz
    from header import NNLOJETexecutable, gccdir, NNLOJETdir
    from header import spGetOutput, tarFiles
    from shutil import copytree, copy
    from os import path, makedirs
    from subprocess import call
    # LHAPDF
    lhapdfdir = spGetOutput("lhapdf-config --prefix").rstrip()
    LHAPDFfin = "LHAPDF"
    if path.exists(LHAPDFfin): call(['rm', '-rf', LHAPDFfin])
    copytree(lhapdfdir, LHAPDFfin)
    # GCC
    if path.exists('gcc'): call(['rm', '-rf', 'gcc'])
    NNLOJETexe = NNLOJETdir + "/driver/" + NNLOJETexecutable
    gcclibs,gccfolders = findLibraries(NNLOJETexe, gccdir)
    for lib, folder in zip(gcclibs, gccfolders):
        destin = 'gcc' + folder
        if not path.exists(destin):
            makedirs(destin)
        copy(lib,destin)
    namefile = "local.tar.gz"
    # Create the new tar file
    tarFiles(["LHAPDF", "gcc"], namefile)
    # Remove local.tar.gz from lfn:input if exists
    print("Removing old local.tar.gz")
    deleteFromInput(namefile)
    print("Sending the new version to Grid Storage")
    # And send the new one to the grid
    sendToInput(namefile)
    return 0

def bringGridFiles(runcard, rname):
    import subprocess as sp
    gridFiles = []
    outnm = "output" + runcard + "-w.tar.gz"
    outnm = "output" + runcard + "-warm-" + rname + ".tar.gz"
    tmpnm = "tmp.tar.gz"
    ## First bring the warmup .tar.gz
    lfncm = "lfn:warmup/" + outnm
    cmd   = ["lcg-cp", lfncm, tmpnm]
    sp.call(cmd)
    gridp = [".RRa", ".RRb", ".vRa", ".vRb", ".vBa", ".vBb"]
    ## Now list the files inside the .tar.gz and extract the grid files
    cmd = ["tar", "-tvf", "tmp.tar.gz"]
    out = sp.Popen(cmd, stdout = sp.PIPE).communicate()[0]
    outlist = out.split("\n")
    logfile = ""
    for fileRaw in outlist:
        if len(fileRaw.split(".y")) == 1: continue
        file = fileRaw.split(" ")[-1]
        if ".log" in file:
            logfile = file
        for grid in gridp:
            if grid in file: gridFiles.append(file)
    ## And now extract those particular files
    extractfiles = gridFiles + [logfile]
    cmd = ["tar", "-xzf", "tmp.tar.gz"] + extractfiles
    sp.call(cmd)
    ## Tag log file as warmup
    newlog = logfile + "-warmup"
    cmd = ["mv", logfile, newlog]
    sp.call(["rm", "tmp.tar.gz"])
    gridFiles.append(newlog)
    return gridFiles

def initialiseNNLOJET(runcard, production = None):
    print("Initialising NNLOJET")
    if ".py" in runcard:
        vessel = {}
        execfile(runcard, vessel)
        dictCard = vessel['dictCard']
        runcards = [key for key in dictCard]
    else:
        runcards = [runcard]
        dictCard = {runcard : 'N' + runcard}
    # Bring NNLOJET executable
    from shutil     import copy
    from subprocess import call
    from os         import getcwd
    from header     import NNLOJETdir, NNLOJETexecutable, tarFiles, runcardDir
    copy(NNLOJETdir + "/driver/" + NNLOJETexecutable, getcwd())
    gridFiles = []
    files = ["NNLOJET"] 
    for i in runcards:
        rname   = dictCard[i]
        copy(runcardDir + "/" + i, getcwd())
        tarfile = rname + ".tar.gz"
        filesToTar = files + [i]
        if production == "production":
            print("Fetching grid files from grid storage")
            gridFiles = bringGridFiles(i, rname)
        filesToTar += gridFiles
        tarFiles(filesToTar, tarfile)
        print("Deleting old version of " + tarfile + " from Grid Storage")
        deleteFromInput(tarfile)
        print("Sending " + tarfile + " to lfn:input/")
        sendToInput(tarfile)
        call(["rm", i, tarfile] + gridFiles)
    call(["rm"] + files)

