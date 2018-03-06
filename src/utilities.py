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
    import src.header as header
    rcards = []
    dictCard = header.dictCard
    for key in dictCard:
        rcards.append(key)
    if header.sockets_active: 
        dictCard["sockets_active"] = header.sockets_active
    if header.port: 
        dictCard["port"] = header.port
    return rcards, dictCard

#
# Subprocess Wrappers
# 
def spCall(cmd, suppress_errors = False):
    from subprocess import call, DEVNULL
    try:
        if not suppress_errors:
            call(cmd)
        else:
            call(cmd, stderr=DEVNULL, stdout=DEVNULL)
        return 0
    except:
        raise Exception("Couldn't issue the following command: ", ' '.join(cmd))
        return -1

def getOutputCall(cmd, suppress_errors = False):
    from subprocess import Popen, PIPE, DEVNULL
    try:
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
    from src.header import finalisation_script
    from os import path, makedirs, environ
    from datetime import datetime
    # Check whether the folder already exist
    date     = datetime.now()
    month    = date.strftime("%B")
    day      = str(date.day)
    homePath = environ['HOME']
    if warmup:
        from src.header import warmup_base_dir as baseDir
    else:
        from src.header import production_base_dir as baseDir
    if baseDir is not None:
        basePath = homePath + baseDir
        monthlyPath = basePath + "/" + month
        dailyPath = monthlyPath + "/" + day
    # Only create the folder structure if we are using the "native" get_data
        from src.header import finalisation_script
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
    import shutil, os 
    import src.header as header
    lha_conf = "lhapdf-config"
    if getOutputCall(["which", lha_conf]) != "":
        print("Using lhapdf-config to get lhapdf directory")
        lha_raw = getOutputCall([lha_conf, "--prefix"])
        lha_dir = lha_raw.rstrip()
    else:
        from src.header import lhapdf as lha_dir
    lhapdf = header.lhapdf_loc
    print("> Copying lhapdf from {0} to {1}".format(lha_dir, lhapdf))
    bring_lhapdf_pwd = ["cp", "-LR", lha_dir, lhapdf]
    spCall(bring_lhapdf_pwd)
    # Remove any unwatend directory from lhapdf
    rmdirs = header.lhapdf_ignore_dirs
    if header.lhapdf_ignore_dirs is not None:
        for root, dirs, files in os.walk(lhapdf):
            for directory in dirs:
                directory_path = os.path.join(root, directory)
                for rname in rmdirs:
                    if rname in directory_path:
                        shutil.rmtree(directory_path)
                        break
    if header.lhapdf_central_scale_only:
        print("> Removing non-central scales from lhapdf")
        for root, dirs, files in os.walk(lhapdf):
            for xfile in files:
                fullpath = os.path.join(root,xfile)
                if "share" and ".dat" in fullpath and "._" not in fullpath:
                    if "_0000.dat" not in fullpath:
                        os.remove(fullpath)
                    else:
                        prettyname = fullpath.split("/")[-1].replace("_0000.dat","")
                        print("Including central PDF for {0} from {1}".format(prettyname,root))
    # Tar lhapdf and prepare it to be sent
    lhapdf_remote = header.lhapdf_grid_loc
    lhapdf_griddir = lhapdf_remote.rsplit("/",1)[0]
    lhapdf_gridname = lhapdf_remote.rsplit("/")[-1]
    tar_w = TarWrap()
    grid_w = GridWrap()
    tar_w.tarDir(lhapdf, lhapdf_gridname)
    size = os.path.getsize(lhapdf_gridname)/float(1<<20)
    print("> LHAPDF tar size: {0:>6.3f} MB".format(size))
    if grid_w.checkForThis(lhapdf_gridname, lhapdf_griddir):
        print("> Removing previous version of lhapdf in the grid")
        grid_w.delete(lhapdf_gridname, lhapdf_griddir)
    print("> Sending new lhapdf to grid as {0}".format(lhapdf_gridname))
    grid_w.send(lhapdf_gridname, lhapdf_griddir)
    shutil.rmtree(lhapdf)
    os.remove(lhapdf_gridname)

#
# Tar wrappers
#

class TarWrap:
    # Defaults
    def init(self):
        pass
    
    def tarDir(self, inputDir, output_name):
        import tarfile
        with tarfile.open(output_name, "w:gz") as output_tar:
            output_tar.add(inputDir)
            
    def tarFiles(self, inputList, output_name):
        import tarfile
        with tarfile.open(output_name, "w:gz") as output_tar:
            for infile in inputList:
                output_tar.add(infile)

    def listFilesTar(self, tarred_file):
        import tarfile
        with tarfile.open(tarred_file, 'r|gz') as tfile:
            outp = tfile.getnames()
        return outp

    def extractThese(self, tarred_file, listFiles):
        import tarfile
        with tarfile.open(tarred_file, 'r|gz') as tfile:
            for t in tfile:
                if t.name in listFiles:
                    tfile.extract(t)

    def extractAll(self, tarred_file):
        import tarfile
        with tarfile.open(tarred_file, 'r|gz') as tfile:
            for t in tfile:
                tfile.extract(t)

    def extract_extensions(self, tarred_file, extensions):
        import tarfile
        matches = []
        with tarfile.open(tarred_file, 'r|gz') as tfile:
            for t in tfile:
                for ext in extensions:
                    if ext in t.name:
                        tfile.extract(t)
                        matches.append(t.name)
                        break
        return matches
#
# GridUtilities
# 
class GridWrap:
    # Defaults
    sendto = ["lcg-cr", "--vo", "pheno", "-l"]
    retriv = ["lcg-cp"]
    delcmd = ["lcg-del", "-a"]
    delete_dir = ["lfc-rm", "-r"]
    rename = ["lfc-rename"]
    listfi = ["lfc-ls"]
    lfn = "lfn:"
    gfal = False
    # Gfal time (testing)
#     from src.header import grid_username as username
#    gfal = True
#    lfn = "lfn://grid/pheno/{0}/".format(username)
#    sendto = ["gfal-copy", "-p"]
#    retriv = ["gfal-copy"]
#    delcmd = ["gfal-rm"]
#    delete_dir = ["gfal-rm"]
#    rename = ["gfal-rename"]
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
            from src.header import gsiftp
            today_str = datetime.today().strftime('%Y-%m-%d')
            unique_str = "ffilef" + str(generateRandom())
            file_str = today_str + "/" + unique_str
            gsiftp_wher = [gsiftp + file_str]
            cmd = self.sendto + what + gsiftp_wher + wher
        else:
            cmd = self.sendto + wher + what
            count = 1
            while True:
                success = spCall(cmd)
                # Check whether we actually sent what we wanted to send
                if self.checkForThis(tarfile, whereTo):
                    break
                elif count < 3: # 3 attempts before asking for input...
                    print("  \033[93m ERROR:\033[0m {0} could not be copied to the grid storage /for some reason/ after {1} attempt(s)".format(tarfile,count))
                    print("Automatically trying again...")
                else:
                    print("  \033[93m ERROR:\033[0m {0} could not be copied to the grid storage /for some reason/ after {1} attempt(s)".format(tarfile,count))
                    yn = input(" Try again? (y/n) ")
                    if not yn.startswith("y"):
                        break
                count +=1
        return success

    def bring(self, tarfile, whereFrom, whereTo):
        args = [self.lfn + whereFrom + "/" + tarfile, whereTo]
        success = spCall(self.retriv + args)
        # lcg-cp returns always 0 even when it fails :___
        from os import path
        return path.isfile(whereTo)

    def delete(self, tarfile, whereFrom):
        args = [self.lfn + whereFrom + "/" + tarfile]
        return spCall(self.delcmd + args)

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

    def delete_directory(self, directory):
        # Get contents and delete them one by one (there is no recursive for this that I could find)
        files = self.get_dir_contents(directory).split()
        for filename in files:
            self.delete(filename, directory)
        return spCall(self.delete_dir + [directory])
        
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





