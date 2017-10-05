class Backend(object):
    cDONE = 0
    cWAIT = 1
    cRUN = 2
    cFAIL = -1
    cUNK = 99

    def __init__(self):
        from utilities import TarWrap, GridWrap
        from header import dbname, baseSeed
        import dbapi
        self.tarw  = TarWrap()
        self.gridw = GridWrap()
        self.dbase = dbapi.database(dbname)
        self.table = None
        self.bSeed = baseSeed

    # Sigh
    def input(self, string):
        try:
            if version_info.major == 2: 
                return raw_input(string)
            else:
                return input(string)
        except:
            return raw_input(string)

    def multiRun(self, function, arguments, threads = 5):
        from multiprocessing.dummy import Pool as ThreadPool
        pool   = ThreadPool(threads)
        result = pool.map(function, arguments)
        pool.close()
        return result

    def dbList(self, fields):
        return self.dbase.listData(self.table, fields)

    # If any of the "naming" function changes
    # they need to be changed as well at ARC.py/DIRAC.py
    def warmupName(self, runcard, rname):
        out = "output" + runcard + "-warm-" + rname + ".tar.gz"
        return out

    def outputName(self, runcard, rname, seed):
        out = "output" + runcard + "-" + rname + "-" + str(seed) + ".tar.gz"
        return out

    def getId(self, db_id):
        jobid = self.dbase.listData(self.table, ["jobid"], db_id)
        try:
            idout = jobid[0]['jobid']
        except IndexError:
            print("Selected job is %s out of bounds", jobid)
            idt   = self.input("> Select id to act upon: ")
            idout = self.getId(idt)
        return idout

    def desactivateJob(self, db_id):
        self.dbase.desactivateEntry(self.table, db_id)
        return 0

    def reactivateJob(self, db_id):
        self.dbase.desactivateEntry(self.table, db_id, revert = True)
        return 0
#
# Management
#
    def listRuns(self):
        fields = ["rowid", "jobid", "runcard", "runfolder", "date"]
        dictC  = self.dbList(fields)
        print("Active runs: " + str(len(dictC)))
        print("id".center(5) + " | " + "runcard".center(22) + " | " + "runname".center(25) + " |" +  "date".center(20))
        for i in dictC:
            rid = str(i['rowid']).center(5)
            ruc = str(i['runcard']).center(22)
            run = str(i['runfolder']).center(25)
            dat = str(i['date']).split('.')[0]
            dat = dat.center(20)
            print(rid + " | " + ruc + " | " + run + " | " + dat)


