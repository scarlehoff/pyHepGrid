import sqlite3 as dbapi

class database(object):
    def __init__(self, db, tables = None, fields = None, logger=None):
        self._setup_logger(logger)
        self.dbname = db
        self.db = dbapi.connect(db, check_same_thread=True)
        self.list_disabled = False
        if tables:
            # check whether table exists and create it othewise
            for table in tables:
                if self._is_this_table_here(table):
                    # if table does exist, check the list of tables is correct and correct it otherwise
                    self._protect_fields(table, fields)
                else:
                    self._create_table(table, fields)

    def close(self):
        self.db = None

    def reopen(self):
        self.db = dbapi.connect(self.dbname)

    def _setup_logger(self, logger):
        if logger is not None:
            self.logger = logger
        else:
            self.logger = type("", (), {})()
            self.logger.info = print
            self.logger.error = print
            self.logger.debug = print
            self.logger.critical = lambda : print

    def _protect_fields(self, table, fields):
        """ Make sure all the necessary fields exist in the table
            assumes text-type fields, but that's all we are using..."""
        old_fields = self._get_fields_in_table(table)
        new_fields = list(set(old_fields) ^ set(fields))
        for field in new_fields:
            self._insert_field_in_table(table, field, "text")

    def _execute_and_commit(self, query, verbose = False):
        """ Executes a query and commits to the database """
        if verbose:
            self.logger.debug(query)
        c = self.db.cursor()
        try:
            c.execute(query)
        except Exception as e:
            self.logger.critical("Executed query: {0}".format(query))
            raise e # For default case w/ no logger
        c.close()
        self.db.commit()

    def _execute_and_retrieve(self, query, verbose = False):
        """ Executes a query and returns the cursor """
        if verbose:
            self.logger.debug(query)
        c = self.db.cursor()
        try:
            c.execute(query)
        except Exception as e:
            self.logger.critical("Executed query: {0}".format(query))
            raise e # For default case w/ no logger
        return c

    def _create_table(self, tablename, fields):
        """ Creates a table tablename
        with fields fields
        """
        head = "create table " + tablename
        tail = "("
        self.logger.info("Creating new table: {0}".format(tablename))
        self.logger.debug(fields)
        for i in fields:
            tail += i + " text, "
        tail = tail[:-2]
        tail += ");"
        self._execute_and_commit(head + tail, verbose = True)
        return 0

    def _is_this_table_here(self, table):
        """ Checks whether table table exists"""
        query = "SELECT name FROM sqlite_master WHERE type='table' AND name='{}';".format(table)
        c = self._execute_and_retrieve(query)
        for i in c:
            c.close()
            return True
        c.close()
        return False

    def _get_fields_in_table(self, table):
        """ Return list with all the fields in the table """
        query = "pragma table_info({})".format(table)
        c = self._execute_and_retrieve(query)
        fields = [i[1] for i in c]
        c.close()
        return fields


    def _is_field_in_table(self, table, field):
        """ Check whether field exists on table"""
        query = "pragma table_info({})".format(table)
        c = self._execute_and_retrieve(query)
        for i in c:
            if field in i:
                c.close()
                return True
        c.close()
        return False

    def _insert_field_in_table(self, table, field, f_type):
        """ Inserts a field field of type f_type in the table table """
        query = "ALTER TABLE {0} ADD {1} {2}".format(table, field, f_type)
        self._execute_and_commit(query)

    def _how_many_tables(self):
        """ Returns number of tables in database """
        query = "select * from sqlite_master WHERE type='table';"
        c = self._execute_and_retrieve(query)
        k = 0
        for i in c: # Why didn't I use len(c)? Let's leave it like that for the moment...
            k += 1
        c.close()
        return k

    def set_list_disabled(self):
        """ Ignore the status field, treat all entries as active """
        self.list_disabled = True

    def insert_data(self, table, dataDict):
        """ Insert dataDict in table table """
        keys = [key for key in dataDict]
        data = [dataDict[k] for k in keys]
        head = "insert into {0} ({1})".format(table, ", ".join(keys))
        tail = "values ('{}');".format("', '".join(data))
        query = head + " " + tail
        self._execute_and_commit(query, verbose=True)

    def list_data(self, table, keys, job_id = None):
        """ List fields keys for active entries in database unless job_id is provided
        in which case only list job_id run"""
        keystr = ",".join(keys)
        if job_id:
            optional = "where rowid = {}".format(job_id)
        elif not self.list_disabled:
            optional = "where status = \"active\""
        else:
            optional = ""
        query = "select {0} from {1} {2};".format(keystr, table, optional)
        c = self._execute_and_retrieve(query)
        dataList = []
        for i in c:
            tmpDic = {}
            for (key,j) in zip(keys,i):
                tmpDic[key]= j
            dataList.append(tmpDic)
        c.close()
        return dataList

    def find_and_list(self, table, keys, find_in, find_this):
        """ List fields keys for active entries in database
        such that the find_this is found in the list of fields find_in"""
        keystr = ",".join(keys)
        if self.list_disabled:
            search_string = "where ("
        else:
            search_string = "where (status = \"active\") AND ("
        search_queries = []
        for field in find_in:
            search_queries.append("{0} like '%{1}%'".format(field, find_this))
        search_string += " OR ".join(search_queries) + ")"
        query = "select {0} from {1} {2};".format(keystr, table, search_string)
        c = self._execute_and_retrieve(query, verbose = True)
        dataList = []
        for i in c:
            tmpDic = {}
            for (key,j) in zip(keys,i):
                tmpDic[key]= j
            dataList.append(tmpDic)
        c.close()
        return dataList

    def update_entry(self, table, rowid, field, new_value):
        """ Update a given field for a given table for a given dbid! """
        query_raw = "update {0} set {1} = \"{2}\" where rowid = {3} ;"
        query = query_raw.format(table, field, new_value, rowid)
        self._execute_and_commit(query)

    def disable_entry(self, table, rowid, revert = None):
        """ Disables (or enables) rowid entry"""
        newStat = "inactive"
        if revert:
            newStat = "active"
        query = "update " + table + " set status = \"" + newStat + "\""
        rid = " where rowid = " + rowid + " ;"
        total_query = query + rid
        self._execute_and_commit(total_query, verbose = True)

def get_next_seed(dbname=None):
    from pyHepGrid.src.header import arctable, arcprodtable, diractable, slurmtable, slurmprodtable, dbfields, logger
    if dbname is None:
        from pyHepGrid.src.header import dbname
    db = database(dbname, tables = [arctable, arcprodtable, diractable, slurmtable, slurmprodtable],
                  fields=dbfields, logger=logger)
    db.list_disabled = True
    alldata = db.list_data(arctable,["iseed","jobid"])
    alldata += db.list_data(arcprodtable,["iseed","jobid"])
    alldata += db.list_data(diractable,["iseed","jobid"])
    slurmdata = db.list_data(slurmtable,["iseed","no_runs"])
    slurmdata += db.list_data(slurmprodtable,["iseed","no_runs"])
    ret_seed = 1
    for run in alldata:
        try:
            max_seed = int(run["iseed"])+len(run["jobid"].split())
        except TypeError as e:
            max_seed = ret_seed
        ret_seed = max(max_seed,ret_seed)
    for run in slurmdata:
        try:
            max_seed = int(run["iseed"])+int(run["no_runs"])
        except TypeError as e:
            max_seed = ret_seed
        ret_seed = max(max_seed,ret_seed)
    return ret_seed
