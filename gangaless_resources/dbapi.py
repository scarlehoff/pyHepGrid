import sqlite3 as dbapi

class database(object):
    def __init__(self, db, tables = None, fields = None):
        self.db = dbapi.connect(db)
        if tables: # check whether table exists and create it othewise
            for table in tables:
                if self._is_this_table_here(table):
                    pass
                else:
                    self._create_table(table, fields)

    def _protect_jobtype(self, keys, table):
        """ Wrapper function for a change that was one
        on 25/10/2017 to add a new field job_type 
        to the database"""
        if "jobtype" in keys:
            if not self._is_field_in_table(table, "jobtype"):
                print("Updating database: table {} with jobtype".format(table))
                self._insert_field_in_table(table, "jobtype", "text")

    def _execute_and_commit(self, query, verbose = False):
        """ Executes a query and commits to the database """
        if verbose:
            print(query)
        c = self.db.cursor()
        c.execute(query)
        c.close()
        self.db.commit()

    def _execute_and_retrieve(self, query, verbose = False):
        """ Executes a query and returns the cursor """
        if verbose:
            print(query)
        c = self.db.cursor()
        c.execute(query)
        return c

    def _create_table(self, tablename, fields):
        """ Creates a table tablename
        with fields fields
        """
        head = "create table " + tablename
        tail = "("
        print(fields)
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
        return False
    
    def _is_field_in_table(self, table, field):
        """ Check whether field exists on table"""
        query = "pragma table_info({})".format(table)
        c = self._execute_and_retrieve(query)
        for i in c:
            if field in i:
                return True
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
        return k

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
        self._protect_jobtype(keys, table)
        keystr = ",".join(keys)
        if job_id:
            optional = "where rowid = {}".format(job_id)
        else:
            optional = "where status = \"active\""
        query = "select {0} from {1} {2};".format(keystr, table, optional)
        c = self._execute_and_retrieve(query)
        dataList = []
        for i in c:
            tmpDic = {}
            for (key,j) in zip(keys,i):
                tmpDic[key]= j
            dataList.append(tmpDic)
        return dataList

    def find_and_list(self, table, keys, find_in, find_this):
        """ List fields keys for active entries in database
        such that the find_this is found in the list of fields find_in"""
        self._protect_jobtype(keys, table)
        keystr = ",".join(keys)
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
        return dataList

    def disable_entry(self, table, rowid, revert = None):
        """ Disables (or enables) rowid entry"""
        c = self.db.cursor()
        newStat = "inactive"
        if revert: 
            newStat = "active"
        query = "update " + table + " set status = \"" + newStat + "\""
        rid = " where rowid = " + rowid + " ;"
        total_query = query + rid
        self._execute_and_commit(total_query, verbose = True)




