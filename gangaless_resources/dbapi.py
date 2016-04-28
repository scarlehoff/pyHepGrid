import sqlite3 as dbapi

class database:
  def __init__(self, db, table = None, fields = None):
    self.db = dbapi.connect(db)
    # (check whether table is in database)
    if self.howManyTables() < 2: self.createTable(table, fields)

  def createTable(self, tablename, fields):
    c = self.db.cursor()
    # Create table
    head = "create table " + tablename
    tail = "("
    for i in fields:
        tail += i + " text, "
    tail = tail[:-2]
    tail += ");"
    print(head + tail)
    c.execute(head + tail)
    c.close()
    self.db.commit()
    return 0

  def howManyTables(self):
    c   = self.db.cursor()
    str = "select * from sqlite_master WHERE type='table';"
    c.execute(str)
    k = 0
    for i in c:
      k += 1
    return k

  def insertData(self, table, dataDict):
    keys = [key for key in dataDict]
    data = [dataDict[k] for k in keys]
    head = "insert into " + table + "("
    for i in keys: head += i + ", "
    head  = head[:-2]
    head += ")"
    tail = "values ("
    for i in data: tail += " '" + i + "'," 
    tail  = tail[:-1]
    tail += ");"
    c = self.db.cursor()
    print(head + tail)
    c.execute(head + tail)
    c.close()
    self.db.commit()

  def listData(self, table, keys, id = None):
    keystr = ""
    for i in keys:
      keystr += i + ","
    keystr = keystr[:-1]
    if id:
      optional = " where rowid = " + id + " "
    else:
      optional = " where status = \"active\""
    str    = "select " + keystr + " from " + table + optional + ";"
    c      = self.db.cursor()
    c.execute(str)
    dataList = []
    for i in c:
      tmpDic = {}
      for (key,j) in zip(keys,i):
        tmpDic[key]= j
      dataList.append(tmpDic)
    return dataList

  def desactivateEntry(self, table, rowid, revert = None):
    c = self.db.cursor()
    newStat = "inactive"
    if revert: newStat = "active"
    str = "update " + table + " set status = \"" + newStat + "\""
    rid = " where rowid = " + rowid + " ;"
    totalstr = str + rid
    print(totalstr)
    c.execute(totalstr)
    c.close()
    self.db.commit()




