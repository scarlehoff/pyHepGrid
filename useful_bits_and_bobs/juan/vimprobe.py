#!/usr/bin/env python

from subprocess import check_output
from os import remove
from argparse import ArgumentParser
from sys import exit

import sqlite3 as dbapi
import pdb

try:
    from vimprobe_config import dbname, targetFolder, repositoryCMD
except:
    dbname        = "/home/jumax9/Lairen_hg/test.db"
    targetFolder  = "/home/jumax9/Coding/"
    repositoryCMD = "hg files"
    repositoryCMD = None

class basedatos:
    def __init__(self, datab):
        self.db = dbapi.connect(datab)

    def createTable(self):
        stquery  = "create table locatedb"
        stquery += " (id INTEGER PRIMARY KEY, "
        stquery += "function text, "
        stquery += "path text, "
        stquery += "line text) "
        self.executeAndCommitDB(stquery)

    def executeAndCommitDB(self, stringRaw, options = None):
        c = self.db.cursor()
        string = stringRaw.lower()
        try:
            if options:
                c.execute(string, options)
            else:
                c.execute(string)
        except:
            print("Couldn't execute sqlite query")
            print(string)
            pdb.set_trace()
        c.close()
        self.db.commit()

    def insertList(self, targetList):
        head  = "insert into locatedb (function, path, line) "
        head += "values (?, ?, ?)"
        for i in targetList:
            self.executeAndCommitDB(head, i)
        return "Data inserted at locatedb"

    def searchRecord(self, functionName):
        functionName = functionName.lower()
        c = self.db.cursor()
        stquery  = "select path from locatedb where function "
        stquery += "== '" + functionName + "'"
        c.execute(stquery)
        for i in c:
            print(i[0])
        c.close()

def grepForThis(target, folder):
    cmd           = ["grep", "-r", "-n", "-I", "-i", folder, "-e", target]
    output        = check_output(cmd).decode('ascii').splitlines()
    filesAccepted = []
    repoCheck     = False
    if repositoryCMD:
        cmdtmp    = ["bash", "-c", "cd " + targetFolder + " && " + repositoryCMD]
        resulttmp = check_output(cmdtmp).splitlines()
        repoCheck = True
        for fileR in resulttmp: filesAccepted.append(fileR.split("/")[-1])
    dataList = []
    for line in output:
        # Is this actually the _end_ of the subroutine/function?
        endTarget = "end " + target
        if endTarget in line:
            continue
        # Any comments in the line?
        if "!" in line:
            lineSane = line.split("!")[0]
            lineSane = lineSane.split(target)
        else:
            lineSane = line.split(target)
        if lineSane[0] == "c":
            continue
        functionName = lineSane[-1].strip()
        filePathLine = lineSane[0].strip()
        # Strip extra stuff from function name
        cleanFunction = functionName.split("(")[0].lower()
        # Get file name and line number
        filePath = filePathLine.split(":")[0]
        if filePath[-1] != "f" and filePath[-3:] != "f90":
            # Not a fortran file anyway, jump
            continue
        fileRaw = filePath.split("/")[-1]
        if repoCheck and fileRaw not in filesAccepted: continue
        lineNumb = filePathLine.split(":")[1]
        dataList.append([cleanFunction, filePath, lineNumb])
    return dataList

parser = ArgumentParser()
parser.add_argument("function", help = "Outputs the path for the file where [function] is located", nargs = '?')
parser.add_argument("-u", "--update", help = "Updates the database (removes old one and creates a new one)", action = "store_true")
parser.add_argument("-v", "--vim", help = "Prints the necessary lines to write at the end of .vimrc", action = "store_true")
args = parser.parse_args()

if args.function:
    db = basedatos(dbname)
    db.searchRecord(args.function)
elif args.update:
    yn = input("This will remove the current database, do you want to continue? (y/n) ")
    if yn != "y": exit(0)
    try:
        remove(dbname)	
    except:
        pass
    db = basedatos(dbname)
    db.createTable()
    dataList = grepForThis("function", targetFolder)
    db.insertList(dataList)
    dataList = grepForThis("subroutine", targetFolder)
    db.insertList(dataList)
elif args.vim:
    print("Write the following lines into .vimrc")
    print("-------------------------------------")
    print("""
nmap gf :call GoToFunction()<cR>
function! GoToFunction()
        " Modified from Barry's .vimrc
        " <cword> = word under the cursor
        "set iskeyword+=.
        let selWord   = shellescape(expand("<cword>"))
        let pythoncmd = 'PATH_TO_vimprobe.py '.selWord

        let l:list      = system(pythoncmd)
        let l:listFiles = split(l:list, "\\n")

        " Hopefully we only found one
        " but we might not be that lucky
        let l:num = len(listFiles)
        let l:i   = 1
        let l:str = ""
        while l:i <= l:num
                let l:str = l:str . l:i . " " . l:listFiles[l:i-1] . "\\n"
                let l:i   = l:i + 1
        endwhile

        " Now select file
        if l:num < 1
                echo "No file found for ".selWord
                return
        elseif l:num != 1
                echo l:str
                let l:input=input("Which?\\n")
                if strlen(l:input)==0
                        return
                endif
                if strlen(substitute(l:input, "[0-9]", "", "g"))>0
                        echo "Not a number"
                        return
                endif
                if l:input <1 || l:input>l:num
                        echo "Fuck off"
                        return
                endif
                let l:line = l:listFiles[l:input-1]
        else
                let l:line = l:list
        endif

        " And now open a new tab!
        execute "tabe ".l:line
endfunction
""")
    print("----------------------------------")
    print("Remember to modifiy PATH_TO_vimprobe.py with the full path to this script")
else:
    print("No argument recognised")
