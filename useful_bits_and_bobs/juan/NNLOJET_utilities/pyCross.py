#!/usr/bin/env python

from argparse import ArgumentParser
import numpy as np
parser = ArgumentParser()
parser.add_argument("cross", help = "Cross section .dat file") 
parser.add_argument("runcard", help = "Runcard or logfile for the .dat file", nargs='?', default = None)
parser.add_argument("-n", "--nscales", default = None, help = "Number of scales")
parser.add_argument("-p", "--pyscale", default = None, help = "pyScale-like output for a given chan")
args = parser.parse_args()

a = None

qQ   =  'q   Q'
qbQ  =  'qb  Q'
qQb  =  'q   Qb'
qbQb =  'qb  Qb'
qg   =  'q   g'
gq   =  'g   q'
qbg  =  'qb  g'
gqb  =  'g   qb'
gg   =  'g   g'
tot  =  'Total'

if args.pyscale:
    a = args.pyscale
    if a ==  'qQ'  or a ==  'qq':  a =  qQ
    if a ==  'qbQ' or a ==  'qbq': a =  qbQ
    if a ==  'qQb' or a ==  'qqb': a =  qQb
    if a == 'qbQb' or a == 'qbqb': a =  qbQb
    if a ==  'gQ'  or a ==  'gq':  a =  gq
    if a ==  'gQb' or a ==  'gqb': a =  gqb
    if a ==  'qg':                 a =  qg
    if a ==  'qbg':                a =  qbg
    if a ==  'gg':                 a =  gg
    if a[:2] == 'to' or a[:2] == 'To': a = tot
    pyscal = []


channelsRead = [
        tot , qbQb , gqb , qQb , qbg ,
        gg  , qg   , qbQ , gq  , qQ
        ]

channelsWrite = [
        qQ , qbQ , qQb , qbQb , gg,
        qg , gq  , qbg , gqb  , tot
        ]

def labelErr(label):
    return label + "error"

def unPack(filename, scales = 1):
    comment   =  '#'
    dat       =  np.loadtxt(filename, comments =  comment, unpack =  True)
    total_res = []
    for i in range(scales):
        tmp = {}
        j   = 2*i
        for chan in channelsRead:
            x              =  dat[j]
            dx             =  dat[j + 1]
            j             +=  2*scales
            label          =  chan
            label_err      =  labelErr(chan)
            tmp[label]     =  x
            tmp[label_err] =  dx
        total_res.append(tmp)
    return total_res

def printPretty(dictionary):
    for chan in channelsWrite:
        label_err = labelErr(chan)
        result    = str(dictionary[chan]).center(12)
        result_er = str(dictionary[label_err]).center(12)
        string    = "   " + chan.center(6) + ": " + result + " +/- " + result_er + " fb"
        print(string)

def parseRuncard(runcard):
    with open(runcard,'r') as f:
        scalflag = False
        nscales  = 0
        data     = []
        for i in f:
            if 'END_SCALES' in i:
                return nscales, data
            if scalflag:
                line = i.strip()
                linlist = line.strip().replace(' mu','=mu').replace(' ','').split('=')
                muf = "0"
                mur = "0"
                fo  = False
                if linlist[0] == 'muf':
                    muf = linlist[1].replace('d0','')
                    mur = linlist[3].replace('d0','')
                    fo  = True
                elif linlist[0] == 'mur':
                    muf = linlist[3].replace('d0','')
                    mur = linlist[1].replace('d0','')
                    fo  = True
                if fo:
                    nscales += 1
                    data.append([mur,muf])
            if 'SCALES' in i:
                scalflag = True

if args.nscales:
    nscl  = int(args.nscales)
else:
    # Try to figure out the number of scales for yourself later
    nscl = -1

runcard = args.runcard
if runcard:
    nscl, scales = parseRuncard(runcard)


if nscl == -1:
    firstLine = open(args.cross).readline().rstrip()
    nscl      = int(int(firstLine.count("tot_scale"))/2)
    print("Scales found: " + str(nscl))
total = unPack(args.cross, nscl)
j     = 0
for i in total[:nscl]:
    print("--------")
    if runcard:
        scale = scales[j]
        muR   = "muR = " + scale[0]
        muF   = "muF = " + scale[1]
        print(muR + "  " + muF)
        if a:
            string = scale[0].center(6) + " : " + "[" + str(i[a]) + ", " + str(i[labelErr(a)]) + "],"
            pyscal.append(string)
    printPretty(i)
    print("\n")
    j += 1

if a:
    for i in pyscal:
        print("   " + i)
    print("\n")
