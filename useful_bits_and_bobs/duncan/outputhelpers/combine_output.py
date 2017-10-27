#!/usr/bin/env python
"""
Quick and dirty script for adding multiple NNLOJET output files. Usage: python combine_output.py <infile 1> <infile 2> ...  -o <outfile>
"""
from __future__ import division, print_function 
import argparse as ap
import os
import numpy as np
import re
import sys
verbose = True


def get_labels(file1):
    with open(file1) as infile:
        for i, line in enumerate(infile):
            if i == 0:
                labels = line.split()[1:]
                return labels


def combine_two_files(infile1, infile2, labels, bufferfunc, verbose):
    if verbose:
        try:
            print("> Adding files {0}, {1}".format(os.path.basename(infile1.name), 
                                                   os.path.basename(infile2.name)))
        except AttributeError as e:
            print("> Adding file {0}".format(os.path.basename(infile1.name)))
    newlines = [combine_lines(line, labels, bufferfunc) for line in zip(infile1, infile2)]
    return newlines


def combine_lines(lineab, labels, bufferfunc):
    linea, lineb = lineab
    if linea.startswith("#"):
        return linea
    else:
        tmpbuffer = ""
        asplit = linea.split()
        bsplit = lineb.split()
        absplit = zip(asplit, bsplit)
        l_absplit = list(absplit)
        assert len(l_absplit) == len(asplit)
        assert len(l_absplit) == len(bsplit)
        return bufferfunc(l_absplit, labels)
    return tmpbuffer


def addbuffers(l_absplit, labels):
    tmpbuffer = ""
    for idx, (a, b) in enumerate(l_absplit):
        if "Err" in labels[idx]:
            tmpbuffer += str(np.sqrt(float(a)
                                     ** 2 + float(b)**2))
        else:
            tmpbuffer += str(float(a) + float(b))
        tmpbuffer += "  "
    tmpbuffer += "\n"
    return tmpbuffer


def meanbuffers(l_absplit, labels):
    ignore_tags = ["_lower", "_center", "_upper"]
    tmpbuffer = ""
    for idx, (a, b) in enumerate(l_absplit):
        if "Err" in labels[idx]:
            err = (float(a),float(b))
            try:
                meanvar = 1/(1/err[0]**2+1/err[1]**2)
                mean = (val[0]/err[0]**2+val[1]/err[1]**2)*meanvar
                meanerr = np.sqrt(meanvar)
            except ZeroDivisionError as e:
                mean, meanerr = val[0], err[0]
            # try: # Disable as catching on equal values w/ different FP representations
            #     assert err[0]>=meanerr
            # except AssertionError as e:
            #     print(err[0], meanerr)
            #     raise e
            tmpbuffer += str(mean)
            tmpbuffer += "  "
            tmpbuffer += str(meanerr)
            tmpbuffer += "  "
        elif any(tag in labels[idx] for tag in ignore_tags):
            assert float(a) == float(b)
            tmpbuffer += a+ "  "
        else:
            val = (float(a),float(b))
    tmpbuffer += "\n"
    return(tmpbuffer)


def get_scale(string):
    scalesearch = re.compile('scale\d\d')
    try:
        scale = re.search(scalesearch, string).group(0)
    except AttributeError as e:
        scale = None
    istot = 'tot' in string
    iserr = 'Err' in string
    return scale, istot, iserr

def meanbuffers_fullwt(l_absplit, labels):
    ignore_tags = ["_lower", "_center", "_upper"]
    tmpbuffer = ""
    total_val_by_scale, total_err_by_scale = {}, {}

    for idx, (a, b) in enumerate(l_absplit):
        scale, istot, iserr =  get_scale(labels[idx])

        if istot:
            if iserr:
                total_err_by_scale[scale] = (float(a), float(b))
            else:
                total_val_by_scale[scale] = (float(a), float(b))

        if iserr:
            err = (float(a), float(b))
            wgt = total_err_by_scale[scale]
            try:
                meanvar = 1/(1/wgt[0]**2+1/wgt[1]**2)
                mean = (val[0]/wgt[0]**2+val[1]/wgt[1]**2)*meanvar
                meanerr = np.sqrt((err[0]/wgt[0]**2)**2+(err[1]/wgt[1]**2)**2)*meanvar
            except ZeroDivisionError as e:
                mean, meanerr = val[0], err[0]

            tmpbuffer += str(mean)
            tmpbuffer += "  "
            tmpbuffer += str(meanerr)
            tmpbuffer += "  "
        elif any(tag in labels[idx] for tag in ignore_tags):
            assert float(a) == float(b)
            tmpbuffer += a+ "  "
        else:
            val = (float(a),float(b))
    tmpbuffer += "\n"
    return(tmpbuffer)


def get_bufferfunc(average, preserve_sum):
    if average and preserve_sum:
        return meanbuffers_fullwt
    elif average:
        return meanbuffers
    else:
        return addbuffers
    

def write_output(resfile, newlines):
    with open(resfile, 'w') as outfile:
        print("> Writing to file: {0}".format(os.path.basename(resfile)))
        outfile.writelines(newlines)


def combine_all_files(addfiles, labels, average, preserve_sum, verbose):
    bufferfunc = get_bufferfunc(average, preserve_sum)

    with open(addfiles[0]) as x:
        with open(addfiles[1]) as y:
            running_total = combine_two_files(x, y, labels, bufferfunc, verbose)

    if len(addfiles)>2:
        for i in addfiles[2:]:
            with open(i) as infile:
                running_total = combine_two_files(infile, running_total, labels,
                                                  bufferfunc, verbose)
    return running_total


def setup_parser():
    parser = ap.ArgumentParser(
        description='Add a number of NNLOJET output files.')
    parser.add_argument('--output', "-o", nargs=1,
                        help='The output file containing the combined results', required=True)
    parser.add_argument('infiles', metavar='<Input file>', nargs='+',
                        help='The input files to combine.')
    parser.add_argument('-av', '--average', action = 'store_true',
			help='average output after summation')
    parser.add_argument('-p', '--preserve_sum', action = 'store_true', default = False,
			help='''preserve the that the channel Sum == Total by using the error on the total cross section as the weight for a partonic channel as opposed to the error on the partonic channel itself.''')
    args = parser.parse_args()

    if len(args.infiles) == 1:
        print("Only one input file specified: {0}.".format(args.infiles[0]))
        print("Exiting...")
        sys.exit()
    elif len(args.infiles) <1 :
        print("No input files specified.")
        print("Exiting...")
        sys.exit()
    return args


def combine_output_API(resfile, infiles, average, preserve_sum, verbose):
    addfiles = infiles
    labels = get_labels(infiles[0])
    newlines = combine_all_files(addfiles, labels, average, preserve_sum, verbose)
    write_output(resfile, newlines)


if __name__ == "__main__":
    args = setup_parser()
    resfile = args.output[0]
    addfiles = args.infiles
    labels = get_labels(args.infiles[0])
    newlines = combine_all_files(addfiles, labels, args.average, args.preserve_sum, verbose)
    write_output(resfile, newlines)
