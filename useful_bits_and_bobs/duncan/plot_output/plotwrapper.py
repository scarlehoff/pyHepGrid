#!/usr/bin/env python2
import argparse as ap
try:
    from builtins import input
except ImportError as e:
    pass
import config as cf
from distutils.util import strtobool
from histplot import plot, plot_single
import itertools
import os
from parse_classes import DYNNLO, FEWZ, MCFMgnu, MCFMtop, NNLOjet, VBFNLO
import shutil

######################################################
#                        TODO                        #
######################################################


def setup(argslist, infiles):
    if argslist is None:
        return []
    argslist, taglist = get_tags(argslist)
    copyinput = []
    for arg in argslist:
        if os.path.isfile(arg):
            newfilename = os.path.join(os.getcwd(), os.path.basename(arg))
            try:
                shutil.copyfile(arg, newfilename)
            except shutil.Error as e:
                pass
            copyinput.append(newfilename)
            infiles.append(newfilename)
        else:
            olddir = filter(None, arg.split("/"))
            newdir = os.path.join(os.getcwd(), olddir[-1])
            if os.path.isdir(newdir):
                a = strtobool(input("Directory "+newdir +
                                    " already exists. Replace? [y/n]\n"))
                if a:
                    rmdir(newdir)
                    shutil.copytree(arg, newdir)
            else:
                shutil.copytree(arg, newdir)
            if newdir[-1] != "/":
                newdir += "/"
            copyinput.append(newdir)
            infiles.append(newdir)
    return zip(copyinput, taglist)


def get_tags(argslist):
    if len(argslist) <= 1:
        return argslist, ["" for i in argslist]
    else:
        tags = [""]
        for pair in zip(argslist, argslist[1:]):
            if "tag=" in pair[1]:
                tags.append(pair[1].split("=")[1])
            else:
                tags.append("")
        newtags, newargs = [], []
        for pair in zip(tags, argslist):
            if pair[0] == "":  # arg not a tag
                newargs.append(pair[1])
        for tag, nexttag in zip(tags, tags[1:]):
            if tag == "":
                if nexttag == "":
                    newtags.append("")
                else:
                    newtags.append(nexttag)
            else:
                if nexttag == "":
                    newtags.append("")
                else:
                    raise Exception("Multiple consecutive tags found.")
        try:
            assert len(newtags) == len(newargs)
        except AssertionError:
            print(newtags)
            raise
    return newargs, newtags


def remove_file_or_dir(path):
    """ param <path> could either be relative or absolute. """
    if os.path.isfile(path):
        os.remove(path)  # remove the file
    elif os.path.isdir(path):
        shutil.rmtree(path)  # remove dir and all contains


def rmdir(indir):
    """Wrapper to delete a directory iff it exists"""
    try:
        shutil.rmtree(indir)
    except OSError as e:
        if e.errno == 2:  # dir doesn't exist to delete
            pass
        else:
            raise


def teardown(infiles, args):
    for infile in infiles:
        if os.path.isfile(infile):  # File case
            path, afile = os.path.split(infile)
            shutil.copyfile(infile, os.path.join(path, cf.outputloc, afile))
        else:  # Directory case
            a = infile.split("/")
            foldername = a[-2]+"/"
            path = os.path.join("/".join(i for i in a[:-2])+"/", "")
            shutil.copytree(infile, os.path.join(path, cf.outputloc,
                                                 foldername))
    if args.dir is not None:  # Move all output to directory if specified
        shutil.move(cf.outputloc, os.path.join(os.getcwd(), args.dir[0]))
    return


def get_args():
    parser = ap.ArgumentParser(
        """General plotting routines for NNLOJET, FEWZ, DYNNLO and MCFM(gnu,top)
        file types.""",
        description="""Tag useage: For any file you wish to add a label tag to
        (e.g to differentiate different NLO,NNLO output files for the same
        program), follow its command line declaration with tag=<TAG>.

        For example, to tag an MCFM file with \"NNLO\", one would call:
        plotter -M file.gnu tag=NNLO

        Multiword tags are not allowed, as they are used in filenames, and
        spaces would mess that up.""")
    parser.add_argument("-NNLOJET", "-NNLO", "-N", nargs="*",
                        help="List of NNLOJET input dirs.")
    parser.add_argument("-MCFM", "-M", "-MCFMgnu", "-Mg", nargs="*",
                        help="List of MCFM .gnu files.")
    parser.add_argument("-FEWZ", "-F", nargs="*",
                        help="List of FEWZ input files.")
    parser.add_argument("-DYNNLO", "-DY", nargs="*",
                        help="List of DYNNLO input files.")
    parser.add_argument("-VBFNLO", "-VBF", nargs="*",
                        help="List of VBFNLO input files.")
    parser.add_argument("-MCFMtop", "-Mt", nargs="*",
                        help="List of MCFM .top files.")
    parser.add_argument('--openpdf', "-o",
                        help="""Open any generated pdf files as they are
                        created.""",
                        action="store_true")
    parser.add_argument('--debug', "-d",
                        help="""Display debug output on the terminal.
                        Mainly useful for latex output.""",
                        action="store_true")
    parser.add_argument('--dir', "-dir", "-D",
                        help="""Output directory. If not set, reverts to the
                        directory set in the config.py file.""",
                        nargs=1)
    parser.add_argument('--plot_asym', "-asym", "-a",
                        help="""Include asymmetry plots in final output.""",
                        action="store_true")
    args = parser.parse_args()

    return args


def cleandir(origstate, args):
    def strip_slash(string):
        if string.endswith("/"):
            return string[:-1]
        else:
            return string

    currstate = os.listdir(os.getcwd())
    outdir = strip_slash(cf.outputdir)

    newfiles = [i for i in currstate if i not in origstate]
    newfiles = [i for i in newfiles if i not in outdir]

    if args.dir is not None:
        argsdir = strip_slash(os.path.expanduser(args.dir[0]))
        newfiles = [i for i in newfiles if i not in argsdir]

    for rmfile in newfiles:
        remove_file_or_dir(rmfile)


if __name__ == "__main__":
    args = get_args()
    rmdir(cf.outputloc)
    os.mkdir(cf.outputloc)
    orig_dir_state = os.listdir(os.getcwd())

    infiles = []

    infilelist = [NNLOjet(x, cf.outputloc, tag=tag,
                          scale=cf.NNLOJET_scale, chan=cf.NNLOJET_chan)
                  for x, tag in setup(args.NNLOJET, infiles)]
    infilelist += [DYNNLO(x, cf.outputloc, tag=tag)
                   for x, tag in setup(args.DYNNLO, infiles)]
    infilelist += [MCFMgnu(x, cf.outputloc, tag=tag)
                   for x, tag in setup(args.MCFM, infiles)]
    infilelist += [MCFMtop(x, cf.outputloc, tag=tag)
                   for x, tag in setup(args.MCFMtop, infiles)]
    infilelist += [FEWZ(x, cf.outputloc, tag=tag)
                   for x, tag in setup(args.FEWZ, infiles)]
    infilelist += [VBFNLO(x, cf.outputloc, tag=tag)
                   for x, tag in setup(args.VBFNLO, infiles)]

    args.outputloc = cf.outputloc
    if len(infilelist) > 1:
        for idx, pair in enumerate(itertools.combinations(infilelist, 2)):
            plot(pair[0], pair[1], args)
    elif len(infilelist) == 1:
        plot_single(infilelist[0], args)
    else:
        print("No input file(s) provided.")

    ##############################
    #  space for +/- logic here  #
    ##############################
    # e.g plot_single(infilelist[0]-infilelist[1],args)

    teardown(infiles, args)
    cleandir(orig_dir_state, args)
