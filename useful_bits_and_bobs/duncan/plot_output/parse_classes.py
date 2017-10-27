from label_maps import DYNNLO_label_lookup, FEWZ_label_lookup,\
    MCFM_label_lookup, VBFNLO_label_lookup
import config as cf
from histclasses import NNLOJEThistogram, FEWZhistogram, Histogram
import numpy as np
import os
import sys

######################################################
#                 INHERITED CLASSES                  #
######################################################


class Outputfile:
    def __init__(self, pltloc, folder):
        self.pltloc = pltloc
        self.fileloc = os.path.join(folder, '')

    def __repr__(self):
        outstring = self.name + "\n"
        outstring += "Histograms: "
        for i in self.histograms:
            outstring += i.label + " "
        outstring += "\nHistograms Filelist: "
        for i in self.filelist:
            outstring += i + " "
        outstring += "\nLocation: " + self.fileloc
        return(outstring)

    def str_float(self, a, formatstr=""):
        try:
            if formatstr == "":
                return str(float(a))
            else:
                return str(formatstr.format(float(a)))
        except ValueError as e:
            return "-"

    def __sub__(self, ofile2):
        name = self.name + "-" + ofile2.name
        fl = self.fileloc
        description = name + " output class"
        achan = sorted(self.channels)
        bchan = sorted(ofile2.channels)
        channels = []
        for i in zip(achan, bchan):
            chan_name = i[0][0]
            chan_val = str(float(i[0][1]) - float(i[1][1]))
            chan_err = self.__adderrs(
                chan_val, i[0][1], i[1][1], i[0][2], i[1][2])
            channels.append([chan_name, chan_val, chan_err])
        sigval = str(float(self.sig[0]) - float(ofile2.sig[0]))
        sig = [sigval, self.__adderrs(
            sigval, self.sig[0], ofile2.sig[0], self.sig[1], ofile2.sig[1])]
        hists = []
        for i in self.histograms:
            for j in ofile2.histograms:
                if i.label == j.label:
                    hists.append(i - j)
                    continue
        return premade_outfile(fl, channels, sig, hists, name, description)

    def __add__(self, ofile2):
        name = self.name + "+" + ofile2.name
        fl = self.fileloc
        description = name + " output class"
        achan = sorted(self.channels)
        bchan = sorted(ofile2.channels)
        channels = []
        for i in zip(achan, bchan):
            chan_name = i[0][0]
            chan_val = str(float(i[0][1]) - float(i[1][1]))
            chan_err = self.__adderrs(
                chan_val, i[0][1], i[1][1], i[0][2], i[1][2])
            channels.append([chan_name, chan_val, chan_err])
        sigval = str(float(self.sig[0]) + float(ofile2.sig[0]))
        sig = [sigval, self.__adderrs(
            sigval, self.sig[0], ofile2.sig[0], self.sig[1], ofile2.sig[1])]
        hists = []
        for i in self.histograms:
            for j in ofile2.histograms:
                if i.label == j.label:
                    hists.append(i + j)
                    continue
        return premade_outfile(fl, channels, sig, hists, name, description)

    def __adderrs(self, newval, val1, val2, err1, err2):
        return str(np.sqrt((float(err1))**2 + (float(err2))**2))

    def write_channel(self, outfile, chan):
        """generates channel output for the multiple outputfile table"""
        x = chan[1:]
        outfile.write(self.str_float(x[0]) + " & ")
        if len(x) == 3:
            outfile.write("$\\pm$" + self.str_float(x[1], "{0:.3f}") + " & ")
        outfile.write(self.str_float(x[-1], "{0:.2f}") + "\% & ")

    def write_tot_sig(self, outfile):
        """generates total output for the multiple outputfile table"""
        x = len(self.channels[0]) - 1
        outfile.write(self.str_float(self.sig[0]))
        outfile.write(" & $\\pm$" + self.str_float(self.sig[1]) + " & ")
        if x == 3:
            outfile.write("- & ")

    def write_channel_single(self, outfile, chan):
        """generates channel output for the single outputfile table"""
        x = chan[1:]
        outfile.write(self.str_float(x[0]) + " & ")
        if len(x) == 3:
            outfile.write("$\\pm$" + self.str_float(x[1], "{0:.3f}") + " & ")
        outfile.write(self.str_float(x[-1], "{0:.2f}") + "\%")

    def write_tot_sig_single(self, outfile):
        """generates total output for the single outputfile table"""
        x = len(self.channels[0]) - 1
        outfile.write(self.str_float(self.sig[0]))
        outfile.write(" & $\\pm$" + self.str_float(self.sig[1]))
        if x == 3:
            outfile.write("& - ")

    def write_hists(self, histlist, fileloc, name, pltloc):
        filenames = []
        for i in histlist:
            outfile = fileloc + name + i.label
            filenames.append(outfile)
            i.write_to_file(outfile, pltloc)
        return filenames

    def post_process_histlist(self, histlist):
        """Renames histograms if there are duplicate labels in a list"""
        labels = [i.label for i in histlist]
        duplicates = []
        for label in labels:
            if labels.count(label) > 1:
                duplicates.append(label)
        duplicates = list(set(duplicates))
        for label in duplicates:
            count = 1
            for hist in histlist:
                if label == hist.label:
                    hist.label += str(count)
                    count += 1
        return histlist


class premade_outfile(Outputfile):
    def __init__(self, folder, chan, sig, hists, name, desc, pltloc="",
                 tag=""):
        Outputfile.__init__(self, pltloc, folder)
        self.channels, self.sig = chan, sig
        self.histograms = hists
        self.name = name
        self.desc = desc


######################################################
#                     NNLOJET                        #
######################################################

class NNLOjet(Outputfile):
    def __init__(self, folder, pltloc="", tag="", scale=1, chan="tot"):
        Outputfile.__init__(self, pltloc, folder)
        self.scale = scale
        self.scale_lab = "scale"+str(self.scale).zfill(2)
        self.chan = chan
        self.name = "NNLOJET" + tag
        self.fileloc = folder
        self.filelist = [
            self.fileloc + a for a in os.listdir(folder)
            if (('.dat' in a) and ('cross' not in a))]
        self.crossfile = [self.fileloc +
                          a for a in os.listdir(folder) if ('cross' in a)]
        if len(self.crossfile) == 1:
            self.crossfile = self.crossfile[0]
        else:
            print(self.crossfile)
            print("no/multiple NNLOJET cross section files present. Exiting...")
            sys.exit()
        # if len(self.filelist) == 0:
        #     print("no NNLOJET files present. Exiting...")
        #     sys.exit()
        self.histograms = self.gethists(self.filelist, self.pltloc)
        self.description = "NNLOJET output class"
        self.channels, self.sig = self.parse_NNLO_cross(self.crossfile)

    def get_labels(self, lines):
        labels = lines[0].split()[4:]
        partchan = str(self.chan)
        chan_labs = [lab for lab in labels if "Err" not in lab]
        err_labs = [lab for lab in labels if "Err" in lab]

        for lab in chan_labs:
            if self.scale_lab in lab and partchan+"_" in lab:
                chan_lab = lab
                break

        for lab in err_labs:
            if self.scale_lab+"_Err" in lab and \
               partchan+"_" in lab:
                err_lab = lab
                break
        try:
            ycol = int(chan_lab.split("[")[-1][:-1])-1
            yerr = int(err_lab.split("[")[-1][:-1])-1
        except UnboundLocalError as e:
            print("Scale/Chan not found.")
            print("Scale = "+str(cf.NNLOJET_scale))
            print("Chan = "+cf.NNLOJET_chan)
            raise
        return ycol, yerr

    def gethists(self, filelist, pltloc):
        histlist, labels = [], []
        for i in filelist:
            try:
                if "rev" not in i:
                    with open(i) as infile:
                        lines = infile.readlines()
                        debug = None
                        x, y, err = [], [], []
                        split_dots = i.split('.')
                        if len(split_dots) == 3:
                            # Get label from combined output
                            label = i.split('.')[-2]
                            self.combined = True
                        else:
                            # Get label from raw output
                            label = i.split('.')[-3]
                            self.combined = False
                        ycol, yerr = self.get_labels(lines)
                        for line in lines[3:]:
                            debug = line
                            line = line.strip()
                            cols = line.split()
                            y.append(cols[ycol])
                            err.append(cols[yerr])
                            x.append(cols[1])
                        hist = NNLOJEThistogram(
                            label, x, y, err, self.name, ycol, yerr,
                            scale=self.scale)
                    hist.set_file_loc(i, pltloc)
                    histlist.append(hist)
                    labels.append(label)
            except IndexError as e:
                print("***************************************")
                print(i)
                print(debug)
                raise
        return histlist

    def parse_NNLO_cross(self, NNLOcross):

        if len(NNLOcross.split(".")) == 3:
            self.combined = True
        else:
            self.combined = False
        lines = []
        with open(NNLOcross, "r") as infile:
            lines = infile.readlines()
        newlines = []
        for line in lines:
            line = ' '.join(line.split())
            newlines.append(line)
        lines = newlines
        labels = lines[0].split()[1:]
        labels = [l.split("[")[0] for l in labels]
        if self.combined:
            sigs = lines[2].split(" ")[:]
        else:
            sigs = lines[2].split(" ")[:-1]
        siglabs = zip(labels, sigs)
        errors = []
        vals = []
        scale_label = self.scale_lab
        for i in siglabs:
            if scale_label in i[0]:
                if 'Err' in i[0]:
                    errors.append(i)
                else:
                    vals.append(i)
        channels = []
        for i in vals:
            for j in errors:
                if i[0] in j[0]:
                    channels.append(
                        [i[0].split("_")[0], i[1], j[1], "percent"])
        NNLOJET_channels = []
        for i in channels:
            if i[0] == 'tot':
                NNLOJET_globalsig = [i[1], i[2]]
            else:
                NNLOJET_channels.append(i)
        for i in NNLOJET_channels:
            i[3] = str(float(i[1]) / float(NNLOJET_globalsig[0]) * 100)
        return NNLOJET_channels, NNLOJET_globalsig

######################################################
#                        FEWZ                        #
######################################################


class FEWZ(Outputfile):
    """MUST BE GIVEN ACTUAL OUTPUT FILE NOT A FOLDER"""

    def __init__(self, filepath, pltloc="", tag=""):
        folder = os.path.dirname(filepath)
        Outputfile.__init__(self, pltloc, folder)
        self.name = "FEWZ" + tag
        self.infile = filepath
        self.description = "FEWZ output class"
        self.channels, self.sig, tofb = self.parse_FEWZ(self.infile)
        self.histograms = self.gethists(self.fileloc, tofb)
        self.filelist = self.write_hists(self.histograms, cf.outputloc,
                                         self.name, self.pltloc)

    def parse_FEWZ(self, infilename):
        tofb = 1000
        with open(infilename, "r") as infile, open("temp.txt", "w") as temp:
            start = False
            for line in infile:
                if "RESULT" in line:
                    start = True
                if "Sigma" in line:
                    s = line.strip().split()[-1]
                    continue
                if "Error" in line:
                    e = line.strip().split()[-1]
                    continue
                if start:
                    if len(line) > 2 and line[1] != "="\
                            and "chi" not in line\
                            and "bin" not in line:
                        temp.write(line)
        chanlist = ['gg', 'qg', 'qbg', 'gqb', 'gq', 'qqb', 'qbq']
        dummy = [[i, "Nan", "Nan"] for i in chanlist]
        # tofb is conversion factor from FEWZ units to fb
        FEWZ_channels = dummy
        try:
            FEWZ_globalsig = [str(float(s) * tofb), str(float(e) * tofb)]
        except UnboundLocalError as e:
            raise Exception("No histograms found in FEWZ file: " + infilename)
        return FEWZ_channels, FEWZ_globalsig, tofb

    def gethists(self, fileloc, tofb):
        histlist = []
        with open("temp.txt", "r") as temp:
            start = True
            for line in temp:
                if "----" in line:
                    if not start:
                        histlist.append(FEWZhistogram(
                            histname, xvals, yvals, errs, self.name))
                    xvals = []
                    yvals = []
                    errs = []
                    start = False
                    hn = line.split("  ")[2].strip().replace(
                        ' ', '').replace('/', '')
                    histname = self.get_name(hn.replace('Z', ''))
                else:
                    vals = line.strip().split()
                    xvals.append(vals[0])
                    yvals.append(vals[1])
                    errs.append(vals[2])
        os.remove("temp.txt")
        histlist = self.post_process_histlist(histlist)
        return histlist

    def get_name(self, name):
        return FEWZ_label_lookup[name]

######################################################
#                      DYNNLO                        #
######################################################


class DYNNLO(Outputfile):
    """MUST BE GIVEN ACTUAL OUTPUT FILE NOT A FOLDER"""

    def __init__(self, filepath, pltloc="", tag=""):
        folder = os.path.dirname(filepath)
        Outputfile.__init__(self, pltloc, folder)
        self.name = "DYNNLO" + tag
        self.infile = filepath
        self.description = "DYNNLO output class"
        self.channels, self.sig = self.parse_DYNNLO(self.infile)
        self.histograms = self.gethists(self.fileloc)
        self.filelist = self.write_hists(self.histograms, cf.outputloc,
                                         self.name, self.pltloc)

    def parse_DYNNLO(self, infilename):
        with open(infilename, "r") as infile, open("temp.txt", "w") as temp:
            start = False
            for line in infile:
                if "SET" in line:
                    start = True
                if "Cross-section is:" in line:
                    s = line.strip().split()[3]
                    e = line.strip().split()[5]
                    continue
                if start:
                    if len(line) > 2 and line[1] != "="\
                            and "PLOT" not in line\
                            and "BOTTOM" not in line\
                            and "BOX" not in line\
                            and "LEFT" not in line\
                            and "INTGRL" not in line\
                            and "Entries" not in line\
                            and "CASE" not in line\
                            and "SET" not in line:
                        temp.write(line)
        chanlist = ['gg', 'qg', 'qbg', 'gqb', 'gq', 'qqb', 'qbq']
        dummy = [[i, "Nan", "Nan"] for i in chanlist]
        DYNNLO_channels = dummy
        try:
            DYNNLO_globalsig = [str(float(s)), str(float(e))]
        except UnboundLocalError as e:
            raise Exception(
                "No histograms found in DYNNLO topdrawer file: " + infilename)
        return DYNNLO_channels, DYNNLO_globalsig

    def gethists(self, fileloc):
        histlist = []
        with open("temp.txt", "r") as temp:
            start = True
            for line in temp:
                if "TITLE" in line:
                    if not start:
                        histlist.append(
                            Histogram(histname, xvals, yvals, errs, self.name))
                    xvals = []
                    yvals = []
                    errs = []
                    start = False
                    hn = line.split("\"")[1]
                    hn = ' '.join(hn.split())
                    histname = self.get_name(hn)
                else:
                    vals = line.strip().split()
                    xvals.append(vals[0])
                    yvals.append(vals[1])
                    errs.append(vals[2])
        os.remove("temp.txt")
        histlist = self.post_process_histlist(histlist)
        return histlist

    def get_name(self, name):
        return DYNNLO_label_lookup[name]

######################################################
#                        MCFM                        #
######################################################


class MCFM(Outputfile):
    def __init__(self, infile, pltloc="", tag=""):
        Outputfile.__init__(self, pltloc, infile)
        self.fileloc = os.path.join(os.path.dirname(infile), '')
        self.name = "MCFM"+tag
        self.infile = infile
        self.description = "MCFM output class"

    def get_name(self, name, labels):
        name = name.replace(" ", "_")
        name = name.replace('(', '').replace(')', '')
        histname = MCFM_label_lookup[name]
        if histname in labels:
            histname = histname + "_" + \
                str(labels.count(histname) + 1)
            labels.append(histname)
        return histname, labels

    def clean_sig_line(self, line):
        """Formats a line containing cross section information"""
        sigline = line.lower()
        sigline = sigline.translate(None, '|#%')
        return sigline

    def getxyerr(self, hist_bins):
        """Turns a list of 3 column lines into separate x, y, err lists.
        Assumes that the columns are in order x,y,err"""
        hist_x, hist_y, hist_err = [], [], []
        for i in hist_bins:
            elements = i.split()
            if len(elements) == 3:
                hist_x.append(elements[0])
                hist_y.append(elements[1])
                hist_err.append(elements[2])
        return hist_x, hist_y, hist_err


class MCFMtop(MCFM):
    def __init__(self, infile, pltloc="", tag=""):
        MCFM.__init__(self, pltloc, infile, tag=tag)
        self.channels, self.sig = self.parse_MCFMtop(infile)
        self.histograms = self.gethists(self.fileloc)
        self.filelist = self.write_hists(self.histograms, cf.outputloc,
                                         self.name, self.pltloc)

    def parse_MCFMtop(self, infile):
        MCFM_crosssection = []
        with open(infile, "r") as infile, open("temp.txt", "w") as temp:
            for line in infile:
                if line[0] != "(" and line[0] != "#" and line[0] != 'e':
                    if len(line) > 1:
                        if line[1] != "(" and line[1] != "#" and line[1] != 'e':
                            temp.write(line)
                    else:
                        temp.write(line)
                try:
                    if line[1] == "(" and len(line) > 1:
                        sigline = self.clean_sig_line(line)
                        sigline = sigline.split()
                        MCFM_crosssection.append(
                            sigline[1:])  # channelname, sig, %
                except IndexError as e:
                    pass
        MCFM_channels = MCFM_crosssection[1:10]
        MCFM_globalsig = [MCFM_crosssection[0][2], MCFM_crosssection[0][4]]
        return MCFM_channels, MCFM_globalsig

    def gethists(self, fileloc):
        labels = []
        with open("temp.txt", "r") as temp:
            txt = temp.read()
            hists = txt.split('SET WINDOW Y 2.5')
            hists = [item for item in hists if len(item) > 0]
            hists = [item for item in hists if "Contribution" not in item]
            histlist = []
            for hist in hists:
                lines = hist.split('\n')
                for line in lines:
                    if "TITLE BOTTOM" in line:
                        words = line.split(' "')
                        histname = words[1].replace("'", "").strip()
                        histname, labels = self.get_name(histname, labels)
                hist_bins = hist.split('\n')[11:]
                end = -0
                for idx, hist_bin in enumerate(hist_bins):
                    if "PLOT" in hist_bin:
                        end = idx
                        break
                hist_bins = hist_bins[:end]
                x, y, err = self.getxyerr(hist_bins)
                histlist.append(Histogram(histname, x, y, err, self.name))
        os.remove("temp.txt")
        histlist = self.post_process_histlist(histlist)
        return histlist


class MCFMgnu(MCFM):
    def __init__(self, infile, pltloc="", tag=""):
        MCFM.__init__(self, pltloc, infile, tag=tag)
        self.channels, self.sig = self.parse_MCFMgnu(infile)
        self.histograms = self.gethists(self.fileloc)
        self.filelist = self.write_hists(self.histograms, cf.outputloc,
                                         self.name, self.pltloc)

    def parse_MCFMgnu(self, infile):
        MCFM_crosssection = []
        with open(infile, "r") as infile, open("temp.txt", "w") as temp:
            for line in infile:
                if line[0] != "(" and line[0] != "#" and line[0] != 'e':
                    if len(line) > 1:
                        if line[1] != "(" and line[1] != "#" and line[1] != 'e':
                            temp.write(line)
                    else:
                        temp.write(line)
                if line[0] == "#" and len(line) > 1:
                    sigline = self.clean_sig_line(line)
                    sigline = sigline.split()
                    MCFM_crosssection.append(sigline)  # channelname, sig, %

        MCFM_channels = MCFM_crosssection[1:10]
        MCFM_globalsig = [MCFM_crosssection[0][2], MCFM_crosssection[0][4]]
        return MCFM_channels, MCFM_globalsig

    def gethists(self, fileloc):
        labels = []
        with open("temp.txt", "r") as temp:
            txt = temp.read()
            hists = txt.split('\n\n')
            hists = [item for item in hists if len(item) > 0]
            hists = [item for item in hists if "terminal" not in item]
            histlist = []
            for hist in hists:
                lines = hist.split('\n')
                for line in lines:
                    if "xlabel" in line:
                        words = line.split(' "')
                        histname = words[1][:-6]
                        histname, labels = self.get_name(histname, labels)
                hist_bins = hist.split('\n')[5:]
                x, y, err = self.getxyerr(hist_bins)
                histlist.append(Histogram(histname, x, y, err, self.name))
        os.remove("temp.txt")
        histlist = self.post_process_histlist(histlist)
        return histlist

######################################################
#                      VBFNLO                        #
######################################################


class VBFNLO(Outputfile):
    """MUST BE GIVEN A FOLDER CONTAINING histograms.gp AND xsection.out FILES"""

    def __init__(self, filepath, pltloc="", tag=""):
        folder = os.path.dirname(filepath)
        Outputfile.__init__(self, pltloc, folder)
        self.xsecfile = os.path.join(folder, "xsection.out")
        self.histfile = os.path.join(folder, "histograms.gp")
        self.name = "VBFNLO" + tag
        self.infile = filepath
        self.description = "VBFNLO output class"
        self.channels, self.sig = self.parse_VBFNLO(self.xsecfile)
        self.setup_hists()
        self.histograms = self.gethists(self.fileloc)
        self.filelist = self.write_hists(self.histograms, cf.outputloc,
                                         self.name, self.pltloc)

    def parse_VBFNLO(self, infilename):
        with open(infilename, "r") as infile:
            xsec = 0
            xsecerr = 0
            for line in infile:
                splitline = line.split()
                xsec = float(splitline[0])
                xsecerr = float(splitline[1])
        chanlist = ['gg', 'qg', 'qbg', 'gqb', 'gq', 'qqb', 'qbq']
        dummy = [[i, "Nan", "Nan"] for i in chanlist]
        VBFNLO_channels = dummy
        VBFNLO_globalsig = [xsec, xsecerr]
        return VBFNLO_channels, VBFNLO_globalsig

    def setup_hists(self):
        with open(self.histfile, "r") as infile, open("temp.txt", "w") as tempfile:
            for line in infile:
                if "set" in line and "title" not in line:
                    continue
                if "#" in line or "plot" in line:
                    continue
                if line.strip() == "e":
                    continue
                # Ignore Double Differentials for now
                if "d2S" in line:
                    continue
                if len(line.strip()) == 0:
                    continue
                a = line.split()
                try:
                    if len(a) == 3:
                        for i in a:
                            float(i)
                        continue
                except TypeError as e:
                    pass
                tempfile.write(line)

    def gethists(self, fileloc):
        histlist = []
        with open("temp.txt", "r") as temp:
            start = True
            for line in temp:
                if "title" in line:
                    if not start:
                        histlist.append(
                            Histogram(histname, xvals, yvals, errs, self.name))
                    xvals = []
                    yvals = []
                    errs = []
                    start = False
                    hn = line.split("\"")[1]
                    add_tag = False
                    if len(hn.split()) == 3:
                        add_tag = True
                        x = hn.split()[2]
                    hn = hn.split()[0].split("dS/d")[1]
                    if add_tag:
                        hn += x
                    histname = self.get_name(hn)
                else:
                    vals = line.strip().split()
                    xvals.append(vals[0])
                    yvals.append(vals[1])
                    errs.append("0.0")
        os.remove("temp.txt")
        histlist = self.post_process_histlist(histlist)
        return histlist

    def get_name(self, name):
        return VBFNLO_label_lookup[name]
