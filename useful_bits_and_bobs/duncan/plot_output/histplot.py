import config as cf
from histclasses import Hist_pair, Histogram
import numpy as np
import os
import subprocess as sp

symmetrylookup = {'qqb': 'qbq', 'qg': 'gq', 'qbg': 'gqb'}
colours = ["\"" + i + "\"" for i in cf.col]


class Config():
    #  Counter to allow multiple plot calls without overwriting plt.gnu/tex
    #  files
    ncalls = 1

    def __init__(self, args):
        #  Setup gnuplot file params
        gnuparts = os.path.splitext(cf.outgnu)
        gnuname = gnuparts[0]+str(Config.ncalls)+gnuparts[1]
        self.plotfile = os.path.join(args.outputloc, gnuname)
        self.outgnu = gnuname

        #  Setup latex file params
        texparts = os.path.splitext(cf.outtex)
        texname = texparts[0]+str(Config.ncalls)+texparts[1]
        self.texfile = os.path.join(args.outputloc, texname)
        self.outtex = texname

        #  Setup pdf output file params
        self.outpdf = self.outtex.replace(".tex", ".pdf")
        Config.ncalls += 1

        #  Transfer appropriate config options from command line args
        self.debug = args.debug
        self.display = args.openpdf
        self.pltloc = args.outputloc
        self.asym_plots = args.plot_asym


def plot(a, b, args):
    conf = Config(args)
    initiategnu(conf)
    initiatetex(conf)
    allhists, pairs, unpairs = get_coincident_plots(a, b)

    for hist in unpairs:
        add_single_hist(hist, conf)

    for pair in pairs:
        add_double_hist(pair, conf)
        add_ratio_hist(pair, conf)

    if conf.asym_plots:
        for h in allhists:
            if h.source == "NNLOJET":
                if "y" in h.label and "abs" not in h.label:
                    add_asym_hist(h, conf)

    write_asym_table(a, conf)
    write_asym_table(b, conf)
    write_sig_table(a, b, conf)
    end_plot(conf)


def plot_single(a, args):
    conf = Config(args)
    initiategnu(conf)
    initiatetex(conf)

    for hist in a.histograms:
        add_single_hist(hist, conf)

    if asym_plots:
        for h in a.histograms:
            # if h.source == "NNLOJET":
            if "y" in h.label and "abs" not in h.label:
                add_asym_hist(h, conf)

    write_asym_table(a, conf)
    write_single_table(a, conf)
    end_plot(conf)


def end_plot(conf):
    f = conf.pltloc
    endtex(conf)
    origdir = os.getcwd()  # record dir for later reset
    if f != '':
        os.chdir(f)
    if not conf.debug:
        FNULL = open(os.devnull, 'w')
        sp.call(["gnuplot", conf.outgnu], stdout=FNULL, stderr=FNULL)
        sp.call(["pdflatex", conf.outtex], stdout=FNULL, stderr=FNULL)
        if conf.display:
            sp.call(["evince", conf.outpdf], stdout=FNULL, stderr=FNULL)
    else:
        sp.call(["gnuplot", conf.outgnu])
        sp.call(["pdflatex", conf.outtex])
        if conf.display:
            sp.call(["evince", conf.outpdf])
    os.chdir(origdir)


def write_sig_table(a, b, conf):

    def get_c_str(a, b):
        """generates a c_str for the multiple outputfile table"""
        c_string = "c | "
        for i in range((len(a.channels[0]) - 1)):
            c_string += "c "
        c_string += "| "
        for i in range((len(b.channels[0]) - 1)):
            c_string += "c "
        c_string += "| c"
        return c_string

    with open(conf.texfile, "a") as outfile:
        init_tex_table(outfile)
        c_str = get_c_str(a, b)
        outfile.write("\\begin{tabular}{" + c_str + "}\n")
        outfile.write("& \\multicolumn{" + str(len(a.channels[0]) - 1) + "}")
        outfile.write("{c|}{\\textbf{" + a.name + "}}")
        outfile.write("& \\multicolumn{" + str(len(b.channels[0]) - 1) + "}")
        outfile.write("{c|}{\\textbf{" + b.name + "}}")
        outfile.write("& $\\frac{" + a.name + "}{" +
                      b.name + "}$\\\\[1.05ex]\n")
        outfile.write("\hline\n")
        for i in a.channels:
            for j in b.channels:
                if i[0] == j[0]:
                    outfile.write(i[0] + " & ")
                    a.write_channel(outfile, i)
                    b.write_channel(outfile, j)
                    try:
                        outfile.write(
                            str("{0:.5f}".format(float(i[1]) / float(j[1]))))
                    except ZeroDivisionError as e:
                        outfile.write("-")
                    outfile.write("\\\\\n")
        outfile.write("\hline\n")
        outfile.write("TOTAL & ")
        a.write_tot_sig(outfile)
        b.write_tot_sig(outfile)
        outfile.write(str("{0:.5f}".format(float(a.sig[0]) / float(b.sig[0]))))
        outfile.write("\\\\\n")
        end_tex_table(outfile, "Results")


def write_single_table(a, conf):

    def get_c_str_single(a):
        """generates a c_str for the single outputfile table"""
        c_string = "c | "
        for i in range((len(a.channels[0]) - 1)):
            c_string += "c "
        return c_string

    with open(conf.texfile, "a") as outfile:
        init_tex_table(outfile)
        c_str = get_c_str_single(a)
        outfile.write("\\begin{tabular}{" + c_str + "}\n")
        outfile.write("& \\multicolumn{" + str(len(a.channels[0]) - 1) + "}")
        outfile.write("{c}{\\textbf{" + a.name + "}}\\\\\n")
        outfile.write("\hline\n")
        for i in a.channels:
            outfile.write(i[0] + " & ")
            a.write_channel_single(outfile, i)
            outfile.write("\\\\\n")
        outfile.write("\hline\n")
        outfile.write("TOTAL & ")
        a.write_tot_sig_single(outfile)
        outfile.write("\\\\\n")
        end_tex_table(outfile, "Results")


def init_tex_table(outfile):
    outfile.write("\\begin{center}\n")
    outfile.write("\\begin{figure}\n")
    outfile.write("\\centering\n")


def end_tex_table(outfile, caption):
    outfile.write("\\end{tabular}\n")
    outfile.write("\\centering\n")
    outfile.write("\\caption{" + caption + "}\n")
    outfile.write("\\end{figure}\n")
    outfile.write("\\end{center}\n")


def init_plot(outfile, pdfout):
    outfile.write("set output \"" + pdfout + "\"\n")
    outfile.write("unset logscale y\n")
    outfile.write("set key off\n")


def get_coincident_plots(h1, h2):
    hist_pairs = []
    unpaired_hists = h1.histograms + h2.histograms
    allhists = unpaired_hists[:]
    for i in h1.histograms:
        for j in h2.histograms:
            if i.label == j.label:
                hist_pairs.append(Hist_pair(i, j))
                unpaired_hists.remove(i)
                try:
                    unpaired_hists.remove(j)
                except ValueError as e:
                    print("ERROR. DUPLICATED HISTOGRAM NAME: "+j.label)
                    print("IGNORING FOR NOW.")
                    pass
                break
    return allhists, hist_pairs, unpaired_hists


def add_single_hist(hist, conf):
    pdfout = hist.source + "_" + hist.label + ".pdf"
    with open(conf.plotfile, "a") as outfile:
        init_plot(outfile, pdfout)
        outfile.write("set key on\n")
        hist.set_title(outfile, hist.source + " " + hist.label)
        hist.set_ylabel(outfile, hist.label)
        hist.plot_histep(outfile, hist.fileloc,
                         colours[0], end=False, start=True)
        hist.plot_errs(outfile, hist.fileloc,
                       colours[0], end=True, start=False)
        writetotex(pdfout, conf)


def add_double_hist(hp, conf):
    pdfout = hp.label + hp.tag + ".pdf"
    with open(conf.plotfile, "a") as outfile:
        init_plot(outfile, pdfout)
        hp.h1.set_title(outfile, hp.title)
        hp.h1.set_ylabel(outfile, hp.label)
        outfile.write("set key on\n")
        hp.h1.plot_histep(outfile, hp.h1.fileloc, colours[0], start=True)
        hp.h2.plot_histep(outfile, hp.h2.fileloc, colours[1])
        hp.h1.plot_errs(outfile, hp.h1.fileloc, colours[0])
        hp.h2.plot_errs(outfile, hp.h2.fileloc, colours[1], end=True)
        writetotex(pdfout, conf)


def add_ratio_hist(hp, conf):
    # work out how to handle two different NNLOJET_files?
    h1 = hp.h1
    h2 = hp.h2
    outfile = cf.outputloc + "ratio-" + h1.source + h2.source + hp.title
    ratio_string = h1.source + "/" + h2.source
    if h1.x.size != h2.x.size:
        return
    xvals = h1.x
    yvals = h1.y / h2.y
    errs = yvals * np.sqrt((h1.err / h1.y)**2 + (h2.err / h2.y)**2)
    write_array(outfile, [xvals, yvals, errs])
    h = Histogram(ratio_string, xvals, yvals, errs, "RATIO")
    h.set_file_loc(outfile, conf.pltloc)
    writeratiohist(hp, h, conf)


def writeratiohist(hp, h, conf):
    pdfout = h.fileloc + hp.label + hp.tag + "ratio.pdf"
    with open(conf.plotfile, "a") as outfile:
        init_plot(outfile, pdfout)
        h.set_title(outfile, "Ratio plot: " + hp.title)
        h.set_ylabel(outfile, h.label)
        h.plot_histep(outfile, h.fileloc, colours[0], start=True)
        h.plot_1(outfile, colours[2])
        h.plot_errs(outfile, h.fileloc, colours[0], end=True)
        writetotex(pdfout, conf)


def add_asym_hist(h, conf):
    revy = h.y[::-1]
    reverr = h.err[::-1]
    rev_h = Histogram(h.label + "-rev", h.x, revy, reverr, "ASYM")
    outfile = os.path.join(cf.outputloc, rev_h.label)
    rev_h.write_to_file(outfile, conf.pltloc)
    asym_pair = Hist_pair(h, rev_h, title=h.label + "_", tag="asym")
    add_double_hist(asym_pair, conf)
    add_ratio_hist(asym_pair, conf)


def write_asym_table(a, conf):
    with open(conf.texfile, "a") as outfile:
        init_tex_table(outfile)
        outfile.write("\\begin{tabular}{c | c}\n")
        outfile.write(" Channels & Ratio \\\\\n")
        outfile.write("\hline\n")
        for i in a.channels:
            for j in a.channels:
                try:
                    if symmetrylookup[i[0]] == j[0]:
                        i[1] = float(i[1])
                        j[1] = float(j[1])
                        try:
                            outfile.write(
                                i[0] + "/" + j[0] + " & " +
                                str("{0:.5f}".format(i[1] / j[1])) + " \\\\\n")
                        except ZeroDivisionError as e:
                            outfile.write(i[0] + "/" + j[0] + " & - \\\\\n")
                except KeyError as e:
                    pass
        end_tex_table(outfile, a.name + " asymmetry checks")


def initiategnu(conf):
    if os.path.isfile(conf.plotfile):
        os.remove(conf.plotfile)
    with open(conf.plotfile, "a") as outfile:
        outfile.write("#!/usr/bin/gnuplot\n")
        outfile.write("reset\n")
        outfile.write("set terminal pdf enhanced\n")


def initiatetex(conf):
    if os.path.isfile(conf.texfile):
        os.remove(conf.texfile)
    with open(conf.texfile, "a") as outfile:
        outfile.write("% AUTO TEX OUTPUT\n")
        outfile.write("\\documentclass[a4paper,12pt]{article}\n")
        outfile.write("\\usepackage[margin=2.5cm]{geometry}\n")
        outfile.write("\\usepackage{amssymb,amsmath,pstricks,color,amsbsy}\n")
        outfile.write("\\usepackage{graphicx}\n")
        outfile.write("\\usepackage{multirow}\n")
        outfile.write("\\begin{document}\n")


def endtex(conf):
    with open(conf.texfile, "a") as outfile:
        outfile.write("\\end{document}\n")


def writetotex(pdfname, conf):
    with open(conf.texfile, "a") as outfile:
        outfile.write("\\begin{center}\n")
        outfile.write(
            "\\includegraphics[width=.6\\textwidth]{" + pdfname + "}\n")
        outfile.write("\\end{center}\n")


def write_array(name, array):
    result = np.array(array)
    result = result.T
    np.savetxt(name, result)
