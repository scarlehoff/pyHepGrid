#!/usr/bin/env python2
"""Prints readable output from an NNLOJET cross section file to terminal.

Can take multiple cross section files as arguments, and will process one
after another. Should be run after combine.py usage.

Usage: python print_output.py <cross_section_file1> <cross_section_file2> ...
"""
from __future__ import print_function, division
import numpy as np
import re
import argparse as ap
from io import StringIO
import tokenize


class HistFile:
    colour_list = {"gg": "Cyan",
                   "tot": "Black",
                   "qg": "Indigo",
                   "gq": "Chartreuse",
                   "qbg": "Red",
                   "qbq": "DarkBlue",
                   "gqb": "Pink",
                   "qqb": "Yellow",
                   "qq": "Teal",
                   "qbqb": "Sienna",
                   "scale01": "Cyan",
                   "scale02": "Black",
                   "scale03": "Indigo",
                   "scale04": "Chartreuse",
                   "scale05": "Red",
                   "scale06": "DarkBlue",
                   "scale07": "Pink",
                   "qg+gq": "Chartreuse",
                   "qbg+gqb": "Red",
                   "qqb+qbq": "DarkBlue",
                   None: "black"}

    def __init__(self, infile):
        self.fileloc = infile
        try:
            self.obs = infile.split(".")[-2]
        except IndexError:
            self.obs = raw_input("Please give the observable name:")
        self.get_bins()
        self.fill_channels()
        self.set_xvals()
        self.setup_with_errors()
        self.chans_by_partons = self.group_by_partons()
        self.chans_by_scale = self.group_by_scale()
        self.chans_sym_input = self.symmetrised_input()

    def set_xvals(self):
        self.xvals_lower, self.xvals, self.xvals_upper = \
            self.chans[self.labels[0]], self.chans[self.labels[1]],\
            self.chans[self.labels[2]]

    def fill_channels(self):
        self.chans = {}
        for label in self.labels:
            self.chans[label] = []
        for i in self.bins:
            for idx, j in enumerate(i):
                self.chans[self.labels[idx]].append(float(j))

    def get_bins(self):
        self.bins = []
        with open(self.fileloc)as ifile:
            for idx, line in enumerate(ifile):
                if idx == 0:
                    self.labels = line.split()[1:]
                elif idx > 2:
                    self.bins.append(line.split())

    def setup_with_errors(self):
        self.chans_with_errors = {}
        for i in self.chans:
            if "_Err" not in i:
                for j in self.chans:
                    if j.split("[")[0] == (i.split("[")[0] + "_Err"):
                        self.chans_with_errors[i.split("[")[0]] =\
                            (np.array(self.chans[i]), np.array(self.chans[j]))

    def group_by_partons(self):
        chan = {}
        for i in self.chans_with_errors:
            partons, scale = i.split("_")
            if partons not in chan:
                chan[partons] = {}
            chan[partons][scale] = self.chans_with_errors[i]
        return chan

    def group_by_scale(self):
        scales = {}
        for i in self.chans_with_errors:
            partons, scale = i.split("_")
            if scale not in scales:
                scales[scale] = {}
            scales[scale][partons] = self.chans_with_errors[i]
        return scales

    def sum_chans(self, a, b):
        vals = a[0]+b[0]
        errs = np.sqrt(a[1]**2+b[1]**2)
        return (vals, errs)

    def symmetrised_input(self):
        reversed_channels = {"qg": "gq", "qbg": "gqb", "qqb": "qbq"}
        scales = {}
        for i in self.chans_with_errors:
            if any(j in i for j in ["qg", "qbg", "qqb"]):
                partons, scale = i.split("_")
                if scale not in scales:
                    scales[scale] = {}
                reversed_chan = "_".join(j for j in
                                         [reversed_channels[partons], scale])
                retval = self.sum_chans(self.chans_with_errors[i],
                                        self.chans_with_errors[reversed_chan])
                retlabel = partons+"+"+reversed_channels[partons]
                scales[scale][retlabel] = retval
        return scales

    def plot_hists(self, sc_var, chan_bd, asym, sym_input):

        def plotfunc(x, y, yerrs, lab=None, title=""):
            if len(np.nonzero(y)) > 0:
                yplot = [val for pair in zip(y, y) for val in pair]
                xplot = [self.xvals_lower[0]]
                xplot += [val for pair in zip(self.xvals_lower[1:],
                                              self.xvals_lower[1:])
                          for val in pair]
                xplot += [self.xvals_upper[-1]]
                try:
                    color = HistFile.colour_list[lab]
                except KeyError as e:
                    print("Observable ", lab, " not found in colour list.")
                    raise
                axis = plt.plot(xplot, yplot, label=lab, color=color)
                if asym:
                    try:
                        asym_lab = lab + " (reversed x)"
                        plt.plot(list(reversed(xplot)), yplot, label=asym_lab)
                        color = axis[0].get_color()
                    except TypeError as e:
                        asym_lab = "(reversed x)"
                plt.errorbar(np.array(x), y, yerr=yerrs, marker=',',
                             linestyle="None", ecolor=color,
                             color=color, label="_nolegend_")
                ylab = r"$\frac{d\sigma}{d" + self.obs + "}$"
                plt.ylabel(ylab)
                plt.xlabel(self.obs)
                plt.title(self.obs)
                return True
            else:
                return False

        def plot_breakdown(chans_by_x):
            for i in sorted(chans_by_x):
                plotted = False
                for pt in chans_by_x[i]:
                    yvals = chans_by_x[i][pt][0]
                    yerrs = chans_by_x[i][pt][1]
                    a = plotfunc(self.xvals, yvals, yerrs, lab=pt, title=i)
                    if a is True:
                        plotted = True
                if plotted:
                    plt.legend(loc='upper right')
                    plt.show()

        if sc_var:
            plot_breakdown(self.chans_by_partons)
        if chan_bd:
            plot_breakdown(self.chans_by_scale)
        if sym_input:
            plot_breakdown(self.chans_sym_input)
        else:
            for i in self.chans_with_errors:
                if "tot" in i:
                    yvals = self.chans_with_errors[i][0]
                    yerrs = self.chans_with_errors[i][1]
                    plotfunc(self.xvals, yvals, yerrs, title=i)
            plt.show()


class Channel:
    formatstr = '{0: <7}'
    errformatstr = '{0: <3}'

    def __init__(self, i):
        sd = re.split('(\d+)', i[1].title())
        sd = sd[0] + " " + str(int(sd[1]))
        self.scale = sd
        self.partons = i[0]
        self.sig = float(i[2])
        self.err = float(i[3])
        self.sigstr = str(self.sig).ljust(9)
        self.errstr = Channel.errformatstr.format(self.err).ljust(9)

    @property
    def sig(self):
        return self.__sig

    @property
    def err(self):
        return self.__err

    @sig.setter
    def sig(self, sig):
        self.__sig = sig
        self.sigstr = str(self.sig).ljust(9)

    @err.setter
    def err(self, err):
        self.__err = err
        self.errstr = Channel.errformatstr.format(self.err).ljust(9)

    def __repr__(self):
        return self.scale + "  " + self.partons + ":  " +\
            str(self.sig) + " +/-" + str(self.err)


class Output:
    dashline = "----------------------------\n"
    equalsline = "============================\n"
    asymdict = {"qqb": "qbq", "qg": "gq", "qbg": "gqb"}

    def __init__(self, a, j, scale_errors):
        self.scale_errors = scale_errors
        self.channels = [Channel(i) for i in a if i[0] != "tot"]
        self.chandict = {}
        for b in self.channels:
            self.chandict[b.partons] = (b.sigstr, b.errstr)
        self.scaleid = self.channels[0].scale
        self.tot = [Channel(i) for i in a if i[0] == "tot"][0]
        self.sum_chans()
        self.chandict["tot"] = (self.tot.sigstr, self.tot.errstr)
        
    def sum_chans(self):
        vals = [parton_channel.sig for parton_channel in self.channels]
        errs = [parton_channel.err**2 for parton_channel in self.channels]
        self.sum = sum(vals)
        self.sumerr = np.sqrt(sum(errs))
        
    def __repr__(self):
        if self.scale_errors:
            scale_factor = float(self.tot.errstr)/float(self.sumerr)
            print("Scaling errors by factor {0}.".format(scale_factor))
        retval = "\n"
        retval += (self.scaleid + "\n")
        retval += Output.dashline
        if self.tot.sig != 0:
            for i in self.channels:
                if self.scale_errors:
                    i.err = scale_factor*i.err
                retval += ("{0:<6}{1:10.3f} +/- {2:7.4f}\n".format(i.partons + ":",
                                                                  float(i.sigstr), 
                                                                  float(i.errstr)))
            if self.scale_errors:
                self.sum_chans()
            retval += Output.dashline
            retval += ("{0:<6}{1:10.3f} +/- {2:7.4f}\n".format("Sum:",
                                                              float(self.sum), 
                                                              float(self.sumerr)))
            retval += Output.equalsline
            retval += ("{0:<6}{1:10.3f} +/- {2:7.4f}\n".format(self.tot.partons.title() + ":",
                                                              float(self.tot.sigstr), 
                                                              float(self.tot.errstr)))
            retval += self.asym_values()
            retval += "\n"
        else:
            retval = self.scaleid + ": No data found"
        return retval

    def __err(self, a, b):
        """Returns a formatted string with the error from a/b.

        a/b are channel objects.
        """
        retval = a.sig / b.sig * \
            np.sqrt((a.err / a.sig)**2 + (b.err / b.sig)**2)
        return '{0: <3}'.format(retval)

    def asym_values(self):
        formatstr = '{0:.6f}'
        retstr = Output.equalsline
        retstr += "Asymmetry checks:  \n"
        for i in self.channels:
            for j in self.channels:
                if i.partons in Output.asymdict.keys() \
                        and Output.asymdict[i.partons] == j.partons:
                    try:
                        retstr += (i.partons+"/"+j.partons+": ").ljust(9) + \
                            formatstr.format(float(i.sig / j.sig)).ljust(9) + "+/- " \
                            + "{0:.4f}".format(float(self.__err(i, j))) + "\n"
                    except ZeroDivisionError as e:
                        try:
                            retstr += (i.partons+"/"+j.partons+": ").ljust(9)\
                                + " -  ".ljust(9) + "+/- " +\
                                "{0:4f}".format(float(self.__err(i, j))) + "\n"
                        except ZeroDivisionError as e:
                            retstr += (i.partons+"/"+j.partons+": ").ljust(9)\
                                + " -  ".ljust(9) + "+/- " + "   -  " + "\n"
                        except ValueError as e:
                            retstr += (i.partons+"/"+j.partons+": ").ljust(9)\
                                + " -  ".ljust(9) + "+/- " + "   -  " + "\n"
                    except ValueError as e:
                        retstr += (i.partons+"/"+j.partons+": ").ljust(9) + \
                            formatstr.format(i.sig / j.sig).ljust(9) + "+/- " \
                            + "NaN" + "\n"
        retstr += Output.dashline
        return retstr


def print_cross_section_file_MCFM(filename, scale_errs):
    raw_channels = []
    scale = "scale01"
    raw_tot = 0
    with open(filename) as infile:
        for i in infile:
            if "|" in i and "#" in i:
                raw_channels.append(
                    i.replace(
                        "#", "").replace(
                        "|", "").strip().lower())
            if "Cross-section is:" in i:
                raw_tot = i.strip()
    channels = []
    for i in raw_channels:
        x = i.split()
        channels.append([x[0], scale, x[1], "0"])
    channels.append(["tot", scale, raw_tot.split()[-4], raw_tot.split()[-2]])
    scales = [Output(channels, "Scale01", scale_errs)]
    for i in scales:
        print(i)
    return scales


def print_cross_section_file_NNLOJET(filename, scale_errs):
    with open(filename) as infile:
        for i, val in enumerate(infile):
            if i == 0:
                labels = val
            elif i == 2:
                values = val.split()
    lablist = [i.split("[")[0] for i in labels.split()[1:]]
    pairs = list(zip(lablist, values))  # List as generator reused
    errlist = [i[1] for i in pairs if "Err" in i[0]]
    siglist = [i for i in pairs if "Err" not in i[0]]
    finvals = [i[0][0].split("_") + [i[0][1], i[1]]
               for i in zip(siglist, errlist)]
    scaleslist = sorted(set([i[1] for i in finvals]))
    scales = [Output([j for j in finvals if j[1] == i], i, scale_errs) for i in scaleslist]
    for i in scales:
        print(i)
    return scales


def print_scales_breakdown(scales, list_output):
    print("\n\n")
    print("Scale breakdown per channel:")
    print("==============================")

    def prt_chan(channel, sc, chan_name=""):
        if len(chan_name) == 0:
            print(channel)
        else:
            print(chan_name)
        print("---------------------------------")
        for idx, scale in enumerate(sc):
            if list_output:
                print("[" + str(scale.chandict[channel][0]) +
                      "," + str(scale.chandict[channel][1]) + "],")
            else:
                print("Scale " + str(idx + 1) + ":  " +
                      str(scale.chandict[channel][0]) + " +/- " +
                      str(scale.chandict[channel][1]))
        print("=================================")
    for channel in scales[0].chandict.keys():
        if channel != "tot":
            prt_chan(channel, scales)
    prt_chan("tot", scales, chan_name="TOTAL")


def setup_parser():
    parser = ap.ArgumentParser(
        description="Print NNLOJET cross section output to terminal.")
    parser.add_argument("infile",
                        help="input cross section file from NNLOJET. \
                        Should be the result of combine.py.")
    parser.add_argument("--breakdown", "-bd",
                        help="include output showing the scale dependence\
                        for each channel.",
                        action="store_true")
    parser.add_argument("--pylistoutput", "-l",
                        help="Ouput scale dependences for each channel as\
                        a python list (val,err). Only active if scale \
                        breakdown is present",
                        action="store_true")
    parser.add_argument("--hist", "-hs",
                        help="Plot a histogram from a file. Only active if \
                        scale breakdown is present",
                        action="store_true")
    parser.add_argument("--scalevar", "-sv",
                        help="Combine histogram output for different scales \
                        into one plot",
                        action="store_true")
    parser.add_argument("--chanbd", "-cbd",
                        help="Show all different channel contributions for \
                        each scale in each plot",
                        action="store_true")
    parser.add_argument("--asym", "-a",
                        help="When plotting histograms, overlay the plot with \
                        reversed x values to check asymmetry in e.g rapidity \
                        distributions",
                        action="store_true")
    parser.add_argument("--syminput", "-s",
                        help="For all potential cantidates, sum over the \
                        symmetrised initial states, e.g qg+gq, qbg+gqb etc.",
                        action="store_true")
    parser.add_argument("--scale_errors", "-se",
                        help="Scale channel errors to agree with total error.",
                        action="store_true", default = False)
    # parser.add_argument("--any", "-any",
    #                     help="Enable arbitrary input mode",
    #                     action="store_true")
    args = parser.parse_args()
    return parser, args


parse_funcs = {
    'dat': print_cross_section_file_NNLOJET,
    'gnu': print_cross_section_file_MCFM}

if __name__ == "__main__":
    parser, args = setup_parser()
    if not args.hist:
        scales = parse_funcs[args.infile.split(".")[-1]](args.infile, args.scale_errors)
        if args.breakdown:
            print_scales_breakdown(scales, args.pylistoutput)
    else:
        try:
            import matplotlib.pyplot as plt
        except RuntimeError as e:
            pass
        hf = HistFile(args.infile)
        hf.plot_hists(args.scalevar, args.chanbd, args.asym, args.syminput)
        # if args.any:
        #     hf.plot_any()
