from __future__ import division
import numpy as np
import os


class Histogram:
    def __init__(self, name, histx, histy, histerr, source, fac=1):
        self.label = name
        self.source = source
        self.x = self.numpy_array(histx)
        self.y = self.numpy_array(histy) * fac
        self.err = self.numpy_array(histerr) * fac
        self.totalweight = np.sum(self.y)

    def set_file_loc(self, name, pltloc):
        # self.fileloc = os.path.basename(name)
        self.fileloc = os.path.relpath(name, pltloc)

    def __repr__(self):
        outstring = "Observable: " + self.label + "\n"
        outstring += "Source: " + self.source + "\n"
        outstring += "Total Weight: " + str(self.totalweight) + "\n"
        for i in zip(self.x, self.y, self.err):
            outstring += str(i[0]) + "  " + str(i[1]) + "  " + str(i[2]) + "\n"
        return outstring

    def __add__(self, hist2):
        try:
            assert(self.label == hist2.label)
            src = self.source + "+" + hist2.source
        except AssertionError as e:
            raise Exception("Added histograms do not have the same label...")
        try:
            assert(np.array_equal(self.x, hist2.x))
        except AssertionError as e:
            raise Exception("Added histograms do not have the same x axis...")
        y = self.y + hist2.y
        err = np.sqrt((self.err)**2 + (hist2.err)**2)
        err = np.absolute(err)
        rethist = Histogram(self.label, self.x, y, err, src)
        nme = "(" + self.label + "_" + src + ")"
        rethist.write_to_file(nme, self.__pltloc)
        return rethist

    def __sub__(self, hist2):
        try:
            assert(self.label == hist2.label)
            src = self.source + "-" + hist2.source
        except AssertionError as e:
            raise Exception(
                "Subtracted histograms do not have the same label...")
        try:
            assert(np.array_equal(self.x, hist2.x))
        except AssertionError as e:
            raise Exception(
                "Subtracted histograms do not have the same x axis...")
        y = self.y - hist2.y
        err = np.sqrt((self.err)**2 + (hist2.err)**2)
        err = np.absolute(err)
        rethist = Histogram(self.label, self.x, y, err, src)
        nme = "(" + self.label + "_" + src + ")"
        rethist.write_to_file(nme, self.__pltloc)
        return rethist

    def write_to_file(self, name, pltloc):
        with open(name, 'w') as outfile:
            for j in zip(self.x, self.y, self.err):
                outfile.write(str(j[0]) + "  " +
                              str(j[1]) + "  " + str(j[2]) + "\n")
        self.set_file_loc(name, pltloc)
        self.__pltloc = pltloc

    def numpy_array(self, inp):
        try:
            x = np.array(inp, dtype='float64')
        except ValueError as e:
            x = np.array([0 for i in inp], dtype='float64')
        return x

    # Plotting functions for the gnuplot file...
    def plot_helper(self, outfile, start, end, string):
        if start:
            outfile.write("plot ")
        outfile.write(string)
        if end:
            outfile.write(" \n")
        else:
            outfile.write(" , ")

    def plot_histep(self, outfile, filename, colour, end=False, start=False):
        self.plot_helper(outfile, start, end, "\"" + filename +
                         "\" using 1:2  title \"" + self.source +
                         "\" with histep lt rgb " + colour)

    def plot_errs(self, outfile, filename, colour, end=False, start=False):
        self.plot_helper(outfile, start, end, "\"" + filename +
                         "\" using 1:2:3 with errorbars lt rgb " + colour +
                         " pt 7 ps 0.2 notitle")

    def plot_1(self, outfile, colour, start=False, end=False):
        self.plot_helper(outfile, start, end, " 1 lt rgb " + colour)

    def set_ylabel(self, outfile, string):
        outfile.write("set ylabel \"" + string + "\" font \"Helvetica, 20\"\n")

    def set_title(self, outfile, label):
        outfile.write("set title " + "\"" + label + "\"" + "\n")


class NNLOJEThistogram(Histogram):
    def __init__(
            self,
            name,
            histx,
            histy,
            histerr,
            source,
            ycol,
            yerr,
            scale=1):
        Histogram.__init__(self, name, histx, histy, histerr, source)
        self.scale = scale
        # +1 as we're not zero counting unlike when ycol was calculated
        self.ycol = str(ycol+1)
        self.yerr = str(yerr+1)

    def plot_histep(self, outfile, filename, colour, end=False, start=False):
        self.plot_helper(outfile, start, end, "\"" + filename + "\" using 2:" +
                         self.ycol + "  title \"" + self.source +
                         "\" with histep lt rgb " + colour)

    def plot_errs(self, outfile, filename, colour, end=False, start=False):
        self.plot_helper(outfile, start, end, "\"" + filename + "\" using 2:" +
                         self.ycol + ":" + self.yerr +
                         " with errorbars lt rgb " + colour +
                         " pt 7 ps 0.2 notitle")


class Hist_pair:
    def __init__(self, hist1, hist2, title="", tag=""):
        self.h1 = hist1
        self.h2 = hist2
        self.title = hist1.label
        self.label = hist1.label  # ylabel for plot
        self.tag = ""
        if title != "":  # name for plot titles
            self.title = title
        if tag != "":  # tag for pdf filenames
            self.tag = tag

    def __repr__(self):
        outstring = self.name + "\n"
        outstring += self.h1.__repr__() + "\n"
        outstring += self.h2.__repr__()
        return outstring


class FEWZhistogram(Histogram):
    def __init__(self, name, histx, histy, histerr, source, fac=1000):
        Histogram.__init__(self, name, histx, histy, histerr, source, fac)
        if len(histx) > 1:
            ledge = self.x[0] - ((self.x[1] - self.x[0]) / 2)
            tmpedge = np.copy(ledge)
            binedges = [ledge]
            for i in self.x:
                halfwidth = i - tmpedge
                tmpedge = i + halfwidth
                binedges.append(tmpedge)
            binedges = np.array(binedges, dtype='float64')
            binwidths = np.ediff1d(binedges)
            self.y = self.y / binwidths
            self.err = self.err / binwidths
            self.totalweight = np.sum(self.y)
