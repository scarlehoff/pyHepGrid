#!/usr/bin/env python3

fmt_set = ['b.', 'r.', 'g.']

import numpy as np
from matplotlib import pyplot as pl

class Plot:

    def __init__(self, filename = None, column = None, x = None, y = None, dy = None):
        self.customized = False
        if filename:
            self.filename = filename
            self.__unpack(column)
            self.__total_xsection()
        else:
            self.x_array = x
            self.y_array = y
            self.dy_array = dy

    def customize_plot(self, title, legend, xlabel, ylabel):
        self.customized = True
        self.title = title
        self.legend = legend
        self.xlabel = xlabel
        self.ylabel = ylabel

    def get_data(self):
        return self.x_array, self.y_array, self.dy_array

    def print_total(self):
        print("Printing total cross section for: " + self.filename)
        print("Total cross section: " + str(self.total_xs) + " +/- " + str(self.total_dy))
        return

    def get_custom(self):
        if self.customized:
            return self.title, self.legend, self.xlabel, self.ylabel
        else:
            return "", "", "", ""

    def __unpack(self, column):
        x_column = 1
        y_column = 2
        dy_column = 3
        cm = "#"
        # Correct the column indices for weird formats
        if ".dat" in self.filename:
            x_column = 2
            y_column = 4
            dy_column = 5
            print("Assumed file is in NNLOJET format")
        elif ".agr" in self.filename:
            com = "@"
            print("Assumed file is in xmgrace format")
        if column:
            y_column = column
            dy_column = y_column + 1

        dat = np.loadtxt(self.filename, comments = cm, unpack = True)
        # python uses c-type format (not gnuplot/fortran) so arrays start at 0
        self.x_array = dat[x_column - 1]
        self.y_array = dat[y_column - 1]
        self.dy_array = dat[dy_column - 1]

    def __total_xsection(self):
        dx = abs(self.x_array[2] - self.x_array[3])
        self.total_xs = sum(self.y_array)*dx
        total_dy = 0.0
        for dy in self.dy_array:
            total_dy += dy*dy
        self.total_dy = np.sqrt(total_dy)*dx


def regular_plot(plot_list, error = True, print_to_png = None):
    pl.rc('text', usetex = True)
    pl.rc('font', family = 'serif')
    pl.ticklabel_format(axis='y', style='sci', scilimits=(-2,2))
    for plot, color in zip(plot_list, fmt_set[:len(plot_list)]):
        x, y, dy = plot.get_data()
        title, legend, xl, yl = plot.get_custom()
        pl.errorbar(x, y, xerr = 0.0, yerr = dy, fmt = color, label = legend)
        pl.xlabel(xl)
        pl.ylabel(yl)
        pl.title(title)
    pl.legend()
    if print_to_png:
        pl.savefig(print_to_png + ".png", bbox_inches = "tight")
    else:
        pl.show()
    pl.close()
    return

def ratio_plots(plot_num, plot_den, no_err = False):
    x_num,y_num,dy_num = plot_num.get_data()
    x_den,y_den,dy_den = plot_den.get_data()
    title_num, legend_num, _, _ = plot_num.get_custom()
    _, legend_den, _, _ = plot_den.get_custom()
    if x_num.all() != x_den.all():
        print("Error trying to get ratio, x axis of the plots are different")
    new_y = []
    new_dy = []
    for i,j, di, dj in zip(y_num, y_den, dy_num, dy_den):
        new_y.append(i/j)
        err = pow(di/j,2) + pow(dj*i/j/j,2)
        new_dy.append(np.sqrt(err))
    if no_err:
        new_dy = 0.0
    new_plot = Plot(x = x_num, y = new_y, dy = new_dy)
    if legend_num == legend_den:
        new_legend = ""
    else:
        new_legend = legend_num + "/" + legend_den
    new_plot.customize_plot(title_num, new_legend, "", "")
    return new_plot


def parse_all_arguments():
    from argparse import ArgumentParser
    parser = ArgumentParser()
    parser.add_argument("histograms", help = "Histograms to plot", nargs = "+")

    parser.add_argument("-r", "--ratio", help = "Plot ratio of histo2 / histo1 ([histo3 / histo1], [histo4 / histo1] ", action = "store_true")

    parser.add_argument("-c", "--column", type = int, help = "Override y-column (dy-column = y+1)")
    parser.add_argument("-p", "--png", help = "Prints to PNG.png file instead of showing the plot")
    parser.add_argument("-xs", "--total", help = "Prints total cross section (sum of the plot)", action = "store_true")
# Plot customization
    parser.add_argument("-t", "--title",   default = "", help = "Plot title")
    parser.add_argument("-l", "--legend", default = "", nargs = "+", help = "Legend for main histogram (accept latex input)")
    parser.add_argument("-x", "--xlabel",  default = "", help = "Label for x axis (accept latex input)")
    parser.add_argument("-y", "--ylabel",  default = "", help = "Label for y axis (accept latex input)")
    return parser.parse_args()




if __name__ == "__main__":

    args = parse_all_arguments()

    histograms = []

    if args.legend == "":
        legends = len(args.histograms)*[""]
    elif len(args.legend) == len(args.histograms):
        legends = args.legend
    else:
        print("ERROR: Legends and histogram arguments don't match")
        print("Emptying leyends array")
        legends = len(args.histograms)*[""]

    for filename,legend in zip(args.histograms, legends):
        plot_instance = Plot(filename = filename, column = args.column)
        if args.total:
            plot_instance.print_total()
        plot_instance.customize_plot(args.title, legend, args.xlabel, args.ylabel)
        histograms.append(plot_instance)

    if args.ratio:
        print("Computing ratio of all histograms wrt the first one")
        reference_plot = histograms[0]
        new_plots = []
        new_plots.append(ratio_plots(reference_plot, reference_plot, True))
        for histogram in histograms[1:]:
            new_plots.append(ratio_plots(histogram, reference_plot))
        regular_plot(new_plots, print_to_png = args.png)

    else:
        regular_plot(histograms, print_to_png = args.png)


###################################################################################
################################ Code graveyard ###################################
###################################################################################
# 
# 
#                                          .,,cccd$$$$$$$$$$$ccc,
#                                      ,cc$$$$$$$$$$$$$$$$$$$$$$$$$cc,
#                                    ,d$$$$$$$$$$$$$$$$"J$$$$$$$$$$$$$$c,
#                                  d$$$$$$$$$$$$$$$$$$,$" ,,`?$$$$$$$$$$$$L
#                                ,$$$$$$$$$$$$$$$$$$$$$',J$$$$$$$$$$$$$$$$$b
#                               ,$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$i `$h
#                               $$$$$$$$$$$$$$$$$$$$$$$$$P'  "$$$$$$$$$$$h $$
#                              ;$$$$$$$$$$$$$$$$$$$$$$$$F,$$$h,?$$$$$$$$$$h$F
#                              `$$$$$$$$$$$$$$$$$$$$$$$F:??$$$:)$$$$P",. $$F
#                               ?$$$$$$$$$$$$$$$$$$$$$$(   `$$ J$$F"d$$F,$F
#                                ?$$$$$$$$$$$$$$$$$$$$$h,  :P'J$$F  ,$F,$"
#                                 ?$$$$$$$$$$$$$$$$$$$$$$$ccd$$`$h, ",d$
#                                  "$$$$$$$$$$$$$$$$$$$$$$$$",cdc $$$$"
#                         ,uu,      `?$$$$$$$$$$$$$$$$$$$$$$$$$$$c$$$$h
#                     .,d$$$$$$$cc,   `$$$$$$$$$$$$$$$$??$$$$$$$$$$$$$$$,
#                   ,d$$$$$$$$$$$$$$$bcccc,,??$$$$$$ccf `"??$$$$??$$$$$$$
#                  d$$$$$$$$$$$$$$$$$$$$$$$$$h`?$$$$$$h`:...  d$$$$$$$$P
#                 d$$$$$$$$$$$$$$$$$$$$$$$$$$$$`$$$$$$$hc,,cd$$$$$$$$P"
#             =$$?$$$$$$$$P' ?$$$$$$$$$$$$$$$$$;$$$$$$$$$???????",,
#                =$$$$$$F       `"?????$$$$$$$$$$$$$$$$$$$$$$$$$$$$$bc
#                d$$F"?$$k ,ccc$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$i
#         .     ,ccc$$c`""u$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$P",$$$$$$$$$$$$h
#      ,d$$$L  J$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$" `""$$$??$$$$$$$
#    ,d$$$$$$c,"$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$F       `?J$$$$$$$'
#   ,$$$$$$$$$$h`$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$F           ?$$$$$$$P""=,
#  ,$$$F?$$$$$$$ $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$F              3$$$$II"?$h,
#  $$$$$`$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$P"               ;$$$??$$$,"?"
#  $$$$F ?$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$P",z'                3$$h   ?$F
#         `?$$$$$$$$$$$$$$$??$$$$$$$$$PF"',d$P"                  "?$F
#            """""""         ,z$$$$$$$$$$$$$P
#                           J$$$$$$$$$$$$$$F
#                          ,$$$$$$$$$$$$$$F
#                          :$$$$$c?$$$$PF'
#                          `$$$$$$$P
#                           `?$$$$F
# #!/usr/bin/env python
# 
# from sys import exit
# import numpy as np
# from argparse import ArgumentParser
# 
# parser = ArgumentParser()
# # Required argument
# parser.add_argument("histo1", help = "Main histogram to plot") # type = string by default
# parser.add_argument("histo2", help = "(optional) Second histogram to plot", nargs = '?', default = None) # type = string by default
# # Optional arguments
# parser.add_argument("-t", "--title",   default = "", help = "Plot title")
# parser.add_argument("-l1", "--legen1", default = "", help = "Legend for main histogram (accept latex input)")
# parser.add_argument("-l2", "--legen2", default = "", help = "Legend for secondary histogram (accept latex input)")
# parser.add_argument("-x", "--xlabel",  default = "", help = "Label for x axis (accept latex input)")
# parser.add_argument("-y", "--ylabel",  default = "", help = "Label for y axis (accept latex input)")
# parser.add_argument("-p", "--png", help = "If present prints plot to a PNG.png file instead of showing a window")
# parser.add_argument("-xo", "--xmgrace", help = "Ignore all previous options and output histo1 as three columns of text xmgrace style", action = "store_true")
# parser.add_argument("-r", "--ratio", help = "Plot ratio of histo2 / histo1 ([histo3 / histo1], [histo4 / histo1] ", action = "store_true")
# 
# args = parser.parse_args()
# 
# def ld(a): return "$" + a + "$" # dress input with latex "$"
# 
# ptitle = args.title
# xlabel = ld(args.xlabel)
# ylabel = ld(args.ylabel)
# legen1 = ld(args.legen1) 
# legen2 = ld(args.legen2)
# 
# def xmprint(x,y,z):
#     print('@  s0 type xydy \n')
#     print('@  s0 line linestyle 0\n')
#     print('@type xydy\n')
#     for i,j,k in zip(x,y,z):
#         print(str(i) + " " + str(j) + " " + str(k))
# 
# def unPack(filename):
#     comment = '#'
#     xarray  = 0
#     yarray  = 1
#     dyarray = 2
#     if "NNLO" in filename or "vRa" in filename or "RRa" in filename or "vBa" in filename:
#         xarray  = 1 # column 1 of the .dat file
#         yarray  = 3 
#         dyarray = 4 
#     if ".agr" in filename:
#         comment = '@'
#     dat = np.loadtxt(filename, comments = comment, unpack = True)
#     x   = dat[xarray]
#     y   = dat[yarray]
#     dy  = dat[dyarray]
# 
#     dx      =  abs(x[2] - x[3])
#     totalxs =  sum(y)*dx
#     totaldy =  0.0
#     for i in dy:
#         totaldy += i*i
#     totaldy = np.sqrt(totaldy)*dx
# 
#     print("Results for " + filename)
#     print("Total cross section: " + str(totalxs) + " +/- " + str(totaldy))
#     return x, y, dy
# 
# def plotMe(x, y, dy, title = '', xlabel = '', ylabel = '', legen1 = '', legen2 = '', secondPlot = None):
#     from matplotlib import pyplot as pl
#     pl.rc('text', usetex = True)
#     pl.rc('font', family = 'serif')
#     pl.ticklabel_format(axis='y', style='sci', scilimits=(-2,2))
#     line = pl.errorbar(x, y, xerr=0.0, yerr=dy, fmt='b.', label=legen1)
#     pl.xlabel(xlabel)
#     pl.ylabel(ylabel)
#     pl.title(title)
#     if secondPlot:
#         pl.errorbar(secondPlot[0],secondPlot[1], xerr = 0.0, label=legen2, yerr = secondPlot[2], fmt='r.')
#     pl.legend()
#     if args.png:
#         pl.savefig(args.png + '.png', bbox_inches = 'tight')
#     else:
#         pl.show()
#     pl.close()
#     return
# 
# ## Assumes files 1 and 2 are the two files to compare
# ## Reads all columns from the files assumming spaces as delimiters
# file1 = args.histo1
# x1,y1,dy1 = unPack(file1)
# if args.xmgrace:
#     xmprint(x1,y1,dy1)
#     exit()
# if args.histo2:
#     file2 = args.histo2
#     x2,y2,dy2 = unPack(file2)
#     plot2 = [x2,y2,dy2]
# else:
#     plot2 = None
# 
# plotMe(x1,y1,dy1, ptitle, xlabel, ylabel, legen1, legen2, secondPlot = plot2)
