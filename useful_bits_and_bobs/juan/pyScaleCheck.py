#!/usr/bin/env python3

import sys

# Parameters that can be overriden in the runcard

# Physical parameters 
N = 3.0 ; CA = N ; Tr = 1.0/2.0; Cf = (N**2 - 1.0)/(2.0*N)
# Define some global defaults
png_basename = "scales"
plot_basetitle = ""
legend = "lower right"

def parse_all():
    import argparse

    parser = argparse.ArgumentParser()

    parser.add_argument("data_file", help = "File with all the data")

    # Defaults are for VBF
    parser.add_argument("-a", "--alpha_lo", help = "Order of alpha at LO", type = int, default = 0)
    parser.add_argument("--pdf", help = "PDF set to compute alpha s from", default = "NNPDF30_nnlo_as_0118")
    parser.add_argument("-q", "--quiet", help = "Only print intermediate steps on failure", default = True)
    parser.add_argument("-c", "--colour", help = "Colour level: leading, nf, all", default = "all")
    parser.add_argument("-p", "--plot", help = "Plot results at the end using matplotlib", default = True)
    parser.add_argument("-f", "--nf", help = "Number of flavours", type = int, default = 5)

    args = parser.parse_args()

    return args

def what_to_do(nlo, nnlo):
    if not nlo or nlo == 0:
        do_nlo = False
    else:
        do_nlo = True
    if not nnlo or nnlo == 0:
        do_nnlo = False
    else:
        do_nnlo = True
        for key in nnlo:
            nnlo[key] != 0.0
            break
    return do_nlo, do_nnlo

def read_runcard(runcard):
    vessel = {}
    with open(runcard) as source_file:
        exec(source_file.read(), vessel)
    return vessel

def populate_variables(args):
    runcard_raw = args.data_file
    runcard = read_runcard(runcard_raw)

    keys = runcard.keys()
    if "n" in keys:
        n = runcard["n"]
    else:
        runcard["n"] = args.n

    if "quiet" in keys:
        quiet = runcard["quiet"]
    else:
        runcard["quiet"] = args.quiet

    if "colour" in keys:
        colour = runcard["colour"]
    else:
        runcard["colour"] = args.colour

    if "plotFl" in keys:
        plotFl = runcard["plotFl"]
    else:
        if args.plot:
            runcard["plotFl"] = 1
        else:
            runcard["plotFl"] = 0

    if "mu0" in keys:
        mu0 = runcard["mu0"]
        if mu0 not in muR:
            runcard["muR"] = runcard["muR"] + [mu0]
    runcard["muR_lst"] = runcard["muR"]
    del runcard["muR"]

    slo = {}
    snlo = {}
    snnlo = {}
    if "slo" in keys:
        slo = runcard["slo"]
        if slo == 0:
            for mu in muR:
                slo[mu] = 0.0
    else:
        for mu in muR:
            slo[mu] = 0.0
        runcard["slo"] = slo
    if "snlo" in keys:
        snlo = runcard["snlo"]
    else:
        runcard["snlo"] = None
    if "snnlo" in keys:
        snnlo = runcard["snnlo"]
    else:
        runcard["snnlo"] = None

    return runcard

def get_betas(colour, Nf):
    if colour == 'all':
        ## Full betaNot &  betaOne
        betaNot = (11.0*CA - 4.0*Tr*Nf)/6.0
        betaOne = (17.0*CA**2 - 10.0*CA*Tr*Nf - 6.0*Cf*Tr*Nf)/6.0
    elif colour == 'leading': 
        ## Colour leading betaNot & betaOne
        betaNot = (11.0*CA)/6.0
        betaOne = (17.0*CA**2)/6.0
    elif colour == 'nf':
        # all nf contributions
        betaNot = (- 4.0*Tr*Nf)/6.0
        betaOne = (- 10.0*CA*Tr*Nf - 6.0*Cf*Tr*Nf)/6.0
    else:
        print("You must select all, nf or leading as your colour level choice")
    return betaNot, betaOne


class value:
    def __init__(self,datalist):
        try:
            self.value = datalist[0]
            self.err = datalist[1]
        except: # backwards compability
            self.value = datalist
            self.err = 0.0
    def __add__(self,other):
        val = self.value + other.value
        err = (self.err**2 + other.err**2)**0.5
        return value([val,err])
    def __sub__(self,other):
        val = self.value - other.value
        err   = (self.err**2 + other.err**2)**0.5
        return value([val,err])
    def __mul__(self,other):
        if type(other) in [float,int]:
            val = self.value*other
            err =  self.err*other
        else:
            raise Exception('Unknown type in value class multiplication: '+type(other))
        return value([val,err])
    def __rmul__(self,other):
        return self.__mul__(other)
    def __str__(self):
        return str(self.value)+' '+'+-'+' '+str(self.err)
    def __repr__(self):
        return str(self.value)+' '+'+-'+' '+str(self.err)

def get_alphas(pdfset, scales):
    import lhapdf
    from numpy import pi
    pdfset = lhapdf.mkPDF(pdfset, 0)
    alpha_s = {}
    for muR in scales:
        alpha_s[muR] = pdfset.alphasQ(muR)/2.0/pi
    return alpha_s

def doLr (muR, mu0):
    from numpy import log
    muR2 = 1.0*muR**2
    mu02 = 1.0*mu0**2
    return log(muR2/mu02)

def results_agree(res1, res2):
    diff = abs(res1.value - res2.value)
    err = (res1.err + res2.err)
    if diff > err:
        return False
    else:
        return True

def make_value(dictionary):
    new_d = {}
    for key in dictionary:
        new_d[key] = value(dictionary[key])
    return new_d

def plot_me(scales, th, nnlojet, title, level, pngname = "scales", legend = "lower right"):
    from matplotlib import pyplot
    x_val = scales
    th_val = []
    th_err = []
    nnlojet_val = []
    nnlojet_err = []
    for i in scales:
        th_val.append(th[i].value)
        th_err.append(th[i].err)
        nnlojet_val.append(nnlojet[i].value)
        nnlojet_err.append(nnlojet[i].err)

    pyplot.errorbar(x_val, th_val, xerr=0.0, yerr=th_err, fmt='bo', mfc = 'none', label = 'scales.pdf', ms = 9)
    pyplot.errorbar(x_val, nnlojet_val, xerr=0.0, yerr=nnlojet_err, fmt='rx', label = 'NNLOJET', ms = 9)
    xxx = abs(x_val[1] - x_val[0])
    pyplot.xlim((min(x_val) - xxx, max(x_val) + xxx))
    pyplot.ylabel('$\sigma$ (fb)')
    pyplot.xlabel('$\mu_R$ (GeV)')
    pyplot.title(title + " " + level)
    pyplot.legend(loc=legend)

    pngfile = "{0}_{1}.png".format(pngname, level)
    pyplot.savefig(pngfile, bbox_inches = 'tight')
    pyplot.close()


if __name__ == "__main__":

    args = parse_all()

    runcard_vessel = populate_variables(args)
    do_nlo, do_nnlo = what_to_do(runcard_vessel["snlo"], runcard_vessel["snnlo"])

    this_file = sys.modules[__name__]
    for i in runcard_vessel:
        setattr(this_file, i, runcard_vessel[i])

    beta_not, beta_one = get_betas(colour, args.nf)

    mu0 = muR_lst[0]

    alpha_s = get_alphas(args.pdf, muR_lst)
    c_alpha = alpha_s[mu0]

    # Make the dictionaries into values and separate central values
    slo = make_value(slo)
    c_lo = slo[mu0]
    if do_nlo:
        snlo = make_value(snlo)
        c_nlo = snlo[mu0]
    if do_nnlo:
        snnlo = make_value(snnlo)
        c_nnlo = snnlo[mu0]

    ## Compute th values
    th_lo = {}
    th_nlo = {}
    th_nnlo = {}


    for muR in muR_lst:
        my_alpha = alpha_s[muR]
        my_lr = doLr(muR, mu0)

        alpha_frac = my_alpha/c_alpha
        n_nlo = n + 1
        n_nnlo = n + 2

        # Leading Order
        my_lo = slo[muR]
        th_lo[muR] = c_lo*alpha_frac**n

        # Next to Leading Order
        if do_nlo:
            my_nlo = snlo[muR]
            tmp = value([0.0, 0.0])
            tmp += c_nlo*alpha_frac**n_nlo
            tmp += my_lr*n*beta_not*(my_alpha*my_lo)
            th_nlo[muR] = tmp

        # Next to Next to leading order
        if do_nnlo:
            tmp = value([0.0, 0.0])
            tmp += c_nnlo*alpha_frac**n_nnlo
            tmp += my_lr*(n+1)*beta_not*c_nlo*(my_alpha**n_nnlo/c_alpha**n_nlo)
            tmp += my_lr*n*beta_one*(my_alpha**2*my_lo)
            tmp += my_lr**2*(n*(n+1.0)/2.0)*beta_not**2*(my_alpha**2*my_lo)
            th_nnlo[muR] = tmp

    # Check whether the theoretical results and NNLOJET agree
    failure = False
    for muR in muR_lst:
        if not results_agree(th_lo[muR], slo[muR]):
            print(" > Results are different @ LO for muR = {}".format(muR))
            print(" >  > Expected result: {}".format(th_lo[muR]))
            print(" >  > NNLOJET result: {}".format(slo[muR]))
            failure = True
        if do_nlo and not results_agree(th_nlo[muR], snlo[muR]):
            print(" > Results are different @ NLO for muR = {}".format(muR))
            print(" >  > Expected result: {}".format(th_nlo[muR]))
            print(" >  > NNLOJET result: {}".format(snlo[muR]))
            failure = True
        if do_nnlo and not results_agree(th_nnlo[muR], snnlo[muR]):
            print(" > Results are different @ NNLO for muR = {}".format(muR))
            print(" >  > Expected result: {}".format(th_nnlo[muR]))
            print(" >  > NNLOJET result: {}".format(snnlo[muR]))
            failure = True
    if not failure:
        print("  ")
        print(" > > > EVERYTHING WORKS!")
        print("  ")

    if plotFl == 1:
        if do_nlo:
            plot_me(muR_lst, th_nlo, snlo, plot_basetitle, "NLO", pngname = png_basename, legend = legend)
        if do_nnlo:
            plot_me(muR_lst, th_nnlo, snnlo, plot_basetitle, "NNLO", pngname = png_basename, legend = legend)

