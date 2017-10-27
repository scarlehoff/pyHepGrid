#!/usr/bin/env python3
from __future__ import division, print_function
import argparse as ap
import combine_output as co
import configparser as cf
import datetime
import glob
import itertools as it
from multiprocessing import Pool, Process, Manager
import operator
import os
import sys

observables = ["cross"]
output_dir = "output"
all_observables = False  # Overrides list of observables to check


def setup_argparser():
    parser = ap.ArgumentParser(
        description='Parses a NNLOJET combine config file and creates results without Tom\'s method of optimising errors.')
    parser.add_argument('-C', "--config", nargs=1,
                        help='The config file containing the steering information',
                        required=True)
    parser.add_argument('-v', "--verbose",
                        help='Enable verbose mode',
                        action="store_true")   
    parser.add_argument('-a', "--all_obs",
                        help='Run for observables that can be found',
                        action="store_true", default=False)    
    parser.add_argument('-p', "--preserve",
                        help='Preserve channel sum = Total',
                        action="store_true", default=False)    
    parser.add_argument('-j', '--cores', type=int, 
                        nargs='?', action='store', 
                        default='1',
                        help='Specifies the number of cores to run simultaneously.')
    parser.add_argument('-o', '--output',
                        help='Specifies the output directory.')
    args = parser.parse_args()
    return args


def add_files(pt):
    mappt = add_files.parser.get('Parts', pt)
    directory = os.path.join(add_files.parser.get('Paths', 'raw_dir'), pt)
    print("> Scanning for files in {0}".format(directory))
    return (mappt, glob.glob(directory +'/*.dat', recursive=add_files.qrecursive))


def setup_configparser(args):
    parser = cf.ConfigParser(allow_no_value=True, delimiters=('=', ':'), 
                             comment_prefixes=('#',), inline_comment_prefixes=('#',), 
                             empty_lines_in_values=False)
    parser.optionxform = lambda option: option  # do not convert to lower case
    parser.read(args.config)
    observables = parser.options('Observables')

    qrecursive = parser.getboolean('Options', 'recursive', fallback=False)

    print(">> Scanning for files:")
    
    # for pt in parser.options('Parts'):
    #     add_files_to_dict(components, pt)
    # zip(it.repeat(args), obs
    add_files.parser = parser
    add_files.qrecursive = qrecursive
    pool = Pool(min(args.cores, len(parser.options('Parts'))))
    components = dict(pool.map(add_files, parser.options('Parts')))
    pool.close()
    pool.join()

    if "ALL" in observables:
        observables = get_all_observables(components)

    return components, observables


class FileList():
    def __init__(self, files, obs, component, output_dir):
        self.observable = obs
        self.files = [xfile for xfile in files if obs in xfile]
        self.outfile = os.path.join(output_dir, component+"."+obs+".dat")

    def combine(self, verbose, preserve):
        print("> Combining files for {0}.".format(self.outfile))
        co.combine_output_API(self.outfile, self.files, True, preserve, verbose)

    def get_outfile(self):
        return self.outfile


class FileListComposite():
    def __init__(self, output_dir, *args):
        self.files = [x.get_outfile() for x in args]
        if [x.observable for x in args].count(args[0].observable) == len(args):
            self.observable = args[0].observable
        else:
            raise Exception("You're trying to combine different observables. Please stop.")
        self.outfile = os.path.join(output_dir, "Full."+self.observable+".dat")

    def combine(self, verbose, preserve):
        print("> Combining files for {0}.".format(self.outfile))
        co.combine_output_API(self.outfile, self.files, False, preserve, verbose)
        

def get_all_observables(components):
    allfiles = []
    for component in components:
        allfiles += components[component]
    observables = list(set([x.split(".")[-3] for x in allfiles]))
    print(">> Observable list: {0}".format(" ".join(i for i in observables)))
    return observables


def combine_observable(args, obs, components, outdir):
    print(">> Starting Observable: {0}".format(obs))
    componentlist = [FileList(components[component], obs, component, outdir) 
                     for component in components]
    for component in componentlist:
        component.combine(args.verbose)
    Fullfiles = FileListComposite(*componentlist)
    Fullfiles.combine(args.verbose)


def combine_component(args, obs, component, outdir):
    print(">> Combining Observable {0}, Component {1}.".format(obs, component))
    retval = FileList(components[component], obs, component, outdir) 
    retval.combine(args.verbose, args.preserve)
    return retval


def combine_composite(args, file_list, outdir):
    Fullfiles = FileListComposite(outdir, *file_list)
    Fullfiles.combine(args.verbose, args.preserve)


def format_timedelta(a):
    a = a.total_seconds()
    hours, remainder = divmod(a, 3600)
    mins, secs = divmod(remainder, 60)
    formatted = '{0:0>2.0f}:{1:0>2.0f}:{2:0>2.0f}'.format(hours, mins, secs)
    return formatted


if __name__ == "__main__":
    start_time = datetime.datetime.now()
    args = setup_argparser()

    if args.output is not None:
        output_dir = args.output

    components, observables = setup_configparser(args)
    all_observables = args.all_obs

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    # if all_observables:
    #     observables = get_all_observables(components)

    obs = [x[0] for x in it.product(observables,components)]
    comp = [x[1] for x in it.product(observables,components)]

    pool = Pool(min(args.cores, len(obs)))
    output = pool.starmap(combine_component, zip(it.repeat(args), obs, comp, 
                                                 it.repeat(output_dir)))
    
    get_attr = operator.attrgetter("observable")
    group_by_obs = [list(g) for k,g in it.groupby(sorted(output, key=get_attr), get_attr)]

    pool.starmap(combine_composite, zip(it.repeat(args),group_by_obs, it.repeat(output_dir)))
    pool.close()
    pool.join()

    end_time = datetime.datetime.now()
    total_time = format_timedelta(end_time - start_time)
    print(">> Combine complete.")
    print(">> Total time taken: {0}".format(total_time))
