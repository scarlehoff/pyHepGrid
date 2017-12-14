#!/usr/bin/env python3
import argparse
import header
import subprocess as sp
import sys

def get_ce(line):
    return line.split()[0].strip()

def get_idx(string, inlist):
    for idx, val in enumerate(inlist):
        if string in val:
            return(idx)

def good_site_present(line, goodelements):
    for ge in goodelements:
        if ge in line:
            return True
    return False

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--all", "-a",
                        help = "Look at all ces, not just known good ones", 
                        action = "store_true",
                        default = False)
    parser.add_argument("--rev", "-r",
                        help = "Reverse output", 
                        action = "store_false",
                        default = True)
    parser.add_argument("--sort", "-s",
                        help = "Sort by given attribute. Case insensitive, and will sort by first attribute that matches the provided string (e.g -s fre will match attribute Free.)", 
                        nargs = "+",
                        default = False)
    parser.add_argument("--print_sort_possibilities","-psp",
                        help="Print all attributes that you can sort by. There will be some duplicates.",
                        action = "store_true")
    args = parser.parse_args()
    return args

class CE_Data():
    def __init__(self, line):
        splitln = line.split()
        self.CPU = int(splitln[0])
        self.TotalCPU = int(splitln[0])
        self.Free = int(splitln[1])
        self.TotJobs = int(splitln[2])
        self.TotalJobs = int(splitln[2])
        self.Running = int(splitln[3])
        self.Waiting = int(splitln[4])
        self.CE = splitln[5].split(":")[0]

    def __repr__(self):
        # Should probably be cleaned up...
        def addline(name, val, colour, total=None):
            if val > 0:
                if total is not None:
                    val = "{0}/{1}".format(val,total)
                    return "{0}{1}: {2:10}  \033[0m".format(colour, name, val)
                return "{0}{1}: {2:5}  \033[0m".format(colour, name, val)
            elif val <0:
                if total is not None:
                    val = "{0}/{1}".format(val,total)
                    return "{0}{1}: {2:10}  \033[0m".format('\033[91m', name, val)
                return "{0}{1}: {2:5}  \033[0m".format('\033[91m', name, val)
            else:
                if total is not None:
                    val = "{0}/{1}".format(val,total)
                    return  "{0}: {1:10}  ".format(name, val)
                return  "{0}: {1:5}  ".format(name, val)

        string = "{0:33} ".format(self.CE)
        string += addline("Free CPUs", self.Free, '\033[92m', 
                          total = self.CPU)
        string += addline("Waiting", self.Waiting, '\033[93m')
        string += addline("Running", self.Running, '\033[94m')
        string += "Total: {0:4}".format(self.TotJobs)
        return string


def get_ces(all_ces):
    with open(header.ce_listfile) as cefile:
        celines = cefile.readlines()

    if not all_ces:
        good_idx=get_idx("Known to work", celines)+1
        end_good_idx=get_idx("Known NOT to work", celines)
        celines = celines[good_idx:end_good_idx]
    good_elements = [get_ce(line) for line in celines 
                     if "." in line and len(line)>0]

    result = sp.Popen(["lcg-infosites","ce","--vo","pheno"], 
                      stdout=sp.PIPE, stderr=sp.PIPE)
    out, err = result.communicate()
    site_info = str(out).split("\\n")
    site_info = [si.replace("\\t","   ") for si in site_info]
    site_info = [CE_Data(si) for si in site_info if 
                 good_site_present(si, good_elements)]
    return site_info

def get_most_free_cores():
    """API for main.py to link in"""
    site_info = get_ces(False)
    site_info = sorted(site_info, 
                       key=lambda x: getattr(x,"Free"), reverse = False)
    return site_info[-1].CE

if __name__ == "__main__":
    args = get_args()
    site_info = get_ces(args.all)
    sortval = "Free"

    if args.print_sort_possibilities:
        attributes = [i for i in dir(site_info[0]) if not i.startswith("__") 
                      and not callable(getattr(site_info[0], i))]
        for attr in attributes:
            print(attr)
        sys.exit(0)

    if args.sort:
        attributes = [i for i in dir(site_info[0]) if not i.startswith("__") 
                      and not callable(getattr(site_info[0], i))]
        for attribute in attributes:
            if args.sort[0].lower() in attribute.lower():
                sortval = attribute
                break

    site_info = sorted(site_info, key=lambda x: getattr(x,sortval), 
                       reverse = args.rev)
    for site in site_info:
        print(site)
