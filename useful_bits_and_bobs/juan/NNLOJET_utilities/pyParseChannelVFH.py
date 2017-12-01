#!/usr/bin/env python3

from argparse import ArgumentParser
parser = ArgumentParser(description="Note: this script assumes there is a $HOME/NNLOJET/driver directory")
parser.add_argument("runcard", help = "Runcard to copy and modify")
parser.add_argument("mode", help = "doble real / real / real virtual / virtual / tree ")
parser.add_argument("-p", "--process", help = "Process of interest", default = "VFH")
parser.add_argument("-c", "--channel", help = "Channel to care about", default = "all")
args = parser.parse_args()

state_dict = {
        'qq' : [],
        'qqb' : [],
        'qbq' : [],
        'qbqb' : [],
        'qg' : [],
        'gq' : [],
        'qbg' : [],
        'gqb' : [],
        'gg' : []
        }

qtype = ['u', 'q', 'd', 'R', 'Q']
qbtype = [i + 'b' for i in qtype]
re_channel = "(?<=ip\()[0-9]+(?=\))"

def sanitise_label(parton):
    if parton in qtype:
        return 'q'
    elif parton in qbtype:
        return 'qb'
    elif parton == 'g' or parton == 'gt':
        return 'g'
    else:
        raise Exception("Unkown parton: [" + parton + "]")

def generate_runcard(runcard_list, process_list, runcard_name):
    # Input: runcard to write down in the form of a list up to and including CHANNELS
    # The list of processes to write in the new runcard
    # The name of the new runcard 
    with open(runcard_name, 'w') as f:
        f.writelines(runcard_list)
        f.writelines(process_list)
        f.write("END_CHANNELS")

## Script start
import re
from os import environ, path

process = args.process
home_dir = environ.get("HOME")
select_channel = home_dir + "/NNLOJET/driver/process/" + process + "/selectchannel" + process + ".f"
if not path.isfile(select_channel):
    print("We could not find " + select_channel)
    print("Note: this script assumes there is a $HOME/NNLOJET/driver directory")

active = False
search_string = " " + args.mode + " "
with open(select_channel, 'r') as f:
    for line in f:
        if search_string in line:
            active = True
        elif active and "ME" in line:
            active = False
            break
        if not active:
            continue
        if ".true." in line:
            if "to" in line:
                re_state = "(?<!\s)[\w\s]+(?=\sto)"
            elif "subtraction" in line:
                re_state = "(?<!\s)[\w\s]+(?=\ssubtraction)"
            else:
                continue
            # Get the initial parton corresponding to that line of selectchannel
            get_initial = re.search(re_state, line).group()
            initial_partons = get_initial.strip().split(" ")
            parton_1 = sanitise_label(initial_partons[0])
            parton_2 = sanitise_label(initial_partons[1])
            initial_partons_sanitised = parton_1 + parton_2
            # Get the channel number corresponding to said initial partons
            channel = re.search(re_channel, line).group()
            # And fill the corresponding member of the dictionary
            state_dict[initial_partons_sanitised].append(channel + "\n")
        else:
            continue

# Write it down to the runcard
runcard_file = []
runcard_name = args.runcard
with open(runcard_name, 'r') as f:
    for line in f:
        runcard_file.append(line)
        if line.strip() == "CHANNELS":
            break

if args.channel == "all":
    for key in state_dict.keys():
        process_list = state_dict[key]
        if process_list:
            new_runcard = key + "_" + runcard_name
            generate_runcard(runcard_file, process_list, new_runcard)
else:
    new_runcard = args.channel + "_" + runcard_name
    generate_runcard(runcard_file, state_dict[args.channel], new_runcard)
 


