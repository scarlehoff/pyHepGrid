#!/usr/bin/python3
import os
import argparse as ap
import sys
sys.path.append(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))))  # Yuck


def get_args():
    parser = ap.ArgumentParser()
    parser.add_argument("runcard", help="Grid scripts runcard to run")
    args = parser.parse_args()
    return args


import src.argument_parser as internal_args  # noqa: E402
internal_args.runcard = get_args().runcard
import src.header as header  # noqa: E402
import src.socket_api as socket_api  # noqa: E402
import src.Backend as backend  # noqa: E402

serverloc = header.socket_exe
killserverloc = header.slurm_kill_exe
desktop_list = header.desktop_list
tot = len(desktop_list)
port = header.port
host = header.server_host
nice_priority = 10


def create_new_session_with_server(name, serverloc, tot, host, port):
    servercommand = "{0} -N {1} -H {2} -p {3}".format(
        serverloc, tot, host, port)
    os.system(
        "TMUX= tmux new-session -d -s {0} 'python3 {1}'".format(
            name, servercommand))
    return name


def create_NNLOJET_instance(port, host, no, tot, window_name, runcard,
                            desktop, runloc):
    socket_info = "-port {0} -sockets {3} -host {1} -ns {2}".format(
        port, host, no+1, tot)
    runcmd = "'ssh {0} cd {1} && nice -n {5} ./NNLOJET -run {2} {3} " +\
             "&& tmux kill-session -t {4}'"
    runcmd = runcmd.format(desktop, runloc, runcard,
                           socket_info, window_name, nice_priority)
    os.system(
        "tmux split-window -c {0} -t {2}: {1}".format(runloc, runcmd,
                                                      window_name))
    arrange_tmux_window(window_name)


def arrange_tmux_window(window_name):
    os.system(
        "tmux select-layout -t {0}:0 tiled >/dev/null 2>&1".format(window_name))


def get_unblocked_port(host, port):
    while socket_api.check_port_blocked(host, port):
        port += 1
    return port


def get_window_name(host, runcard, port):
    short_host = host.split(".")[0]
    window_name = "_".join([short_host, runcard, str(port)]).replace(".run", "")
    return window_name


def do_single_run(host, port, runcard, serverloc, tot, desktop_list,
                  run_directory):
    valid_port = get_unblocked_port(host, port)
    print("Using port {0}".format(valid_port))

    window_name = get_window_name(host, runcard, valid_port)
    create_new_session_with_server(
        window_name, serverloc, tot, host, valid_port)
    print("Server up and running at {0}:{1}".format(host, valid_port))
    print("TMUX window name: {0}".format(window_name))

    for no, desktop in enumerate(desktop_list):
        create_NNLOJET_instance(valid_port, host, no, tot,
                                window_name, runcard, desktop, run_directory)
        print(
            "NNLOJET instance [{0}/{1}] submitted to {2}.".format(no+1, tot,
                                                                  desktop))
    arrange_tmux_window(window_name)


if __name__ == "__main__":
    for runcard in header.dictCard:
        backend = backend.Backend()
        tag = header.dictCard[runcard]
        run_directory = backend.get_local_dir_name(runcard, tag)
        do_single_run(host, port, runcard, serverloc,
                      tot, desktop_list, run_directory)
