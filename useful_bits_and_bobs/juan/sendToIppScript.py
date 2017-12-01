#!/usr/bin/env python3

import subprocess as sp
from datetime import datetime
import sys

socket_exe = "vegas_socket.py"

def parse_cmdl_arguments():
    import argparse

    parser = argparse.ArgumentParser()

    parser.add_argument("executable", help = "NNLOJET executable")
    parser.add_argument("runcard", help = "runcard.run")
    parser.add_argument("dN", help = "target computer", nargs = "*")

    parser.add_argument("-nR","--noRetrieval", help = "Don't fill up retrieval script", action = "store_true")
    parser.add_argument("-nE","--noExecution", help = "Send the stuff to the sandbox but dont execute", action = "store_true")
    parser.add_argument("-s", "--socket", help = "Send socketed run as many sockets as computers given", action = "store_true")
    parser.add_argument("-p", "--port", help = "Send socketed run as many sockets as computers given", default = "8888")
    parser.add_argument("-H", "--host", help = "Send socketed run as many sockets as computers given", default = "d20")
    parser.add_argument("-r", "--remoteExe", help = "Use remote executable as given, don't tar NNLOJET or send it to remote server", action = "store_true")
    parser.add_argument("-c", "--cores", help = "Number of cores to use", default = "6")
    parser.add_argument("-w", "--warmup", help = "Warmup grid to use")

    # Clean up function (overrides everything)
    class KillAction(argparse.Action):
        def __call__(self, parser, namespace, values, option_string=None):
            for remote_host in values: 
                tmux_kill_all(remote_host)
            parser.exit()
    parser.add_argument("-k", "--kill", help = "Kills tmux servers on all given computers. Ignores all other arguments", nargs = "*", action = KillAction)

    args = parser.parse_args()

    # Some checks
    if len(args.dN) != len(set(args.dN)):
        y = input("You are sending jobs to repeated desktops. Do you want to continue? (y/n)")
        if y[0].lower() != "y":
            exit(-1)

    return args

def tmux_cmd(tms, cmd):
    return "tmux send -t {0} {1} ENTER".format(tms, cmd)

def tmux_kill_session(tms, target_computer):
    return send_command("tmux kill-session -t {}".format(tms), target_computer)

def tmux_kill_all(target_computer):
    print("Killing tmux server at {}".format(target_computer))
    return send_command("tmux kill-server", target_computer)

def tmux_check_session(tms, target_computer):
    result = send_command("tmux has-session -t {}".format(tms), target_computer)
    if result.returncode == 0:
        return True
    else:
        return False

def send_command(command, target_computer):
    ssh_cmd = ["ssh", target_computer, command]
    return sp.run(ssh_cmd, stdout=sp.DEVNULL, stderr=sp.DEVNULL)

def copy_to_ippp(path_original, path_destination, target_computer = "login", enforce = False):
    print("Sending {0} to {1}".format(path_original, path_destination))
    if enforce:
        print("Creating remote directory {0} at {1}".format(path_destination, target_computer))
        enforce_folder_cmd = "mkdir -p {}".format(path_destination)
        send_command(enforce_folder_cmd, target_computer)
    target_destination = "{0}:{1}".format(target_computer, path_destination)
    scp_cmd = ["scp", "-r", path_original, target_destination]
    return sp.run(scp_cmd, stdout=sp.DEVNULL, stderr=sp.DEVNULL)

script_name = "runScript.sh"

run_template = """
#!/bin/bash
runme() {{
    # Initialisation
    runname={0}-N  # runname
    basedailypath={1}/$runname  # daily_path
    nplug=$(receiverScript.py $basedailypath)
    dailypath=$basedailypath$nplug

    cwdlaptop={2} # where was this called from (laptop folder)

    mkdir -p $dailypath
    mv {3} $dailypath # name of the incoming tar file
    cd $dailypath

    tar xfz {3}
    OMP_NUM_THREADS={4} {5} -run {6} {7}

    if [[ "{5}" != *\/* ]] 
    then
        rm -f {5}
    fi
    rm -f {3}

    {8}

    # Fill in retrieveAll script
    echo cd $cwdlaptop >> {9}
    echo scp \\$ippp:$dailypath/tmp.tar.gz tmp.tar.gz >> {9}
    echo tar xfz tmp.tar.gz >> {9}
    echo rm tmp.tar.gz >> {9}

    echo "Runfolder: " ${dailypath}

}}
runme
"""

run_sandbox = """
#!/bin/bash
runme() {{
    # Initialisation
    tar xfz {3}
    OMP_NUM_THREADS={4} {5} -run {6} {7}

    if [[ "{5}" != *\/* ]] 
    then
        rm -f {5}
    fi
    rm -f {3}
}}
runme
"""

class Run_NNLOJET:

    def __init__(self, executable, runcard, n_threads = 6, remote_folder = "$HOME/laptopFolder"):
        self.exe_file = executable
        self.exe = "./" + executable.split("/")[-1]
        self.runcard = runcard
        self.n_sockets = None
        self.sockets = ""
        self.n_threads = n_threads


        self.daily_path = self._generate_daily_path(remote_folder)
        self.remote_folder = remote_folder
        self.tar_in = "tmpIN.tar.gz"
        self.tar_retrieve = "tar -czf tmp.tar.gz *"
        self.retrieve_file = remote_folder + "/toLaptop/retrieveAll"
        self.script_template = run_template

    def _generate_daily_path(self, base_path, base_folder = "fromLaptop"):
        today = datetime.now().strftime("%Y/%B/%d") 
        daily_path = base_path + "/" + base_folder + "/" +today
        return daily_path

    def _get_throwback_dir(self):
        from os import getcwd
        return getcwd()

    def _generate_runname(self):
        runname = ""
        with open(self.runcard, 'r') as f:
            for line in f:
                if "Job name id" in line:
                    runname = line.split('!')[0].strip()
        return self.runcard + "-" + runname

    def _generate_runscript(self, script_file):
        run_script = self.script_template.format(self._generate_runname(), self.daily_path,
                self._get_throwback_dir(), self.tar_in,
                self.n_threads, self.exe, self.runcard, self.sockets,
                self.tar_retrieve, self.retrieve_file)
        f = open(script_file, 'w')
        f.write(run_script)
        f.close()
        sp.call(["chmod", "-x", script_file])

    def _generate_socketline(self, socket_num):
        self.sockets = "-sockets {0} -ns {1} -host {2} -port {3}".format(
                self.n_sockets, socket_num, self.host, self.port)
        if socket_num == 1:
            self.script_template = run_template
        else:
            self.script_template = run_sandbox

    def _fire_up_server(self, host, port, n_sockets):
        tms = "socket-server"
        counter = 0
        while tmux_check_session(tms, host):
            counter += 1
            tms = "socket-server-{}".format(counter)
        tmux_creation = "tmux new-session -d -s {0}".format(tms)
        send_command(tmux_creation, host)
        cmd = "\'{0} -N {1} -p {2}\'".format(socket_exe, n_sockets, port)
        tmux_run = tmux_cmd(tms, cmd)
        send_command(tmux_run, host)

    def send_and_run(self, target_computer, execute = True, sandbox_folder = "sandbox", socket_num = None):
        if sandbox_folder.startswith("/"):
            sandbox = sandbox_folder
        else:
            sandbox = self.remote_folder + "/" + sandbox_folder
        copy_to_ippp(self.tar_in, sandbox, target_computer = target_computer, enforce = True)

        if self.n_sockets:
            self._generate_socketline(socket_num)

        self._generate_runscript(script_name)
        copy_to_ippp(script_name, sandbox, target_computer = target_computer)

        if execute:
            tms = "laptop-run"
            counter = 0
            while tmux_check_session(tms, target_computer):
                counter += 1
                tms = "laptop-run-{}".format(counter)
            tmux_creation = "tmux new-session -d -s {0}".format(tms)
            send_command(tmux_creation, target_computer)

            cmd = "\'cd {0} && date && chmod +x {1} && ./{1} \'".format(sandbox, script_name)
            tmux_run = tmux_cmd(tms, cmd)
            send_command(tmux_run, target_computer)


    def activate_sockets(self, n_sockets, host = "d20", port = "8888"):
        print(" > Activating sockets")
        self.n_sockets = n_sockets
        self.host = host
        self.port = port
        self._fire_up_server(host, port, n_sockets)

    def tar_everything(self, extra_files = [], remote_exe = None):
        if remote_exe:
            files = [self.runcard] + extra_files
            self.exe = self.exe_file
        else:
            files = [self.exe_file, self.runcard] + extra_files
        sp.call(["tar", "-czf", self.tar_in] + files)

    def no_retrieval(self):
        self.tar_retrieve = ""
        self.retrive_file = "/dev/null"

    def clean_up(self):
        # Remove local version of runscript and tar file
        sp.call(["rm", script_name, self.tar_in])


if __name__ == "__main__":
    args = parse_cmdl_arguments()

    # Compulsory arguments
    if args.executable == "IPPP":
        nnlojet_exe = "/mt/home/jmartinez/Coding/driver/NNLOJET"
        remote_exe = True
    else:
        nnlojet_exe = args.executable
        remote_exe = args.remoteExe
    target_computer_list = args.dN
    runcard_name = args.runcard

    nnlojet = Run_NNLOJET(nnlojet_exe, runcard_name,
            n_threads = args.cores)

    if args.warmup:
        warmup_file = [args.warmup]
    else:
        warmup_file = []
    nnlojet.tar_everything(warmup_file, remote_exe)

    if args.noRetrieval:
        nnlojet.no_retrieval()

    if args.noExecution:
        exe = False
    else:
        exe = True

    if args.socket:
        socket_num = len(target_computer_list)
        nnlojet.activate_sockets(socket_num, port = args.port, host = args.host)
    else:
        socket_num = 1

    sandbox = "sandbox"
    for i in range(socket_num):
        target_computer = target_computer_list[i]

        if (i > 0):
            sandbox = "/scratch/jcruzmartinez/sandbox_{}".format(i)

        nnlojet.send_and_run(target_computer, exe, 
                sandbox_folder = sandbox, socket_num = (i+1))

    nnlojet.clean_up()


    print("Done!")
    print("Running at IPPP computers {}".format(' '.join(args.dN)))
