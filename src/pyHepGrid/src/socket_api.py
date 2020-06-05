""" Utilities for running socketed runs """

# If you are planning to use sockets
# I suggest using ssh-copy-id to make sure you don't have to input your password
# many times
import subprocess as sp


def send_command(cmd, target_host):
    ssh_cmd = ["ssh", target_host, cmd]
    return sp.call(ssh_cmd, stdout=sp.DEVNULL)

# Tmux Wrapper


class Tmux:

    def __init__(self, session_name, target_host, create_new=True,
                 tmuxloc="tmux"):
        """
        Creates a tmux session 'session_name' in remote 'target_host'
        """
        self.tmux = tmuxloc
        self.target_computer = target_host
        while self._check_session(session_name) and create_new:
            # Keep appending X to the session name until there is no other with
            # the same name
            session_name += "X"
        self.tms = session_name
        self._new_session()

    def _new_session(self):
        """
        Fires up tmux session
        """
        if self._check_session(self.tms):
            # Session already exist
            return 0
        cmd = "{1} new-session -d -s {0}".format(self.tms, self.tmux)
        return send_command(cmd, self.target_computer)

    def run_cmd(self, cmd):
        """
        Run some external command in the remote tmux session
        """
        tmux_cmd = self._cmd_str(cmd)
        return send_command(tmux_cmd, self.target_computer)

    def _cmd_str(self, cmd):
        """
        Returns the command str to send a command to a specific tmux session
        """
        return "{2} send -t {0} '{1}' ENTER".format(self.tms, cmd, self.tmux)

    def kill_all(self):
        """
        Kills all tmux instances in the remote host
        """
        print("Killing tmux server at {}".format(self.target_computer))
        return send_command(
            "{} kill-server".format(self.tmux), self.target_computer)

    def get_kill_cmd(self):
        cmd = "{1} kill-session -t {0}".format(self.tms, self.tmux)
        return cmd

    def kill_session(self):
        """
        Kills the tmux session
        """
        cmd = self.get_kill_cmd()
        return send_command(cmd, self.target_computer)

    def _check_session(self, tms):
        """
        Check whether a tmux session with a given name exists in remote host
        """
        result = send_command(
            "{1} has-session -t {0}".format(tms, self.tmux),
            self.target_computer)
        if result == 0:
            return True
        else:
            return False


##########
def check_port_blocked(host, port):
    # nc_cmd= 'echo -n "Are you alive?"| nc {0} {1}'.format(host, port)
    # return_code = sp.call(["bash", "-c", nc_cmd])
    # if return_code == 0:
    #     blocked = True
    # else:
    #     blocked = False
    # return blocked
    # Below, only works with host == localhost
    import socket
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.bind((host, port))
        blocked = False
    except socket.error as e:
        if e.errno == 98:  # Port already in use
            blocked = True
        elif e.errno == 99:
            print(
                "\033[91m WARNING:\033[0m Can't assign address for socket "
                "server. Check that the server host is your current machine. ")
            raise(e)
        else:  # some other error
            raise(e)
    finally:
        s.close()

    return blocked


def fire_up_socket_server(
        host, port, n_sockets, wait_time="18000",
        socket_exe="/mt/home/jmartinez/Gangaless_new/src/socket_server.py",
        tag="", tmuxloc="tmux"):
    """
    Fires up the vegas socket server inside a tmux terminal
    in the given 'host' for 'n_sockets' and with a 'wait_time' (s)
    'port' is the starting point, it won't fire up the server until
    finding one free

    On success return the free port
    """
    # Check whether the port is free in the given host
    while check_port_blocked(host, port):
        port += 1

    # Once we have a free port, fire up the tmux session
    tms = "socket-server-{0}-{1}".format(port, tag.replace(".", "-"))
    tmux = Tmux(tms, host, tmuxloc=tmuxloc)

    # Send the server activation command!
    if wait_time is None:
        waitstr = ""
    else:
        waitstr = "-w {0}".format(wait_time)
    cmd_server = "{0} -N {1} -p {2} {3} -l {4}".format(socket_exe, n_sockets,
                                                       port, waitstr, tms)
    kill_session_cmd = tmux.get_kill_cmd()
    cmd = "{0} && {1}".format(cmd_server, kill_session_cmd)
    tmux.run_cmd(cmd)

    return port


def socket_sync_str(host, port, handshake):
    # Blocking call, it will receive a str of the form
    # -sockets {0} -ns {1}
    import socket
    sid = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sid.connect((host, int(port)))
    sid.send(handshake)
    return sid.recv(32)


if __name__ == "__main__":
    test_host = "gridui1"
    test_port = 8888
    test_nsoc = 2
    fire_up_socket_server(test_host, test_port, test_nsoc)
