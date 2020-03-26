#!/usr/bin/env python3

import socket
import struct
import datetime


class Generic_Socket:
    """
    Based on
    https://docs.python.org/3.6/howto/sockets.html
    """

    def __init__(self, sock=None, address=2 * ["UNK"], logger=None):
        self.double_size = 8
        """Create a IPv4 TCP socket
        """
        if sock is None:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.address = address
        else:
            self.sock = sock
            self.address = address

        if logger:
            self._info_print = logger.info
            self._debug_print = logger.debug
            self._critical_print = logger.critical
        else:
            self._info_print = print
            self._debug_print = print
            self._critical_print = print

    def connect(self, host, port):
        """ Connects socket to given host-port
        """
        self.address = [host, port]
        self.sock.connect((host, port))

    def bind(self, host, port, max_connections=10):
        """ Makes the socket into a server
        """
        self.sock.bind((host, port))
        self.sock.listen(max_connections)

    def wait_for_client(self):
        """ Blocking call
        when a client connects to the socket
        returns a Vegas_Socket instance associated to the client socket
        """
        (client_socket, address) = self.sock.accept()
        return self.__class__(client_socket, address)

    def receive_str(self, max_len=2048):
        """ Receive a string and strips out padding bytes
        """
        data = self.sock.recv(max_len)
        if data == b"":
            raise RuntimeError("socket connection broken")
        data_str = data.decode().strip()
        return data_str

    def receive_data(self, msg_len=32, verbose=None):
        """
        Receive any kind of binary data until it fullfills msg_len bytes of data
        Returns binary data received
        """
        chunks = []
        bytes_received = 0
        while bytes_received < msg_len:
            if verbose:
                self._info_print("Waiting for a connection")
            chunk = self.sock.recv(min(msg_len - bytes_received, 2048))
            if chunk == b"":
                self._critical_print("Data up to now:")
                self._critical_print(chunks)
                raise RuntimeError("socket connection broken")
            chunks.append(chunk)
            bytes_received = bytes_received + len(chunk)
        return b"".join(chunks)

    def send_data(self, msg):
        """ Sends binary data
        """
        total_sent = 0
        while total_sent < len(msg):
            sent = self.sock.send(msg)
            total_sent += sent

    def get_host_by_address(self, host_addr):
        """ Given an ip address, get hostname
        """
        try:
            info = socket.gethostbyaddr(host_addr)
            hostname = info[0]
            return hostname
        except BaseException:  # Assume your failure
            return host_addr

    def close(self):
        self.sock.close()


class Vegas_Socket(Generic_Socket):
    """ Extends Generic_Socket for its
    use with Vegas with sockets support
    """

    def double_to_bytes(self, double):
        """ takes a double and returns
        its byte representation (len = 8 bytes)
        """
        return struct.pack("d", *[double])

    def bytes_to_double(self, bytedata):
        """ takes the byte representation of a double
        and returns the double
        """
        return struct.unpack("d", bytedata)[0]

    def double_array_to_bytes(self, double_array):
        """ takes a list of doubles and returns
        its byte representation so it can be sent
        via socket
        """
        arr_len = len(double_array)
        s = struct.pack("d" * arr_len, double_array)
        return s

    def read_partial_integral(self, size=8, verbose=None):
        """ Read the total integral from one of the jobs
        and returns a double
        """
        data = self.receive_data(size, verbose=verbose)
        double_array = []
        for i in range(0, size - 1, self.double_size):
            double_array.append(self.bytes_to_double(data[i : i + 8]))
        return double_array

    def send_total_integral(self, total):
        """ Sends the total sum to a job
        """
        data = []
        for double in total:
            data.append(self.double_to_bytes(double))
        self.send_data(b"".join(data))

    def get_size(self):
        """ Gets the size of the data we are going to receive
        ie, gets an integer (size = 4 bytes)
        """
        data = self.receive_data(4)
        try:
            if data.decode() == "gree":
                return -1
            elif data.decode() == "bye!":
                return -99  # Exit code
        except BaseException:
            pass
        return int.from_bytes(data, byteorder="little")

    def harmonize_integral(self, n_jobs, verbose=None):
        """ Get the partial vegas integrals from the different n_jobs
        and send the sum back to each and every job.
        Only exits with success once all jobs receive their data
        """
        # Connect to the endpoint
        job_sockets = []
        array_partial = []
        while len(job_sockets) < n_jobs:
            new_endpoint = self.wait_for_client()

            adr = self.get_host_by_address(str(new_endpoint.address[0]))
            prt = str(new_endpoint.address[1])
            self._info_print(
                "   New endpoint connected: {0}:{1} [{2}/{3}]".format(
                    adr, prt, len(job_sockets) + 1, n_jobs
                )
            )

            # Get the size of the array of doubles we are going to receive
            size = new_endpoint.get_size()
            if size == -1:
                new_endpoint.send_data(b"die")
                self._info_print(" > Killed orphan instance of nnlorun.py")
                continue
            elif size == -99:
                self._info_print(" > nnlorun.py sent exit code, exiting with success")
                exit(0)
            doubles = int(size / 8)
            if verbose:
                self._info_print("Size of array: " + str(size))
                self._info_print("Meaning we will get " + str(doubles) + " doubles")

            # Get the actual array of data
            partial_value = new_endpoint.read_partial_integral(size, verbose=verbose)
            if verbose:
                self._info_print("Partial value obtained: " + str(partial_value))

            # Store the socket and the array we just received, we will use it in
            # the future
            array_partial.append(partial_value)
            job_sockets.append(new_endpoint)

        integral_value = doubles * [0.0]
        for array_values in array_partial:
            if len(array_values) != doubles:
                raise Exception("Received arrays of different length!")
            integral_value = list(map(lambda x, y: x + y, integral_value, array_values))

        if verbose:
            self._info_print(
                "Total value of the integral received: " + str(integral_value)
            )
            self._info_print("Sending it back to all clients")
        while job_sockets:
            job_socket = job_sockets.pop()
            job_socket.send_total_integral(integral_value)

        return 0


def timeout_handler(signum, frame):
    raise Exception("The time has passed, it's time to run")


def create_stdout_log(logname):
    import logging
    import sys
    import os
    import getpass
    import datetime

    # TODO: add more logging levels

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    h = logging.StreamHandler(sys.stdout)
    h.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime) 8s %(message)s", datefmt="[%H:%M:%S %d/%m]"
    )
    h.setFormatter(formatter)
    logger.addHandler(h)

    username = getpass.getuser()
    datestr = datetime.datetime.now().strftime("%d-%m-%Y")
    logloc = "/tmp/{1}/{0}/{3}/{2}".format(
        os.path.splitext(os.path.basename(__file__))[0], username, logname, datestr
    )
    logger.info("Logfile: {0}".format(logloc))
    os.makedirs(os.path.dirname(logloc), exist_ok=True)

    logfile = logging.FileHandler(logloc)
    logfile.setLevel(logging.DEBUG)
    logfile.setFormatter(formatter)
    logger.addHandler(logfile)

    return logger


def parse_all_arguments():
    from argparse import ArgumentParser

    parser = ArgumentParser()
    parser.add_argument("-H", "--hostname", help="Hostname", default="")
    parser.add_argument("-p", "--port", help="Port", default="8888")
    parser.add_argument(
        "-w",
        "--wait",
        help="Wait for a given number of seconds. "
        "This options is only to be used on the gridui",
    )
    parser.add_argument(
        "-N",
        "--N_clients",
        help="Number of clients to wait for, if used alongside wait, "
        "stop waiting after N clients",
        default="2",
    )
    parser.add_argument(
        "-m", "--manual", help="Print the manual and exit", action="store_true"
    )
    parser.add_argument(
        "-l",
        "--logfile",
        help="Set the output logfile name " "(Stored in /tmp/<username>/socket_server/",
        default=None,
    )
    args = parser.parse_args()

    if args.wait:
        if "gridui" not in socket.gethostname():
            parser.error("Wait only to be used in gridui")

    if args.logfile is None:
        import datetime

        timestr = datetime.datetime.now().strftime("%H-%M-%S")
        args.logfile = "{2}_Port_{0}_No_clients_{1}.log".format(
            args.port, args.N_clients, timestr
        )

    return args


def do_server(args, log):
    """
    Main server running routine. Placed here so that we can catch exceptions
    with logger
    """
    HOST = str(args.hostname)
    PORT = int(args.port)
    n_clients = int(args.N_clients)

    # enable the server, and bind it to HOST:PORT
    server = Vegas_Socket(logger=log)
    server.bind(HOST, PORT)

    if HOST == "":
        host_str = socket.gethostname()
    else:
        host_str = HOST

    log.info("Server up. Connect to " + host_str + ":" + str(PORT))

    # If wait (this whole block is for its use with the grid scripts)
    if args.wait:
        # Wait for a number of ARC jobs to start before allowing the program to
        # run it will "capture" the different instances of nnlorun.py until it
        # gets all of them
        # TODO: If we capture a rogue instance instead, skip to the while loop
        # or make it fail? What should I do?

        clients = []

        signal.signal(signal.SIGALRM, timeout_handler)

        n_clients_max = n_clients

        n_clients = 0

        while n_clients < n_clients_max:
            try:
                log.info(
                    ("Waiting for {} more instances of nnlorun.py to " "salute").format(
                        n_clients_max - n_clients
                    )
                )
                new_client = server.wait_for_client()
                if not clients:  # Start the timer
                    log.info("Starting timer for {0} secs".format(int(args.wait)))
                    signal.alarm(int(args.wait))
            except BaseException:
                break
            greetings = new_client.receive_str()

            if greetings == "greetings":
                # nnlorun.py sends the word "greetins" at the start
                # print("ARC.py captured")
                clients.append(new_client)
            elif greetings == "oupsities":
                n_clients_max -= 1
                new_client.close()
                continue
            else:
                log.info("Waiting for nnlorun.py instance, " "got nonsense instead.")
                log.info("Received msg: {0}".format(greetings))
                log.info("Sending kill signal...")
                new_client.close()
                continue
            n_clients = len(clients)

        signal.alarm(0)

        if n_clients == 0:
            log.critical("[WARNING] Something went wrong, no clients registered")
            exit(-1)

        for i in range(n_clients):
            socket_str = "-sockets {0} -ns {1}".format(n_clients, i + 1)
            client_out = clients.pop()
            client_out.send_data(socket_str.encode())
    # endif wait

    counter = 0

    log.info("Waiting for " + str(n_clients) + " clients")

    start_time = datetime.datetime.now()
    # set a number of iterations
    while True:
        counter += 1

        # Wait for n_clients connections.
        # Once every client has sent its share of the data, sum it
        # together and send it back
        success = server.harmonize_integral(n_clients, verbose=False)
        end_time = datetime.datetime.now()
        iteration_duration = end_time - start_time
        start_time = end_time
        hours, remainder = divmod(iteration_duration.total_seconds(), 3600)
        minutes, seconds = divmod(remainder, 60)
        log.info(
            "Iteration {0} completed in {1:02.0f}:{2:02.0f}:{3:02.0f}".format(
                counter, hours, minutes, seconds
            )
        )
        if success < 0:
            print("[WARNING] Something went wrong")

    server.close()


if __name__ == "__main__":
    from sys import exit
    import signal

    args = parse_all_arguments()

    log = create_stdout_log(args.logfile)

    try:
        do_server(args, log)
    except (Exception, KeyboardInterrupt):
        log.exception("Encountered Exception")  # Prints stack trace
        exit(-1)
