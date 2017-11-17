#!/usr/bin/env python3

import socket
import struct

class Generic_Socket:
    """
    Based on
    https://docs.python.org/3.6/howto/sockets.html
    """

    def __init__(self, sock=None, address=2*["UNK"]):
        self.double_size = 8
        """Create a IPv4 TCP socket
        """
        if sock is None:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.address = address
        else:
            self.sock = sock
            self.address = address

    def connect(self, host, port):
        """ Connects socket to given host-port
        """
        self.address = [host, port]
        self.sock.connect((host, port))

    def bind(self, host, port, max_connections = 10):
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

    def receive_str(self, max_len = 2048):
        """ Receive a string and strips out padding bytes
        """ 
        data = self.sock.recv(max_len)
        if data == b'':
            raise RuntimeError("socket connection broken")
        data_str = data.decode().strip()
        return data_str

    def receive_data(self, msg_len = 32, verbose=None):
        """ Receive any kind of binary data until it fullfills msg_len bytes of data
        Returns binary data received
        """
        chunks = []
        bytes_received = 0
        while bytes_received < msg_len:
            if verbose:
                print("Waiting for a connection")
            chunk = self.sock.recv(min(msg_len - bytes_received, 2048))
            if chunk == b'': 
                raise RuntimeError("socket connection broken")
            chunks.append(chunk)
            bytes_received = bytes_received + len(chunk)
        return b''.join(chunks)

    def send_data(self, msg):
        """ Sends binary data
        """
        total_sent = 0
        while total_sent < len(msg):
            sent = self.sock.send(msg)
            total_sent += sent

    def close(self):
        self.sock.close()

class Vegas_Socket(Generic_Socket):
    """ Extends Generic_Socket for its
    use with Vegas within NNLOJET
    """

    def double_to_bytes(self, double):
        """ takes a double and returns 
        its byte representation (len = 8 bytes)
        """
        return struct.pack('d', *[double])

    def bytes_to_double(self, bytedata):
        """ takes the byte representation of a double
        and returns the double
        """
        return struct.unpack('d', bytedata)[0]

    def double_array_to_bytes(self, double_array):
        """ takes a list of doubles and returns
        its byte representation so it can be sent
        via socket
        """
        arr_len = len(double_array)
        s = struct.pack('d'*arr_len, double_array)
        return s

    def read_partial_integral(self, size = 8, verbose = None):
        """ Read the total integral from one of the jobs
        and returns a double
        """
        data = self.receive_data(size, verbose = verbose)
        double_array = []
        for i in range(0, size-1, self.double_size):
            double_array.append(self.bytes_to_double(data[i:i+8]))
        return double_array

    def send_total_integral(self, total):
        """ Sends the total sum to a job
        """
        data = []
        for double in total:
            data.append(self.double_to_bytes(double))
        self.send_data(b''.join(data))

    def get_size(self):
        """ Gets the size of the data we are going to receive
        ie, gets an integer (size = 4 bytes)
        """
        data = self.receive_data(4)
        try:
            if data.decode() == "gree":
                return -1
        except:
            pass
        return int.from_bytes(data, byteorder="little")

    def harmonize_integral(self, n_jobs, verbose = None):
        """ Get the partial vegas integrals from the different n_jobs
        and send the sum back to each and every job.
        Only exits with success once all jobs receive their data
        """
        # Connect to the endpoint
        job_sockets = []
        array_partial = []
        while len(job_sockets) < n_jobs:
            new_endpoint = self.wait_for_client()

            if verbose:
                adr = str(new_endpoint.address[0])
                prt = str(new_endpoint.address[1])
                print("New endpoint connected: %s:%s" % (adr, prt))
        
            # Get the size of the array of doubles we are going to receive
            size = new_endpoint.get_size()
            if size == -1:
                new_endpoint.send_data(b'die')
                continue
            doubles = int(size / 8)
            if verbose:
                print("Size of array: " + str(size))
                print("Meaning we will get " + str(doubles) + " doubles")

            # Get the actual array of data
            partial_value = new_endpoint.read_partial_integral(size, verbose = verbose)
            if verbose:
                print("Partial value obtained: " + str(partial_value))


            # Store the socket and the array we just received, we will use it in the future
            array_partial.append(partial_value)
            job_sockets.append(new_endpoint)

        integral_value = doubles*[0.0]
        for array_values in array_partial:
            if len(array_values) != doubles:
                raise Exception("Received arrays of different length!")
            integral_value = list(map(lambda x,y: x+y, integral_value, array_values))

        if verbose:
            print("Total value of the integral received: " + str(integral_value))
            print("Sending it back to all clients")
        while job_sockets:
            job_socket = job_sockets.pop()
            job_socket.send_total_integral(integral_value)

        return 0
        
def NNLOJET_tuto():
    str_pr = """
     >>>>> MANUAL <<<<
Server side:
To start the server, give the number of instances of NNLOJET to be sent (ex: 3)
the port the server will be waiting for (1234) and the hostname of the computer 
where the server is active. hostname:port needs to be accessible by all NNLOJET instances
If host is not provided, the socket will try to listen to all available network interfaces.
    ~$ ./vegas_socket.py -n 3 -p 1234 -H gridui1

NNLOJET side:
First of all, the NNLOJET code needs to be compiled with the option "sockets=true"
    ~$ make sockets=true
(tip: with ~$ touch core/Vegas.f90 && make sockets=true, makedepend will force the compilation of all relevant files)
After that, run every NNLOJET instance with the following commandline options:
    ~$ ./NNLOJET -run runcard.run -sockets 3 -ns 1 -port 1234 -hostname gridui1
 -sockets: number of instance (each with its own socket) of NNLOJET that will be run, need to match the server -n option
 -ns: position of this instance, ie, which share of the total number of events will this instance receive. Each instance will run the events from (events)*((ns-1)/sockets) to (events)*(ns/sockets)
 -port: port to connect to the socket (needs to match server port)
 -hostname: hostname to connect (needs to match server host, by default: gridui1)
ie, if for three sockets you need to have run
    ~$ ./NNLOJET -run runcard.run -sockets 3 -ns 1 -port 1234 -hostname gridui1
    ~$ ./NNLOJET -run runcard.run -sockets 3 -ns 2 -port 1234 -hostname gridui1
    ~$ ./NNLOJET -run runcard.run -sockets 3 -ns 3 -port 1234 -hostname gridui1

(
    important: this example server should only be used for one run at a time
    If you want to different runs at the same time, start up two instances of the 
    server listening to two different ports
)
"""
    print(str_pr)

def timeout_handler(signum, frame):
    raise Exception("The time has passed, it's time to run")


if __name__ == "__main__":
    from argparse import ArgumentParser
    from sys import exit
    import signal
    parser = ArgumentParser()
    parser.add_argument("-H", "--hostname", help = "Hostname", default = "")
    parser.add_argument("-p", "--port", help = "Port", default = "8888")
    parser.add_argument("-n", "--nclients", help = "Number of clientes to wait for", default = "2")
    parser.add_argument("-w", "--wait", help = "Wait for a given number of seconds. This options is only to be used on the gridui")
    parser.add_argument("-N", "--waitfor", help = "To be used alongside wait. Stop waiting after N clients are found", default = "10")
    parser.add_argument("-m", "--manual", help = "Print the manual and exit", action = "store_true")
    args = parser.parse_args()

    if args.manual:
        NNLOJET_tuto()
        exit(0)

    if args.wait:
        if "gridui" not in socket.gethostname():
            print("Wait only to be used in gridui")
            exit(-1)

    HOST = str(args.hostname)
    PORT = int(args.port)
    n_clients = int(args.nclients)

    # enable the server, and bind it to HOST:PORT
    server = Vegas_Socket()
    server.bind(HOST, PORT)

    if HOST == "":
        host_str = socket.gethostname()
    else:
        host_str = HOST

    print("Server up. Connect to " + host_str + ":" + str(PORT))

    counter = 0

    if args.wait:
        # Wait for a number of ARC jobs to start before allowing NNLOJET to run
        # it will "capture" the different instances of ARC.py until it gets all of them
        # TODO: If we capture a instance of NNLOJET instead, skip to the while loop or make it fail? What should I do?

        clients = []

        signal.signal(signal.SIGALRM, timeout_handler)

        n_clients = 0
        n_clients_max = int(args.waitfor)

        while n_clients < n_clients_max:
            try:
                print("Waiting for {} more instances of ARC.py to salute".format(n_clients_max - n_clients))
                new_client = server.wait_for_client()
                if not clients: # Start the timer
                    signal.alarm(int(args.wait))
            except:
                break
            greetings = new_client.receive_str()
                
            if greetings == "greetings":
                print("ARC.py captured")
                clients.append(new_client)
            else:
                print("Waiting for ARC.py instance, got nonsense instead:")
                print(greetings)
                server.close()
                exit(-1)
            n_clients = len(clients)

        signal.alarm(0)

        if n_clients == 0:
            print("Something went wrong, no clients registered")
            exit(-1)

        for i in range(n_clients):
            socket_str = "-sockets {0} -ns {1}".format(n_clients, i+1)
            client_out = clients.pop()
            client_out.send_data(socket_str.encode())


    print("Waiting for " + str(n_clients) + " clients")

    # set a number of iterations
    while True:
        counter += 1
        
        # Wait for n_clients connections.
        # Once every client has sent its share of the data, sum it 
        # together and send it back
        success = server.harmonize_integral(n_clients, verbose = False)
        print("Iteration {} completed".format(counter))

        if success < 0:
            print("Something went wrong")

    server.close()
