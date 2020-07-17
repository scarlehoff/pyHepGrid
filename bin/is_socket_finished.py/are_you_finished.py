#!/usr/bin/env python3
import socket
import sys

if __name__ == "__main__":
    # arg[1] : host
    # arg[2] : port
    sid = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    if len(sys.argv[1:]) == 1:
        spstr = sys.argv[1].split(":")
        host = spstr[0]
        port = spstr[1]
    else:
        host = sys.argv[1]
        port = sys.argv[2]
    try:
        sid.connect((host, int(port)))
    except ConnectionRefusedError:
        print("There is no server to connect to")
        sys.exit(0)
    sid.send(b"gree")
    out = sid.recv(32)
    if out.decode() == "die":
        sys.exit(-1)
    else:
        sys.exit(-1)  # better safe than sorry?
