"""
Short script to kill tmux servers when a warmup is complete. Essentially does the job that nnlorun.py
does at the end of an arc warmup. 
"""
import socket,sys

if len(sys.argv)==1: # No socketstr
    sys.exit(0)

host = sys.argv[6]
port = sys.argv[2]

def socket_sync_str(host, port, handshake = "greetings"):
    # Blocking call, it will receive a str of the form
    # -sockets {0} -ns {1}
    import socket
    sid = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sid.connect((host, int(port)))
    sid.send(handshake)
    return sid.recv(32)

socket_sync_str(host,port,"bye!")
