import socket

class Connection:

    #check if a port is open
    @staticmethod
    def isPortOpen(host, port, timeout=5):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        result = sock.connect_ex((host,port))
        return result is 0