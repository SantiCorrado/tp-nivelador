import socket

# TODO: Complete with a short-read/short-write tolerant implementation


def recv_all(socket: socket.socket, size):
    data = b""
    while len(data) < size:
        chunk = socket.recv(size - len(data))
        if not chunk:
            # aca habria algun error
            return None
        data += chunk
    return data

def send_all(socket: socket.socket, bytes):
    total = 0
    while total < len(bytes):
        n = socket.send(bytes[total:])
        if n == 0:
            # aca habria algun error
            return None
        total += n
    return total