import socket

class ConnClosedError(Exception):
	pass

def myrecv(sock, length):
    chunks = []
    bytes_recd = 0
    while bytes_recd < length:
        chunk = sock.recv(min(length - bytes_recd, 2048))
        if chunk == b'':
                raise ConnClosedError()
        chunks.append(chunk)
        bytes_recd = bytes_recd + len(chunk)

    return b''.join(chunks)
