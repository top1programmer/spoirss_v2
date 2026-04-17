import socket
import os
import shlex
import time
from common.config import *

def recv_line_tcp(sock):
    data = b''
    while True:
        try:
            chunk = sock.recv(1024)
        except socket.error:
            return None
        if not chunk:
            return None
        data += chunk
        if data.endswith(b'\n'):
            break
    return data.decode().rstrip('\r\n')

def upload_tcp(sock, filename, filesize, offset=0):
    cmd = f"UPLOAD {shlex.quote(os.path.basename(filename))} {filesize} {offset}"
    sock.sendall((cmd + '\n').encode())
    resp = recv_line_tcp(sock)
    if not resp or not resp.startswith('OK'):
        print("Server error:", resp)
        return False

    server_offset = int(resp.split()[1])
    if server_offset != offset:
        print(f"Offset mismatch: server {server_offset}, client {offset}")
        return False

    start_time = time.time()
    with open(filename, 'rb') as f:
        f.seek(offset)
        sent = offset
        while sent < filesize:
            chunk = f.read(BUFFER_SIZE)
            if not chunk:
                break
            sock.sendall(chunk)
            sent += len(chunk)

    final = recv_line_tcp(sock)
    elapsed = time.time() - start_time
    if final:
        print(final)
    speed = (filesize - offset) / elapsed / 1024
    print(f"Upload speed: {speed:.2f} KB/s")
    return True

def download_tcp(sock, filename, offset=0):
    local_filename = 'downloaded_' + os.path.basename(filename)
    cmd = f"DOWNLOAD {shlex.quote(os.path.basename(filename))} {offset}"
    sock.sendall((cmd + '\n').encode())
    resp = recv_line_tcp(sock)
    if not resp or not resp.startswith('OK'):
        print("Server error:", resp)
        return False

    parts = resp.split()
    server_filesize = int(parts[1])
    server_offset = int(parts[2])
    if server_offset != offset:
        print(f"Offset mismatch: server {server_offset}, client {offset}")
        return False

    mode = 'ab' if offset > 0 else 'wb'
    sock.settimeout(300)
    start_time = time.time()
    with open(local_filename, mode) as f:
        received = offset
        while received < server_filesize:
            chunk = sock.recv(min(BUFFER_SIZE, server_filesize - received))
            if not chunk:
                break
            f.write(chunk)
            received += len(chunk)

    final = recv_line_tcp(sock)
    sock.settimeout(TCP_TIMEOUT)
    elapsed = time.time() - start_time
    if final:
        print(final)
    speed = (server_filesize - offset) / elapsed / 1024
    print(f"Download speed: {speed:.2f} KB/s")
    return True