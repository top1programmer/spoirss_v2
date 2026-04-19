# client/tcp_client.py
import os
import shlex
import socket
import time

from common.config import *


def recv_line_tcp(sock):
    data = b''

    while True:
        chunk = recv_chunk(sock)

        if chunk is None:
            return None

        data += chunk

        if data.endswith(b'\n'):
            break

    return data.decode().rstrip('\r\n')


def recv_chunk(sock):
    try:
        chunk = sock.recv(1024)
    except socket.error:
        return None

    if not chunk:
        return None

    return chunk


def send_line(sock, text):
    sock.sendall((text + '\n').encode())


def build_upload_cmd(filename, filesize, offset):
    name = shlex.quote(os.path.basename(filename))
    return f"UPLOAD {name} {filesize} {offset}"


def build_download_cmd(filename, offset):
    name = shlex.quote(os.path.basename(filename))
    return f"DOWNLOAD {name} {offset}"


def is_ok_response(resp):
    return bool(resp) and resp.startswith('OK')


def print_server_error(resp):
    print("Server error:", resp)


# ==========================================================
# upload
# ==========================================================
def upload_tcp(sock, filename, filesize, offset=0):
    cmd = build_upload_cmd(filename, filesize, offset)
    send_line(sock, cmd)

    resp = recv_line_tcp(sock)

    if not is_ok_response(resp):
        print_server_error(resp)
        return False

    if not validate_upload_offset(resp, offset):
        return False

    start_time = time.time()
    send_upload_file(sock, filename, filesize, offset)
    final = recv_line_tcp(sock)

    print_final_message(final)
    print_upload_speed(filesize, offset, start_time)

    return True


def validate_upload_offset(resp, offset):
    server_offset = int(resp.split()[1])

    if server_offset == offset:
        return True

    print(f"Offset mismatch: server {server_offset}, client {offset}")
    return False


def send_upload_file(sock, filename, filesize, offset):
    with open(filename, 'rb') as f:
        f.seek(offset)

        sent = offset

        while sent < filesize:
            chunk = f.read(BUFFER_SIZE)

            if not chunk:
                break

            sock.sendall(chunk)
            sent += len(chunk)


def print_upload_speed(filesize, offset, start_time):
    elapsed = time.time() - start_time
    speed = (filesize - offset) / elapsed / 1024
    print(f"Upload speed: {speed:.2f} KB/s")


# ==========================================================
# download
# ==========================================================
def download_tcp(sock, filename, offset=0):
    local_name = 'downloaded_' + os.path.basename(filename)

    cmd = build_download_cmd(filename, offset)
    send_line(sock, cmd)

    resp = recv_line_tcp(sock)

    if not is_ok_response(resp):
        print_server_error(resp)
        return False

    filesize, server_offset = parse_download_info(resp)

    if server_offset != offset:
        print(f"Offset mismatch: server {server_offset}, client {offset}")
        return False

    start_time = time.time()
    receive_download_file( sock, local_name, filesize, offset )
    final = recv_line_tcp(sock)
    sock.settimeout(TCP_TIMEOUT)
    print_final_message(final)
    print_download_speed(filesize, offset, start_time)
    return True

def parse_download_info(resp):
    parts = resp.split()
    filesize = int(parts[1])
    offset = int(parts[2])

    return filesize, offset


def receive_download_file(sock, filename, filesize, offset):
    mode = 'ab' if offset > 0 else 'wb'

    sock.settimeout(300)

    with open(filename, mode) as f:
        received = offset

        while received < filesize:
            chunk = sock.recv(min(BUFFER_SIZE, filesize - received))

            if not chunk:
                break

            f.write(chunk)
            received += len(chunk)


def print_download_speed(filesize, offset, start_time):
    elapsed = time.time() - start_time
    speed = (filesize - offset) / elapsed / 1024
    print(f"Download speed: {speed:.2f} KB/s")


def print_final_message(text):
    if text:
        print(text)