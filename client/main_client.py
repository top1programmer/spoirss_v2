# client/main_client.py
#!/usr/bin/env python3
import argparse
import os
import shlex
import socket
import sys
import time

from common.config import *
from tcp_client import *
from udp_client import UDPClient


def parse_args():
    parser = argparse.ArgumentParser(
        description='Клиент для передачи файлов'
    )

    parser.add_argument(
        '--protocol',
        choices=['tcp', 'udp'],
        default='tcp'
    )

    parser.add_argument(
        'action',
        choices=['upload', 'download', 'echo', 'time', 'close']
    )

    parser.add_argument('filename', nargs='?')
    parser.add_argument('--host', default=HOST)
    parser.add_argument('--port', type=int, default=PORT)

    return parser.parse_args()


def require_filename(args):
    if args.action in ('upload', 'download') and not args.filename:
        print("Для upload/download необходимо указать имя файла")
        sys.exit(1)


def create_tcp_socket(host, port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(TCP_TIMEOUT)

    try:
        sock.connect((host, port))
    except Exception as e:
        print("TCP connection failed:", e)
        sys.exit(1)

    return sock


def create_udp_socket():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 32 * 2**20)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 32 * 2**20)
    except Exception:
        pass

    sock.settimeout(UDP_TIMEOUT)
    sock.setblocking(False)

    return sock


def create_socket(args):
    if args.protocol == 'tcp':
        return create_tcp_socket(args.host, args.port)

    return create_udp_socket()


def ensure_file_exists(path):
    if os.path.exists(path):
        return

    print("File not found")
    sys.exit(1)


def local_download_name(filename):
    return 'downloaded_' + os.path.basename(filename)


def file_size(path):
    if os.path.exists(path):
        return os.path.getsize(path)

    return 0


# ==========================================================
# TCP
# ==========================================================
def handle_tcp(args, sock):
    if args.action == 'echo':
        handle_tcp_echo(sock, args.filename)
        return

    if args.action == 'time':
        handle_tcp_time(sock)
        return

    if args.action == 'close':
        handle_tcp_close(sock)
        return

    if args.action == 'upload':
        handle_tcp_upload(sock, args.filename)
        return

    if args.action == 'download':
        handle_tcp_download(sock, args.filename)


def handle_tcp_echo(sock, text):
    cmd = f"ECHO {text}" if text else "ECHO"
    sock.sendall((cmd + '\n').encode())
    print(recv_line_tcp(sock))


def handle_tcp_time(sock):
    sock.sendall(b"TIME\n")
    print(recv_line_tcp(sock))


def handle_tcp_close(sock):
    sock.sendall(b"CLOSE\n")
    print(recv_line_tcp(sock))


def handle_tcp_upload(sock, filename):
    ensure_file_exists(filename)
    upload_tcp(sock, filename, os.path.getsize(filename), 0)


def handle_tcp_download(sock, filename):
    offset = file_size(local_download_name(filename))
    download_tcp(sock, filename, offset)


# ==========================================================
# UDP
# ==========================================================
def handle_udp(args, sock):
    client = UDPClient(sock, (args.host, args.port))

    if args.action == 'upload':
        handle_udp_upload(client, args.filename)
        return

    if args.action == 'download':
        handle_udp_download(client, args.filename)
        return

    print("UDP поддерживает только upload/download")


def handle_udp_upload(client, filename):
    ensure_file_exists(filename)

    filesize = os.path.getsize(filename)
    offset = 0

    cmd = build_upload_cmd(filename, filesize, offset)
    resp = request_udp(client, cmd)

    server_offset = parse_server_offset(resp, filesize)

    if server_offset is None:
        print("Cannot parse server response:", resp)
        sys.exit(1)

    if server_offset != offset:
        print(f"[INFO] resume from {server_offset}")
        offset = server_offset

    speed = client.send_file(filename, filesize, offset)
    print(f"UDP upload finished. Speed: {speed:.2f} KB/s")


def handle_udp_download(client, filename):
    local_name = local_download_name(filename)
    offset = file_size(local_name)

    cmd = build_download_cmd(filename, offset)
    resp = request_udp(client, cmd)

    filesize, server_offset = parse_download_response(resp)

    if filesize is None:
        print("Cannot parse server response:", resp)
        sys.exit(1)

    if server_offset != offset:
        print(f"[INFO] resume from {server_offset}")
        offset = server_offset

    speed = client.receive_file(local_name, filesize, offset)
    print(f"UDP download finished. Speed: {speed:.2f} KB/s")


def build_upload_cmd(filename, filesize, offset):
    name = shlex.quote(os.path.basename(filename))
    return f"UPLOAD {name} {filesize} {offset}"


def build_download_cmd(filename, offset):
    name = shlex.quote(os.path.basename(filename))
    return f"DOWNLOAD {name} {offset}"


def request_udp(client, cmd):
    try:
        return client.send_command(cmd)
    except Exception as e:
        print("Server error:", e)
        sys.exit(1)


def parse_download_response(resp):
    nums = [int(x) for x in resp.replace(',', ' ').split() if x.isdigit()]

    if len(nums) < 2:
        return None, None

    return nums[0], nums[1]


def parse_server_offset(resp_text, filesize):
    if not resp_text:
        return None

    nums = []

    for item in resp_text.replace(',', ' ').split():
        try:
            nums.append(int(item))
        except Exception:
            pass

    if not nums:
        return None

    total_packets = (filesize + UDP_DATA_SIZE - 1) // UDP_DATA_SIZE
    value = nums[-1]

    if 0 <= value <= total_packets:
        return value * UDP_DATA_SIZE

    return value


# ==========================================================
# main
# ==========================================================
def main():
    args = parse_args()
    require_filename(args)

    sock = create_socket(args)

    try:
        if args.protocol == 'tcp':
            handle_tcp(args, sock)
        else:
            handle_udp(args, sock)
    finally:
        sock.close()


if __name__ == '__main__':
    main()