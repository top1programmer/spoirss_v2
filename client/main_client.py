from tcp_client import *
from udp_client import UDPClient
from common.config import *
import socket
import argparse
import os
import sys
import shlex



def main():
    parser = argparse.ArgumentParser(description='Клиент для передачи файлов')
    parser.add_argument('--protocol', choices=['tcp', 'udp'], default='tcp',
                        help='Протокол передачи (tcp или udp)')
    parser.add_argument('action', choices=['upload', 'download', 'echo', 'time', 'close'],
                        help='Действие')
    parser.add_argument('filename', nargs='?', help='Имя файла (для upload/download)')
    parser.add_argument('--host', default=HOST, help='Адрес сервера')
    parser.add_argument('--port', type=int, default=PORT, help='Порт сервера')
    args = parser.parse_args()

    print("CWD =", os.getcwd())
    print("FILE =", args.filename)
    print("EXISTS =", os.path.exists(args.filename) if args.filename else None)

    if args.action in ('upload', 'download') and not args.filename:
        print("Для upload/download необходимо указать имя файла")
        sys.exit(1)

    if args.protocol == 'tcp':
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(TCP_TIMEOUT)
        try:
            sock.connect((args.host, args.port))
        except Exception as e:
            print(f"TCP connection failed: {e}")
            sys.exit(1)
    else:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 16*2**20)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 16*2**20)
        sock.settimeout(UDP_TIMEOUT)

    try:
        if args.protocol == 'tcp':
            if args.action == 'echo':
                cmd = f"ECHO {args.filename}" if args.filename else "ECHO"
                sock.sendall((cmd + '\n').encode())
                resp = recv_line_tcp(sock)
                print(resp)
            elif args.action == 'time':
                sock.sendall(b"TIME\n")
                resp = recv_line_tcp(sock)
                print(resp)
            elif args.action == 'close':
                sock.sendall(b"CLOSE\n")
                resp = recv_line_tcp(sock)
                print(resp)
            elif args.action == 'upload':
                if not os.path.exists(args.filename):
                    print("File not found")
                    sys.exit(1)
                filesize = os.path.getsize(args.filename)
                offset = 0
                upload_tcp(sock, args.filename, filesize, offset)
            elif args.action == 'download':
                local_filename = 'downloaded_' + os.path.basename(args.filename)
                offset = os.path.getsize(local_filename) if os.path.exists(local_filename) else 0
                download_tcp(sock, args.filename, offset)
        else:
            udp_client = UDPClient(sock, (args.host, args.port))
            if args.action == 'echo':
                resp = udp_client.send_command(f"ECHO {args.filename}" if args.filename else "ECHO")
                print(resp)
            elif args.action == 'time':
                resp = udp_client.send_command("TIME")
                print(resp)
            elif args.action == 'close':
                resp = udp_client.send_command("CLOSE")
                print(resp)
            elif args.action == 'upload':
                if not os.path.exists(args.filename):
                    print("File not found")
                    sys.exit(1)
                filesize = os.path.getsize(args.filename)
                offset = 0
                cmd = f"UPLOAD {shlex.quote(os.path.basename(args.filename))} {filesize} {offset}"
                resp = udp_client.send_command(cmd)
                if not resp.startswith('OK'):
                    print("Server error:", resp)
                    sys.exit(1)
                expected_offset = int(resp.split()[1])
                if expected_offset != offset:
                    print(f"Offset mismatch: server {expected_offset}, client {offset}")
                    sys.exit(1)
                speed = udp_client.send_file(args.filename, filesize, offset)
                print(f"UDP upload finished. Speed: {speed:.2f} KB/s")
            elif args.action == 'download':
                offset = 0
                local_filename = 'downloaded_' + os.path.basename(args.filename)
                if os.path.exists(local_filename):
                    offset = os.path.getsize(local_filename)
                cmd = f"DOWNLOAD {shlex.quote(os.path.basename(args.filename))} {offset}"
                resp = udp_client.send_command(cmd)
                if not resp.startswith('OK'):
                    print("Server error:", resp)
                    sys.exit(1)
                parts = resp.split()
                filesize = int(parts[1])
                server_offset = int(parts[2])
                if server_offset != offset:
                    print(f"Offset mismatch: server {server_offset}, client {offset}")
                    sys.exit(1)
                speed = udp_client.receive_file(local_filename, filesize, offset)
                print(f"UDP download finished. Speed: {speed:.2f} KB/s")
    finally:
        sock.close()

if __name__ == '__main__':
    main()