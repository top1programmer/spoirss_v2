#!/usr/bin/env python3
import socket
import sys
import time
import os
import shlex
import argparse
import struct

DEFAULT_HOST = '127.0.0.1'
DEFAULT_PORT = 12345
BUFFER_SIZE = 8192
UDP_BUFFER_SIZE = 1472          # 1500 - 28 (IP+UDP) = 1472
UDP_HEADER_SIZE = 4
UDP_DATA_SIZE = UDP_BUFFER_SIZE - UDP_HEADER_SIZE
TIMEOUT = 300
UDP_TIMEOUT = 0.05              # увеличен для реальных сетей
MAX_RETRIES = 50
WINDOW_SIZE = 2000                 # уменьшенное окно
MAX_TOTAL_RETRIES = 30

class UDPClient:
    def __init__(self, sock, addr):
        self.sock = sock
        self.addr = addr
        self.sock.settimeout(UDP_TIMEOUT)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 16*2**20)  # 8 MB
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 16*2**20)

    def send_command(self, cmd):
        data = cmd.encode() + b'\n'
        for attempt in range(MAX_RETRIES):
            try:
                self.sock.sendto(data, self.addr)
                ack, _ = self.sock.recvfrom(UDP_BUFFER_SIZE)
                if ack.startswith(b'ACK '):
                    return ack[4:].decode().strip()
            except socket.timeout:
                continue
        raise ConnectionError("No response to command after multiple retries")

    def send_file(self, filename, filesize, offset=0):
        base = offset // UDP_DATA_SIZE
        next_seq = base
        last_ack = base - 1
        total_retries = 0

        BATCH_SIZE = 64
        batch = []

        MAX_TOTAL_RETRIES = 30
        pack = struct.Struct('!I').pack

        total_packets = (filesize + UDP_DATA_SIZE - 1) // UDP_DATA_SIZE

        with open(filename, 'rb') as f:
            f.seek(offset)

            start_time = time.time()

            while last_ack < total_packets - 1:

                if total_retries > MAX_TOTAL_RETRIES:
                    raise TimeoutError("Too many retransmissions")

                #  отправка окна
                while next_seq < base + WINDOW_SIZE and next_seq < total_packets:
                    data = f.read(UDP_DATA_SIZE)
                    if not data:
                        break

                    batch.append(pack(next_seq) + data)
                    next_seq += 1

                    if len(batch) >= BATCH_SIZE:
                        for pkt in batch:
                            self.sock.sendto(pkt, self.addr)
                        batch.clear()
                    
                    if next_seq % 200 == 0:
                        time.sleep(0.0005)

                #  дослать остаток batch
                if batch:
                    for pkt in batch:
                        self.sock.sendto(pkt, self.addr)
                    batch.clear()

                #  ACK
                try:
                    ack_data, _ = self.sock.recvfrom(UDP_BUFFER_SIZE)

                    if ack_data.startswith(b'ACK '):
                        try:
                            ack_seq = int(ack_data[4:].strip())
                        except:
                            continue

                        if ack_seq > last_ack:
                            last_ack = ack_seq
                            base = ack_seq
                            total_retries = 0

                except socket.timeout:
                    total_retries += 1

                    #  быстрый ретрай (малый кусок)
                    resend_end = min(base + 200, total_packets)

                    pos = base * UDP_DATA_SIZE
                    f.seek(pos)

                    for seq in range(base, resend_end):
                        data = f.read(UDP_DATA_SIZE)
                        if not data:
                            break

                        batch.append(pack(seq) + data)

                        if len(batch) >= BATCH_SIZE:
                            for pkt in batch:
                                self.sock.sendto(pkt, self.addr)
                            batch.clear()

                    if batch:
                        for pkt in batch:
                            self.sock.sendto(pkt, self.addr)
                        batch.clear()

                    next_seq = max(next_seq, resend_end)

            elapsed = time.time() - start_time
            speed = filesize / elapsed / 1024

            # финальное ожидание
            end_time = time.time() + 0.5
            while time.time() < end_time:
                try:
                    data, _ = self.sock.recvfrom(UDP_BUFFER_SIZE)
                    if data.startswith(b'UPLOAD complete'):
                        break
                except socket.timeout:
                    break

            return speed

    def receive_file(self, filename, filesize, offset=0):
        base = offset // UDP_DATA_SIZE
        total_packets = (filesize + UDP_DATA_SIZE - 1) // UDP_DATA_SIZE

        received = {}
        mode = 'ab' if offset > 0 else 'wb'

        MAX_RETRIES = 50
        retries = 0

        ACK_EVERY = 512
        last_acked = base

        start_time = time.time()
        self.sock.settimeout(UDP_TIMEOUT)

        with open(filename, mode) as f:
            f.seek(offset)

            while base < total_packets:
                try:
                    data, _ = self.sock.recvfrom(UDP_BUFFER_SIZE)

                    if len(data) < 4:
                        continue

                    seq = struct.unpack('!I', data[:4])[0]
                    payload = data[4:]

                    if seq not in received:
                        received[seq] = payload

                    advanced = False

                    while base in received:
                        f.write(received.pop(base))
                        base += 1
                        advanced = True

                    # ACK батчами
                    if base - last_acked >= ACK_EVERY:
                        self.sock.sendto(f"ACK {base}".encode(), self.addr)
                        last_acked = base

                    retries = 0

                except socket.timeout:
                    retries += 1

                    if retries > MAX_RETRIES:
                        raise TimeoutError("UDP download failed (too many retries)")

                    # при timeout всегда просим
                    self.sock.sendto(f"ACK {base}".encode(), self.addr)
                    last_acked = base

        # финальный ACK (важно!)
        for _ in range(3):
            try:
                self.sock.sendto(f"ACK {base}".encode(), self.addr)
                time.sleep(0.002)
            except:
                pass

        elapsed = time.time() - start_time
        return (filesize - offset) / elapsed / 1024 if elapsed > 0 else 0

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
    sock.settimeout(TIMEOUT)
    elapsed = time.time() - start_time
    if final:
        print(final)
    speed = (server_filesize - offset) / elapsed / 1024
    print(f"Download speed: {speed:.2f} KB/s")
    return True

def main():
    parser = argparse.ArgumentParser(description='Клиент для передачи файлов')
    parser.add_argument('--protocol', choices=['tcp', 'udp'], default='tcp',
                        help='Протокол передачи (tcp или udp)')
    parser.add_argument('action', choices=['upload', 'download', 'echo', 'time', 'close'],
                        help='Действие')
    parser.add_argument('filename', nargs='?', help='Имя файла (для upload/download)')
    parser.add_argument('--host', default=DEFAULT_HOST, help='Адрес сервера')
    parser.add_argument('--port', type=int, default=DEFAULT_PORT, help='Порт сервера')
    args = parser.parse_args()

    if args.action in ('upload', 'download') and not args.filename:
        print("Для upload/download необходимо указать имя файла")
        sys.exit(1)

    if args.protocol == 'tcp':
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(TIMEOUT)
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