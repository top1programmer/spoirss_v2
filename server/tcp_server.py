import socket
import os
import shlex
import time
from common.config import *

def send_ok_tcp(sock, msg=''):
    sock.sendall(f"OK {msg}\r\n".encode())

def send_error_tcp(sock, msg):
    sock.sendall(f"ERROR {msg}\r\n".encode())

class TCPClient:
    def __init__(self, conn, addr):
        self.conn = conn
        self.addr = addr
        self.buffer = b''
        self.conn.setblocking(False)
        print(f"[+] TCP client {addr} connected")

    def fileno(self):
        return self.conn.fileno()

    def close(self):
        self.conn.close()
        print(f"[-] TCP client {self.addr} disconnected")

    def handle_input(self):
        try:
            data = self.conn.recv(1024)
            if not data:
                return False
            self.buffer += data
            if b'\n' in self.buffer:
                line, self.buffer = self.buffer.split(b'\n', 1)
                line = line.decode().strip()
                upper_line = line.upper()
                if upper_line.startswith('CLOSE') or upper_line.startswith('EXIT') or upper_line.startswith('QUIT'):
                    self.process_command(line)
                    return False
                else:
                    self.process_command(line)
        except socket.error:
            return False
        return True

    def process_command(self, line):
        print(f"[TCP command] {self.addr}: {line}")
        parts = line.split(maxsplit=1)
        cmd = parts[0].upper()
        args = parts[1] if len(parts) > 1 else ''

        if cmd in ('CLOSE', 'EXIT', 'QUIT'):
            self.conn.sendall(b"BYE\r\n")
        elif cmd == 'ECHO':
            self.handle_echo(args)
        elif cmd == 'TIME':
            self.handle_time()
        elif cmd == 'UPLOAD':
            self.handle_upload(args)
        elif cmd == 'DOWNLOAD':
            self.handle_download(args)
        else:
            send_error_tcp(self.conn, "unknown command")

    def handle_echo(self, args):
        if not args:
            send_error_tcp(self.conn, "missing argument")
        else:
            self.conn.sendall(f"{args}\r\n".encode())

    def handle_time(self):
        current = time.strftime("%Y-%m-%d %H:%M:%S")
        self.conn.sendall(f"{current}\r\n".encode())

    def handle_upload(self, args):
        try:
            parts = shlex.split(args)
        except ValueError as e:
            send_error_tcp(self.conn, f"invalid args: {e}")
            return
        if len(parts) < 2:
            send_error_tcp(self.conn, "need filename and size")
            return
        filename = parts[0]
        try:
            filesize = int(parts[1])
        except ValueError:
            send_error_tcp(self.conn, "invalid size")
            return
        offset = int(parts[2]) if len(parts) > 2 else 0

        temp_path = os.path.join(INCOMPLETE_DIR, filename)
        final_path = os.path.join(UPLOAD_DIR, filename)

        if os.path.exists(temp_path):
            current_size = os.path.getsize(temp_path)
        else:
            current_size = 0
            open(temp_path, 'wb').close()

        if current_size != offset:
            send_error_tcp(self.conn, f"expected offset {current_size}")
            return

        if filesize < offset:
            send_error_tcp(self.conn, "offset > filesize")
            return

        send_ok_tcp(self.conn, str(current_size))

        self.conn.settimeout(TCP_TIMEOUT)
        start_time = time.time()
        received = current_size
        try:
            with open(temp_path, 'ab') as f:
                while received < filesize:
                    try:
                        chunk = self.conn.recv(min(BUFFER_SIZE, filesize - received))
                        if not chunk:
                            raise ConnectionError("Connection lost")
                        f.write(chunk)
                        received += len(chunk)
                    except socket.timeout:
                        pass
        except Exception as e:
            print(f"[!] Upload from {self.addr} interrupted: {e}")
            self.conn.settimeout(None)
            return

        self.conn.settimeout(None)
        os.rename(temp_path, final_path)
        elapsed = time.time() - start_time
        speed = filesize / elapsed / 1024 if elapsed > 0 else 0
        self.conn.sendall(f"UPLOAD complete. Speed: {speed:.2f} KB/s\r\n".encode())

    def handle_download(self, args):
        try:
            parts = shlex.split(args)
        except ValueError as e:
            send_error_tcp(self.conn, f"invalid args: {e}")
            return
        if len(parts) < 1:
            send_error_tcp(self.conn, "need filename")
            return
        filename = parts[0]
        offset = int(parts[1]) if len(parts) > 1 else 0

        filepath = os.path.join(UPLOAD_DIR, filename)
        if not os.path.exists(filepath):
            send_error_tcp(self.conn, "file not found")
            return

        filesize = os.path.getsize(filepath)
        if offset > filesize:
            send_error_tcp(self.conn, "offset beyond file size")
            return

        send_ok_tcp(self.conn, f"{filesize} {offset}")

        self.conn.settimeout(TCP_TIMEOUT)
        start_time = time.time()
        sent = offset
        try:
            with open(filepath, 'rb') as f:
                f.seek(offset)
                while sent < filesize:
                    chunk = f.read(BUFFER_SIZE)
                    if not chunk:
                        break
                    self.conn.sendall(chunk)
                    sent += len(chunk)
        except Exception as e:
            print(f"[!] Download to {self.addr} interrupted: {e}")
            self.conn.settimeout(None)
            return

        self.conn.settimeout(None)
        elapsed = time.time() - start_time
        speed = (filesize - offset) / elapsed / 1024 if elapsed > 0 else 0
        self.conn.sendall(f"DOWNLOAD complete. Speed: {speed:.2f} KB/s\r\n".encode())
