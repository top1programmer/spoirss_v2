# server/tcp_server.py
import os
import shlex
import socket
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
        except socket.error:
            return False

        if not data:
            return False

        self.buffer += data

        if b'\n' not in self.buffer:
            return True

        line = self.extract_line()
        return self.process_line(line)

    def extract_line(self):
        line, self.buffer = self.buffer.split(b'\n', 1)
        return line

    def process_line(self, raw_line):
        line = self.decode_line(raw_line)

        if line is None:
            return True

        should_close = self.is_close_command(line)
        self.process_command(line)

        return not should_close

    def decode_line(self, raw_line):
        try:
            return raw_line.decode('utf-8').strip()
        except UnicodeDecodeError:
            self.send_error("invalid text command")
            return None

    def is_close_command(self, line):
        upper = line.upper()
        return (
            upper.startswith('CLOSE') or
            upper.startswith('EXIT') or
            upper.startswith('QUIT')
        )

    def process_command(self, line):
        print(f"[TCP command] {self.addr}: {line}")

        cmd, args = self.parse_command(line)

        if cmd in ('CLOSE', 'EXIT', 'QUIT'):
            self.send_bye()
            return

        if cmd == 'ECHO':
            self.handle_echo(args)
            return

        if cmd == 'TIME':
            self.handle_time()
            return

        if cmd == 'UPLOAD':
            self.handle_upload(args)
            return

        if cmd == 'DOWNLOAD':
            self.handle_download(args)
            return

        self.send_error("unknown command")

    def parse_command(self, line):
        parts = line.split(maxsplit=1)

        cmd = parts[0].upper()
        args = parts[1] if len(parts) > 1 else ''

        return cmd, args

    def send_error(self, msg):
        send_error_tcp(self.conn, msg)

    def send_ok(self, msg=''):
        send_ok_tcp(self.conn, msg)

    def send_bye(self):
        self.conn.sendall(b"BYE\r\n")

    def handle_echo(self, args):
        if not args:
            self.send_error("missing argument")
            return

        self.conn.sendall(f"{args}\r\n".encode())

    def handle_time(self):
        current = time.strftime("%Y-%m-%d %H:%M:%S")
        self.conn.sendall(f"{current}\r\n".encode())

    def handle_upload(self, args):
        params = self.parse_upload_args(args)

        if not params:
            return

        filename, filesize, offset = params

        temp_path = os.path.join(INCOMPLETE_DIR, filename)
        final_path = os.path.join(UPLOAD_DIR, filename)

        current_size = self.prepare_upload_file(temp_path)

        if not self.validate_upload_sizes(current_size, offset, filesize):
            return

        self.send_ok(str(current_size))

        if not self.receive_upload_data(temp_path, filesize, current_size):
            return

        os.rename(temp_path, final_path)
        self.send_upload_complete(filesize, offset)

    def parse_upload_args(self, args):
        try:
            parts = shlex.split(args)
        except ValueError as e:
            self.send_error(f"invalid args: {e}")
            return None

        if len(parts) < 2:
            self.send_error("need filename and size")
            return None

        filename = parts[0]

        try:
            filesize = int(parts[1])
        except ValueError:
            self.send_error("invalid size")
            return None

        offset = int(parts[2]) if len(parts) > 2 else 0
        return filename, filesize, offset

    def prepare_upload_file(self, path):
        if os.path.exists(path):
            return os.path.getsize(path)

        open(path, 'wb').close()
        return 0

    def validate_upload_sizes(self, current_size, offset, filesize):
        if current_size != offset:
            self.send_error(f"expected offset {current_size}")
            return False

        if filesize < offset:
            self.send_error("offset > filesize")
            return False

        return True

    def receive_upload_data(self, path, filesize, received):
        self.conn.settimeout(TCP_TIMEOUT)

        try:
            with open(path, 'ab') as f:
                while received < filesize:
                    chunk = self.read_upload_chunk(filesize, received)

                    if chunk is None:
                        continue

                    if not chunk:
                        raise ConnectionError("Connection lost")

                    f.write(chunk)
                    received += len(chunk)

        except Exception as e:
            print(f"[!] Upload from {self.addr} interrupted: {e}")
            self.conn.settimeout(None)
            return False

        self.conn.settimeout(None)
        return True

    def read_upload_chunk(self, filesize, received):
        try:
            size = min(BUFFER_SIZE, filesize - received)
            return self.conn.recv(size)
        except socket.timeout:
            return None

    def send_upload_complete(self, filesize, offset):
        speed = self.calc_speed(filesize - offset)
        self.conn.sendall(
            f"UPLOAD complete. Speed: {speed:.2f} KB/s\r\n".encode()
        )

    def handle_download(self, args):
        params = self.parse_download_args(args)

        if not params:
            return

        filename, offset = params

        filepath = os.path.join(UPLOAD_DIR, filename)

        if not os.path.exists(filepath):
            self.send_error("file not found")
            return

        filesize = os.path.getsize(filepath)

        if offset > filesize:
            self.send_error("offset beyond file size")
            return

        self.send_ok(f"{filesize} {offset}")

        if not self.send_download_file(filepath, filesize, offset):
            return

        self.send_download_complete(filesize, offset)

    def parse_download_args(self, args):
        try:
            parts = shlex.split(args)
        except ValueError as e:
            self.send_error(f"invalid args: {e}")
            return None

        if not parts:
            self.send_error("need filename")
            return None

        filename = parts[0]
        offset = int(parts[1]) if len(parts) > 1 else 0

        return filename, offset

    def send_download_file(self, filepath, filesize, offset):
        self.conn.settimeout(TCP_TIMEOUT)
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
            return False

        self.conn.settimeout(None)
        return True

    def send_download_complete(self, filesize, offset):
        speed = self.calc_speed(filesize - offset)

        self.conn.sendall(
            f"DOWNLOAD complete. Speed: {speed:.2f} KB/s\r\n".encode()
        )

    def calc_speed(self, size):
        now = time.time()

        if not hasattr(self, '_speed_start'):
            self._speed_start = now

        elapsed = now - self._speed_start
        self._speed_start = now

        if elapsed <= 0:
            return 0

        return size / elapsed / 1024