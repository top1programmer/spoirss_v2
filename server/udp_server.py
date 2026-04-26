# server/udp_server.py
import io
import os
import shlex
import socket
import struct
import time

from common.config import *


class UDPServer:
    def __init__(self, sock, addr):
        self.sock = sock
        self.client_addr = addr
        self.last_activity = time.time()

        self.sock.settimeout(UDP_TIMEOUT)
        self.setup_buffers()

    # ==========================================================
    # common
    # ==========================================================
    def setup_buffers(self):
        try:
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 16 * 1024 * 1024)
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 16 * 1024 * 1024)
        except Exception:
            pass

    def update_activity(self):
        self.last_activity = time.time()

    def sendto(self, data):
        try:
            self.sock.sendto(data, self.client_addr)
        except Exception:
            pass

    def send_ack(self, seq):
        self.sendto(struct.pack('!I', seq))

    def send_ok(self, msg=''):
        self.sendto(f"ACK OK {msg}".encode() if msg else b"ACK OK")

    def send_error(self, msg):
        self.sendto(f"ACK ERROR {msg}".encode())

    def recv_packet(self):
        try:
            return self.sock.recvfrom(UDP_BUFFER_SIZE)
        except BlockingIOError:
            return None
        except socket.timeout:
            return None

    def packet_count(self, filesize):
        return (filesize + UDP_DATA_SIZE - 1) // UDP_DATA_SIZE

    # ==========================================================
    # upload
    # ==========================================================
    def handle_upload(self, args):
        params = self.parse_upload_args(args)
        if not params:
            return

        filename, filesize, offset = params

        os.makedirs(INCOMPLETE_DIR, exist_ok=True)
        os.makedirs(UPLOAD_DIR, exist_ok=True)

        temp_path = os.path.join(INCOMPLETE_DIR, filename)
        final_path = os.path.join(UPLOAD_DIR, filename)

        current_size = self.file_size(temp_path)

        if current_size != offset:
            self.send_error(f"expected offset {current_size}")
            return

        expected_seq = offset // UDP_DATA_SIZE
        total_packets = self.packet_count(filesize)

        self.send_ok(str(expected_seq))

        ok = self.receive_upload_file(
            temp_path,
            expected_seq,
            total_packets
        )

        if not ok:
            return

        self.finish_upload(temp_path, final_path, filesize)

    def parse_upload_args(self, args):
        try:
            parts = shlex.split(args)
            filename = parts[0]
            filesize = int(parts[1])
            offset = int(parts[2]) if len(parts) > 2 else 0
            return filename, filesize, offset
        except Exception:
            self.send_error("invalid args")
            return None

    def file_size(self, path):
        if os.path.exists(path):
            return os.path.getsize(path)
        return 0

    def receive_upload_file(self, path, expected_seq, total_packets):
        received = {}
        last_progress = time.time()

        with open(path, 'ab') as raw:
            raw.seek(expected_seq * UDP_DATA_SIZE)
            buf = io.BufferedWriter(raw, buffer_size=BUFFER_SIZE)

            while expected_seq < total_packets:
                self.collect_upload_packets(received)
                new_seq = self.flush_upload_packets(
                    buf,
                    received,
                    expected_seq
                )

                if new_seq != expected_seq:
                    expected_seq = new_seq
                    last_progress = time.time()
                    self.update_activity()

                self.send_ack(expected_seq)

                if time.time() - last_progress > 30:
                    print("[UDP upload] stalled")
                    buf.flush()
                    return False

            buf.flush()

        self.send_final_acks(expected_seq)
        return True

    def collect_upload_packets(self, received):
        for _ in range(1024):
            packet = self.recv_packet()

            if not packet:
                break

            data, addr = packet

            if addr != self.client_addr:
                continue

            if len(data) < 4:
                continue

            seq = struct.unpack('!I', data[:4])[0]

            if seq not in received:
                received[seq] = data[4:]

    def flush_upload_packets(self, buf, received, expected_seq):
        while expected_seq in received:
            buf.write(received.pop(expected_seq))
            expected_seq += 1

        return expected_seq

    def send_final_acks(self, seq):
        for _ in range(5):
            self.send_ack(seq)
            time.sleep(0.001)

    def finish_upload(self, temp_path, final_path, filesize):
        try:
            os.replace(temp_path, final_path)
        except Exception:
            pass

        self.sendto(b"UPLOAD complete")
        print(f"[UDP upload] FINISHED {filesize} bytes from {self.client_addr}")

    # ==========================================================
    # download
    # ==========================================================
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

        self.send_ok(f"{filesize} {offset}")
        self.send_download_file(filepath, filesize, offset)

    def parse_download_args(self, args):
        try:
            parts = shlex.split(args)
            filename = parts[0]
            offset = int(parts[1]) if len(parts) > 1 else 0
            return filename, offset
        except Exception:
            self.send_error("invalid args")
            return None

    def send_download_file(self, filepath, filesize, offset):
        base = offset // UDP_DATA_SIZE
        next_seq = base
        last_ack = base - 1

        total = self.packet_count(filesize)

        retries = 0
        start_time = time.time()
        last_progress = start_time

        cache = {}

        packet_buf = bytearray(UDP_HEADER_SIZE + UDP_DATA_SIZE)
        mv = memoryview(packet_buf)

        sent_packets = 0
        resent_packets = 0

        BURST = 128

        with open(filepath, 'rb') as f:
            f.seek(offset)

            while last_ack < total - 1:

                # send only burst packets
                limit = min(next_seq + BURST, base + WINDOW_SIZE, total)

                while next_seq < limit:

                    if next_seq not in cache:
                        packet = self.build_packet(f, mv, next_seq)
                        if packet is None:
                            break
                        cache[next_seq] = packet

                    self.sendto(cache[next_seq])

                    next_seq += 1
                    sent_packets += 1

                # immediately read ACK
                ack = self.read_ack(last_ack)

                if ack > last_ack:
                    while base < ack:
                        cache.pop(base, None)
                        base += 1

                    last_ack = ack
                    retries = 0
                    last_progress = time.time()

                else:
                    retries += 1

                    # resend first lost packets
                    end = min(base + 32, total)

                    for seq in range(base, end):
                        packet = cache.get(seq)
                        if packet:
                            self.sendto(packet)
                            resent_packets += 1

                if retries > MAX_RETRIES:
                    print("[UDP download] aborted")
                    return

                if time.time() - last_progress > 15:
                    print("[UDP download] stalled")
                    return

        elapsed = time.time() - start_time
        speed = (filesize - offset) / elapsed / 1024 if elapsed > 0 else 0

        self.sendto(
            f"DOWNLOAD complete. Speed: {speed:.2f} KB/s".encode()
        )

        print(f"[UDP download] packets={sent_packets} retrans={resent_packets}")

    def send_window(self, f, cache, mv, base, next_seq, total):
        limit = min(base + WINDOW_SIZE, total)

        count = 0

        while next_seq < limit:
            if next_seq not in cache:
                packet = self.build_packet(f, mv, next_seq)
                if not packet:
                    break
                cache[next_seq] = packet

            self.sendto(cache[next_seq])

            next_seq += 1
            count += 1

            if count % 128 == 0:
                time.sleep(0)

        return next_seq

    def build_packet(self, f, mv, seq):
        data = f.read(UDP_DATA_SIZE)

        if not data:
            return None

        struct.pack_into('!I', mv.obj, 0, seq)
        mv[UDP_HEADER_SIZE:UDP_HEADER_SIZE + len(data)] = data

        return bytes(mv[:UDP_HEADER_SIZE + len(data)])

    def read_ack(self, last_ack):
        best = last_ack

        old = self.sock.gettimeout()
        self.sock.settimeout(0.01)

        for _ in range(4096):
            packet = self.recv_packet()

            if not packet:
                break

            data, addr = packet

            if addr != self.client_addr:
                continue

            if len(data) != 4:
                continue

            ack = struct.unpack('!I', data)[0]

            if ack > best:
                best = ack

        self.sock.settimeout(old)
        return best

    def slide_window(self, cache, base, ack):
        while base < ack:
            cache.pop(base, None)
            base += 1

        return base

    def resend_window(self, cache, base, next_seq):
        end = min(base + 256, next_seq)

        for seq in range(base, end):
            packet = cache.get(seq)

            if packet:
                self.sendto(packet)

    # ==========================================================
    # loop
    # ==========================================================


def receive_udp(sock):
    try:
        return sock.recvfrom(UDP_BUFFER_SIZE)
    except BlockingIOError:
        return None
    except socket.timeout:
        return None
    except ConnectionResetError:
        return None


def decode_udp(data):
    try:
        return data.decode('utf-8').strip()
    except UnicodeDecodeError:
        return None


def create_udp_session(sock, addr):
    print(f"[+] UDP client {addr} started session")
    return addr, UDPServer(sock, addr)

def process_udp_command(handler, cmd, args):
    if cmd == 'UPLOAD':
        handler.handle_upload(args)
        return

    if cmd == 'DOWNLOAD':
        handler.handle_download(args)
        return

    handler.send_error("unknown command")


def udp_server_loop(sock, current_client, handler, is_busy):
    packet = receive_udp(sock)

    if not packet:
        return current_client, handler, False

    data, addr = packet

    if is_busy:
        sock.sendto(b"ACK ERROR BUSY (TCP in progress)", addr)
        return current_client, handler, False

    line = decode_udp(data)

    if not line:
        return current_client, handler, False

    parts = line.split(maxsplit=1)
    cmd = parts[0].upper()
    args = parts[1] if len(parts) > 1 else ''

    allowed = {"UPLOAD", "DOWNLOAD", "LIST", "DELETE"}

    if cmd not in allowed:
        return current_client, handler, False

    if current_client is None:
        current_client, handler = create_udp_session(sock, addr)

    if addr != current_client:
        sock.sendto(b"ACK ERROR BUSY (another UDP client)", addr)
        return current_client, handler, False

    handler.update_activity()

    # transfer-команды вызываем напрямую
    if cmd == "DOWNLOAD":
        handler.handle_download(args)
        return None, None, False

    if cmd == "UPLOAD":
        handler.handle_upload(args)
        return None, None, False

    # остальные команды
    process_udp_command(handler, cmd, args)

    return current_client, handler, False