import socket
import struct
import time
from common.config import *

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

        pack = struct.Struct('!I').pack
        total_packets = (filesize + UDP_DATA_SIZE - 1) // UDP_DATA_SIZE

        batch = []
        BATCH_SIZE = 32   # ⚠️ уменьшили (64 часто фризит UDP)

        retries = 0
        MAX_RETRIES = 50

        start_time = time.time()
        last_progress = start_time

        with open(filename, 'rb') as f:
            f.seek(offset)

            while last_ack < total_packets - 1:

                # -------------------------
                # 1. SENDING WINDOW
                # -------------------------
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

                for pkt in batch:
                    self.sock.sendto(pkt, self.addr)
                batch.clear()

                # -------------------------
                # 2. ACK (ВАЖНО — 1 попытка)
                # -------------------------
                try:
                    ack_data, _ = self.sock.recvfrom(UDP_BUFFER_SIZE)

                    if ack_data.startswith(b'ACK '):
                        try:
                            ack_seq = int(ack_data[4:])
                        except:
                            continue

                        if ack_seq > last_ack:
                            last_ack = ack_seq
                            base = ack_seq
                            retries = 0
                            last_progress = time.time()

                except socket.timeout:
                    retries += 1

                    # resend НЕМНОГО, не всё окно
                    resend_end = min(base + 100, total_packets)

                    f.seek(base * UDP_DATA_SIZE)

                    for seq in range(base, resend_end):
                        data = f.read(UDP_DATA_SIZE)
                        if not data:
                            break
                        self.sock.sendto(pack(seq) + data, self.addr)

                    next_seq = max(next_seq, resend_end)

                # -------------------------
                # 3. FAIL SAFE
                # -------------------------
                if retries > MAX_RETRIES:
                    print("[UDP upload] too many retries → abort")
                    return None

                if time.time() - last_progress > 20:
                    print("[UDP upload] stalled → abort")
                    return None

        elapsed = time.time() - start_time

        if elapsed <= 0:
            return None

        return filesize / elapsed / 1024

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
                time.sleep(0.001)
            except:
                pass

        elapsed = time.time() - start_time
        return (filesize - offset) / elapsed / 1024 if elapsed > 0 else 0
