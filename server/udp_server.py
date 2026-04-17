import socket
import time
import struct
import os
import shlex
from common.config import *

class UDPServer:
    def __init__(self, sock, addr):
        self.sock = sock
        self.client_addr = addr
        self.last_activity = time.time()
        self.completed = False
        self.completion_until = 0
        self.sock.settimeout(UDP_TIMEOUT)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 16*2**20)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 16*2**20)

    def update_activity(self):
        self.last_activity = time.time()

    def mark_completed(self):
        self.completed = True
        self.completion_until = time.time() + COMPLETION_WAIT

    def is_completion_expired(self):
        return self.completed and time.time() > self.completion_until

    def send_ack(self, seq):
        self.sock.sendto(f"ACK {seq}".encode(), self.client_addr)

    def send_error(self, msg):
        self.sock.sendto(f"ACK ERROR {msg}".encode(), self.client_addr)

    def send_ok(self, msg=''):
        self.sock.sendto(f"ACK OK {msg}".encode(), self.client_addr)

    def send_response(self, data):
        self.sock.sendto(f"ACK {data}".encode(), self.client_addr)

    def handle_upload(self, args):
        self.update_activity()

        parts = shlex.split(args)
        filename = parts[0]
        filesize = int(parts[1])
        offset = int(parts[2]) if len(parts) > 2 else 0

        os.makedirs(INCOMPLETE_DIR, exist_ok=True)
        os.makedirs(UPLOAD_DIR, exist_ok=True)

        temp_path = os.path.join(INCOMPLETE_DIR, filename)
        final_path = os.path.join(UPLOAD_DIR, filename)

        current_size = os.path.getsize(temp_path) if os.path.exists(temp_path) else 0

        if current_size != offset:
            self.send_error(f"expected offset {current_size}")
            return

        expected_seq = offset // UDP_DATA_SIZE
        total_packets = (filesize + UDP_DATA_SIZE - 1) // UDP_DATA_SIZE

        self.send_ok(str(expected_seq))

        last_ack = expected_seq

        print(f"[UDP upload] receiving {total_packets} packets")

        with open(temp_path, 'ab') as f:
            f.seek(offset)

            while expected_seq < total_packets:
                try:
                    data, addr = self.sock.recvfrom(UDP_BUFFER_SIZE)

                    if addr != self.client_addr:
                        continue

                    if len(data) < 4:
                        continue

                    seq = struct.unpack('!I', data[:4])[0]
                    payload = data[4:]

                    # -----------------------
                    # IN ORDER ONLY
                    # -----------------------
                    if seq == expected_seq:
                        f.write(payload)
                        expected_seq += 1
                        self.update_activity()

                    # всегда ACK текущее ожидаемое
                    if expected_seq - last_ack >= 256:
                        self.send_ack(expected_seq)
                        last_ack = expected_seq

                except socket.timeout:
                    # keep alive ACK
                    self.send_ack(expected_seq)

        # финальные ACK
        for _ in range(3):
            self.send_ack(expected_seq)

        try:
            os.rename(temp_path, final_path)
        except:
            pass

        self.sock.sendto(b"UPLOAD complete", self.client_addr)

        print("[UDP upload] finished")

    def handle_download(self, args):
        self.update_activity()

        try:
            parts = shlex.split(args)
            filename = parts[0]
            offset = int(parts[1]) if len(parts) > 1 else 0

            filepath = os.path.join(UPLOAD_DIR, filename)
            if not os.path.exists(filepath):
                self.send_error("file not found")
                return

            filesize = os.path.getsize(filepath)
            self.send_ok(f"{filesize} {offset}")

        except:
            self.send_error("invalid args")
            return

        start_seq = offset // UDP_DATA_SIZE
        total_packets = (filesize + UDP_DATA_SIZE - 1) // UDP_DATA_SIZE

        base = start_seq
        next_seq = start_seq

        sent_packets = {}

        MAX_RETRIES = 50
        retries = 0

        BATCH_SIZE = 64
        batch = []
        pack = struct.Struct('!I').pack

        print(f"[UDP download] start: {total_packets} packets")

        with open(filepath, 'rb') as f:
            f.seek(offset)
            start_time = time.time()

            while base < total_packets:

                # отправка окна
                while next_seq < base + WINDOW_SIZE and next_seq < total_packets:
                    if next_seq not in sent_packets:
                        data = f.read(UDP_DATA_SIZE)
                        if not data:
                            break

                        packet = pack(next_seq) + data
                        sent_packets[next_seq] = packet

                        batch.append(packet)

                        if len(batch) >= BATCH_SIZE:
                            for pkt in batch:
                                self.sock.sendto(pkt, self.client_addr)
                            batch.clear()
                            time.sleep(0.001)

                    next_seq += 1

                # дослать остаток
                for pkt in batch:
                    self.sock.sendto(pkt, self.client_addr)
                batch.clear()

                # получение ACK
                try:
                    ack_data, addr = self.sock.recvfrom(UDP_BUFFER_SIZE)

                    if addr == self.client_addr and ack_data.startswith(b'ACK '):
                        ack_seq = int(ack_data[4:].strip())

                        if ack_seq > base:
                            while base < ack_seq:
                                sent_packets.pop(base, None)
                                base += 1
                            retries = 0

                except socket.timeout:
                    retries += 1

                    if retries > MAX_RETRIES:
                        print("[!] UDP download aborted")
                        return

                    # resend окно
                    for seq in range(base, min(base + 200, next_seq)):
                        if seq in sent_packets:
                            batch.append(sent_packets[seq])

                            if len(batch) >= BATCH_SIZE:
                                for pkt in batch:
                                    self.sock.sendto(pkt, self.client_addr)
                                batch.clear()
                                time.sleep(0.001)

                    for pkt in batch:
                        self.sock.sendto(pkt, self.client_addr)
                    batch.clear()

            elapsed = time.time() - start_time
            speed = (filesize - offset) / elapsed / 1024 if elapsed > 0 else 0

            self.sock.sendto(
                f"DOWNLOAD complete. Speed: {speed:.0f} KB/s".encode(),
                self.client_addr
            )

            print(f"[UDP download] FINISHED {speed:.0f} KB/s")

    def handle_echo(self, args):
        if self.completed:
            return
        self.update_activity()
        if not args:
            self.send_error("missing argument")
        else:
            self.send_response(args)

    def handle_time(self):
        if self.completed:
            return
        self.update_activity()
        current = time.strftime("%Y-%m-%d %H:%M:%S")
        self.send_response(current)

    def handle_close(self):
        if self.completed:
            return
        self.update_activity()
        self.send_response("BYE")
        return True

    def handle_ack(self, seq):
        if self.completed:
            return
        self.update_activity()
        pass

def udp_server_loop(sock, current_client, handler, is_busy):
    # Проверка таймаута неактивности и завершённых сессий
    if current_client is not None and handler is not None:
        if handler.is_completion_expired():
            print(f"[-] UDP client {current_client} completion period expired, freeing session")
            current_client = None
            handler = None
        elif time.time() - handler.last_activity > 10.0:
            print(f"[-] UDP client {current_client} timed out (inactive)")
            current_client = None
            handler = None

    try:
        data, addr = sock.recvfrom(UDP_BUFFER_SIZE)
    except socket.timeout:
        return current_client, handler, False
    except BlockingIOError:
        return current_client, handler, False
    except ConnectionResetError:
        if current_client is not None:
            print(f"[!] UDP client {current_client} reset connection")
            return None, None, False
        return current_client, handler, False

    if is_busy:
        sock.sendto(b"ACK ERROR BUSY (TCP in progress)", addr)
        print(f"[!] Rejected UDP client {addr} (TCP busy)")
        return current_client, handler, False

    # Попытка декодировать как UTF-8
    try:
        cmd_line = data.decode('utf-8').strip()
    except UnicodeDecodeError:
        # Бинарный пакет – обновляем активность, если это текущий клиент
        if current_client == addr and handler is not None:
            handler.update_activity()
        return current_client, handler, False

    # Обработка ACK-пакетов
    if cmd_line.startswith("ACK "):
        if handler and addr == current_client:
            try:
                seq = int(cmd_line[4:].strip())
                handler.handle_ack(seq)
            except:
                pass
        return current_client, handler, False

    # Если пришла команда от того же клиента, но сессия завершена, начинаем новую
    if handler and handler.completed and addr == current_client:
        # Завершаем старую сессию и создадим новую
        current_client = None
        handler = None

    # Обработка команд
    print(f"[UDP command] {addr}: {cmd_line}")
    parts = cmd_line.split(maxsplit=1)
    cmd = parts[0].upper()
    args = parts[1] if len(parts) > 1 else ''

    if current_client is None or addr == current_client:
        if current_client is None:
            current_client = addr
            handler = UDPServer(sock, addr)
            print(f"[+] UDP client {addr} started session")
        else:
            handler.update_activity()

        if cmd in ('CLOSE', 'EXIT', 'QUIT'):
            handler.handle_close()
            print(f"[-] UDP client {addr} ended session")
            return None, None, False
        elif cmd == 'ECHO':
            handler.handle_echo(args)
        elif cmd == 'TIME':
            handler.handle_time()
        elif cmd == 'UPLOAD':
            handler.handle_upload(args)
            return current_client, handler, False
        elif cmd == 'DOWNLOAD':
            handler.handle_download(args)
            return current_client, handler, False
        else:
            handler.send_error("unknown command")
        return current_client, handler, False
    else:
        sock.sendto(b"ACK ERROR BUSY (another UDP client)", addr)
        print(f"[!] Rejected UDP client {addr} (busy with another UDP client)")
        return current_client, handler, False
