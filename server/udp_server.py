# udp_server.py (обновлённый)
import socket
import time
import struct
import os
import shlex
import select
import io
from common.config import *

class UDPServer:
    def __init__(self, sock, addr):
        self.sock = sock
        self.client_addr = addr
        self.last_activity = time.time()
        self.completed = False
        self.completion_until = 0
        self.sock.settimeout(UDP_TIMEOUT)
        # попытка увеличить системные буферы
        try:
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 32 * 1024 * 1024)
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 32 * 1024 * 1024)
        except Exception:
            pass
        snd = self.sock.getsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF)
        rcv = self.sock.getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF)
        print("SO_SNDBUF =", snd, "SO_RCVBUF =", rcv)

    def update_activity(self):
        self.last_activity = time.time()

    def mark_completed(self):
        self.completed = True
        self.completion_until = time.time() + COMPLETION_WAIT

    def is_completion_expired(self):
        return self.completed and time.time() > self.completion_until

    def send_ack(self, seq):
        try:
            self.sock.sendto(struct.pack('!I', seq), self.client_addr)
            #self.sock.sendto(f"ACK {seq}".encode(), self.client_addr)
        except Exception:
            pass

    def send_error(self, msg):
        try:
            self.sock.sendto(f"ACK ERROR {msg}".encode(), self.client_addr)
        except Exception:
            pass

    def send_ok(self, msg=''):
        try:
            # формат: "ACK OK <msg>"
            if msg:
                self.sock.sendto(f"ACK OK {msg}".encode(), self.client_addr)
            else:
                self.sock.sendto(b"ACK OK", self.client_addr)
        except Exception:
            pass

    def send_response(self, data):
        try:
            self.sock.sendto(f"ACK {data}".encode(), self.client_addr)
        except Exception:
            pass

    def handle_upload(self, args):
        self.update_activity()

        try:
            parts = shlex.split(args)
            filename = parts[0]
            filesize = int(parts[1])
            offset = int(parts[2]) if len(parts) > 2 else 0
        except Exception:
            self.send_error("invalid args")
            return

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

        received = {}
        last_ack = expected_seq
        last_progress = time.time()

        STALL_TIMEOUT = 30

        with open(temp_path, 'ab') as raw:
            buf = io.BufferedWriter(raw, buffer_size=BUFFER_SIZE)
            raw.seek(offset)

            while expected_seq < total_packets:
                try:
                    # 🔥 БЕЗ select — просто читаем всё что есть
                    for _ in range(1024):  # ограничиваем burst
                        try:
                            data, addr = self.sock.recvfrom(UDP_BUFFER_SIZE)
                        except BlockingIOError:
                            break
                        except socket.timeout:
                            break

                        if addr != self.client_addr:
                            continue

                        if len(data) < 4:
                            continue

                        seq = struct.unpack('!I', data[:4])[0]
                        payload = data[4:]

                        if seq not in received:
                            received[seq] = payload

                    advanced = False

                    while expected_seq in received:
                        buf.write(received.pop(expected_seq))
                        expected_seq += 1
                        advanced = True

                    if advanced:
                        last_progress = time.time()
                        self.update_activity()

                    # 🔥 бинарный ACK
                    if expected_seq - last_ack >= ACK_EVERY:
                        self.send_ack(expected_seq)
                        last_ack = expected_seq

                    # 🔥 keepalive ACK
                    self.send_ack(expected_seq)

                except Exception as e:
                    print(f"[UDP upload] exception: {e}")
                    buf.flush()
                    return

                if time.time() - last_progress > STALL_TIMEOUT:
                    print("[UDP upload] stalled → abort")
                    buf.flush()
                    return

            for _ in range(5):
                self.send_ack(expected_seq)
                time.sleep(0.001)

            buf.flush()

        try:
            os.replace(temp_path, final_path)
        except Exception:
            pass

        try:
            self.sock.sendto(b"UPLOAD complete", self.client_addr)
        except Exception:
            pass

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

        except Exception:
            self.send_error("invalid args")
            return

        start_seq = offset // UDP_DATA_SIZE
        total_packets = (filesize + UDP_DATA_SIZE - 1) // UDP_DATA_SIZE

        base = start_seq
        next_seq = start_seq

        retries = 0
        last_progress = time.time()

        # 🔥 cache окна
        window_cache = {}

        # 🔥 единый буфер
        packet_buf = bytearray(UDP_HEADER_SIZE + UDP_DATA_SIZE)
        mv = memoryview(packet_buf)

        print(f"[UDP download] start to {self.client_addr}: {total_packets} packets")

        with open(filepath, 'rb') as f:
            f.seek(offset)
            start_time = time.time()

            while base < total_packets:

                # ================= SEND =================
                while next_seq < base + WINDOW_SIZE and next_seq < total_packets:
                    if next_seq not in window_cache:
                        data = f.read(UDP_DATA_SIZE)
                        if not data:
                            break

                        struct.pack_into('!I', packet_buf, 0, next_seq)
                        mv[UDP_HEADER_SIZE:UDP_HEADER_SIZE+len(data)] = data

                        pkt = bytes(mv[:UDP_HEADER_SIZE + len(data)])
                        window_cache[next_seq] = pkt

                    self.sock.sendto(window_cache[next_seq], self.client_addr)
                    next_seq += 1

                # ================= RECV ACK (как upload) =================
                got_ack = False

                for _ in range(512):
                    try:
                        ack_data, addr = self.sock.recvfrom(UDP_BUFFER_SIZE)
                    except BlockingIOError:
                        break
                    except socket.timeout:
                        break

                    if addr != self.client_addr:
                        continue

                    # 🔥 ТОЛЬКО бинарный ACK
                    if len(ack_data) != 4:
                        continue

                    ack_seq = struct.unpack('!I', ack_data)[0]

                    if ack_seq > base:
                        while base < ack_seq:
                            window_cache.pop(base, None)
                            base += 1

                        retries = 0
                        last_progress = time.time()
                        got_ack = True

                # ================= RETRANSMIT =================
                if not got_ack:
                    retries += 1

                    resend_end = min(base + 32, next_seq)

                    for seq in range(base, resend_end):
                        pkt = window_cache.get(seq)
                        if pkt:
                            self.sock.sendto(pkt, self.client_addr)

                if retries > MAX_RETRIES:
                    print("[!] UDP download aborted (too many retries)")
                    return

                if time.time() - last_progress > 30:
                    print("[!] UDP download stalled")
                    return

            elapsed = time.time() - start_time
            speed = (filesize - offset) / elapsed / 1024 if elapsed > 0 else 0

            try:
                self.sock.sendto(
                    f"DOWNLOAD complete. Speed: {speed:.0f} KB/s".encode(),
                    self.client_addr
                )
            except Exception:
                pass

            print(f"[UDP download] FINISHED {speed:.0f} KB/s for {self.client_addr}")

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
    #if cmd_line.startswith("ACK "):
    if handler and addr == current_client:
        try:
            seq = int(cmd_line[4:].strip())
            handler.handle_ack(seq)
        except:
            pass
    #    return current_client, handler, False

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

