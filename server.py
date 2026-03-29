#!/usr/bin/env python3
"""
Последовательный сервер для TCP и UDP с поддержкой команд:
ECHO, TIME, CLOSE, UPLOAD, DOWNLOAD.
Сервер одновременно слушает TCP и UDP, обрабатывая их в одном потоке через select.
"""
import socket
import select
import time
import os
import shlex
import struct
import argparse

HOST = '0.0.0.0'
PORT = 12345
BUFFER_SIZE = 8192
UDP_BUFFER_SIZE = 1472
UDP_HEADER_SIZE = 4
UDP_DATA_SIZE = UDP_BUFFER_SIZE - UDP_HEADER_SIZE
UPLOAD_DIR = 'uploads'
INCOMPLETE_DIR = 'incomplete'
BACKLOG = 5
TCP_TIMEOUT = 30
UDP_TIMEOUT = 0.002
WINDOW_SIZE = 1000
retries = 0
MAX_RETRIES = 30
COMPLETION_WAIT = 5.0

def setup_keepalive(sock):
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    try:
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 30)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 5)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 3)
    except AttributeError:
        pass

def recv_line_tcp(sock):
    data = b''
    while True:
        try:
            chunk = sock.recv(1024)
        except socket.timeout:
            raise
        except socket.error:
            return None
        if not chunk:
            return None
        data += chunk
        if data.endswith(b'\n'):
            break
    return data.decode().rstrip('\r\n')

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

# ----------------------------------------------------------------------
# UDP часть
# ----------------------------------------------------------------------
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

        try:
            parts = shlex.split(args)
        except ValueError as e:
            self.send_error(f"invalid args: {e}")
            return

        if len(parts) < 2:
            self.send_error("need filename and size")
            return

        filename = parts[0]
        filesize = int(parts[1])
        offset = int(parts[2]) if len(parts) > 2 else 0

        temp_path = os.path.join(INCOMPLETE_DIR, filename)
        final_path = os.path.join(UPLOAD_DIR, filename)

        if os.path.exists(temp_path):
            current_size = os.path.getsize(temp_path)
        else:
            current_size = 0
            open(temp_path, 'wb').close()

        if current_size != offset:
            self.send_error(f"expected offset {current_size}")
            return

        expected_seq = offset // UDP_DATA_SIZE
        self.send_ok(str(expected_seq))

        total_packets = (filesize + UDP_DATA_SIZE - 1) // UDP_DATA_SIZE

        ACK_EVERY = 128
        last_acked = expected_seq

        print(f"[UDP upload] receiving {total_packets} packets")

        with open(temp_path, 'ab') as f:
            f.seek(offset)
            start_time = time.time()

            while expected_seq < total_packets:
                try:
                    data, addr = self.sock.recvfrom(UDP_BUFFER_SIZE)
                    if addr != self.client_addr:
                        continue

                    if len(data) < UDP_HEADER_SIZE:
                        continue

                    seq = struct.unpack('!I', data[:4])[0]
                    payload = data[4:]

                    if seq == expected_seq:
                        f.write(payload)
                        expected_seq += 1
                        self.update_activity()

                        # ACK batching
                        if expected_seq - last_acked >= ACK_EVERY:
                            self.send_ack(expected_seq)
                            last_acked = expected_seq

                    elif seq > expected_seq:
                        self.send_ack(expected_seq)

                except socket.timeout:
                    self.send_ack(expected_seq)

            # финальные ACK
            for _ in range(5):
                self.send_ack(expected_seq)
                time.sleep(0.001)

        elapsed = time.time() - start_time
        speed = filesize / elapsed / 1024 if elapsed > 0 else 0

        try:
            os.rename(temp_path, final_path)
        except Exception as e:
            print(f"[!] Rename error: {e}")

        self.sock.sendto(
            f"UPLOAD complete. Speed: {speed:.2f} KB/s".encode(),
            self.client_addr
        )

        print(f"[UDP upload] FINISHED {speed:.0f} KB/s")

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

def clean_dirs():
    os.makedirs(INCOMPLETE_DIR, exist_ok=True)
    os.makedirs(UPLOAD_DIR, exist_ok=True)
    for f in os.listdir(INCOMPLETE_DIR):
        os.remove(os.path.join(INCOMPLETE_DIR, f))

def main():
    parser = argparse.ArgumentParser(description='Сервер для передачи файлов (TCP+UDP одновременно)')
    parser.add_argument('--port', type=int, default=PORT, help='Порт для прослушивания')
    args = parser.parse_args()

    clean_dirs()

    tcp_listen = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcp_listen.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    setup_keepalive(tcp_listen)
    tcp_listen.bind((HOST, args.port))
    tcp_listen.listen(BACKLOG)
    tcp_listen.setblocking(False)

    udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    udp_sock.bind((HOST, args.port))
    udp_sock.setblocking(False)
    udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 16*2**20)
    udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 16*2**20)

    print(f"[*] Server listening on port {args.port} (TCP and UDP)")

    udp_current_client = None
    udp_handler = None
    tcp_clients = []

    try:
        while True:
            rlist = [tcp_listen, udp_sock]
            if tcp_clients:
                rlist.append(tcp_clients[0])
            readable, _, _ = select.select(rlist, [], [], 1.0)

            is_tcp_busy = bool(tcp_clients)

            for sock in readable:
                if sock is tcp_listen:
                    conn, addr = tcp_listen.accept()
                    if tcp_clients:
                        setup_keepalive(conn)
                        try:
                            conn.sendall(b"BUSY: server is handling another TCP client. Try later.\r\n")
                        except:
                            pass
                        conn.close()
                        print(f"[!] Rejected TCP connection from {addr} (busy)")
                    else:
                        setup_keepalive(conn)
                        tcp_clients.append(TCPClient(conn, addr))
                elif sock is udp_sock:
                    udp_current_client, udp_handler, _ = udp_server_loop(
                        udp_sock, udp_current_client, udp_handler, is_tcp_busy
                    )
                elif tcp_clients and sock is tcp_clients[0]:
                    if not tcp_clients[0].handle_input():
                        tcp_clients[0].close()
                        tcp_clients.pop()
    except KeyboardInterrupt:
        print("\n[!] Server stopped by user")
    finally:
        tcp_listen.close()
        udp_sock.close()

if __name__ == '__main__':
    main()