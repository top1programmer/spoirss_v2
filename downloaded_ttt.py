#!/usr/bin/env python3
"""
Последовательный сервер для TCP и UDP с поддержкой команд:
ECHO, TIME, CLOSE, UPLOAD, DOWNLOAD.
Сервер одновременно обслуживает множество TCP и UDP клиентов в одном потоке через select.
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
UDP_BUFFER_SIZE = 1472          # 1500 - 28 (IP+UDP)
UDP_HEADER_SIZE = 4
UDP_DATA_SIZE = UDP_BUFFER_SIZE - UDP_HEADER_SIZE
UPLOAD_DIR = 'uploads'
INCOMPLETE_DIR = 'incomplete'
BACKLOG = 5
TCP_TIMEOUT = 30
UDP_TIMEOUT = 0.2                # таймаут для ожидания ACK
WINDOW_SIZE = 20                 # размер окна для UDP download
MAX_RETRIES = 30                 # макс. число повторных передач
COMPLETION_WAIT = 5.0            # время ожидания завершения сессии

def setup_keepalive(sock):
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    try:
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 30)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 5)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 3)
    except AttributeError:
        pass

def recv_line_tcp(sock):
    """Чтение строки из TCP сокета (блокирующий режим, для начального приветствия)"""
    data = b''
    while True:
        try:
            chunk = sock.recv(1)
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
    """Клиент TCP, работающий в неблокирующем режиме."""
    def __init__(self, conn, addr):
        self.conn = conn
        self.addr = addr
        self.buffer = b''
        self.state = 'command'          # 'command', 'upload', 'download'
        self.upload_file = None
        self.upload_size = 0
        self.upload_received = 0
        self.upload_temp_path = None
        self.upload_filename = None
        self.download_file = None
        self.download_size = 0
        self.download_sent = 0
        self.download_path = None
        self.conn.setblocking(False)
        print(f"[+] TCP client {addr} connected")

    def fileno(self):
        return self.conn.fileno()

    def close(self):
        if self.upload_file:
            self.upload_file.close()
        if self.download_file:
            self.download_file.close()
        self.conn.close()
        print(f"[-] TCP client {self.addr} disconnected")

    def wants_write(self):
        """Нужно ли следить за записью в этом сокете."""
        return self.state == 'download' and self.download_file and self.download_sent < self.download_size

    def handle_read(self):
        """Обработка события чтения из сокета."""
        try:
            data = self.conn.recv(BUFFER_SIZE)
            if not data:
                return False
            if self.state == 'upload':
                # Режим приема файла – данные пишем как есть
                self.upload_file.write(data)
                self.upload_received += len(data)
                if self.upload_received >= self.upload_size:
                    self.upload_file.close()
                    final_path = os.path.join(UPLOAD_DIR, self.upload_filename)
                    try:
                        os.rename(self.upload_temp_path, final_path)
                    except Exception as e:
                        print(f"[!] Rename failed: {e}")
                    self.conn.sendall(f"UPLOAD complete.\r\n".encode())
                    self.state = 'command'
                    self.upload_file = None
            else:  # command mode
                self.buffer += data
                while b'\n' in self.buffer:
                    line, self.buffer = self.buffer.split(b'\n', 1)
                    line = line.rstrip(b'\r')  # удаляем возможный \r
                    try:
                        line_str = line.decode('utf-8').strip()
                    except UnicodeDecodeError:
                        # Если пришли не-ASCII данные (например, телнетовский IAC), игнорируем строку и отвечаем ошибкой
                        self.conn.sendall(b"ERROR invalid command encoding\r\n")
                        continue
                    if line_str == '':
                        continue  # пустая строка игнорируется
                    if not self._process_command(line_str):
                        return False
        except socket.error as e:
            if e.errno in (socket.EAGAIN, socket.EWOULDBLOCK):
                return True
            return False
        return True

    def handle_write(self):
        """Обработка события записи в сокет."""
        if self.state == 'download' and self.download_file:
            try:
                chunk = self.download_file.read(BUFFER_SIZE)
                if chunk:
                    self.conn.sendall(chunk)
                    self.download_sent += len(chunk)
                    if self.download_sent >= self.download_size:
                        self.download_file.close()
                        self.conn.sendall(f"DOWNLOAD complete.\r\n".encode())
                        self.state = 'command'
                        self.download_file = None
                else:
                    # Файл закончился раньше времени (маловероятно)
                    self.download_file.close()
                    self.state = 'command'
            except socket.error as e:
                if e.errno not in (socket.EAGAIN, socket.EWOULDBLOCK):
                    self.close()
                    return False
        return True

    def _process_command(self, line):
        """Разбор команды в командном режиме."""
        print(f"[TCP command] {self.addr}: {line}")
        parts = line.split(maxsplit=1)
        cmd = parts[0].upper()
        args = parts[1] if len(parts) > 1 else ''

        if cmd in ('CLOSE', 'EXIT', 'QUIT'):
            self.conn.sendall(b"BYE\r\n")
            self.close()
            return False
        elif cmd == 'ECHO':
            self._handle_echo(args)
        elif cmd == 'TIME':
            self._handle_time()
        elif cmd == 'UPLOAD':
            self._handle_upload(args)
        elif cmd == 'DOWNLOAD':
            self._handle_download(args)
        else:
            send_error_tcp(self.conn, "unknown command")
        return True

    def _handle_echo(self, args):
        if not args:
            send_error_tcp(self.conn, "missing argument")
        else:
            self.conn.sendall(f"{args}\r\n".encode())

    def _handle_time(self):
        current = time.strftime("%Y-%m-%d %H:%M:%S")
        self.conn.sendall(f"{current}\r\n".encode())

    def _handle_upload(self, args):
        try:
            parts = shlex.split(args)
            filename = parts[0]
            filesize = int(parts[1])
            offset = int(parts[2]) if len(parts) > 2 else 0
        except Exception as e:
            send_error_tcp(self.conn, f"invalid args: {e}")
            return

        temp_path = os.path.join(INCOMPLETE_DIR, filename)
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

        self.state = 'upload'
        self.upload_file = open(temp_path, 'ab')
        self.upload_file.seek(offset)
        self.upload_size = filesize
        self.upload_received = offset
        self.upload_temp_path = temp_path
        self.upload_filename = filename

    def _handle_download(self, args):
        try:
            parts = shlex.split(args)
            filename = parts[0]
            offset = int(parts[1]) if len(parts) > 1 else 0
        except Exception as e:
            send_error_tcp(self.conn, f"invalid args: {e}")
            return

        filepath = os.path.join(UPLOAD_DIR, filename)
        if not os.path.exists(filepath):
            send_error_tcp(self.conn, "file not found")
            return

        filesize = os.path.getsize(filepath)
        if offset > filesize:
            send_error_tcp(self.conn, "offset beyond file size")
            return

        send_ok_tcp(self.conn, f"{filesize} {offset}")

        self.state = 'download'
        self.download_file = open(filepath, 'rb')
        self.download_file.seek(offset)
        self.download_size = filesize
        self.download_sent = offset
        self.download_path = filepath

class UDPServer:
    """Обработчик UDP клиента. Хранит состояние передачи."""
    def __init__(self, sock, addr):
        self.sock = sock
        self.client_addr = addr
        self.state = 'command'               # 'command', 'upload', 'download'
        self.completed = False
        self.completion_until = 0

        # Upload
        self.upload_file = None
        self.upload_filename = None
        self.upload_size = 0
        self.upload_expected_seq = 0
        self.upload_temp_path = None

        # Download
        self.download_file = None
        self.download_filename = None
        self.download_size = 0
        self.download_offset = 0
        self.download_next_seq = 0
        self.download_base = 0
        self.download_last_ack = -1
        self.download_sent_packets = {}       # seq -> packet
        self.download_retries = 0
        self.download_last_activity = time.time()
        self.download_total_packets = 0
        self.download_start_time = 0

        self.update_activity()

    def update_activity(self):
        self.download_last_activity = time.time()

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

    def handle_datagram(self, data):
        """Основной вход для обработки дейтаграммы от этого клиента."""
        if self.state == 'command':
            self._handle_command(data)
        elif self.state == 'upload':
            self._handle_upload_data(data)
        elif self.state == 'download':
            self._handle_download_ack(data)

    def _handle_command(self, data):
        try:
            cmd_line = data.decode().strip()
        except UnicodeDecodeError:
            return
        print(f"[UDP command] {self.client_addr}: {cmd_line}")
        parts = cmd_line.split(maxsplit=1)
        cmd = parts[0].upper()
        args = parts[1] if len(parts) > 1 else ''

        if cmd in ('CLOSE', 'EXIT', 'QUIT'):
            self.send_ok("BYE")
            self.mark_completed()
        elif cmd == 'ECHO':
            self._handle_echo(args)
        elif cmd == 'TIME':
            self._handle_time()
        elif cmd == 'UPLOAD':
            self._handle_upload_command(args)
        elif cmd == 'DOWNLOAD':
            self._handle_download_command(args)
        else:
            self.send_error("unknown command")

    def _handle_echo(self, args):
        if not args:
            self.send_error("missing argument")
        else:
            self.send_ok(args)

    def _handle_time(self):
        current = time.strftime("%Y-%m-%d %H:%M:%S")
        self.send_ok(current)

    def _handle_upload_command(self, args):
        try:
            parts = shlex.split(args)
            filename = parts[0]
            filesize = int(parts[1])
            offset = int(parts[2]) if len(parts) > 2 else 0
        except Exception as e:
            self.send_error(f"invalid args: {e}")
            return

        temp_path = os.path.join(INCOMPLETE_DIR, filename)
        if os.path.exists(temp_path):
            current_size = os.path.getsize(temp_path)
        else:
            current_size = 0
            open(temp_path, 'wb').close()

        if current_size != offset:
            self.send_error(f"expected offset {current_size}")
            return

        self.upload_expected_seq = offset // UDP_DATA_SIZE
        self.send_ok(str(self.upload_expected_seq))

        self.state = 'upload'
        self.upload_file = open(temp_path, 'ab')
        self.upload_file.seek(offset)
        self.upload_size = filesize
        self.upload_filename = filename
        self.upload_temp_path = temp_path
        print(f"[UDP upload] started for {self.client_addr}")

    def _handle_upload_data(self, data):
        if len(data) < UDP_HEADER_SIZE:
            return
        seq = struct.unpack('!I', data[:4])[0]
        payload = data[4:]
        if seq == self.upload_expected_seq:
            self.upload_file.write(payload)
            self.upload_expected_seq += 1
            self.send_ack(self.upload_expected_seq)
            if self.upload_expected_seq * UDP_DATA_SIZE >= self.upload_size:
                self.upload_file.close()
                final_path = os.path.join(UPLOAD_DIR, self.upload_filename)
                try:
                    os.rename(self.upload_temp_path, final_path)
                except Exception as e:
                    print(f"[!] Rename failed: {e}")
                self.sock.sendto(b"UPLOAD complete", self.client_addr)
                self.state = 'command'
                self.mark_completed()
        elif seq > self.upload_expected_seq:
            self.send_ack(self.upload_expected_seq)
        # дубликаты можно игнорировать или тоже подтверждать (необязательно)

    def _handle_download_command(self, args):
        try:
            parts = shlex.split(args)
            filename = parts[0]
            offset = int(parts[1]) if len(parts) > 1 else 0
        except Exception as e:
            self.send_error(f"invalid args: {e}")
            return

        filepath = os.path.join(UPLOAD_DIR, filename)
        if not os.path.exists(filepath):
            self.send_error("file not found")
            return

        filesize = os.path.getsize(filepath)
        if offset > filesize:
            self.send_error("offset beyond file size")
            return

        self.send_ok(f"{filesize} {offset}")

        self.state = 'download'
        self.download_file = open(filepath, 'rb')
        self.download_size = filesize
        self.download_offset = offset
        self.download_filename = filename
        self.download_next_seq = offset // UDP_DATA_SIZE
        self.download_base = self.download_next_seq
        self.download_last_ack = self.download_next_seq - 1
        self.download_sent_packets = {}
        self.download_retries = 0
        self.update_activity()
        self.download_total_packets = (filesize + UDP_DATA_SIZE - 1) // UDP_DATA_SIZE
        self.download_start_time = time.time()
        print(f"[UDP download] started for {self.client_addr}, {self.download_total_packets} packets")

    def _handle_download_ack(self, data):
        try:
            ack_line = data.decode().strip()
            if ack_line.startswith('ACK '):
                ack_seq = int(ack_line[4:])
                self.update_activity()
                if ack_seq > self.download_last_ack:
                    self.download_last_ack = ack_seq
                    self.download_base = self.download_last_ack + 1
                    self.download_retries = 0
                    # Удаляем подтверждённые пакеты
                    for seq in list(self.download_sent_packets.keys()):
                        if seq < self.download_base:
                            del self.download_sent_packets[seq]
                    self._try_send_window()
                    # Проверка завершения
                    if self.download_last_ack >= self.download_total_packets - 1:
                        self._finish_download()
        except Exception:
            pass

    def _try_send_window(self):
        """Отправляет новые пакеты в пределах окна."""
        if self.state != 'download':
            return
        while self.download_next_seq < min(self.download_base + WINDOW_SIZE, self.download_total_packets):
            if self.download_next_seq not in self.download_sent_packets:
                # Читаем данные из файла по смещению
                self.download_file.seek(self.download_next_seq * UDP_DATA_SIZE)
                data = self.download_file.read(UDP_DATA_SIZE)
                if not data:
                    break
                packet = struct.pack('!I', self.download_next_seq) + data
                self.sock.sendto(packet, self.client_addr)
                self.download_sent_packets[self.download_next_seq] = packet
            self.download_next_seq += 1

    def check_timeouts(self):
        """Проверка таймаута для download и ретрансмиссия."""
        if self.state != 'download':
            return
        if time.time() - self.download_last_activity > UDP_TIMEOUT:
            self.download_retries += 1
            if self.download_retries > MAX_RETRIES:
                print(f"[UDP download] timeout, aborting {self.client_addr}")
                self.state = 'command'
                self.download_file.close()
                self.mark_completed()
                return
            # Ретрансмиссия всего текущего окна
            self.download_next_seq = self.download_base
            for seq in range(self.download_base, min(self.download_base + WINDOW_SIZE, self.download_total_packets)):
                if seq in self.download_sent_packets:
                    self.sock.sendto(self.download_sent_packets[seq], self.client_addr)
            self.update_activity()

    def _finish_download(self):
        elapsed = time.time() - self.download_start_time
        speed = (self.download_size - self.download_offset) / elapsed / 1024
        self.sock.sendto(f"DOWNLOAD complete. Speed: {speed:.2f} KB/s".encode(), self.client_addr)
        print(f"[UDP download] completed for {self.client_addr}, speed {speed:.2f} KB/s")
        self.download_file.close()
        self.state = 'command'
        self.mark_completed()

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
    udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 32 * 1024 * 1024)  # 32 MB
    udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 32 * 1024 * 1024)

    print(f"[*] Server listening on port {args.port} (TCP and UDP)")

    tcp_clients = []               # список активных TCP клиентов
    udp_handlers = {}               # словарь {addr: UDPServer}

    try:
        while True:
            rlist = [tcp_listen, udp_sock]
            wlist = []
            for client in tcp_clients:
                rlist.append(client.fileno())
                if client.wants_write():
                    wlist.append(client.fileno())

            readable, writable, _ = select.select(rlist, wlist, [], 1.0)

            # Новые TCP подключения
            if tcp_listen in readable:
                conn, addr = tcp_listen.accept()
                conn.setblocking(False)
                tcp_clients.append(TCPClient(conn, addr))

            # Обработка UDP
            if udp_sock in readable:
                try:
                    data, addr = udp_sock.recvfrom(UDP_BUFFER_SIZE)
                    if addr not in udp_handlers:
                        udp_handlers[addr] = UDPServer(udp_sock, addr)
                    handler = udp_handlers[addr]
                    handler.handle_datagram(data)
                    if handler.completed and handler.is_completion_expired():
                        del udp_handlers[addr]
                except socket.error:
                    pass

            # Обработка готовых TCP клиентов на чтение
            for sock in readable:
                if sock is tcp_listen or sock is udp_sock:
                    continue
                # sock — целочисленный дескриптор
                for client in tcp_clients[:]:
                    if client.fileno() == sock:
                        if not client.handle_read():
                            client.close()
                            tcp_clients.remove(client)
                        break

            # Обработка готовых TCP клиентов на запись
            for sock in writable:
                if sock is tcp_listen or sock is udp_sock:
                    continue
                for client in tcp_clients:
                    if client.fileno() == sock:
                        client.handle_write()
                        break

            # Проверка таймаутов для всех UDP обработчиков
            for addr, handler in list(udp_handlers.items()):
                handler.check_timeouts()
                if handler.completed and handler.is_completion_expired():
                    del udp_handlers[addr]

    except KeyboardInterrupt:
        print("\n[!] Server stopped by user")
    finally:
        tcp_listen.close()
        udp_sock.close()

if __name__ == '__main__':
    main()
