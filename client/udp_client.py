# udp_client.py (обновлённый)
import socket
import struct
import time
import select
import os
import shlex
from common.config import *

class UDPClient:
    def __init__(self, sock, addr):
        self.sock = sock
        self.addr = addr
        # попытка увеличить системные буферы
        try:
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 32 * 1024 * 1024)
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 32 * 1024 * 1024)
        except Exception:
            pass
        # fallback значения (если не удалось установить выше)
        # try:
        #     self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 16 * 2**20)
        #     self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 16 * 2**20)
        # except Exception:
        #     pass
        self.sock.settimeout(UDP_TIMEOUT)

        snd = self.sock.getsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF)
        rcv = self.sock.getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF)
        print("SO_SNDBUF =", snd, "SO_RCVBUF =", rcv)

    def send_command(self, cmd):
        """Отправить команду и дождаться ответа ACK/OK. Возвращает строку ответа без префикса ACK/OK."""
        data = cmd.encode() + b'\n'
        backoff = 0.05
        for attempt in range(MAX_RETRIES):
            try:
                self.sock.sendto(data, self.addr)
                resp, _ = self.sock.recvfrom(UDP_BUFFER_SIZE)
                # ожидаем текстовый ответ
                try:
                    text = resp.decode('utf-8', errors='ignore').strip()
                except:
                    text = ''
                # ответы формата "ACK OK ..." или "ACK ERROR ..." или "ACK <value>"
                if text.startswith('ACK '):
                    return text[4:].strip()
                # совместимость: сервер мог ответить просто "OK ..."
                if text.startswith('OK '):
                    return text[3:].strip()
                if text:
                    return text
            except socket.timeout:
                time.sleep(min(backoff, 1.0))
                backoff *= 1.5
                continue
        raise ConnectionError("No response to command after multiple retries")

    def _query_server_offset(self, filename, filesize, offset):
        """Повторно запросить сервер о текущем ожидаемом смещении (для восстановления)."""
        cmd = f"UPLOAD {shlex.quote(os.path.basename(filename))} {filesize} {offset}"
        try:
            resp = self.send_command(cmd)
        except Exception:
            return None, None
        # возможные форматы: "OK <seq>" или "ERROR expected offset <bytes>" или "ERROR <msg>"
        parts = resp.split()
        if not parts:
            return None, resp
        if parts[0].upper() == 'OK':
            # OK <expected_seq>
            if len(parts) >= 2:
                try:
                    expected_seq = int(parts[1])
                    return expected_seq * UDP_DATA_SIZE, resp
                except:
                    return None, resp
            return None, resp
        if parts[0].upper() == 'ERROR':
            # "ERROR expected offset <bytes>"
            if 'expected' in parts and 'offset' in parts:
                # ищем число в ответе
                for p in parts[::-1]:
                    try:
                        val = int(p)
                        return val, resp
                    except:
                        continue
            return None, resp
        # если сервер вернул просто число (старый формат)
        try:
            val = int(parts[0])
            return val, resp
        except:
            return None, resp

    # Вставьте/вызовите перед началом отправки данных (send_file)
    def parse_server_offset(resp_text, filesize):
        """
        Возвращает смещение в байтах, которое ожидает сервер.
        Поддерживает ответы:
        - "ACK OK <n>"
        - "ACK <n>"
        - "OK <n>"
        - "ERROR expected offset <n>"
        - просто "<n>"
        Если не найдено число, возвращает None.
        Если найдено число <= total_packets, интерпретирует как seq и переводит в байты.
        """
        if not resp_text:
            return None
        # извлечь последнее целое число в тексте
        parts = resp_text.replace(',', ' ').split()
        num = None
        for p in parts[::-1]:
            try:
                num = int(p)
                break
            except:
                continue
        if num is None:
            return None
        # интерпретация: если число <= total_packets — это seq
        total_packets = (filesize + UDP_DATA_SIZE - 1) // UDP_DATA_SIZE
        if 0 <= num <= total_packets:
            return num * UDP_DATA_SIZE
        return num

    def send_file(self, filename, filesize, offset=0):
        server_offset, _ = self._query_server_offset(filename, filesize, offset)

        if server_offset is not None and server_offset != offset:
            print(f"[UDP upload] server expects offset {server_offset}, resuming")
            offset = server_offset

        base = offset // UDP_DATA_SIZE
        next_seq = base
        last_ack = base - 1

        total_packets = (filesize + UDP_DATA_SIZE - 1) // UDP_DATA_SIZE

        retries = 0
        start_time = time.time()
        last_progress = start_time

        # 📦 window cache (seq -> bytes)
        window_cache = {}

        # 🔥 единый буфер пакета (zero-copy)
        packet_buf = bytearray(UDP_HEADER_SIZE + UDP_DATA_SIZE)
        mv = memoryview(packet_buf)

        packets_sent = 0
        packets_retransmitted = 0
        bytes_payload_sent = 0

        with open(filename, 'rb') as f:
            f.seek(offset)

            while last_ack < total_packets - 1:

                # ================= SEND =================
                while next_seq < base + WINDOW_SIZE and next_seq < total_packets:
                    data = f.read(UDP_DATA_SIZE)
                    if not data:
                        break

                    # header
                    struct.pack_into('!I', packet_buf, 0, next_seq)

                    # payload (zero-copy)
                    mv[UDP_HEADER_SIZE:UDP_HEADER_SIZE+len(data)] = data

                    pkt = bytes(mv[:UDP_HEADER_SIZE + len(data)])  # сохраняем в cache

                    window_cache[next_seq] = pkt
                    self.sock.sendto(pkt, self.addr)

                    packets_sent += 1
                    bytes_payload_sent += len(data)

                    next_seq += 1

                # ================= RECV ACK (фиксированный drain) =================
                got_ack = False

                for _ in range(512):   # 🔥 вместо бесконечного while
                    try:
                        ack_data, _ = self.sock.recvfrom(UDP_BUFFER_SIZE)
                    except BlockingIOError:
                        break
                    except socket.timeout:
                        break

                    if len(ack_data) != 4:
                        continue

                    ack_seq = struct.unpack('!I', ack_data)[0]

                    if ack_seq > last_ack:
                        # удаляем подтверждённые пакеты
                        while base < ack_seq:
                            window_cache.pop(base, None)
                            base += 1

                        last_ack = ack_seq
                        retries = 0
                        last_progress = time.time()
                        got_ack = True

                # ================= RETRANSMIT =================
                if not got_ack:
                    retries += 1

                    resend_end = min(base + 32, total_packets)

                    for seq in range(base, resend_end):
                        pkt = window_cache.get(seq)
                        if pkt:
                            self.sock.sendto(pkt, self.addr)
                            packets_retransmitted += 1
                            packets_sent += 1
                            bytes_payload_sent += len(pkt) - UDP_HEADER_SIZE

                if retries > MAX_RETRIES:
                    raise TimeoutError("Too many retransmissions")

                if time.time() - last_progress > 30:
                    raise TimeoutError("UDP upload stalled")

        elapsed = time.time() - start_time
        speed = filesize / elapsed / 1024 if elapsed > 0 else None

        print(f"[UDP upload] packets_sent={packets_sent} retrans={packets_retransmitted} bytes_payload={bytes_payload_sent}")
        return speed

    def receive_file(self, filename, filesize, offset=0):
        base = offset // UDP_DATA_SIZE
        total_packets = (filesize + UDP_DATA_SIZE - 1) // UDP_DATA_SIZE

        # 🔥 ring buffer вместо dict
        received = [None] * WINDOW_SIZE

        retries = 0
        last_acked = base
        last_progress = time.time()

        start_time = time.time()

        packets_received = 0
        bytes_payload_recv = 0

        with open(filename, 'ab' if offset else 'wb') as f:
            import io
            buf = io.BufferedWriter(f, buffer_size=BUFFER_SIZE)
            f.seek(offset)

            while base < total_packets:
                got_data = False

                # 🔥 быстрый drain без select
                for _ in range(512):
                    try:
                        data, _ = self.sock.recvfrom(UDP_BUFFER_SIZE)
                    except BlockingIOError:
                        break
                    except socket.timeout:
                        break

                    if len(data) < 4:
                        continue

                    seq = struct.unpack('!I', data[:4])[0]
                    payload = data[4:]

                    # 🔥 ограничиваем только текущим окном
                    if seq < base or seq >= base + WINDOW_SIZE:
                        continue

                    idx = seq % WINDOW_SIZE

                    if received[idx] is None:
                        received[idx] = payload
                        packets_received += 1
                        bytes_payload_recv += len(payload)

                    # 🔥 продвигаем окно максимально быстро
                    while True:
                        idx_base = base % WINDOW_SIZE
                        chunk = received[idx_base]

                        if chunk is None:
                            break

                        buf.write(chunk)
                        received[idx_base] = None
                        base += 1
                        got_data = True

                # ================= ACK batching =================
                if base - last_acked >= 32:
                    try:
                        self.sock.sendto(struct.pack('!I', base), self.addr)
                        last_acked = base
                    except Exception:
                        pass

                # ================= RETRY =================
                if got_data:
                    retries = 0
                    last_progress = time.time()
                else:
                    retries += 1

                    # 🔥 keepalive ACK
                    try:
                        self.sock.sendto(struct.pack('!I', base), self.addr)
                    except Exception:
                        pass

                    if retries > MAX_RETRIES:
                        raise TimeoutError("UDP download failed")

                # 🔥 защита от зависания
                if time.time() - last_progress > 30:
                    raise TimeoutError("UDP download stalled")

            # 🔥 финальные ACK
            for _ in range(3):
                try:
                    self.sock.sendto(struct.pack('!I', base), self.addr)
                except Exception:
                    pass

            buf.flush()

        elapsed = time.time() - start_time

        print(f"[UDP download] packets_received={packets_received} bytes_payload={bytes_payload_recv}")

        return (filesize - offset) / elapsed / 1024 if elapsed > 0 else 0