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
        """
        Надёжная отправка файла по UDP с попытками восстановления.
        Если сервер сообщает, что у него уже есть часть файла, клиент подстраивается.
        """
        # Сначала запросим сервер, чтобы узнать ожидаемое смещение
        server_offset, raw = self._query_server_offset(filename, filesize, offset)
        if server_offset is None:
            # если сервер вернул ошибку с указанием ожидаемого смещения в байтах,
            # _query_server_offset попытается вернуть число; если нет — пробуем продолжить с offset
            # но лучше попытаться ещё раз с небольшим ожиданием
            try:
                time.sleep(0.01)
                server_offset, raw = self._query_server_offset(filename, filesize, offset)
            except Exception:
                server_offset = None

        if server_offset is not None and server_offset != offset:
            # сервер ожидает докачку с другого смещения — подстроимся
            print(f"[UDP upload] server expects offset {server_offset}, client had {offset} → resuming")
            offset = server_offset

        base = offset // UDP_DATA_SIZE
        next_seq = base
        last_ack = base - 1

        pack = struct.Struct('!I').pack
        total_packets = (filesize + UDP_DATA_SIZE - 1) // UDP_DATA_SIZE

        batch = []
        retries = 0
        MAX_RETRIES_LOCAL = MAX_RETRIES

        start_time = time.time()
        last_progress = start_time

        # диагностические счётчики
        packets_sent = 0
        packets_retransmitted = 0
        bytes_payload_sent = 0

        # Открываем файл и позиционируемся на offset
        with open(filename, 'rb') as f:
            f.seek(offset)

            while last_ack < total_packets - 1:
                # наполняем окно пакетами
                while next_seq < base + WINDOW_SIZE and next_seq < total_packets:
                    data = f.read(UDP_DATA_SIZE)
                    if not data:
                        break
                    pkt = pack(next_seq) + data
                    batch.append((next_seq, pkt))
                    next_seq += 1

                    if len(batch) >= BATCH_SIZE:
                        for _, pkt in batch:
                            self.sock.sendto(memoryview(pkt), self.addr)
                            packets_sent += 1
                            bytes_payload_sent += len(pkt) - UDP_HEADER_SIZE
                        batch.clear()

                # flush remaining
                for _, pkt in batch:
                    self.sock.sendto(memoryview(pkt), self.addr)
                    packets_sent += 1
                    bytes_payload_sent += len(pkt) - UDP_HEADER_SIZE
                batch.clear()

                # ACK drain через select с лимитом
                got_ack = False
                drain_deadline = time.time() + 0.01
                acks_processed = 0
                MAX_ACKS_PER_ITER = 256

                while time.time() < drain_deadline and acks_processed < MAX_ACKS_PER_ITER:
                    r, _, _ = select.select([self.sock], [], [], max(0, drain_deadline - time.time()))
                    if not r:
                        break
                    try:
                        ack_data, _ = self.sock.recvfrom(UDP_BUFFER_SIZE)
                    except socket.timeout:
                        break
                    # ожидаем текстовый ACK
                    try:
                        text = ack_data.decode('utf-8', errors='ignore').strip()
                    except:
                        continue
                    if not text.startswith('ACK'):
                        continue
                    # форматы: "ACK <seq>" или "ACK OK <seq>"
                    parts = text.split()
                    seq_val = None
                    for p in parts[::-1]:
                        try:
                            seq_val = int(p)
                            break
                        except:
                            continue
                    if seq_val is None:
                        continue
                    ack_seq = seq_val
                    if ack_seq > last_ack:
                        last_ack = ack_seq
                        base = ack_seq
                        retries = 0
                        last_progress = time.time()
                        got_ack = True
                    acks_processed += 1

                # если не получили ACK — попытка контролируемой повторной отправки
                if not got_ack:
                    retries += 1
                    # перед повторной отправкой попробуем опросить сервер о текущем смещении
                    server_offset_bytes, _ = self._query_server_offset(filename, filesize, base * UDP_DATA_SIZE)
                    if server_offset_bytes is not None:
                        # если сервер продвинулся — синхронизируемся
                        if server_offset_bytes // UDP_DATA_SIZE > base:
                            new_base = server_offset_bytes // UDP_DATA_SIZE
                            print(f"[UDP upload] server reports progress → new base {new_base}")
                            base = new_base
                            last_ack = base - 1
                            f.seek(base * UDP_DATA_SIZE)
                            next_seq = base
                            retries = 0
                            continue

                    # resend half-window starting from base
                    resend_end = min(base + max(1, WINDOW_SIZE // 2), total_packets)
                    f.seek(base * UDP_DATA_SIZE)
                    for seq in range(base, resend_end):
                        data = f.read(UDP_DATA_SIZE)
                        if not data:
                            break
                        pkt = pack(seq) + data
                        self.sock.sendto(pkt, self.addr)
                        packets_retransmitted += 1
                        packets_sent += 1
                        bytes_payload_sent += len(pkt) - UDP_HEADER_SIZE

                    # мягкий backoff
                    time.sleep(min(0.01 * (1.5 ** retries), 0.5))

                # fail-safe: если слишком много повторов — пробуем полностью переинициализировать сессии
                if retries > MAX_RETRIES_LOCAL // 2:
                    # повторно запросим сервер текущее ожидаемое смещение и подстроимся
                    server_offset_bytes, resp = self._query_server_offset(filename, filesize, base * UDP_DATA_SIZE)
                    if server_offset_bytes is not None and server_offset_bytes != base * UDP_DATA_SIZE:
                        print(f"[UDP upload] resync with server offset {server_offset_bytes}")
                        base = server_offset_bytes // UDP_DATA_SIZE
                        last_ack = base - 1
                        f.seek(base * UDP_DATA_SIZE)
                        next_seq = base
                        retries = 0
                        continue

                if retries > MAX_RETRIES_LOCAL:
                    print("[UDP upload] too many retries → abort")
                    raise TimeoutError("Too many retransmissions")

                if time.time() - last_progress > 30:
                    print("[UDP upload] stalled → abort")
                    raise TimeoutError("UDP upload stalled")

        elapsed = time.time() - start_time
        speed = filesize / elapsed / 1024 if elapsed > 0 else None

        # диагностический вывод
        print(f"[UDP upload] packets_sent={packets_sent} retrans={packets_retransmitted} bytes_payload={bytes_payload_sent}")
        return speed

    def receive_file(self, filename, filesize, offset=0):
        base = offset // UDP_DATA_SIZE
        total_packets = (filesize + UDP_DATA_SIZE - 1) // UDP_DATA_SIZE

        received = {}
        mode = 'ab' if offset > 0 else 'wb'

        MAX_RETRIES_LOCAL = MAX_RETRIES
        retries = 0

        last_acked = base

        start_time = time.time()
        self.sock.settimeout(UDP_TIMEOUT)

        # диагностика
        packets_received = 0
        bytes_payload_recv = 0

        with open(filename, mode) as f:
            import io
            buf = io.BufferedWriter(f, buffer_size=BUFFER_SIZE)
            f.seek(offset)

            while base < total_packets:
                got_data = False

                # batch drain с select
                drain_deadline = time.time() + 0.01
                while time.time() < drain_deadline:
                    r, _, _ = select.select([self.sock], [], [], max(0, drain_deadline - time.time()))
                    if not r:
                        break
                    try:
                        data, _ = self.sock.recvfrom(UDP_BUFFER_SIZE)
                    except socket.timeout:
                        break

                    if len(data) < 4:
                        continue

                    seq = struct.unpack('!I', data[:4])[0]
                    payload = data[4:]

                    if seq not in received:
                        received[seq] = payload
                        packets_received += 1
                        bytes_payload_recv += len(payload)

                    advanced = False
                    while base in received:
                        buf.write(received.pop(base))
                        base += 1
                        advanced = True

                    if advanced:
                        got_data = True

                # кумулятивный ACK
                if base - last_acked >= ACK_EVERY or not got_data:
                    try:
                        self.sock.sendto(f"ACK {base}".encode(), self.addr)
                        last_acked = base
                    except Exception:
                        pass

                if got_data:
                    retries = 0
                else:
                    retries += 1
                    try:
                        self.sock.sendto(f"ACK {base}".encode(), self.addr)
                        last_acked = base
                    except Exception:
                        pass

                    if retries > MAX_RETRIES_LOCAL:
                        raise TimeoutError("UDP download failed (too many retries)")

            # финальные ACK
            for _ in range(3):
                try:
                    self.sock.sendto(f"ACK {base}".encode(), self.addr)
                    time.sleep(0.001)
                except:
                    pass

            buf.flush()

        elapsed = time.time() - start_time
        print(f"[UDP download] packets_received={packets_received} bytes_payload={bytes_payload_recv}")
        return (filesize - offset) / elapsed / 1024 if elapsed > 0 else 0
