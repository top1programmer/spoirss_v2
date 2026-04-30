# client/udp_client.py
import io
import os
import shlex
import socket
import struct
import time

from common.config import *


class UDPClient:
    def __init__(self, sock, addr):
        self.sock = sock
        self.addr = addr

        self.setup_buffers()
        self.sock.settimeout(UDP_TIMEOUT)

        self.print_buffers()

    # ==========================================================
    # common
    # ==========================================================
    def setup_buffers(self):
        try:
            self.sock.setsockopt( socket.SOL_SOCKET, socket.SO_RCVBUF, 32 * 1024 * 1024 )
            self.sock.setsockopt( socket.SOL_SOCKET, socket.SO_SNDBUF, 32 * 1024 * 1024 )
        except Exception:
            pass

    def print_buffers(self):
        snd = self.sock.getsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF )
        rcv = self.sock.getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF )

        print("SO_SNDBUF =", snd, "SO_RCVBUF =", rcv)

    def sendto(self, data):
        self.sock.sendto(data, self.addr)

    def recvfrom(self):
        return self.sock.recvfrom(UDP_BUFFER_SIZE)

    def packet_count(self, filesize):
        return (filesize + UDP_DATA_SIZE - 1) // UDP_DATA_SIZE

    # ==========================================================
    # commands
    # ==========================================================
    def send_command(self, cmd):
        data = cmd.encode() + b'\n'
        delay = 0.05

        for _ in range(MAX_RETRIES):
            try:
                self.sendto(data)
                resp, _ = self.recvfrom()
                return self.parse_command_response(resp)

            except socket.timeout:
                time.sleep(delay)
                delay = min(delay * 1.5, 1.0)

        raise ConnectionError( "No response to command after retries" )

    def parse_command_response(self, data):
        text = data.decode( 'utf-8', errors='ignore' ).strip()

        if text.startswith('ACK '):
            return text[4:].strip()

        if text.startswith('OK '):
            return text[3:].strip()

        return text

    # ==========================================================
    # upload
    # ==========================================================
    def send_file(self, filename, filesize, offset=0):
        base = offset // UDP_DATA_SIZE
        next_seq = base
        last_ack = base - 1
        
        total = self.packet_count(filesize)

        retries = 0
        start_time = time.time()
        last_progress = start_time

        cache = {}
        packet_buf = bytearray( UDP_HEADER_SIZE + UDP_DATA_SIZE )

        mv = memoryview(packet_buf)

        sent_packets = 0
        resent_packets = 0

        with open(filename, 'rb') as f:
            f.seek(offset)

            while last_ack < total - 1:
                next_seq, sent = self.send_window( f, cache, mv, base, next_seq, total)
                sent_packets += sent
                ack = self.read_upload_ack(last_ack)
                
                if ack > last_ack:
                    base = self.slide_window( cache,  base, ack )
                    last_ack = ack
                    retries = 0
                    last_progress = time.time()

                else:
                    retries += 1

                    if retries % 8 == 0:
                        resent = self.resend_window(cache, base, total)
                        resent_packets += resent

                self.check_upload_timeout( retries, last_progress )

        speed = self.calc_speed( filesize - offset, start_time )

        print(
            f"[UDP upload] packets={sent_packets} "
            f"retrans={resent_packets}"
        )

        return speed

    def send_window( self, f, cache, mv, base, next_seq, total ):
        count = 0

        while next_seq < base + WINDOW_SIZE:
            if next_seq >= total:
                break

            packet = self.get_packet( f, cache, mv, next_seq )

            if packet is None:
                break
            if count % 64 == 0:
                time.sleep(0)
            self.sendto(packet)

            next_seq += 1
            count += 1

        return next_seq, count

    def get_packet( self, f, cache, mv, seq ):
        if seq in cache:
            return cache[seq]

        data = f.read(UDP_DATA_SIZE)

        if not data:
            return None

        packet = self.build_packet( mv, seq, data )

        cache[seq] = packet
        return packet

    def build_packet( self, mv, seq, data ):
        struct.pack_into('!I', mv.obj, 0, seq)

        mv[UDP_HEADER_SIZE:UDP_HEADER_SIZE + len(data)] = data

        return bytes( mv[ :UDP_HEADER_SIZE + len(data) ] )
   
    def read_upload_ack(self, last_ack):
        best = last_ack
        old = self.sock.gettimeout()
        self.sock.settimeout(0.03)

        for _ in range(2048):
            try:
                data, _ = self.recvfrom()
            except socket.timeout:
                break

            if len(data) == 4:
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

    def resend_window(self, cache, base, total):
        end = min(base + 32, total)
        count = 0

        for seq in range(base, end):
            packet = cache.get(seq)

            if packet:
                self.sendto(packet)
                count += 1

        return count

    def check_upload_timeout( self, retries, last_progress ):
        if retries > MAX_RETRIES:
            raise TimeoutError( "Too many retransmissions" )

        if time.time() - last_progress > 30:
            raise TimeoutError( "UDP upload stalled" )

    # ==========================================================
    # download
    # ==========================================================
    def receive_file(self, filename, filesize, offset=0):
        base = offset // UDP_DATA_SIZE
        total = self.packet_count(filesize)

        received = {}   # вместо списка

        retries = 0
        last_progress = time.time()
        start_time = time.time()
        packets = 0

        with open(filename, 'ab' if offset else 'wb') as f:
            f.seek(offset)
            buf = io.BufferedWriter(f, buffer_size=BUFFER_SIZE)

            while base < total:
                base, got, count = self.read_packets(received, buf, base)

                packets += count

                if got:
                    retries = 0
                    last_progress = time.time()
                else:
                    retries += 1

                self.check_download_timeout(retries, last_progress)

            buf.flush()

        self.send_final_acks(base)

        speed = self.calc_speed(filesize - offset, start_time)
        print(f"[UDP download] packets={packets}")

        return speed

    def read_packets(self, received, buf, base):
        got = False
        count = 0

        while True:
            try:
                data, _ = self.recvfrom()
            except socket.timeout:
                break

            if len(data) < 4:
                continue

            seq = struct.unpack('!I', data[:4])[0]

            if seq < base:
                continue

            if seq >= base + WINDOW_SIZE:
                continue

            if seq not in received:
                received[seq] = data[4:]
                count += 1

            moved = False

            while base in received:
                buf.write(received.pop(base))
                base += 1
                moved = True

            if moved:
                got = True
            
            self.send_ack(base)   # ACK сразу при продвижении окна

        return base, got, count

    def flush_window( self, received, buf, base ):
        moved = False

        while True:
            idx = base % WINDOW_SIZE
            chunk = received[idx]

            if chunk is None:
                break

            buf.write(chunk)
            received[idx] = None
            base += 1
            moved = True

        return base, moved

    def send_ack_if_needed( self, base, last_acked ):
        if base - last_acked < 128:
            return last_acked

        self.send_ack(base)
        return base

    def send_keepalive_ack(self, base):
        self.send_ack(base)

    def send_ack(self, seq):
        try:
            self.sendto( struct.pack('!I', seq) )
        except Exception:
            pass

    def send_final_acks(self, base):
        for _ in range(3):
            self.send_ack(base)

    def check_download_timeout(self, retries, last_progress ):
        if retries > MAX_RETRIES:
            raise TimeoutError("UDP download failed")

        if time.time() - last_progress > 30:
            raise TimeoutError("UDP download stalled")


    def calc_speed(self, bytes_count, start_time ):
        elapsed = time.time() - start_time
        if elapsed <= 0:
            return 0
        return bytes_count / elapsed / 1024