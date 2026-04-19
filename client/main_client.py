#!/usr/bin/env python3
#client/main_client.py
from tcp_client import *
from udp_client import UDPClient
from common.config import *
import socket
import argparse
import os
import sys
import shlex
import time

def parse_server_offset(resp_text, filesize):
    """
    Возвращает ожидаемое сервером смещение в байтах или None.
    Правила:
      - если в тексте есть 'offset' или 'expected' — берём ближайшее число к этому слову (байты)
      - если есть два числа — считаем (filesize, offset)
      - если одно число <= total_packets => seq -> bytes = seq * UDP_DATA_SIZE
      - иначе число интерпретируем как байты
    """
    if not resp_text:
        return None

    txt = resp_text.lower()
    parts = txt.replace(',', ' ').split()
    nums = []
    for p in parts:
        try:
            nums.append(int(p))
        except:
            continue

    total_packets = (filesize + UDP_DATA_SIZE - 1) // UDP_DATA_SIZE

    # 1) ключевые слова
    if 'offset' in parts or 'expected' in parts:
        # найти ближайшее число к слову 'offset' или 'expected'
        for i, token in enumerate(parts):
            if token in ('offset', 'expected'):
                # ищем число справа, затем слева
                for j in range(i+1, min(i+6, len(parts))):
                    try:
                        return int(parts[j])
                    except:
                        continue
                for j in range(i-1, max(i-6, -1), -1):
                    try:
                        return int(parts[j])
                    except:
                        continue
        # fallback: если не нашли рядом — взять последнее число
        if nums:
            return nums[-1]

    # 2) два числа -> filesize, offset
    if len(nums) >= 2:
        # если первый примерно равен filesize (или близко) — второй это offset
        if abs(nums[0] - filesize) < 1024*1024 or nums[0] == filesize:
            return nums[1]
        # иначе, если второй <= total_packets -> seq
        if nums[1] <= total_packets:
            return nums[1] * UDP_DATA_SIZE
        return nums[1]

    # 3) одно число
    if len(nums) == 1:
        n = nums[0]
        if 0 <= n <= total_packets:
            return n * UDP_DATA_SIZE
        return n

    return None

def main():
    parser = argparse.ArgumentParser(description='Клиент для передачи файлов')
    parser.add_argument('--protocol', choices=['tcp', 'udp'], default='tcp',
                        help='Протокол передачи (tcp или udp)')
    parser.add_argument('action', choices=['upload', 'download', 'echo', 'time', 'close'],
                        help='Действие')
    parser.add_argument('filename', nargs='?', help='Имя файла (для upload/download)')
    parser.add_argument('--host', default=HOST, help='Адрес сервера')
    parser.add_argument('--port', type=int, default=PORT, help='Порт сервера')
    args = parser.parse_args()

    print("CWD =", os.getcwd())
    print("FILE =", args.filename)
    print("EXISTS =", os.path.exists(args.filename) if args.filename else None)

    if args.action in ('upload', 'download') and not args.filename:
        print("Для upload/download необходимо указать имя файла")
        sys.exit(1)

    if args.protocol == 'tcp':
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(TCP_TIMEOUT)
        try:
            sock.connect((args.host, args.port))
        except Exception as e:
            print(f"TCP connection failed: {e}")
            sys.exit(1)
    else:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 32*2**20)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 32*2**20)
        except Exception:
            pass
        sock.settimeout(UDP_TIMEOUT)
        sock.setblocking(False)

    try:
        if args.protocol == 'tcp':
            if args.action == 'echo':
                cmd = f"ECHO {args.filename}" if args.filename else "ECHO"
                sock.sendall((cmd + '\n').encode())
                resp = recv_line_tcp(sock)
                print(resp)
            elif args.action == 'time':
                sock.sendall(b"TIME\n")
                resp = recv_line_tcp(sock)
                print(resp)
            elif args.action == 'close':
                sock.sendall(b"CLOSE\n")
                resp = recv_line_tcp(sock)
                print(resp)
            elif args.action == 'upload':
                if not os.path.exists(args.filename):
                    print("File not found")
                    sys.exit(1)
                filesize = os.path.getsize(args.filename)
                offset = 0
                upload_tcp(sock, args.filename, filesize, offset)
            elif args.action == 'download':
                local_filename = 'downloaded_' + os.path.basename(args.filename)
                offset = os.path.getsize(local_filename) if os.path.exists(local_filename) else 0
                download_tcp(sock, args.filename, offset)
        else:
            udp_client = UDPClient(sock, (args.host, args.port))
            if args.action == 'echo':
                resp = udp_client.send_command(f"ECHO {args.filename}" if args.filename else "ECHO")
                print(resp)
            elif args.action == 'time':
                resp = udp_client.send_command("TIME")
                print(resp)
            elif args.action == 'close':
                resp = udp_client.send_command("CLOSE")
                print(resp)
            elif args.action == 'upload':
                if not os.path.exists(args.filename):
                    print("File not found")
                    sys.exit(1)
                filesize = os.path.getsize(args.filename)
                offset = 0

                # initial UPLOAD command and server response
                cmd = f"UPLOAD {shlex.quote(os.path.basename(args.filename))} {filesize} {offset}"
                try:
                    resp = udp_client.send_command(cmd)
                except Exception as e:
                    print("Server error (no response):", e)
                    sys.exit(1)

                print("[DEBUG] server response:", resp)
                server_offset = parse_server_offset(resp, filesize)

                # retry once if parsing failed
                if server_offset is None:
                    try:
                        time.sleep(0.1)
                        resp2 = udp_client.send_command(cmd)
                        print("[DEBUG] server response retry:", resp2)
                        server_offset = parse_server_offset(resp2, filesize)
                    except Exception:
                        server_offset = None

                if server_offset is None:
                    print("[ERROR] cannot parse server offset, server replied:", resp)
                    sys.exit(1)

                if server_offset != offset:
                    print(f"[INFO] resuming from server_offset={server_offset} (client had {offset})")
                    offset = server_offset

                # start sending file from offset
                try:
                    speed = udp_client.send_file(args.filename, filesize, offset)
                except TimeoutError as e:
                    print("UDP upload failed:", e)
                    sys.exit(1)
                if speed is None:
                    print("UDP upload failed")
                    sys.exit(1)
                print(f"UDP upload finished. Speed: {speed:.2f} KB/s")

            elif args.action == 'download':
                offset = 0
                local_filename = 'downloaded_' + os.path.basename(args.filename)
                if os.path.exists(local_filename):
                    offset = os.path.getsize(local_filename)
                cmd = f"DOWNLOAD {shlex.quote(os.path.basename(args.filename))} {offset}"
                try:
                    resp = udp_client.send_command(cmd)
                except Exception as e:
                    print("Server error (no response):", e)
                    sys.exit(1)
                if not resp.upper().startswith('ACK') and not resp.upper().startswith('OK'):
                    print("Server error:", resp)
                    sys.exit(1)
                # try to extract filesize and server offset
                parts = resp.replace(',', ' ').split()
                nums = [p for p in parts if p.isdigit()]
                if len(nums) >= 2:
                    filesize = int(nums[0])
                    server_offset = int(nums[1])
                else:
                    # fallback: ask again in a clearer format
                    try:
                        resp2 = udp_client.send_command(cmd)
                        parts2 = resp2.replace(',', ' ').split()
                        nums2 = [p for p in parts2 if p.isdigit()]
                        if len(nums2) >= 2:
                            filesize = int(nums2[0])
                            server_offset = int(nums2[1])
                        else:
                            print("Cannot parse server response for download:", resp2)
                            sys.exit(1)
                    except Exception as e:
                        print("Server error (no response):", e)
                        sys.exit(1)

                if server_offset != offset:
                    print(f"[INFO] resuming download from server_offset={server_offset} (client had {offset})")
                    offset = server_offset

                try:
                    speed = udp_client.receive_file(local_filename, filesize, offset)
                except Exception as e:
                    print("UDP download failed:", e)
                    sys.exit(1)
                print(f"UDP download finished. Speed: {speed:.2f} KB/s")
    finally:
        sock.close()

if __name__ == '__main__':
    main()
