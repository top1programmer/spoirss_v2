#!/usr/bin/env python3
import argparse
import select
import socket

from common.config import PORT
from server.tcp_server import TCPClient
from server.udp_server import udp_server_loop


def setup_keepalive(sock):
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

    try:
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 30)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 5)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 3)
    except AttributeError:
        pass


def clean_dirs():
    import os
    from common.config import INCOMPLETE_DIR, UPLOAD_DIR

    os.makedirs(INCOMPLETE_DIR, exist_ok=True)
    os.makedirs(UPLOAD_DIR, exist_ok=True)

    for name in os.listdir(INCOMPLETE_DIR):
        os.remove(os.path.join(INCOMPLETE_DIR, name))


def parse_args():
    parser = argparse.ArgumentParser(
        description='Сервер для передачи файлов (TCP+UDP одновременно)'
    )

    parser.add_argument(
        '--port',
        type=int,
        default=PORT,
        help='Порт для прослушивания'
    )

    return parser.parse_args()


def create_tcp_socket(port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    setup_keepalive(sock)

    sock.bind(('0.0.0.0', port))
    sock.listen(5)
    sock.setblocking(False)

    return sock


def tune_udp_buffers(sock):
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 16 * 1024 * 1024)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 16 * 1024 * 1024)
    except Exception:
        pass


def print_udp_buffers(sock):
    snd = sock.getsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF)
    rcv = sock.getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF)
    print(snd, rcv)


def create_udp_socket(port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    tune_udp_buffers(sock)
    print_udp_buffers(sock)

    sock.bind(('0.0.0.0', port))
    sock.setblocking(False)

    return sock


def build_read_list(tcp_listen, udp_sock, tcp_clients):
    sockets = [tcp_listen, udp_sock]

    if tcp_clients:
        sockets.append(tcp_clients[0])

    return sockets


def reject_tcp_client(conn, addr):
    setup_keepalive(conn)

    try:
        conn.sendall(
            b"BUSY: server is handling another TCP client. Try later.\r\n"
        )
    except Exception:
        pass

    conn.close()
    print(f"[!] Rejected TCP connection from {addr} (busy)")


def accept_tcp_client(tcp_listen, tcp_clients):
    conn, addr = tcp_listen.accept()

    if tcp_clients:
        reject_tcp_client(conn, addr)
        return

    setup_keepalive(conn)
    tcp_clients.append(TCPClient(conn, addr))


def process_udp(udp_sock, udp_current_client, udp_handler, is_tcp_busy):
    return udp_server_loop(
        udp_sock,
        udp_current_client,
        udp_handler,
        is_tcp_busy
    )


def process_tcp_client(tcp_clients):
    client = tcp_clients[0]

    if client.handle_input():
        return

    client.close()
    tcp_clients.pop()


def process_socket(
    sock,
    tcp_listen,
    udp_sock,
    tcp_clients,
    udp_current_client,
    udp_handler
):
    is_tcp_busy = bool(tcp_clients)

    if sock is tcp_listen:
        accept_tcp_client(tcp_listen, tcp_clients)
        return udp_current_client, udp_handler

    if sock is udp_sock:
        udp_current_client, udp_handler, _ = process_udp(
            udp_sock,
            udp_current_client,
            udp_handler,
            is_tcp_busy
        )
        return udp_current_client, udp_handler

    if tcp_clients and sock is tcp_clients[0]:
        process_tcp_client(tcp_clients)

    return udp_current_client, udp_handler


def run_server_loop(tcp_listen, udp_sock):
    tcp_clients = []
    udp_current_client = None
    udp_handler = None

    while True:
        rlist = build_read_list(tcp_listen, udp_sock, tcp_clients)
        readable, _, _ = select.select(rlist, [], [], 1.0)

        for sock in readable:
            udp_current_client, udp_handler = process_socket(
                sock,
                tcp_listen,
                udp_sock,
                tcp_clients,
                udp_current_client,
                udp_handler
            )


def main():
    args = parse_args()

    clean_dirs()

    tcp_listen = create_tcp_socket(args.port)
    udp_sock = create_udp_socket(args.port)

    print(f"[*] Server listening on port {args.port} (TCP and UDP)")

    try:
        run_server_loop(tcp_listen, udp_sock)

    except KeyboardInterrupt:
        print("\n[!] Server stopped by user")

    finally:
        tcp_listen.close()
        udp_sock.close()


if __name__ == '__main__':
    main()