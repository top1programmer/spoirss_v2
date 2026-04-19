#!/usr/bin/env python3
import socket
import select
import argparse

from server.tcp_server import TCPClient
from server.udp_server import udp_server_loop
from common.config import PORT


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

    for f in os.listdir(INCOMPLETE_DIR):
        os.remove(os.path.join(INCOMPLETE_DIR, f))


def main():
    parser = argparse.ArgumentParser(
        description='Сервер для передачи файлов (TCP+UDP одновременно)'
    )
    parser.add_argument('--port', type=int, default=PORT,
                        help='Порт для прослушивания')
    args = parser.parse_args()

    clean_dirs()

    # --- TCP ---
    tcp_listen = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcp_listen.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    setup_keepalive(tcp_listen)
    tcp_listen.bind(('0.0.0.0', args.port))
    tcp_listen.listen(5)
    tcp_listen.setblocking(False)

    # --- UDP ---
    udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 32 * 1024 * 1024)
        udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 32 * 1024 * 1024)
    except Exception:
        pass

    print(udp_sock.getsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF),
      udp_sock.getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF))
    udp_sock.bind(('0.0.0.0', args.port))
    udp_sock.setblocking(False)

    print(f"[*] Server listening on port {args.port} (TCP and UDP)")

    tcp_clients = []
    udp_current_client = None
    udp_handler = None

    try:
        while True:
            rlist = [tcp_listen, udp_sock]

            if tcp_clients:
                rlist.append(tcp_clients[0])

            readable, _, _ = select.select(rlist, [], [], 1.0)

            is_tcp_busy = bool(tcp_clients)

            for sock in readable:

                # --- Новый TCP клиент ---
                if sock is tcp_listen:
                    conn, addr = tcp_listen.accept()

                    if tcp_clients:
                        setup_keepalive(conn)
                        try:
                            conn.sendall(
                                b"BUSY: server is handling another TCP client. Try later.\r\n"
                            )
                        except Exception:
                            pass
                        conn.close()
                        print(f"[!] Rejected TCP connection from {addr} (busy)")
                    else:
                        setup_keepalive(conn)
                        tcp_clients.append(TCPClient(conn, addr))

                # --- UDP ---
                elif sock is udp_sock:
                    udp_current_client, udp_handler, _ = udp_server_loop(
                        udp_sock,
                        udp_current_client,
                        udp_handler,
                        is_tcp_busy
                    )

                # --- Данные от TCP клиента ---
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