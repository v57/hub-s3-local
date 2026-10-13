#!/usr/bin/env python3
import socket
import sys


def serve(kind: str, host: str, port: int) -> None:
    sock_type = socket.SOCK_STREAM if kind == "tcp" else socket.SOCK_DGRAM
    with socket.socket(socket.AF_INET, sock_type) as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((host, port))
        if kind == "tcp":
            server.listen()
            while True:
                connection, _ = server.accept()
                with connection:
                    connection.sendall(connection.recv(65535))
        while True:
            payload, peer = server.recvfrom(65535)
            server.sendto(payload, peer)


def call(kind: str, host: str, port: int, payload: str) -> None:
    sock_type = socket.SOCK_STREAM if kind == "tcp" else socket.SOCK_DGRAM
    with socket.socket(socket.AF_INET, sock_type) as client:
        client.settimeout(1)
        if kind == "tcp":
            client.connect((host, port))
            client.sendall(payload.encode())
            response = client.recv(65535)
        else:
            client.sendto(payload.encode(), (host, port))
            response, _ = client.recvfrom(65535)
    if response.decode() != payload:
        raise SystemExit("echo response differs")


if len(sys.argv) < 5:
    raise SystemExit("usage: game_fixture.py serve|call tcp|udp host port [payload]")

action, protocol, address, raw_port = sys.argv[1:5]
if protocol not in {"tcp", "udp"}:
    raise SystemExit("protocol must be tcp or udp")
if action == "serve":
    serve(protocol, address, int(raw_port))
elif action == "call" and len(sys.argv) == 6:
    call(protocol, address, int(raw_port), sys.argv[5])
else:
    raise SystemExit("invalid action")
