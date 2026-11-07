import argparse
import json
import socket
import threading
import time
from datetime import datetime

parser = argparse.ArgumentParser(description="Token-ring mutual exclusion node")
parser.add_argument("--id", type=int, required=True)
parser.add_argument("--port", type=int, required=True)
parser.add_argument("--next-host", required=True)
parser.add_argument("--next-port", type=int, required=True)
parser.add_argument("--initial-token", action="store_true")
args = parser.parse_args()

PROCESS_ID = args.id
HOST = "127.0.0.1"
PORT = args.port
NEXT_HOST = args.next_host
NEXT_PORT = args.next_port

request_pending = False
request_lock = threading.Lock()
server_ready = threading.Event()
entry_number = 0


def log(message):
    print(f"[{datetime.now():%H:%M:%S}] P{PROCESS_ID}: {message}", flush=True)


def send_token(token):
    message = (json.dumps(token) + "\n").encode("utf-8")

    while True:
        try:
            with socket.create_connection((NEXT_HOST, NEXT_PORT), timeout=3) as client:
                client.sendall(message)
            log(f"Token sent to successor at {NEXT_HOST}:{NEXT_PORT}")
            return
        except OSError as error:
            log(f"Successor unavailable ({error}); retrying in 1 second")
            time.sleep(1)


def handle_token(token):
    global request_pending, entry_number

    log(f"Token received (token_id={token.get('token_id', 'unknown')})")

    with request_lock:
        has_request = request_pending
        if has_request:
            request_pending = False
            entry_number += 1
            current_entry = entry_number

    if has_request:
        start_time = time.perf_counter()
        log(f"ENTER critical section | Entry {current_entry}")
        time.sleep(2)
        duration = time.perf_counter() - start_time
        log(f"EXIT critical section | Entry {current_entry} | Duration: {duration:.3f}s")
    else:
        log("No pending request; forwarding token")

    time.sleep(0.5)
    send_token(token)


def listener():
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.bind((HOST, PORT))
            server.listen()
            server_ready.set()
            log(f"Listening on {HOST}:{PORT}")

            while True:
                connection, address = server.accept()
                with connection:
                    data = bytearray()
                    while b"\n" not in data:
                        chunk = connection.recv(1024)
                        if not chunk:
                            break
                        data.extend(chunk)

                    if not data:
                        continue

                    try:
                        raw_message = bytes(data).split(b"\n", 1)[0]
                        message = json.loads(raw_message.decode("utf-8"))
                        if message.get("type") == "TOKEN":
                            handle_token(message)
                        else:
                            log("Received an unknown message type")
                    except (json.JSONDecodeError, UnicodeDecodeError) as error:
                        log(f"Invalid token message: {error}")

    except OSError as error:
        log(f"Listener error: {error}")
        server_ready.set()


def main():
    global request_pending

    threading.Thread(target=listener, daemon=True).start()
    server_ready.wait()

    if args.initial_token:
        time.sleep(1)
        log("Creating the initial token")
        send_token({"type": "TOKEN", "token_id": 1})

    log("Commands: r = request critical section, q = quit")

    while True:
        try:
            command = input(f"P{PROCESS_ID}> ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            print()
            log("Stopping process")
            break

        if command == "r":
            with request_lock:
                if request_pending:
                    log("A request is already pending")
                else:
                    request_pending = True
                    log("Critical-section request queued; waiting for token")
        elif command == "q":
            log("Stopping process")
            break
        elif command:
            log("Unknown command. Enter r or q.")


if __name__ == "__main__":
    main()
