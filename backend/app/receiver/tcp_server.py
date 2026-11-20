"""Concurrent TCP/UDP receiver for framed Huaten watch packets."""

import os
import socket
import threading
import time
import traceback
from typing import Dict, Optional, Set

from ..config import settings
from ..utils.logger import tcp_logger
from .packet_parser import build_frame, parse_packet
from .packet_validator import extract_frames_with_errors

active_connections: Dict[str, socket.socket] = {}
conn_lock = threading.RLock()
_connection_write_locks: Dict[socket.socket, threading.Lock] = {}
_connection_last_seen: Dict[str, float] = {}
_client_connections: Set[socket.socket] = set()
_client_threads: Set[threading.Thread] = set()
_retired_connections: Set[socket.socket] = set()

_lifecycle_lock = threading.Lock()
_receiver_started = False
_receiver_stop_event: Optional[threading.Event] = None
_listener_threads: Dict[str, threading.Thread] = {}
_tcp_server_sock: Optional[socket.socket] = None
_udp_server_sock: Optional[socket.socket] = None

CONNECTION_TIMEOUT_SECONDS = 600
LISTENER_READY_TIMEOUT_SECONDS = 5
_DIAGNOSTICS_ENABLED = os.getenv("GPS_RECEIVER_DIAGNOSTICS", "").strip().lower() in {
    "1", "true", "yes", "on"
}


def _log_diagnostic(message: str, *args):
    if _DIAGNOSTICS_ENABLED:
        tcp_logger.info(message, *args)


def _connection_write_lock(conn: socket.socket) -> threading.Lock:
    with conn_lock:
        return _connection_write_locks.setdefault(conn, threading.Lock())


def _close_socket(conn: socket.socket):
    try:
        conn.shutdown(socket.SHUT_RDWR)
    except (AttributeError, OSError):
        pass
    try:
        conn.close()
    except OSError:
        pass


def _unregister_connection(conn: socket.socket, final: bool = False):
    removed_devices = []
    with conn_lock:
        _retired_connections.add(conn)
        for device_id, registered_conn in list(active_connections.items()):
            if registered_conn is conn:
                active_connections.pop(device_id, None)
                _connection_last_seen.pop(device_id, None)
                removed_devices.append(device_id)
        write_lock = _connection_write_locks.get(conn)
        _client_connections.discard(conn)

    for dev_id in removed_devices:
        try:
            from ..services.alert_service import alert_service
            alert_service.record_disconnection_event(dev_id)
        except Exception:
            pass

    if write_lock is not None:
        with write_lock:
            _close_socket(conn)
    else:
        _close_socket(conn)

    with conn_lock:
        _connection_write_locks.pop(conn, None)
        if final:
            _retired_connections.discard(conn)


def register_client(dev_id: str, conn: socket.socket) -> bool:
    old_conn = None
    was_registered = False
    with conn_lock:
        if conn in _retired_connections:
            return False
        old_conn = active_connections.get(dev_id)
        was_registered = old_conn is conn
        active_connections[dev_id] = conn
        _connection_last_seen[dev_id] = time.monotonic()
        _connection_write_locks.setdefault(conn, threading.Lock())
        _client_connections.add(conn)

    if old_conn is not None and old_conn is not conn:
        tcp_logger.info("Closing replaced TCP connection for device %s", dev_id)
        _unregister_connection(old_conn)
    if not was_registered:
        tcp_logger.info("Device ID registered on TCP connection: %s", dev_id)
    return True


def unregister_client(dev_id: str, conn: Optional[socket.socket] = None):
    """Unregister only the connection that still owns this device ID."""
    stale_conn = None
    with conn_lock:
        current_conn = active_connections.get(dev_id)
        if current_conn is not None and (conn is None or current_conn is conn):
            stale_conn = active_connections.pop(dev_id)
            _connection_last_seen.pop(dev_id, None)
        if stale_conn is not None and any(
            registered_conn is stale_conn
            for registered_conn in active_connections.values()
        ):
            stale_conn = None
    if stale_conn is not None:
        _unregister_connection(stale_conn)


def is_device_connected(device_id: str) -> bool:
    expired_conn = None
    with conn_lock:
        conn = active_connections.get(device_id)
        last_seen = _connection_last_seen.get(device_id)
        if conn is None:
            return False
        if last_seen is not None and time.monotonic() - last_seen <= CONNECTION_TIMEOUT_SECONDS:
            return True
        active_connections.pop(device_id, None)
        _connection_last_seen.pop(device_id, None)
        if not any(registered_conn is conn for registered_conn in active_connections.values()):
            expired_conn = conn
    if expired_conn is not None:
        _unregister_connection(expired_conn)
    return False


def get_active_clients_count() -> int:
    with conn_lock:
        device_ids = list(active_connections)
    return sum(1 for device_id in device_ids if is_device_connected(device_id))


def _send_tcp_payload(conn: socket.socket, payload: bytes, device_id: Optional[str] = None) -> bool:
    write_lock = _connection_write_lock(conn)
    try:
        with write_lock:
            if device_id is not None:
                with conn_lock:
                    if active_connections.get(device_id) is not conn:
                        return False
            conn.sendall(payload)
        return True
    except OSError as exc:
        tcp_logger.warning(
            "TCP send failed device=%s exception_type=%s",
            device_id or "unknown",
            type(exc).__name__
        )
        _unregister_connection(conn)
        return False


def send_command_to_device(device_id: str, command: str, maker: str = "MNYD") -> bool:
    """Send a manufacturer-framed command to the currently registered watch."""
    with conn_lock:
        conn = active_connections.get(device_id)
    if conn is None or not is_device_connected(device_id):
        tcp_logger.warning(
            "Cannot send command=%s device=%s reason=not_connected",
            command,
            device_id
        )
        return False

    frame = build_frame(maker, device_id, command)
    if not _send_tcp_payload(conn, frame, device_id):
        return False
    tcp_logger.info("[%s*%s] Sent Command <- %s", maker, device_id, command)
    return True


def _dispatch_tcp_frame(conn: socket.socket, maker: str, device_id: str, content: bytes):
    if not register_client(device_id, conn):
        return
    received_at = time.time()
    try:
        cmd_type, reply_bytes = parse_packet(
            maker,
            device_id,
            content,
            gps_source="watch_tcp",
            received_at=received_at
        )
    except Exception as exc:
        tcp_logger.error(
            "TCP packet processing failed device=%s exception_type=%s stack=%s",
            device_id,
            type(exc).__name__,
            "".join(traceback.format_tb(exc.__traceback__))
        )
        return

    _log_diagnostic(
        "TCP parsed frame device=%s command=%s",
        device_id,
        cmd_type or "UNKNOWN"
    )
    if reply_bytes and _send_tcp_payload(conn, reply_bytes, device_id):
        tcp_logger.info("[%s*%s] Manufacturer acknowledgement sent", maker, device_id)

    if cmd_type == "PING" and settings.AUTO_POLL_GPS_ON_PING:
        def poll_gps_delayed(maker_name=maker, device=device_id):
            time.sleep(0.5)
            send_command_to_device(device, "CR", maker_name)

        threading.Thread(target=poll_gps_delayed, daemon=True).start()


def handle_tcp_client(conn: socket.socket, addr):
    client_str = "{}:{}".format(addr[0], addr[1])
    with conn_lock:
        _client_threads.add(threading.current_thread())
    tcp_logger.info("TCP connection established from %s", client_str)
    buffer = b""

    try:
        conn.settimeout(300)
        while True:
            data = conn.recv(4096)
            if not data:
                break

            _log_diagnostic("TCP received %d bytes from %s", len(data), client_str)
            frames, buffer, rejected = extract_frames_with_errors(buffer + data)
            if rejected:
                tcp_logger.warning(
                    "TCP discarded malformed or unframed data from %s count=%d",
                    client_str,
                    rejected
                )
            _log_diagnostic(
                "TCP extracted %d complete frame(s) from %s",
                len(frames),
                client_str
            )
            for maker, device_id, content in frames:
                _dispatch_tcp_frame(conn, maker, device_id, content)

    except socket.timeout:
        tcp_logger.info("TCP connection %s timed out", client_str)
    except OSError as exc:
        tcp_logger.info(
            "TCP connection closed client=%s exception_type=%s",
            client_str,
            type(exc).__name__
        )
    except Exception as exc:
        tcp_logger.error(
            "TCP connection %s error exception_type=%s stack=%s",
            client_str,
            type(exc).__name__,
            "".join(traceback.format_tb(exc.__traceback__))
        )
    finally:
        _unregister_connection(conn, final=True)
        with conn_lock:
            _client_threads.discard(threading.current_thread())
        tcp_logger.info("TCP client disconnected: %s", client_str)


def _set_listener_exclusive(listener: socket.socket):
    if os.name == "nt" and hasattr(socket, "SO_EXCLUSIVEADDRUSE"):
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
    else:
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)


def _publish_listener_ready(
    protocol: str,
    ready_event: Optional[threading.Event],
    startup_errors: Optional[Dict[str, str]],
    error: Optional[BaseException] = None
):
    if error is not None and startup_errors is not None:
        startup_errors[protocol] = "{}: {}".format(type(error).__name__, error)
    if ready_event is not None:
        ready_event.set()


def run_tcp_server(
    ready_event: Optional[threading.Event] = None,
    stop_event: Optional[threading.Event] = None,
    startup_errors: Optional[Dict[str, str]] = None
):
    global _tcp_server_sock
    stop_event = stop_event or threading.Event()
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        _set_listener_exclusive(server)
        server.bind((settings.TCP_HOST, settings.TCP_PORT))
        server.listen(128)
        server.settimeout(1.0)
        _tcp_server_sock = server
        tcp_logger.info(
            "TCP Watch Receiver listening on %s:%s (Public IP: %s)",
            settings.TCP_HOST,
            settings.TCP_PORT,
            settings.PUBLIC_SERVER_IP
        )
        _publish_listener_ready("tcp", ready_event, startup_errors)
        while not stop_event.is_set():
            try:
                conn, addr = server.accept()
            except socket.timeout:
                continue
            except OSError as exc:
                if stop_event.is_set():
                    break
                tcp_logger.warning(
                    "TCP listener stopped after socket error exception_type=%s",
                    type(exc).__name__
                )
                break
            with conn_lock:
                _client_connections.add(conn)
            threading.Thread(
                target=handle_tcp_client,
                args=(conn, addr),
                daemon=True,
                name="TCPClient-{}:{}".format(addr[0], addr[1])
            ).start()
    except OSError as exc:
        _publish_listener_ready("tcp", ready_event, startup_errors, exc)
        tcp_logger.critical(
            "TCP receiver failed to bind/listen on %s:%s exception_type=%s",
            settings.TCP_HOST,
            settings.TCP_PORT,
            type(exc).__name__
        )
    finally:
        if _tcp_server_sock is server:
            _tcp_server_sock = None
        try:
            server.close()
        except OSError:
            pass


def run_udp_server(
    ready_event: Optional[threading.Event] = None,
    stop_event: Optional[threading.Event] = None,
    startup_errors: Optional[Dict[str, str]] = None
):
    global _udp_server_sock
    stop_event = stop_event or threading.Event()
    udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        _set_listener_exclusive(udp_sock)
        udp_sock.bind((settings.TCP_HOST, settings.TCP_PORT))
        udp_sock.settimeout(1.0)
        _udp_server_sock = udp_sock
        tcp_logger.info(
            "UDP Watch Receiver listening on %s:%s",
            settings.TCP_HOST,
            settings.TCP_PORT
        )
        _publish_listener_ready("udp", ready_event, startup_errors)
        while not stop_event.is_set():
            try:
                data, address = udp_sock.recvfrom(4096)
            except socket.timeout:
                continue
            except OSError as exc:
                if stop_event.is_set():
                    break
                tcp_logger.warning(
                    "UDP receiver stopped after socket error exception_type=%s",
                    type(exc).__name__
                )
                break

            _log_diagnostic("UDP received %d bytes", len(data))
            frames, _, rejected = extract_frames_with_errors(data)
            if rejected:
                tcp_logger.warning("UDP discarded malformed data count=%d", rejected)
            _log_diagnostic("UDP extracted %d complete frame(s)", len(frames))
            for maker, device_id, content in frames:
                try:
                    cmd_type, reply_bytes = parse_packet(
                        maker,
                        device_id,
                        content,
                        gps_source="watch_udp",
                        received_at=time.time()
                    )
                except Exception as exc:
                    tcp_logger.error(
                        "UDP packet processing failed device=%s exception_type=%s stack=%s",
                        device_id,
                        type(exc).__name__,
                        "".join(traceback.format_tb(exc.__traceback__))
                    )
                    continue
                _log_diagnostic(
                    "UDP parsed frame device=%s command=%s",
                    device_id,
                    cmd_type or "UNKNOWN"
                )
                if reply_bytes:
                    try:
                        udp_sock.sendto(reply_bytes, address)
                    except OSError as exc:
                        tcp_logger.warning(
                            "UDP reply failed device=%s exception_type=%s",
                            device_id,
                            type(exc).__name__
                        )
    except OSError as exc:
        _publish_listener_ready("udp", ready_event, startup_errors, exc)
        tcp_logger.critical(
            "UDP receiver failed to bind/listen on %s:%s exception_type=%s",
            settings.TCP_HOST,
            settings.TCP_PORT,
            type(exc).__name__
        )
    finally:
        if _udp_server_sock is udp_sock:
            _udp_server_sock = None
        try:
            udp_sock.close()
        except OSError:
            pass


def stop_receiver():
    """Stop listeners and active client sockets before the application exits."""
    global _receiver_started, _receiver_stop_event
    with _lifecycle_lock:
        stop_event = _receiver_stop_event
        threads = list(_listener_threads.values())
        _receiver_started = False
        if stop_event is not None:
            stop_event.set()
        tcp_socket = _tcp_server_sock
        udp_socket = _udp_server_sock
        with conn_lock:
            client_sockets = list(_client_connections)
            client_threads = list(_client_threads)

    for listener in (tcp_socket, udp_socket):
        if listener is not None:
            try:
                listener.close()
            except OSError:
                pass
    for conn in client_sockets:
        _unregister_connection(conn)

    current_thread = threading.current_thread()
    for thread in threads + client_threads:
        if thread is not current_thread:
            thread.join(timeout=3)

    with _lifecycle_lock:
        _listener_threads.clear()
        _receiver_stop_event = None


def start_receiver_background():
    """Start exactly one TCP and one UDP listener and fail startup on bind errors."""
    global _receiver_started, _receiver_stop_event
    with _lifecycle_lock:
        if _receiver_started:
            return
        _receiver_started = True
        stop_event = threading.Event()
        tcp_ready = threading.Event()
        udp_ready = threading.Event()
        startup_errors: Dict[str, str] = {}
        _receiver_stop_event = stop_event
        tcp_thread = threading.Thread(
            target=run_tcp_server,
            args=(tcp_ready, stop_event, startup_errors),
            daemon=True,
            name="TCPReceiverThread"
        )
        udp_thread = threading.Thread(
            target=run_udp_server,
            args=(udp_ready, stop_event, startup_errors),
            daemon=True,
            name="UDPReceiverThread"
        )
        _listener_threads.clear()
        _listener_threads["tcp"] = tcp_thread
        _listener_threads["udp"] = udp_thread
        tcp_thread.start()
        udp_thread.start()

    if not tcp_ready.wait(LISTENER_READY_TIMEOUT_SECONDS):
        stop_receiver()
        raise RuntimeError("TCP receiver did not complete startup")
    if not udp_ready.wait(LISTENER_READY_TIMEOUT_SECONDS):
        stop_receiver()
        raise RuntimeError("UDP receiver did not complete startup")
    if startup_errors:
        errors = "; ".join(
            "{}: {}".format(protocol, error)
            for protocol, error in sorted(startup_errors.items())
        )
        stop_receiver()
        raise OSError("GPS receiver startup failed: {}".format(errors))
    tcp_logger.info("TCP and UDP watch receivers started")
