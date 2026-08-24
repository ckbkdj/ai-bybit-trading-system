from __future__ import annotations

import ipaddress
import os
import socket
from typing import Any


if os.environ.get("TRUTH_REGRESSION_BLOCK_EXTERNAL_NETWORK") == "1":
    _original_socket_class = socket.socket
    _original_create_connection = socket.create_connection

    def _loopback_address(address: Any) -> bool:
        # Unix-domain sockets and non-IP families are local by construction.
        if not isinstance(address, tuple) or not address:
            return True
        host = str(address[0]).strip("[]")
        try:
            return ipaddress.ip_address(host).is_loopback
        except ValueError:
            try:
                resolved = socket.getaddrinfo(host, address[1], type=socket.SOCK_STREAM)
            except Exception:
                return False
            return bool(resolved) and all(
                ipaddress.ip_address(item[4][0]).is_loopback for item in resolved
            )

    class GuardedSocket(_original_socket_class):
        """Drop-in socket that rejects every non-loopback TCP/UDP endpoint."""

        def connect(self, address: Any):
            if self.family in {socket.AF_INET, socket.AF_INET6} and not _loopback_address(address):
                raise OSError(
                    f"truth regression blocked non-loopback network connection: {address!r}"
                )
            return super().connect(address)

        def connect_ex(self, address: Any):
            if self.family in {socket.AF_INET, socket.AF_INET6} and not _loopback_address(address):
                raise OSError(
                    f"truth regression blocked non-loopback network connection: {address!r}"
                )
            return super().connect_ex(address)

    def _guarded_create_connection(address: Any, *args, **kwargs):
        if not _loopback_address(address):
            raise OSError(
                f"truth regression blocked non-loopback network connection: {address!r}"
            )
        # The original function resolves the module-global ``socket`` class at
        # call time, which now points at GuardedSocket as an additional defense.
        return _original_create_connection(address, *args, **kwargs)

    socket.socket = GuardedSocket
    socket.SocketType = GuardedSocket
    socket.create_connection = _guarded_create_connection
