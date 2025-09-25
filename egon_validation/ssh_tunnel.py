import os
import subprocess
import time
import socket
from typing import Optional
from pathlib import Path


class SSHTunnel:
    """Manages SSH tunnel for database connections"""

    def __init__(
        self,
        ssh_host: str,
        ssh_port: int,
        ssh_user: str,
        ssh_key_file: str,
        local_port: int,
        remote_port: int,
    ):
        self.ssh_host = ssh_host
        self.ssh_port = ssh_port
        self.ssh_user = ssh_user
        self.ssh_key_file = os.path.expanduser(ssh_key_file)
        self.local_port = local_port
        self.remote_port = remote_port
        self.process: Optional[subprocess.Popen] = None

    def is_port_open(self, port: int, host: str = "127.0.0.1") -> bool:
        """Check if a port is open/accessible"""
        try:
            with socket.create_connection((host, port), timeout=1):
                return True
        except (socket.error, ConnectionRefusedError):
            return False

    def start(self) -> bool:
        """Start SSH tunnel if not already running"""
        if self.is_port_open(self.local_port):
            print(
                f"Port {self.local_port} already in use (tunnel may already be active)"
            )
            return True

        if not Path(self.ssh_key_file).exists():
            raise FileNotFoundError(f"SSH key file not found: {self.ssh_key_file}")

        cmd = [
            "ssh",
            "-N",  # Don't execute remote command
            "-L",
            f"{self.local_port}:127.0.0.1:{self.remote_port}",
            "-p",
            str(self.ssh_port),
            "-i",
            self.ssh_key_file,
            f"{self.ssh_user}@{self.ssh_host}",
        ]

        try:
            self.process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                preexec_fn=os.setsid,  # Create new process group
            )

            # Wait for tunnel to establish
            for _ in range(10):  # 10 second timeout
                if self.is_port_open(self.local_port):
                    print(f"SSH tunnel established on port {self.local_port}")
                    return True
                time.sleep(1)

            print("SSH tunnel failed to establish within timeout")
            return False

        except Exception as e:
            print(f"Failed to start SSH tunnel: {e}")
            return False

    def stop(self):
        """Stop SSH tunnel"""
        if self.process:
            try:
                # Kill the entire process group
                os.killpg(os.getpgid(self.process.pid), subprocess.signal.SIGTERM)
                self.process.wait(timeout=5)
            except (subprocess.TimeoutExpired, ProcessLookupError):
                try:
                    os.killpg(os.getpgid(self.process.pid), subprocess.signal.SIGKILL)
                except ProcessLookupError:
                    pass
            self.process = None
            print("SSH tunnel stopped")

    def __enter__(self):
        """Context manager entry"""
        if self.start():
            return self
        raise RuntimeError("Failed to establish SSH tunnel")

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.stop()


def create_tunnel_from_env() -> SSHTunnel:
    """Create SSH tunnel from environment variables"""
    return SSHTunnel(
        ssh_host=os.getenv("SSH_HOST"),
        ssh_port=int(os.getenv("SSH_PORT", 22)),
        ssh_user=os.getenv("SSH_USER"),
        ssh_key_file=os.getenv("SSH_KEY_FILE"),
        local_port=int(os.getenv("SSH_LOCAL_PORT")),
        remote_port=int(os.getenv("SSH_REMOTE_PORT")),
    )
