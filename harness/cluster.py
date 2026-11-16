
import subprocess


class LocalCluster:
    """Manage local server and worker processes."""

    def __init__(self):
        self.nodes = {}
        self.next_id = 1

    def _start(self, kind, command, capture_output=False):
        """Launch and register a child process."""

        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE if capture_output else None,
            stderr=subprocess.STDOUT if capture_output else None,
            text=capture_output,
            bufsize=1 if capture_output else -1,
        )

        node_id = self.next_id
        self.next_id += 1

        self.nodes[node_id] = {
            "kind": kind,
            "process": process,
        }

        return node_id

    def start_server(self, command):
        """Launch a server process."""
        return self._start("server", command)

    def start_worker(self, command, capture_output=False):
        """Launch a worker process."""
        return self._start(
            "worker",
            command,
            capture_output=capture_output,
        )

    def is_running(self, node_id):
        """Check whether a registered process is alive."""
        node = self.nodes[node_id]
        return node["process"].poll() is None

    def _kill(self, node_id, expected_kind):
        """Abruptly terminate a registered child process."""

        node = self.nodes[node_id]

        if node["kind"] != expected_kind:
            raise ValueError("Incorrect node type")

        process = node["process"]

        if process.poll() is not None:
            raise RuntimeError("Process already stopped")

        process.kill()
        process.wait(timeout=5)

        return f"KILLED {expected_kind} {node_id}"

    def kill_server(self, node_id):
        return self._kill(node_id, "server")

    def kill_worker(self, node_id):
        return self._kill(node_id, "worker")

    def execute(self, action, args):
        """Execute an event from the scenario engine."""

        if action == "kill_server":
            return self.kill_server(args["target"])

        if action == "kill_worker":
            return self.kill_worker(args["target"])

        raise NotImplementedError(
            f"Action not supported yet: {action}"
        )

    def close(self):
        """Clean up any remaining child processes."""

        for node in self.nodes.values():
            process = node["process"]

            if process.poll() is None:
                process.kill()

            process.wait(timeout=5)
