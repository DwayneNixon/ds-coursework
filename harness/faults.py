
import json
import math
import os
import random
import time
from pathlib import Path


class LinkDroppedError(ConnectionError):
    """An outgoing RPC was deliberately dropped."""


class LinkFaultController:
    """Store fault settings for worker-to-server links."""

    def __init__(self, state_file):
        self.path = Path(state_file)
        self.links = {}
        self._save()

    def _save(self):
        # Replace the complete file atomically.
        temporary = self.path.with_name(
            self.path.name + ".tmp"
        )

        temporary.write_text(
            json.dumps(self.links),
            encoding="utf-8",
        )

        os.replace(temporary, self.path)

    def execute(self, action, args):
        if action not in ("delay_link", "drop_link"):
            raise NotImplementedError(action)

        a = str(args["a"])
        b = str(args["b"])
        link = f"{a}|{b}"

        settings = dict(self.links.get(link, {
            "delay_ms": 0.0,
            "jitter_ms": 0.0,
            "drop_p": 0.0,
        }))

        if action == "delay_link":
            delay = float(args["ms"])
            jitter = float(args.get("jitter_ms", 0))

            if not (
                math.isfinite(delay)
                and math.isfinite(jitter)
                and delay >= 0
                and jitter >= 0
            ):
                raise ValueError("Invalid link delay")

            settings["delay_ms"] = delay
            settings["jitter_ms"] = jitter

        else:
            probability = float(args["p"])

            if not (
                math.isfinite(probability)
                and 0 <= probability <= 1
            ):
                raise ValueError("Invalid drop probability")

            settings["drop_p"] = probability

        self.links[link] = settings
        self._save()

        return f"APPLIED {action} {link}"


class FaultInjectingClient:
    """Apply faults before forwarding an RPC to ParameterClient."""

    def __init__(
        self,
        client,
        worker_id,
        server_id,
        state_file,
        seed,
    ):
        self.client = client
        self.worker_id = worker_id
        self.server_id = server_id
        self.path = Path(state_file)

        self.rng = random.Random(
            f"{seed}:{worker_id}:faults"
        )

    def _before_rpc(self, operation):
        if not self.path.exists():
            return

        settings = json.loads(
            self.path.read_text(encoding="utf-8")
        ).get(
            f"{self.worker_id}|{self.server_id}",
            {},
        )

        delay = settings.get("delay_ms", 0.0)
        jitter = settings.get("jitter_ms", 0.0)
        drop_p = settings.get("drop_p", 0.0)

        # Apply configured latency and jitter.
        actual_delay = max(
            0.0,
            delay + self.rng.uniform(-jitter, jitter),
        )

        if actual_delay:
            time.sleep(actual_delay / 1000.0)

            print(
                f"FAULT_DELAY worker={self.worker_id} "
                f"op={operation} ms={actual_delay:.1f}",
                flush=True,
            )

        # Simulate an outgoing request being dropped.
        if self.rng.random() < drop_p:
            raise LinkDroppedError(
                f"Dropped {operation} from worker "
                f"{self.worker_id}"
            )

    def pull(self, *args, **kwargs):
        self._before_rpc("pull")
        return self.client.pull(*args, **kwargs)

    def push(self, *args, **kwargs):
        self._before_rpc("push")
        return self.client.push(*args, **kwargs)

    def close(self):
        self.client.close()
