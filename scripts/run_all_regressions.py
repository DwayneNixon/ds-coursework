"""Run EVERY Part A–F test script in an isolated Python subprocess.

Each test gets a fresh standalone localhost server for legacy tests that
connect to port 50051, plus a fresh Python interpreter. Tests that start
their own ephemeral servers still work. Results are written to JSON and
per-script logs; failures are collected rather than stopping at the first.

Usage: python -m scripts.run_all_regressions
       python -m scripts.run_all_regressions --part A
"""
import argparse
import json
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import grpc

ROOT = Path(__file__).resolve().parents[1]


def run_one(path, output_dir, timeout):
    module = ".".join(path.relative_to(ROOT).with_suffix("").parts)
    output_dir.mkdir(parents=True, exist_ok=True)
    logpath = output_dir / (path.stem + ".log")
    begin = time.perf_counter()
    server = None
    result = None
    with tempfile.TemporaryDirectory(prefix="ds_test_server_") as folder:
        serverlog = Path(folder) / "server.log"
        try:
            with serverlog.open("w", encoding="utf-8") as logs:
                server = subprocess.Popen(
                    [sys.executable, "-u", str(ROOT / "server.py")],
                    cwd=str(ROOT),
                    stdin=subprocess.DEVNULL,
                    stdout=logs,
                    stderr=subprocess.STDOUT,
                )
            with grpc.insecure_channel("127.0.0.1:50051") as channel:
                grpc.channel_ready_future(channel).result(timeout=7)
            done = subprocess.run(
                [sys.executable, "-m", module],
                cwd=str(ROOT), capture_output=True, text=True,
                encoding="utf-8", errors="replace", timeout=timeout,
            )
            result = {
                "module": module,
                "status": "PASS" if done.returncode == 0 else "FAIL",
                "exit_code": done.returncode,
                "seconds": time.perf_counter() - begin,
            }
            logpath.write_text(
                "COMMAND: python -m " + module + "\n"
                + "EXIT CODE: " + str(done.returncode) + "\n"
                + "STDOUT:\n" + done.stdout + "\nSTDERR:\n" + done.stderr,
                encoding="utf-8",
            )
        except (Exception, KeyboardInterrupt) as error:
            result = {
                "module": module, "status": "FAIL",
                "exit_code": None, "error": str(error),
                "seconds": time.perf_counter() - begin,
            }
            logpath.write_text(
                f"COMMAND: python -m {module}\nEXCEPTION: {error}\n",
                encoding="utf-8",
            )
        finally:
            if server is not None:
                if server.poll() is None:
                    server.terminate()
                try:
                    server.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    server.kill()
                    server.wait(timeout=3)
            if result["status"] == "FAIL" and serverlog.exists():
                with logpath.open("a", encoding="utf-8") as handle:
                    handle.write("\nSTANDALONE SERVER LOG:\n")
                    handle.write(serverlog.read_text(encoding="utf-8"))
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--part", choices=list("ABCDEF"))
    parser.add_argument("--timeout", type=float, default=120)
    parser.add_argument(
        "--output", default="results/audit_regressions"
    )
    args = parser.parse_args()
    if args.timeout <= 0:
        parser.error("--timeout must be >0")
    paths = sorted(
        p
        for letter in (args.part,) if args.part is not None
        for p in (ROOT / "tests" / f"part_{letter.lower()}").glob("test_*.py")
    ) if args.part else sorted(
        p for letter in "ABCDEF"
        for p in (ROOT / "tests" / f"part_{letter.lower()}").glob("test_*.py")
    )
    if not paths:
        raise RuntimeError("No test files discovered")

    output_dir = Path(args.output)
    if not output_dir.is_absolute():
        output_dir = ROOT / output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    results = []
    for i, path in enumerate(paths, 1):
        module = ".".join(path.relative_to(ROOT).with_suffix("").parts)
        print(f"[{i}/{len(paths)}] {module}", flush=True)
        result = run_one(path, output_dir / path.parent.name, args.timeout)
        results.append(result)
        print(f"  {result['status']}: {result['seconds']:.2f}s", flush=True)
    summary = {
        "scripts": len(results),
        "passed": sum(x["status"] == "PASS" for x in results),
        "failed": sum(x["status"] != "PASS" for x in results),
        "results": results,
    }
    (output_dir / "summary.json").write_text(
        json.dumps(summary, indent=2), encoding="utf-8")
    print(f"Full regression: {summary['passed']}/{summary['scripts']} scripts passed")
    if summary["failed"]:
        print("Failures:")
        for result in results:
            if result["status"] != "PASS":
                print(f" - {result['module']} ({result.get('error', result['exit_code'])})")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
