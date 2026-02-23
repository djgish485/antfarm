#!/usr/bin/env python3
"""Deploy merged main to live checkout and verify origin recovers."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import pathlib
import re
import subprocess
import time
import urllib.error
import urllib.request
from urllib.parse import urlparse
from typing import Any, Dict, List


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--repo", default="/opt/the-algo")
    p.add_argument("--remote", default="origin")
    p.add_argument("--branch", default="main")
    p.add_argument("--service", default="the-algo")
    p.add_argument("--expected-sha", default="")
    p.add_argument("--origin-url", default="http://127.0.0.1:3000")
    p.add_argument("--command-timeout", type=int, default=90)
    p.add_argument("--ready-timeout", type=int, default=180)
    p.add_argument("--poll-seconds", type=float, default=2.0)
    p.add_argument("--http-timeout", type=float, default=8.0)
    p.add_argument("--max-bytes", type=int, default=500_000)
    p.add_argument("--json-out", default="")
    return p.parse_args()


def iso_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def one_line(text: str, max_len: int = 400) -> str:
    compact = " ".join((text or "").split())
    return compact if len(compact) <= max_len else compact[: max_len - 3] + "..."


def run_cmd(cmd: List[str], timeout: int) -> Dict[str, Any]:
    started = time.monotonic()
    try:
        proc = subprocess.run(
            cmd,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        elapsed = int((time.monotonic() - started) * 1000)
        return {
            "ok": proc.returncode == 0,
            "rc": proc.returncode,
            "elapsed_ms": elapsed,
            "stdout": (proc.stdout or "").strip(),
            "stderr": (proc.stderr or "").strip(),
        }
    except subprocess.TimeoutExpired as exc:
        elapsed = int((time.monotonic() - started) * 1000)
        return {
            "ok": False,
            "rc": 124,
            "elapsed_ms": elapsed,
            "stdout": one_line(str(exc.stdout or ""), 200),
            "stderr": one_line(str(exc.stderr or ""), 200),
            "error": "timeout",
        }
    except Exception as exc:
        elapsed = int((time.monotonic() - started) * 1000)
        return {
            "ok": False,
            "rc": 1,
            "elapsed_ms": elapsed,
            "stdout": "",
            "stderr": one_line(f"{type(exc).__name__}: {exc}", 300),
            "error": "exception",
        }


def fetch(url: str, timeout: float, max_bytes: int) -> Dict[str, Any]:
    started = time.monotonic()
    req = urllib.request.Request(url)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read(max_bytes)
            elapsed = int((time.monotonic() - started) * 1000)
            return {
                "ok": True,
                "status": int(resp.getcode() or 0),
                "elapsed_ms": elapsed,
                "body": body.decode("utf-8", errors="replace"),
            }
    except urllib.error.HTTPError as exc:
        elapsed = int((time.monotonic() - started) * 1000)
        body = ""
        try:
            body = exc.read(max_bytes).decode("utf-8", errors="replace")
        except Exception:
            body = str(exc)
        return {
            "ok": False,
            "status": int(exc.code or 0),
            "elapsed_ms": elapsed,
            "error": f"http_error:{exc.code}",
            "body": body,
        }
    except Exception as exc:
        elapsed = int((time.monotonic() - started) * 1000)
        return {
            "ok": False,
            "status": 0,
            "elapsed_ms": elapsed,
            "error": f"{type(exc).__name__}: {exc}",
            "body": "",
        }


def collect_logs(service: str) -> Dict[str, str]:
    logs: Dict[str, str] = {}
    commands = [
        ("service", ["journalctl", "-u", service, "-n", "120", "--no-pager"]),
        ("cloudflared", ["journalctl", "-u", "cloudflared", "-n", "80", "--no-pager"]),
    ]
    for key, cmd in commands:
        try:
            proc = subprocess.run(cmd, check=False, capture_output=True, text=True, timeout=12)
        except Exception:
            continue
        text = (proc.stdout or "").strip()
        if text:
            logs[key] = text
    return logs


def append_check(report: Dict[str, Any], check: Dict[str, Any]) -> None:
    report.setdefault("checks", []).append(check)


def parse_origin_port(origin_url: str) -> int:
    try:
        parsed = urlparse(origin_url)
    except Exception:
        return 3000
    if parsed.port:
        return int(parsed.port)
    return 443 if parsed.scheme == "https" else 80


def pids_listening_on_port(port: int) -> List[int]:
    try:
        proc = subprocess.run(
            ["ss", "-ltnp"],
            check=False,
            capture_output=True,
            text=True,
            timeout=8,
        )
    except Exception:
        return []
    pids: set[int] = set()
    port_token = f":{port}"
    for line in (proc.stdout or "").splitlines():
        if port_token not in line:
            continue
        for m in re.finditer(r"pid=(\d+)", line):
            try:
                pids.add(int(m.group(1)))
            except Exception:
                continue
    return sorted(pids)


def _read_proc_cmdline(pid: int) -> str:
    try:
        with open(f"/proc/{pid}/cmdline", "rb") as handle:
            raw = handle.read().replace(b"\x00", b" ").decode("utf-8", errors="replace")
            return one_line(raw, 300)
    except Exception:
        return ""


def _read_proc_cwd(pid: int) -> str:
    try:
        return os.path.realpath(f"/proc/{pid}/cwd")
    except Exception:
        return ""


def _pid_exists(pid: int) -> bool:
    return os.path.exists(f"/proc/{pid}")


def clear_run_workspace_port_owners(port: int) -> Dict[str, Any]:
    listeners = pids_listening_on_port(port)
    details: List[Dict[str, Any]] = []

    for pid in listeners:
        cwd = _read_proc_cwd(pid)
        cmd = _read_proc_cmdline(pid)
        is_run_workspace = "/root/.openclaw/workspaces/workflows/" in cwd
        entry: Dict[str, Any] = {
            "pid": pid,
            "cwd": cwd,
            "cmd": cmd,
            "run_workspace": is_run_workspace,
        }
        if not is_run_workspace:
            details.append(entry)
            continue

        try:
            os.kill(pid, 15)
            entry["term_sent"] = True
        except Exception as exc:
            entry["term_error"] = one_line(f"{type(exc).__name__}: {exc}", 180)

        time.sleep(0.6)
        if _pid_exists(pid):
            try:
                os.kill(pid, 9)
                entry["kill_sent"] = True
            except Exception as exc:
                entry["kill_error"] = one_line(f"{type(exc).__name__}: {exc}", 180)

        time.sleep(0.2)
        entry["still_alive"] = _pid_exists(pid)
        details.append(entry)

    remaining = []
    for pid in pids_listening_on_port(port):
        cwd = _read_proc_cwd(pid)
        if "/root/.openclaw/workspaces/workflows/" in cwd:
            remaining.append({"pid": pid, "cwd": cwd, "cmd": _read_proc_cmdline(pid)})

    return {
        "ok": len(remaining) == 0,
        "port": port,
        "listeners_before": listeners,
        "details": details,
        "remaining_run_workspace_listeners": remaining,
    }


def fail(report: Dict[str, Any], err_type: str, message: str, failed_check: str, service: str) -> int:
    sig_src = f"{err_type}|{failed_check}|{one_line(message, 1000)}"
    report["ok"] = False
    report["failure"] = {
        "error_type": err_type,
        "failed_check": failed_check,
        "message": one_line(message, 1000),
        "failure_signature": hashlib.sha256(sig_src.encode("utf-8")).hexdigest()[:24],
    }
    report["logs"] = collect_logs(service)
    return 1


def finish(report: Dict[str, Any], json_out: str, code: int) -> int:
    report["ok"] = code == 0
    text = json.dumps(report, ensure_ascii=True)
    if json_out:
        try:
            with open(json_out, "w", encoding="utf-8") as handle:
                handle.write(text + "\n")
        except Exception:
            pass
    print(text)
    return code


def wait_until_ready(args: argparse.Namespace, report: Dict[str, Any]) -> bool:
    deadline = time.monotonic() + float(args.ready_timeout)
    last_root: Dict[str, Any] = {}
    last_feed: Dict[str, Any] = {}

    while time.monotonic() < deadline:
        root = fetch(args.origin_url.rstrip("/") + "/", args.http_timeout, args.max_bytes)
        feed = fetch(args.origin_url.rstrip("/") + "/api/content?limit=1", args.http_timeout, args.max_bytes)
        last_root = root
        last_feed = feed

        root_ok = int(root.get("status", 0)) == 200 and (
            "The Algo" in root.get("body", "") or "<html" in root.get("body", "").lower()
        )
        feed_ok = int(feed.get("status", 0)) == 200

        if root_ok and feed_ok:
            append_check(
                report,
                {
                    "name": "origin_ready",
                    "ok": True,
                    "root_status": root.get("status", 0),
                    "feed_status": feed.get("status", 0),
                    "root_elapsed_ms": root.get("elapsed_ms", 0),
                    "feed_elapsed_ms": feed.get("elapsed_ms", 0),
                },
            )
            return True

        time.sleep(max(0.2, float(args.poll_seconds)))

    append_check(
        report,
        {
            "name": "origin_ready",
            "ok": False,
            "root_status": last_root.get("status", 0),
            "feed_status": last_feed.get("status", 0),
            "root_error": one_line(str(last_root.get("error", "")), 160),
            "feed_error": one_line(str(last_feed.get("error", "")), 160),
        },
    )
    return False


def main() -> int:
    args = parse_args()
    report: Dict[str, Any] = {
        "ok": True,
        "ts": iso_now(),
        "repo": args.repo,
        "remote": args.remote,
        "branch": args.branch,
        "service": args.service,
        "expected_sha": args.expected_sha,
        "checks": [],
        "logs": {},
    }

    commands = [
        ("git_fetch", ["git", "-C", args.repo, "fetch", args.remote, "--prune"]),
        ("git_checkout", ["git", "-C", args.repo, "checkout", args.branch]),
        ("git_reset", ["git", "-C", args.repo, "reset", "--hard", f"{args.remote}/{args.branch}"]),
    ]

    for name, cmd in commands:
        res = run_cmd(cmd, args.command_timeout)
        append_check(
            report,
            {
                "name": name,
                "ok": res.get("ok", False),
                "rc": res.get("rc", 1),
                "elapsed_ms": res.get("elapsed_ms", 0),
                "stderr": one_line(str(res.get("stderr", "")), 220),
            },
        )
        if not res.get("ok", False):
            code = fail(
                report,
                "git_sync_failed",
                f"{name} rc={res.get('rc')} err={res.get('stderr', '')}",
                name,
                args.service,
            )
            return finish(report, args.json_out, code)

    sha_res = run_cmd(["git", "-C", args.repo, "rev-parse", "HEAD"], args.command_timeout)
    current_sha = one_line(sha_res.get("stdout", ""), 100)
    append_check(
        report,
        {
            "name": "git_head",
            "ok": sha_res.get("ok", False),
            "sha": current_sha,
            "rc": sha_res.get("rc", 1),
        },
    )
    if not sha_res.get("ok", False) or not current_sha:
        code = fail(report, "git_head_missing", "unable to read HEAD after reset", "git_head", args.service)
        return finish(report, args.json_out, code)

    report["deployed_sha"] = current_sha
    if args.expected_sha and args.expected_sha.strip() and current_sha != args.expected_sha.strip():
        code = fail(
            report,
            "sha_mismatch",
            f"deployed_sha={current_sha} expected_sha={args.expected_sha.strip()}",
            "git_head",
            args.service,
        )
        return finish(report, args.json_out, code)

    # Rebuild if the .next build is stale (BUILD_ID timestamp < latest commit)
    next_dir = pathlib.Path(args.repo) / ".next"
    build_id_path = next_dir / "BUILD_ID"
    needs_build = True

    if build_id_path.exists():
        build_mtime = build_id_path.stat().st_mtime
        # Get timestamp of HEAD commit
        commit_ts_res = run_cmd(
            ["git", "-C", args.repo, "log", "-1", "--format=%ct", "HEAD"],
            args.command_timeout,
        )
        if commit_ts_res.get("ok") and commit_ts_res.get("stdout", "").strip():
            commit_ts = float(commit_ts_res["stdout"].strip())
            if build_mtime >= commit_ts:
                needs_build = False

    if needs_build:
        build_res = run_cmd(
            ["bash", "-c", f"cd {args.repo} && npm run build"],
            600,  # 10 min timeout for builds
        )
        append_check(
            report,
            {
                "name": "npm_build",
                "ok": build_res.get("ok", False),
                "rc": build_res.get("rc", 1),
                "elapsed_ms": build_res.get("elapsed_ms", 0),
                "stderr": one_line(str(build_res.get("stderr", "")), 220),
            },
        )
        if not build_res.get("ok", False):
            code = fail(
                report,
                "build_failed",
                f"npm run build rc={build_res.get('rc')} err={build_res.get('stderr', '')}",
                "npm_build",
                args.service,
            )
            return finish(report, args.json_out, code)
    else:
        append_check(
            report,
            {
                "name": "npm_build",
                "ok": True,
                "skipped": True,
                "reason": "build is newer than HEAD commit",
            },
        )

    guard = clear_run_workspace_port_owners(parse_origin_port(args.origin_url))
    append_check(
        report,
        {
            "name": "port_guard",
            "ok": guard.get("ok", False),
            "port": guard.get("port"),
            "listeners_before": guard.get("listeners_before", []),
            "remaining": guard.get("remaining_run_workspace_listeners", []),
        },
    )
    if not guard.get("ok", False):
        code = fail(
            report,
            "port_guard_failed",
            f"run-workspace listener still owns origin port {guard.get('port')}",
            "port_guard",
            args.service,
        )
        return finish(report, args.json_out, code)

    restart = run_cmd(["systemctl", "restart", args.service], args.command_timeout)
    append_check(
        report,
        {
            "name": "service_restart",
            "ok": restart.get("ok", False),
            "rc": restart.get("rc", 1),
            "elapsed_ms": restart.get("elapsed_ms", 0),
            "stderr": one_line(str(restart.get("stderr", "")), 220),
        },
    )
    if not restart.get("ok", False):
        code = fail(
            report,
            "service_restart_failed",
            f"systemctl restart rc={restart.get('rc')} err={restart.get('stderr', '')}",
            "service_restart",
            args.service,
        )
        return finish(report, args.json_out, code)

    if not wait_until_ready(args, report):
        code = fail(
            report,
            "origin_not_ready",
            f"origin did not recover within {args.ready_timeout}s after restart",
            "origin_ready",
            args.service,
        )
        return finish(report, args.json_out, code)

    return finish(report, args.json_out, 0)


if __name__ == "__main__":
    raise SystemExit(main())
