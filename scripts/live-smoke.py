#!/usr/bin/env python3
"""Live smoke gate for The Algo deployment."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, List, Tuple


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--public-url", default="https://algo2.dangish.net")
    p.add_argument("--origin-url", default="http://127.0.0.1:3000")
    p.add_argument("--public-timeout", type=float, default=12.0)
    p.add_argument("--origin-timeout", type=float, default=10.0)
    p.add_argument("--json-out", default="")
    p.add_argument("--max-bytes", type=int, default=1_000_000)
    return p.parse_args()


def iso_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def one_line(text: str, max_len: int = 400) -> str:
    compact = " ".join((text or "").split())
    return compact if len(compact) <= max_len else compact[: max_len - 3] + "..."


def cloudflare_headers() -> Dict[str, str]:
    client_id = (
        (os_get("CF_ACCESS_CLIENT_ID"))
        or (os_get("CLOUDFLARE_ACCESS_CLIENT_ID"))
        or ""
    ).strip()
    client_secret = (
        (os_get("CF_ACCESS_CLIENT_SECRET"))
        or (os_get("CLOUDFLARE_ACCESS_CLIENT_SECRET"))
        or ""
    ).strip()
    headers: Dict[str, str] = {}
    if client_id and client_secret:
        headers["CF-Access-Client-Id"] = client_id
        headers["CF-Access-Client-Secret"] = client_secret
    return headers


def os_get(name: str) -> str:
    try:
        import os

        return os.environ.get(name, "")
    except Exception:
        return ""


def fetch(url: str, timeout: float, max_bytes: int, headers: Dict[str, str] | None = None) -> Dict[str, Any]:
    started = time.monotonic()
    req = urllib.request.Request(url, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read(max_bytes)
            status = int(resp.getcode() or 0)
            elapsed = time.monotonic() - started
            return {
                "ok": True,
                "status": status,
                "elapsed_ms": int(elapsed * 1000),
                "body": body.decode("utf-8", errors="replace"),
                "headers": dict(resp.headers.items()),
            }
    except urllib.error.HTTPError as exc:
        elapsed = time.monotonic() - started
        body = ""
        try:
            body = exc.read(max_bytes).decode("utf-8", errors="replace")
        except Exception:
            body = str(exc)
        return {
            "ok": False,
            "status": int(exc.code or 0),
            "elapsed_ms": int(elapsed * 1000),
            "error": f"http_error:{exc.code}",
            "body": body,
            "headers": dict(exc.headers.items()) if exc.headers else {},
        }
    except Exception as exc:
        elapsed = time.monotonic() - started
        return {
            "ok": False,
            "status": 0,
            "elapsed_ms": int(elapsed * 1000),
            "error": f"{type(exc).__name__}: {exc}",
            "body": "",
            "headers": {},
        }


def collect_logs() -> Dict[str, str]:
    out: Dict[str, str] = {}
    commands: List[Tuple[str, List[str]]] = [
        ("the_algo", ["journalctl", "-u", "the-algo", "-n", "80", "--no-pager"]),
        ("cloudflared", ["journalctl", "-u", "cloudflared", "-n", "60", "--no-pager"]),
    ]
    for key, cmd in commands:
        try:
            proc = subprocess.run(cmd, check=False, capture_output=True, text=True, timeout=10)
            text = (proc.stdout or "").strip()
            if text:
                out[key] = text
        except Exception:
            continue
    return out


def fail(report: Dict[str, Any], error_type: str, message: str, failed_check: str) -> int:
    signature_raw = f"{error_type}|{failed_check}|{one_line(message, 1000)}"
    report["ok"] = False
    report["failure"] = {
        "error_type": error_type,
        "failed_check": failed_check,
        "message": one_line(message, 1000),
        "failure_signature": hashlib.sha256(signature_raw.encode("utf-8")).hexdigest()[:24],
    }
    report["logs"] = collect_logs()
    return 1


def append_check(report: Dict[str, Any], check: Dict[str, Any]) -> None:
    report.setdefault("checks", []).append(check)


def main() -> int:
    args = parse_args()
    report: Dict[str, Any] = {
        "ok": True,
        "ts": iso_now(),
        "public_url": args.public_url,
        "origin_url": args.origin_url,
        "checks": [],
        "logs": {},
    }

    cf_headers = cloudflare_headers()

    # 1) Public URL health: reject only 5xx/timeouts to avoid false negatives with Access redirects.
    public = fetch(args.public_url, args.public_timeout, args.max_bytes, headers=cf_headers or None)
    append_check(
        report,
        {
            "name": "public_http",
            "status": public.get("status"),
            "elapsed_ms": public.get("elapsed_ms"),
            "ok": bool(public.get("status", 0) and int(public.get("status", 0)) < 500),
            "note": "Cloudflare Access redirects (302/401/403) are allowed for this check",
            "error": public.get("error", ""),
        },
    )
    if int(public.get("status", 0)) >= 500 or int(public.get("status", 0)) == 0:
        code = fail(
            report,
            "public_http_unhealthy",
            f"public_url status={public.get('status')} error={public.get('error', '')}",
            "public_http",
        )
        return finish(report, args.json_out, code)

    # 2) App shell on origin.
    origin_root = fetch(args.origin_url.rstrip("/") + "/", args.origin_timeout, args.max_bytes)
    shell_ok = int(origin_root.get("status", 0)) == 200 and (
        "The Algo" in origin_root.get("body", "") or "<html" in origin_root.get("body", "").lower()
    )
    append_check(
        report,
        {
            "name": "origin_app_shell",
            "status": origin_root.get("status"),
            "elapsed_ms": origin_root.get("elapsed_ms"),
            "ok": shell_ok,
            "error": origin_root.get("error", ""),
        },
    )
    if not shell_ok:
        code = fail(
            report,
            "origin_shell_failed",
            f"origin shell check failed status={origin_root.get('status')} err={origin_root.get('error', '')}",
            "origin_app_shell",
        )
        return finish(report, args.json_out, code)

    # 3) Feed endpoint returns data.
    feed_url = args.origin_url.rstrip("/") + "/api/content?limit=5&order=interesting"
    feed_resp = fetch(feed_url, args.origin_timeout, args.max_bytes)
    feed_items: List[Dict[str, Any]] = []
    feed_parse_error = ""
    if int(feed_resp.get("status", 0)) == 200:
        try:
            payload = json.loads(feed_resp.get("body", ""))
            if isinstance(payload, list):
                feed_items = [x for x in payload if isinstance(x, dict)]
            elif isinstance(payload, dict):
                if isinstance(payload.get("items"), list):
                    feed_items = [x for x in payload["items"] if isinstance(x, dict)]
        except Exception as exc:
            feed_parse_error = str(exc)
    feed_ok = int(feed_resp.get("status", 0)) == 200 and len(feed_items) > 0
    append_check(
        report,
        {
            "name": "origin_feed_data",
            "status": feed_resp.get("status"),
            "elapsed_ms": feed_resp.get("elapsed_ms"),
            "ok": feed_ok,
            "count": len(feed_items),
            "parse_error": one_line(feed_parse_error, 200),
            "error": feed_resp.get("error", ""),
        },
    )
    if not feed_ok:
        code = fail(
            report,
            "feed_empty_or_unavailable",
            f"feed status={feed_resp.get('status')} count={len(feed_items)} parse_error={feed_parse_error}",
            "origin_feed_data",
        )
        return finish(report, args.json_out, code)

    # 4) Post detail route opens.
    first = feed_items[0]
    source_id = (
        first.get("sourceId")
        or first.get("source_id")
        or first.get("id")
        or ""
    )
    source_id = str(source_id)
    detail_ok = bool(source_id)
    detail_status = 0
    detail_elapsed = 0
    detail_err = ""
    if detail_ok:
        detail_url = args.origin_url.rstrip("/") + "/post/" + urllib.parse.quote(source_id)
        detail = fetch(detail_url, args.origin_timeout, args.max_bytes)
        detail_status = int(detail.get("status", 0))
        detail_elapsed = int(detail.get("elapsed_ms", 0))
        body = detail.get("body", "")
        detail_err = detail.get("error", "")
        detail_ok = detail_status == 200 and "Application error" not in body
    append_check(
        report,
        {
            "name": "origin_post_detail",
            "status": detail_status,
            "elapsed_ms": detail_elapsed,
            "ok": detail_ok,
            "source_id": source_id,
            "error": detail_err,
        },
    )
    if not detail_ok:
        code = fail(
            report,
            "post_detail_failed",
            f"post detail failed source_id={source_id} status={detail_status} err={detail_err}",
            "origin_post_detail",
        )
        return finish(report, args.json_out, code)

    return finish(report, args.json_out, 0)


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


if __name__ == "__main__":
    raise SystemExit(main())
