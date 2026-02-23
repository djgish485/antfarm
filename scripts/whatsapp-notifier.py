#!/usr/bin/env python3
"""Send WhatsApp updates for Antfarm run lifecycle without dev-dashboard."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sqlite3
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional

DB_PATH = Path.home() / ".openclaw" / "antfarm" / "antfarm.db"
STATE_PATH = Path.home() / ".openclaw" / "antfarm" / "whatsapp-notifier-state.json"
LOG_DIR = Path("/tmp/openclaw")
DEFAULT_STALLED_MINUTES = 45
DEFAULT_REPEAT_MINUTES = 30

TERMINAL = {"completed", "failed", "cancelled"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default=str(DB_PATH))
    parser.add_argument("--state", default=str(STATE_PATH))
    parser.add_argument("--log-dir", default=str(LOG_DIR))
    parser.add_argument("--target", default=os.environ.get("ANTFARM_STATUS_WHATSAPP_TARGET", ""))
    parser.add_argument("--stalled-minutes", type=int, default=DEFAULT_STALLED_MINUTES)
    parser.add_argument("--repeat-minutes", type=int, default=DEFAULT_REPEAT_MINUTES)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def valid_target(value: object) -> bool:
    if not isinstance(value, str):
        return False
    text = value.strip()
    return text.startswith("+") and text[1:].isdigit() and 8 <= len(text[1:]) <= 18


def parse_iso(value: object) -> Optional[dt.datetime]:
    if not isinstance(value, str):
        return None
    raw = value.strip()
    if not raw:
        return None
    try:
        parsed = dt.datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def parse_sqlite_ts(value: object) -> Optional[dt.datetime]:
    if not isinstance(value, str):
        return None
    raw = value.strip()
    if not raw:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f"):
        try:
            return dt.datetime.strptime(raw, fmt).replace(tzinfo=dt.timezone.utc)
        except ValueError:
            continue
    return parse_iso(raw)


def load_state(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {
            "bootstrapped": False,
            "started_sent": {},
            "terminal_sent": {},
            "stalled_sent": {},
            "smoke_retry_sent": {},
            "escalation_sent": {},
        }
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {
            "bootstrapped": False,
            "started_sent": {},
            "terminal_sent": {},
            "stalled_sent": {},
            "smoke_retry_sent": {},
            "escalation_sent": {},
        }
    if not isinstance(data, dict):
        return {
            "bootstrapped": False,
            "started_sent": {},
            "terminal_sent": {},
            "stalled_sent": {},
            "smoke_retry_sent": {},
            "escalation_sent": {},
        }
    for key in ("started_sent", "terminal_sent", "stalled_sent", "smoke_retry_sent", "escalation_sent"):
        if not isinstance(data.get(key), dict):
            data[key] = {}
    if not isinstance(data.get("bootstrapped"), bool):
        data["bootstrapped"] = False
    return data


def save_state(path: Path, state: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(path)


def config_targets() -> List[str]:
    try:
        proc = subprocess.run(
            ["openclaw", "config", "get", "channels.whatsapp"],
            check=False,
            capture_output=True,
            text=True,
            timeout=20,
        )
    except Exception:
        return []
    if proc.returncode != 0:
        return []
    raw = (proc.stdout or "").strip()
    if not raw:
        return []
    try:
        cfg = json.loads(raw)
    except json.JSONDecodeError:
        return []
    if not isinstance(cfg, dict):
        return []

    out: List[str] = []

    def add_many(values: object) -> None:
        if isinstance(values, list):
            for v in values:
                if valid_target(v):
                    s = str(v).strip()
                    if s not in out:
                        out.append(s)

    add_many(cfg.get("allowFrom"))
    accounts = cfg.get("accounts")
    if isinstance(accounts, dict):
        for acct in accounts.values():
            if isinstance(acct, dict):
                add_many(acct.get("allowFrom"))
    return out


def latest_inbound_target(log_dir: Path) -> Optional[str]:
    files = sorted(log_dir.glob("openclaw-*.log"))
    if not files:
        return None

    best_ts: Optional[dt.datetime] = None
    best_target: Optional[str] = None

    for path in files[-3:]:
        try:
            with path.open("r", encoding="utf-8") as handle:
                for raw in handle:
                    line = raw.strip()
                    if not line:
                        continue
                    try:
                        entry = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if entry.get("2") != "inbound web message":
                        continue
                    payload = entry.get("1")
                    if not isinstance(payload, dict):
                        continue
                    sender = payload.get("from")
                    if not valid_target(sender):
                        continue
                    stamp = parse_iso(entry.get("time"))
                    if stamp is None:
                        continue
                    if best_ts is None or stamp >= best_ts:
                        best_ts = stamp
                        best_target = str(sender).strip()
        except Exception:
            continue
    return best_target


def resolve_target(explicit: str, log_dir: Path) -> Optional[str]:
    candidates: List[str] = []
    if valid_target(explicit):
        candidates.append(explicit.strip())

    recent = latest_inbound_target(log_dir)
    if recent and recent not in candidates:
        candidates.append(recent)

    for c in config_targets():
        if c not in candidates:
            candidates.append(c)

    return candidates[0] if candidates else None


def send_whatsapp(target: str, message: str, dry_run: bool) -> bool:
    if dry_run:
        print(f"[dry-run] to {target}: {message}")
        return True

    proc = subprocess.run(
        [
            "openclaw",
            "message",
            "send",
            "--channel",
            "whatsapp",
            "--target",
            target,
            "--message",
            message,
            "--json",
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=45,
    )
    if proc.returncode == 0:
        return True
    detail = (proc.stderr or proc.stdout or "").strip()
    print(f"[antfarm-notify] send failed: {detail}")
    return False


def compact_task(text: object) -> str:
    if not isinstance(text, str):
        return "(task unavailable)"
    flat = " ".join(text.split())
    if not flat:
        return "(task unavailable)"
    return flat


def run_label(run_number: object, workflow_id: object, run_id: str) -> str:
    run_piece = f"#{run_number}" if isinstance(run_number, int) else run_id[:8]
    wf = workflow_id if isinstance(workflow_id, str) and workflow_id else "workflow"
    return f"run {run_piece} ({wf})"


def started_message(row: sqlite3.Row) -> str:
    return f"[Antfarm] Started {run_label(row['run_number'], row['workflow_id'], row['id'])}: {compact_task(row['task'])}"


def terminal_message(row: sqlite3.Row) -> str:
    status = str(row["status"]).lower()
    label = run_label(row["run_number"], row["workflow_id"], row["id"])
    task = compact_task(row["task"])
    if status == "completed":
        return f"[Antfarm] Completed {label}: {task}"
    return f"[Antfarm] Needs attention: {label} is {status}. Task: {task}"


def stalled_message(row: sqlite3.Row, minutes_running: int) -> str:
    label = run_label(row["run_number"], row["workflow_id"], row["id"])
    task = compact_task(row["task"])
    return f"[Antfarm] Running {minutes_running}m: {label}. Task: {task}"


def parse_context(raw: object) -> Dict[str, str]:
    if not isinstance(raw, str) or not raw.strip():
        return {}
    try:
        value = json.loads(raw)
    except Exception:
        return {}
    if not isinstance(value, dict):
        return {}
    out: Dict[str, str] = {}
    for k, v in value.items():
        if isinstance(k, str) and isinstance(v, (str, int, float, bool)):
            out[k] = str(v)
    return out


def smoke_retry_message(row: sqlite3.Row, retry_count: int, detail: str) -> str:
    label = run_label(row["run_number"], row["workflow_id"], row["id"])
    suffix = f" {detail}" if detail else ""
    return f"[Antfarm] Live smoke failed on {label}; retry {retry_count}/2 started.{suffix}".strip()


def escalation_message(row: sqlite3.Row, ctx: Dict[str, str]) -> str:
    label = run_label(row["run_number"], row["workflow_id"], row["id"])
    target = ctx.get("escalation_workflow", "unknown-workflow")
    target_run = ctx.get("escalation_run_id", "unknown-run")
    return f"[Antfarm] Retries exhausted for {label}; escalated to {target} ({target_run})."


def bootstrap_state(state: Dict[str, Any], rows: List[sqlite3.Row]) -> None:
    now = dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")
    for row in rows:
        run_id = str(row["id"])
        status = str(row["status"]).lower()
        if status == "running":
            state["started_sent"][run_id] = now
        elif status in TERMINAL:
            state["terminal_sent"][f"{run_id}:{status}"] = now
    state["bootstrapped"] = True


def prune_state(state: Dict[str, Any], rows: List[sqlite3.Row]) -> None:
    active_ids = {str(r["id"]) for r in rows}
    for key in list(state["started_sent"].keys()):
        if key not in active_ids:
            state["started_sent"].pop(key, None)
    for key in list(state["stalled_sent"].keys()):
        if key not in active_ids:
            state["stalled_sent"].pop(key, None)
    for key in list(state["terminal_sent"].keys()):
        run_id = key.split(":", 1)[0]
        if run_id not in active_ids:
            state["terminal_sent"].pop(key, None)
    for key in list(state["smoke_retry_sent"].keys()):
        run_id = key.split(":", 1)[0]
        if run_id not in active_ids:
            state["smoke_retry_sent"].pop(key, None)
    for key in list(state["escalation_sent"].keys()):
        if key not in active_ids:
            state["escalation_sent"].pop(key, None)


def main() -> int:
    args = parse_args()
    db_path = Path(args.db)
    state_path = Path(args.state)

    if not db_path.exists():
        print(f"[antfarm-notify] db missing: {db_path}")
        return 0

    target = resolve_target(args.target, Path(args.log_dir))
    if not target:
        print("[antfarm-notify] no WhatsApp target found")
        return 0

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT id, workflow_id, task, status, run_number, created_at, updated_at, context
            FROM runs
            ORDER BY created_at ASC
            LIMIT 1000
            """
        ).fetchall()

    state = load_state(state_path)

    if not state.get("bootstrapped", False):
        bootstrap_state(state, rows)
        save_state(state_path, state)
        print(f"[antfarm-notify] bootstrapped state for {len(rows)} runs")
        return 0

    prune_state(state, rows)

    now = dt.datetime.now(dt.timezone.utc)
    stalled_after = dt.timedelta(minutes=max(1, int(args.stalled_minutes)))
    repeat_after = dt.timedelta(minutes=max(1, int(args.repeat_minutes)))
    now_iso = now.isoformat().replace("+00:00", "Z")

    started_sent = 0
    terminal_sent = 0
    stalled_sent = 0
    smoke_retry_sent = 0
    escalation_sent = 0

    for row in rows:
        run_id = str(row["id"])
        status = str(row["status"]).lower()
        ctx = parse_context(row["context"])

        if status == "running" and run_id not in state["started_sent"]:
            if send_whatsapp(target, started_message(row), args.dry_run):
                state["started_sent"][run_id] = now_iso
                started_sent += 1

        retry_from = ctx.get("retry_from_step", "")
        retry_count_raw = ctx.get("retry_count", "0")
        retry_count = int(retry_count_raw) if retry_count_raw.isdigit() else 0
        if status == "running" and retry_from == "live-smoke" and retry_count > 0:
            retry_key = f"{run_id}:{retry_count}"
            if retry_key not in state["smoke_retry_sent"]:
                detail = " " + " ".join((ctx.get("retry_feedback", "") or "").split())[:120]
                if send_whatsapp(target, smoke_retry_message(row, retry_count, detail.strip()), args.dry_run):
                    state["smoke_retry_sent"][retry_key] = now_iso
                    smoke_retry_sent += 1

        if status in TERMINAL:
            key = f"{run_id}:{status}"
            if key not in state["terminal_sent"]:
                if send_whatsapp(target, terminal_message(row), args.dry_run):
                    state["terminal_sent"][key] = now_iso
                    state["stalled_sent"].pop(run_id, None)
                    terminal_sent += 1

            if status == "failed" and ctx.get("escalation_run_id") and run_id not in state["escalation_sent"]:
                if send_whatsapp(target, escalation_message(row, ctx), args.dry_run):
                    state["escalation_sent"][run_id] = now_iso
                    escalation_sent += 1
            continue

        if status != "running":
            continue

        created_at = parse_sqlite_ts(row["created_at"])
        if created_at is None:
            continue
        age = now - created_at
        if age < stalled_after:
            continue

        prior = parse_iso(state["stalled_sent"].get(run_id))
        if prior is not None and (now - prior) < repeat_after:
            continue

        mins = max(1, int(age.total_seconds() // 60))
        if send_whatsapp(target, stalled_message(row, mins), args.dry_run):
            state["stalled_sent"][run_id] = now_iso
            stalled_sent += 1

    save_state(state_path, state)
    print(
        f"[antfarm-notify] target={target} started={started_sent} terminal={terminal_sent} stalled={stalled_sent} smoke_retry={smoke_retry_sent} escalated={escalation_sent}"
        + (" dry_run=true" if args.dry_run else "")
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
