#!/usr/bin/env python3
"""Live runtime receipt bus for network-scoped Thunder/VORTEX/Starburst telemetry.

A device receipt is an immutable, timestamped runtime observation. This module
does not fabricate device state: callers must provide the observation they
actually received. The same receipt can be delivered to CIN, EVE, and ORACLE
within the owning network.

Environment:
  <PREFIX>_RECEIPT_RECIPIENTS: comma-separated HTTPS/HTTP(S) URLs.
  <PREFIX>_RECEIPT_TOKEN: shared bearer token for the owning network.
  <PREFIX>_RECEIPT_TIMEOUT: request timeout in seconds.
"""
from __future__ import annotations

import hashlib
import json
import os
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

NETWORK = os.getenv("RUNTIME_RECEIPT_NETWORK", "lattice")
PREFIX = os.getenv("RUNTIME_RECEIPT_PREFIX", "LATTICE")
ROOT = Path(__file__).resolve().parent
RECEIPT_DIR = ROOT / "runtime_receipts"
RECEIPT_DIR.mkdir(parents=True, exist_ok=True)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _recipients() -> list[str]:
    raw = os.getenv(f"{PREFIX}_RECEIPT_RECIPIENTS", "")
    return [item.strip().rstrip("/") for item in raw.split(",") if item.strip()]


def _token() -> str:
    return os.getenv(f"{PREFIX}_RECEIPT_TOKEN", "")


def _canonical(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


@dataclass(frozen=True)
class RuntimeReceipt:
    network: str
    device: str
    source: str
    event: str
    observed_utc: str
    payload: dict[str, Any]
    receipt_sha256: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": "mxc.live_runtime_receipt.v1",
            "network": self.network,
            "device": self.device,
            "source": self.source,
            "event": self.event,
            "observed_utc": self.observed_utc,
            "payload": self.payload,
            "receipt_sha256": self.receipt_sha256,
        }


def build_receipt(
    device: str,
    source: str,
    event: str,
    payload: dict[str, Any],
) -> RuntimeReceipt:
    observed = _now()
    unsigned = {
        "network": NETWORK,
        "device": device,
        "source": source,
        "event": event,
        "observed_utc": observed,
        "payload": payload,
    }
    digest = hashlib.sha256(_canonical(unsigned)).hexdigest()
    return RuntimeReceipt(NETWORK, device, source, event, observed, payload, digest)


def persist_receipt(receipt: RuntimeReceipt) -> Path:
    path = RECEIPT_DIR / f"{int(time.time() * 1000)}_{receipt.device}.jsonl"
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(receipt.to_dict(), separators=(",", ":"), ensure_ascii=False) + "\n")
    return path


def send_receipt(receipt: RuntimeReceipt, recipients: list[str] | None = None) -> dict[str, Any]:
    targets = recipients if recipients is not None else _recipients()
    token = _token()
    body = _canonical(receipt.to_dict())
    results: dict[str, Any] = {"network": NETWORK, "device": receipt.device, "receipt_sha256": receipt.receipt_sha256, "delivered": [], "failed": []}
    for target in targets:
        request = urllib.request.Request(
            target + "/v1/runtime-receipt",
            data=body,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                "Cache-Control": "no-store",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=float(os.getenv(f"{PREFIX}_RECEIPT_TIMEOUT", "5"))) as response:
                if 200 <= response.status < 300:
                    results["delivered"].append(target)
                else:
                    results["failed"].append({"target": target, "status": response.status})
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            results["failed"].append({"target": target, "error": str(exc)})
    return results


def emit_receipt(device: str, source: str, event: str, payload: dict[str, Any]) -> dict[str, Any]:
    receipt = build_receipt(device, source, event, payload)
    persist_receipt(receipt)
    return {"receipt": receipt.to_dict(), "delivery": send_receipt(receipt)}


def receive_receipt(headers: dict[str, str], body: bytes) -> Path:
    expected = _token()
    supplied = headers.get("Authorization", "")
    if not expected or supplied != f"Bearer {expected}":
        raise PermissionError("runtime receipt authorization failed")
    data = json.loads(body.decode("utf-8"))
    if data.get("schema") != "mxc.live_runtime_receipt.v1":
        raise ValueError("unsupported runtime receipt schema")
    if data.get("network") != NETWORK:
        raise ValueError(f"receipt network mismatch: expected {NETWORK!r}")
    unsigned = {
        "network": data["network"],
        "device": data["device"],
        "source": data["source"],
        "event": data["event"],
        "observed_utc": data["observed_utc"],
        "payload": data["payload"],
    }
    digest = hashlib.sha256(_canonical(unsigned)).hexdigest()
    if digest != data.get("receipt_sha256"):
        raise ValueError("runtime receipt integrity check failed")
    receipt = RuntimeReceipt(
        network=data["network"],
        device=str(data["device"]),
        source=str(data["source"]),
        event=str(data["event"]),
        observed_utc=str(data["observed_utc"]),
        payload=dict(data["payload"]),
        receipt_sha256=digest,
    )
    return persist_receipt(receipt)
