#!/usr/bin/env python3
"""Independent network-scoped live runtime receipts for Thunder, VORTEX, and Starburst."""

from __future__ import annotations
import hashlib, json, os, time, urllib.error, urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

NETWORK = os.getenv("RUNTIME_RECEIPT_NETWORK", "lattice").strip().lower()
PREFIX = os.getenv("RUNTIME_RECEIPT_PREFIX", "LATTICE").strip().upper()
NETWORKS = {"coremesh", "starburst", "starfield", "lattice"}
SOURCES = {"Thunder", "VORTEX", "Starburst"}
ROLES = ("CIN", "EVE", "ORACLE")
ROOT = Path(__file__).resolve().parent
RECEIPT_DIR = ROOT / "runtime_receipts"
RECEIPT_DIR.mkdir(parents=True, exist_ok=True)

def _canonical(value: dict[str, Any]) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()

def _validate_network(network: str) -> str:
    network = network.strip().lower()
    if network not in NETWORKS:
        raise ValueError(f"runtime receipt network outside authorized scope: {network!r}")
    if network != NETWORK:
        raise ValueError(f"cross-network receipt rejected: {network!r} -> {NETWORK!r}")
    return network

def _validate_source(source: str) -> None:
    if source not in SOURCES:
        raise ValueError(f"runtime receipt source outside Thunder/VORTEX/Starburst: {source!r}")

def _token() -> str:
    return os.getenv(f"{PREFIX}_RECEIPT_TOKEN", "")

def _endpoint(role: str) -> str:
    return os.getenv(f"{PREFIX}_RECEIPT_{role}_ENDPOINT", "").strip().rstrip("/")

@dataclass(frozen=True)
class RuntimeReceipt:
    network: str
    device: str
    source: str
    event: str
    observed_utc: str
    payload: dict[str, Any]
    receipt_sha256: str
    recipients: tuple[str, ...] = ROLES

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": "mxc.live_runtime_receipt.v2",
            "network": self.network,
            "device": self.device,
            "source": self.source,
            "event": self.event,
            "observed_utc": self.observed_utc,
            "payload": self.payload,
            "receipt_sha256": self.receipt_sha256,
            "recipients": list(self.recipients),
        }

def build_receipt(device: str, source: str, event: str, payload: dict[str, Any]) -> RuntimeReceipt:
    _validate_network(NETWORK)
    _validate_source(source)
    observed = datetime.now(timezone.utc).isoformat()
    unsigned = {
        "network": NETWORK, "device": device, "source": source,
        "event": event, "observed_utc": observed, "payload": payload,
        "recipients": list(ROLES),
    }
    digest = hashlib.sha256(_canonical(unsigned)).hexdigest()
    return RuntimeReceipt(NETWORK, device, source, event, observed, payload, digest)

def persist_receipt(receipt: RuntimeReceipt) -> Path:
    path = RECEIPT_DIR / f"{int(time.time() * 1000)}_{receipt.device}.jsonl"
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(receipt.to_dict(), separators=(",", ":"), ensure_ascii=False) + "\n")
    return path

def send_receipt(receipt: RuntimeReceipt) -> dict[str, Any]:
    body = _canonical(receipt.to_dict())
    result = {"network": receipt.network, "device": receipt.device,
              "receipt_sha256": receipt.receipt_sha256, "delivered": [], "failed": []}
    token = _token()
    for role in receipt.recipients:
        target = _endpoint(role)
        if not target:
            result["failed"].append({"role": role, "error": "endpoint_not_configured"})
            continue
        request = urllib.request.Request(
            target + "/v1/runtime-receipt", data=body,
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json",
                     "Cache-Control": "no-store"}, method="POST")
        try:
            with urllib.request.urlopen(request, timeout=float(os.getenv(f"{PREFIX}_RECEIPT_TIMEOUT", "5"))) as response:
                if 200 <= response.status < 300:
                    result["delivered"].append(role)
                else:
                    result["failed"].append({"role": role, "status": response.status})
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            result["failed"].append({"role": role, "error": str(exc)})
    return result

def emit_receipt(device: str, source: str, event: str, payload: dict[str, Any]) -> dict[str, Any]:
    receipt = build_receipt(device, source, event, payload)
    persist_receipt(receipt)
    return {"receipt": receipt.to_dict(), "delivery": send_receipt(receipt)}

def receive_receipt(headers: dict[str, str], body: bytes) -> Path:
    supplied = headers.get("Authorization", "")
    expected = _token()
    if not expected or supplied != f"Bearer {expected}":
        raise PermissionError("runtime receipt authorization failed")
    data = json.loads(body.decode("utf-8"))
    if data.get("schema") != "mxc.live_runtime_receipt.v2":
        raise ValueError("unsupported runtime receipt schema")
    if data.get("network") != NETWORK:
        raise ValueError(f"receipt network mismatch: expected {NETWORK!r}")
    _validate_source(str(data.get("source", "")))
    recipients = tuple(data.get("recipients", ROLES))
    if set(recipients) != set(ROLES):
        raise ValueError("runtime receipt recipient set must remain CIN/EVE/ORACLE")
    unsigned = {
        "network": data["network"], "device": data["device"], "source": data["source"],
        "event": data["event"], "observed_utc": data["observed_utc"],
        "payload": data["payload"], "recipients": list(recipients),
    }
    digest = hashlib.sha256(_canonical(unsigned)).hexdigest()
    if digest != data.get("receipt_sha256"):
        raise ValueError("runtime receipt integrity check failed")
    receipt = RuntimeReceipt(data["network"], str(data["device"]), str(data["source"]),
                             str(data["event"]), str(data["observed_utc"]),
                             dict(data["payload"]), digest, recipients)
    return persist_receipt(receipt)
