#!/usr/bin/env python3
"""Independent network-scoped live runtime receipts.

Network scopes are CoreMesh, Lattice, and Starfield. Telemetry suppliers are
Thunder, VORTEX, and Starburst. A receipt may explicitly belong to any
combination of the three network scopes; receivers accept it only when their
own configured scope is present. No network is implicitly merged with another.
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

NETWORK = os.getenv("RUNTIME_RECEIPT_NETWORK", "lattice").strip().lower()
PREFIX = os.getenv("RUNTIME_RECEIPT_PREFIX", "LATTICE").strip().upper()
NETWORKS = {"coremesh", "lattice", "starfield"}
SOURCES = {"Thunder", "VORTEX", "Starburst"}
ROLES = ("CIN", "EVE", "ORACLE")
ROOT = Path(__file__).resolve().parent
RECEIPT_DIR = ROOT / "runtime_receipts"
RECEIPT_DIR.mkdir(parents=True, exist_ok=True)


def _canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def _normalize_networks(values: Any) -> tuple[str, ...]:
    if isinstance(values, str):
        raw = values.replace(",", "+").split("+")
    elif isinstance(values, (list, tuple, set)):
        raw = list(values)
    else:
        raise ValueError("receipt networks must be a string or list")
    normalized = sorted({str(v).strip().lower() for v in raw if str(v).strip()})
    if not normalized or any(v not in NETWORKS for v in normalized):
        raise ValueError(f"receipt network scope outside CoreMesh/Lattice/Starfield: {normalized!r}")
    return tuple(normalized)


def _configured_network() -> str:
    value = NETWORK.strip().lower()
    if value not in NETWORKS:
        raise ValueError(f"configured runtime receipt network outside scope: {value!r}")
    return value


def _validate_source(source: str) -> None:
    if source not in SOURCES:
        raise ValueError(f"runtime receipt source outside Thunder/VORTEX/Starburst: {source!r}")


def _token() -> str:
    return os.getenv(f"{PREFIX}_RECEIPT_TOKEN", "")


def _endpoint(role: str) -> str:
    return os.getenv(f"{PREFIX}_RECEIPT_{role}_ENDPOINT", "").strip().rstrip("/")


@dataclass(frozen=True)
class RuntimeReceipt:
    networks: tuple[str, ...]
    device: str
    source: str
    event: str
    observed_utc: str
    payload: dict[str, Any]
    receipt_sha256: str
    recipients: tuple[str, ...] = ROLES

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": "mxc.live_runtime_receipt.v3",
            "networks": list(self.networks),
            "device": self.device,
            "source": self.source,
            "event": self.event,
            "observed_utc": self.observed_utc,
            "payload": self.payload,
            "receipt_sha256": self.receipt_sha256,
            "recipients": list(self.recipients),
        }


def build_receipt(device: str, source: str, event: str, payload: dict[str, Any],
                  networks: Any = None) -> RuntimeReceipt:
    configured = _configured_network()
    scope = _normalize_networks(networks if networks is not None else configured)
    if configured not in scope:
        raise ValueError(f"configured network {configured!r} is not included in receipt scope {scope!r}")
    _validate_source(source)
    observed = datetime.now(timezone.utc).isoformat()
    unsigned = {
        "networks": list(scope), "device": device, "source": source,
        "event": event, "observed_utc": observed, "payload": payload,
        "recipients": list(ROLES),
    }
    digest = hashlib.sha256(_canonical(unsigned)).hexdigest()
    return RuntimeReceipt(scope, device, source, event, observed, payload, digest)


def persist_receipt(receipt: RuntimeReceipt) -> Path:
    safe_device = "".join(ch if ch.isalnum() or ch in "-_." else "_" for ch in receipt.device)
    path = RECEIPT_DIR / f"{int(time.time() * 1000)}_{safe_device}.jsonl"
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(receipt.to_dict(), separators=(",", ":"), ensure_ascii=False) + "\n")
    return path


def send_receipt(receipt: RuntimeReceipt) -> dict[str, Any]:
    body = _canonical(receipt.to_dict())
    configured = _configured_network()
    result = {"network": configured, "receipt_networks": list(receipt.networks),
              "device": receipt.device, "receipt_sha256": receipt.receipt_sha256,
              "delivered": [], "failed": []}
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


def emit_receipt(device: str, source: str, event: str, payload: dict[str, Any],
                 networks: Any = None) -> dict[str, Any]:
    receipt = build_receipt(device, source, event, payload, networks=networks)
    persist_receipt(receipt)
    return {"receipt": receipt.to_dict(), "delivery": send_receipt(receipt)}


def receive_receipt(headers: dict[str, str], body: bytes) -> Path:
    supplied = headers.get("Authorization", "")
    expected = _token()
    if not expected or supplied != f"Bearer {expected}":
        raise PermissionError("runtime receipt authorization failed")
    data = json.loads(body.decode("utf-8"))
    if data.get("schema") != "mxc.live_runtime_receipt.v3":
        raise ValueError("unsupported runtime receipt schema")
    scope = _normalize_networks(data.get("networks"))
    if _configured_network() not in scope:
        raise ValueError(f"receipt scope {scope!r} does not include configured network {_configured_network()!r}")
    _validate_source(str(data.get("source", "")))
    recipients = tuple(data.get("recipients", ROLES))
    if set(recipients) != set(ROLES):
        raise ValueError("runtime receipt recipient set must remain CIN/EVE/ORACLE")
    unsigned = {
        "networks": list(scope), "device": data["device"], "source": data["source"],
        "event": data["event"], "observed_utc": data["observed_utc"],
        "payload": data["payload"], "recipients": list(recipients),
    }
    digest = hashlib.sha256(_canonical(unsigned)).hexdigest()
    if digest != data.get("receipt_sha256"):
        raise ValueError("runtime receipt integrity check failed")
    receipt = RuntimeReceipt(scope, str(data["device"]), str(data["source"]),
                             str(data["event"]), str(data["observed_utc"]),
                             dict(data["payload"]), digest, recipients)
    return persist_receipt(receipt)
