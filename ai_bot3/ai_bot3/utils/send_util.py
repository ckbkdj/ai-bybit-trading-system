# -*- coding: utf-8 -*-
from __future__ import annotations

import base64
import hashlib
import hmac
import os
import threading
import time

import requests


def create_element_json(content):
    return {"tag": "div", "text": {"content": content, "tag": "lark_md"}}


def create_json(elements, header_content):
    return {
        "msg_type": "interactive",
        "card": {
            "elements": elements,
            "header": {"title": {"content": header_content, "tag": "plain_text"}},
        },
    }


def gen_sign(timestamp, secret):
    string_to_sign = f"{timestamp}\n{secret}"
    digest = hmac.new(string_to_sign.encode("utf-8"), digestmod=hashlib.sha256).digest()
    return base64.b64encode(digest).decode("utf-8")


def _post_lark(message) -> bool:
    webhook_url = os.environ.get("LARK_WEBHOOK_URL", "").strip()
    secret = os.environ.get("LARK_WEBHOOK_SECRET", "").strip()
    if not webhook_url:
        return False
    timestamp = str(int(time.time()))
    payload = dict(message)
    if secret:
        payload.update({"timestamp": timestamp, "sign": gen_sign(timestamp, secret)})
    response = requests.post(webhook_url, json=payload, timeout=10)
    response.raise_for_status()
    return True


def send_algemeen_dagblad(msg):
    try:
        return _post_lark(msg)
    except Exception:
        return False


def send_error_warning(msg):
    return _post_lark(
        {"msg_type": "text", "content": {"text": str(msg)}}
    )


_alert_cooldown_lock = threading.Lock()
_alert_cooldown_state: dict[str, float] = {}


def send_error_warning_with_cooldown(key: str, msg: str, cooldown_seconds: int = 1800) -> bool:
    """Send at most one alert for a key during the cooldown window."""
    now = time.time()
    with _alert_cooldown_lock:
        last = _alert_cooldown_state.get(key, 0.0)
        if now - last < cooldown_seconds:
            return False
        _alert_cooldown_state[key] = now
    try:
        send_error_warning(msg)
        return True
    except Exception:
        return False
