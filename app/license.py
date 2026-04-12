# -*- coding: utf-8 -*-
"""
授权管理模块
- 机器码：MAC + 主机名 的 SHA256 前16位
- 激活码：Base64(JSON payload + ED25519 签名)
- 公钥内置于此文件；私钥由开发者保管（tools/private_key.pem）
"""
import hashlib
import json
import base64
import os
import logging
from datetime import date, datetime
from typing import Tuple, Optional, Dict

from cryptography.hazmat.primitives.serialization import load_pem_public_key
from cryptography.exceptions import InvalidSignature

_logger = logging.getLogger(__name__)

# ── 内置公钥（永远不更换，对应 tools/private_key.pem）────────
_PUBLIC_KEY_PEM = b"""-----BEGIN PUBLIC KEY-----
MCowBQYDK2VwAyEAkRaoiMnI82Pk/ajeGvgH28lkKFvUEzceKVUlq4vsQGo=
-----END PUBLIC KEY-----"""

# 试用天数（首次运行起）
TRIAL_DAYS = 15

# 激活码文件路径（与 stock_data.db 同目录）
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LICENSE_FILE = os.path.join(_ROOT, "license.key")
INSTALL_DATE_FILE = os.path.join(_ROOT, "install.dat")


# ── 机器码 ─────────────────────────────────────────────────
def get_machine_id() -> str:
    """生成本机唯一机器码（16位大写十六进制）"""
    try:
        import uuid
        import socket
        mac = str(uuid.getnode())
        hostname = socket.gethostname()
        raw = f"{mac}:{hostname}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16].upper()
    except Exception:
        return "0000000000000000"


# ── 安装日期 ────────────────────────────────────────────────
def _get_install_date() -> date:
    """获取首次运行日期，不存在则写入今天"""
    if os.path.exists(INSTALL_DATE_FILE):
        try:
            with open(INSTALL_DATE_FILE, "r") as f:
                return datetime.strptime(f.read().strip(), "%Y-%m-%d").date()
        except Exception:
            pass
    today = date.today()
    try:
        with open(INSTALL_DATE_FILE, "w") as f:
            f.write(today.strftime("%Y-%m-%d"))
    except Exception:
        pass
    return today


def _trial_remaining() -> int:
    """返回剩余试用天数（负数表示已过期）"""
    install_date = _get_install_date()
    elapsed = (date.today() - install_date).days
    return TRIAL_DAYS - elapsed


# ── 激活码验证 ──────────────────────────────────────────────
def validate_license_text(license_text: str) -> Tuple[bool, str, Optional[Dict]]:
    """
    验证激活码字符串。
    返回 (is_valid, reason, payload_dict)
    reason: 'valid' | 'invalid_signature' | 'expired' | 'machine_mismatch' | 'malformed'
    """
    try:
        raw = base64.b64decode(license_text.strip().encode())
        data = json.loads(raw)
        payload: Dict = data["payload"]
        signature: bytes = base64.b64decode(data["signature"])

        # 签名验证
        pub_key = load_pem_public_key(_PUBLIC_KEY_PEM)
        payload_bytes = json.dumps(payload, sort_keys=True, ensure_ascii=False).encode()
        pub_key.verify(signature, payload_bytes)  # 抛出 InvalidSignature 则无效

        # 到期日验证
        expiry = datetime.strptime(payload["expiry"], "%Y-%m-%d").date()
        if expiry < date.today():
            return False, "expired", payload

        # 机器码验证（若激活码中含机器码）
        if payload.get("machine_id"):
            if payload["machine_id"] != get_machine_id():
                return False, "machine_mismatch", payload

        return True, "valid", payload

    except InvalidSignature:
        return False, "invalid_signature", None
    except (KeyError, ValueError, json.JSONDecodeError):
        return False, "malformed", None
    except Exception as e:
        _logger.warning(f"License validation error: {e}")
        return False, "malformed", None


# ── 激活码读写 ──────────────────────────────────────────────
def load_license_text() -> Optional[str]:
    if not os.path.exists(LICENSE_FILE):
        return None
    try:
        with open(LICENSE_FILE, "r", encoding="utf-8") as f:
            return f.read().strip() or None
    except Exception:
        return None


def save_license_text(license_text: str) -> bool:
    try:
        with open(LICENSE_FILE, "w", encoding="utf-8") as f:
            f.write(license_text.strip())
        return True
    except Exception as e:
        _logger.error(f"Failed to save license: {e}")
        return False


# ── 对外统一状态接口 ────────────────────────────────────────
def get_license_status() -> Dict:
    """
    返回当前授权状态字典（已禁用授权校验，始终返回已激活）
    """
    return {
        "status": "valid",
        "valid": True,
        "machine_id": get_machine_id(),
        "trial_remaining": 0,
        "customer": "",
        "expiry": "",
        "edition": "standard",
    }
