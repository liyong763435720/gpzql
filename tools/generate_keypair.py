# -*- coding: utf-8 -*-
"""
【一次性工具】生成 ED25519 密钥对
运行后：
  - private_key.pem  → 妥善保管，绝不泄露，用于生成激活码
  - public_key.pem   → 内容已嵌入 app/license.py，无需再次使用
"""
import os
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import (
    Encoding, PublicFormat, PrivateFormat, NoEncryption
)

TOOLS_DIR = os.path.dirname(os.path.abspath(__file__))

key = Ed25519PrivateKey.generate()
pub_pem = key.public_key().public_bytes(Encoding.PEM, PublicFormat.SubjectPublicKeyInfo)
priv_pem = key.private_bytes(Encoding.PEM, PrivateFormat.PKCS8, NoEncryption())

priv_path = os.path.join(TOOLS_DIR, "private_key.pem")
pub_path  = os.path.join(TOOLS_DIR, "public_key.pem")

with open(priv_path, "wb") as f:
    f.write(priv_pem)
with open(pub_path, "wb") as f:
    f.write(pub_pem)

print("=" * 50)
print("密钥对生成成功")
print(f"私钥: {priv_path}")
print(f"公钥: {pub_path}")
print("=" * 50)
print("\n公钥内容（复制到 app/license.py 的 _PUBLIC_KEY_PEM）：")
print(pub_pem.decode())
print("⚠️  请勿将 private_key.pem 上传到任何地方！")
