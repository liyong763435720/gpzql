# -*- coding: utf-8 -*-
"""
【开发者工具】为客户生成激活码
使用方法：
    python tools/generate_license.py
依赖：tools/private_key.pem 存在
"""
import json
import base64
import os
import sys
from datetime import datetime, date, timedelta

# Windows 控制台强制 UTF-8，避免中文/emoji 乱码
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stdin.reconfigure(encoding='utf-8')

try:
    from cryptography.hazmat.primitives.serialization import load_pem_private_key
except ImportError:
    print("[错误] 请先安装 cryptography: pip install cryptography")
    sys.exit(1)

if getattr(sys, 'frozen', False):
    # PyInstaller 打包后，资源文件在 sys._MEIPASS 下
    TOOLS_DIR = sys._MEIPASS
else:
    TOOLS_DIR = os.path.dirname(os.path.abspath(__file__))
PRIVATE_KEY_PATH = os.path.join(TOOLS_DIR, "tools", "private_key.pem")


def load_private_key():
    if not os.path.exists(PRIVATE_KEY_PATH):
        print(f"[错误] 私钥文件不存在: {PRIVATE_KEY_PATH}")
        print("请先运行 python tools/generate_keypair.py 生成密钥对")
        sys.exit(1)
    with open(PRIVATE_KEY_PATH, "rb") as f:
        return load_pem_private_key(f.read(), password=None)


def generate_license(machine_id: str, customer: str, expiry: str,
                     edition: str = "professional") -> str:
    private_key = load_private_key()
    payload = {
        "machine_id": machine_id,
        "customer":   customer,
        "expiry":     expiry,
        "edition":    edition,
        "issued_at":  date.today().strftime("%Y-%m-%d"),
    }
    payload_bytes = json.dumps(payload, sort_keys=True, ensure_ascii=False).encode()
    signature = private_key.sign(payload_bytes)
    license_data = {
        "payload":   payload,
        "signature": base64.b64encode(signature).decode(),
    }
    return base64.b64encode(json.dumps(license_data, ensure_ascii=False).encode()).decode()


def main():
    print("=" * 55)
    print("  涌金阁 - 激活码生成工具")
    print("=" * 55)

    machine_id = input("\n客户机器码（16位）: ").strip().upper()
    if len(machine_id) != 16:
        print("[警告] 机器码长度不是16位，请确认")

    customer = input("客户名称: ").strip()
    if not customer:
        customer = "用户"

    print("\n授权时长:")
    print("  1. 1年（365天）")
    print("  2. 2年（730天）")
    print("  3. 永久（2099-12-31）")
    print("  4. 自定义日期")
    choice = input("选择 [1-4]: ").strip()

    today = date.today()
    if choice == "1":
        expiry = (today + timedelta(days=365)).strftime("%Y-%m-%d")
    elif choice == "2":
        expiry = (today + timedelta(days=730)).strftime("%Y-%m-%d")
    elif choice == "3":
        expiry = "2099-12-31"
    else:
        expiry = input("到期日期 (YYYY-MM-DD): ").strip()
        try:
            datetime.strptime(expiry, "%Y-%m-%d")
        except ValueError:
            print("[错误] 日期格式错误")
            sys.exit(1)

    print("\n授权版本:")
    print("  1. standard     - 标准版")
    print("  2. professional - 专业版（全功能）")
    choice2 = input("选择 [1-2，默认2]: ").strip()
    edition = "standard" if choice2 == "1" else "professional"

    license_text = generate_license(machine_id, customer, expiry, edition)

    print("\n" + "=" * 55)
    print("✅ 激活码生成成功，发送以下内容给客户：")
    print("=" * 55)
    print(license_text)
    print("=" * 55)
    print(f"\n客户:   {customer}")
    print(f"机器码: {machine_id}")
    print(f"到期:   {expiry}")
    print(f"版本:   {edition}")

    # 同时保存到文件（exe 旁边，或脚本目录）
    if getattr(sys, 'frozen', False):
        save_dir = os.path.dirname(sys.executable)
    else:
        save_dir = TOOLS_DIR
    out_file = os.path.join(save_dir, f"license_{customer}_{expiry}.txt")
    with open(out_file, "w", encoding="utf-8") as f:
        f.write(f"客户: {customer}\n机器码: {machine_id}\n到期: {expiry}\n版本: {edition}\n\n")
        f.write(license_text)
    print(f"\n已保存到: {out_file}")


if __name__ == "__main__":
    main()
