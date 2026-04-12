"""
支付模块：JeePay 统一支付网关（支持支付宝、微信等）
"""
import hashlib
import time
import logging
import requests
from typing import Optional, Dict

_logger = logging.getLogger(__name__)


class PaymentError(Exception):
    pass


# ========== JeePay ==========

def _jeepay_sign(params: dict, secret: str) -> str:
    """JeePay MD5 签名"""
    filtered = {
        k: v for k, v in params.items()
        if k != 'sign' and v is not None and str(v) != ''
    }
    sign_str = '&'.join(f'{k}={v}' for k, v in sorted(filtered.items()))
    sign_str += f'&key={secret}'
    return hashlib.md5(sign_str.encode('utf-8')).hexdigest().upper()


def create_jeepay_qr(order_id: str, amount_fen: int, subject: str,
                     way_code: str, cfg) -> str:
    """
    调用 JeePay 统一下单，返回二维码内容字符串。
    way_code: ALI_QR（支付宝扫码）或 WX_NATIVE（微信扫码）
    """
    jeepay = cfg.get('jeepay') or {}
    if not jeepay.get('app_id'):
        raise PaymentError("JeePay 未配置，请在支付配置中填写相关信息")

    gateway   = jeepay['gateway'].rstrip('/')
    mch_no    = jeepay['mch_no']
    app_id    = jeepay['app_id']
    secret    = jeepay['app_secret']
    notify_url = jeepay.get('notify_url', '')

    params = {
        'mchNo':      mch_no,
        'appId':      app_id,
        'mchOrderNo': order_id,
        'wayCode':    way_code,
        'amount':     int(amount_fen),
        'currency':   'CNY',
        'subject':    subject,
        'body':       subject,
        'notifyUrl':  notify_url,
        'reqTime':    str(int(time.time())),
        'version':    '1.0',
        'signType':   'MD5',
    }
    params['sign'] = _jeepay_sign(params, secret)

    try:
        resp = requests.post(
            f'{gateway}/api/pay/unifiedOrder',
            json=params, timeout=15
        )
        resp.raise_for_status()
        result = resp.json()
    except requests.RequestException as e:
        raise PaymentError(f"请求 JeePay 失败: {e}")

    if result.get('code') != 0:
        raise PaymentError(f"JeePay 下单失败: {result.get('msg', '未知错误')}")

    pay_data = result.get('data', {}).get('payData', '')
    if not pay_data:
        raise PaymentError("JeePay 未返回支付数据，请检查通道配置")

    return pay_data


def verify_jeepay_notify(data: dict, secret: str) -> bool:
    """验证 JeePay 异步回调签名"""
    data = dict(data)
    sign = data.pop('sign', '')
    return _jeepay_sign(data, secret) == sign


def _read_key(path: str) -> str:
    with open(path, 'r', encoding='utf-8') as f:
        return f.read()


# ========== 支付宝 ==========

def create_alipay_qr(order_id: str, amount_yuan: float, subject: str, cfg) -> str:
    """
    创建支付宝当面付订单，返回二维码内容字符串（qr_code）。
    前端用 qrcode.js 将此字符串渲染成二维码图片。
    """
    try:
        from alipay import AliPay
    except ImportError:
        raise PaymentError("缺少 alipay-sdk-python，请执行 pip install alipay-sdk-python")

    alipay_cfg = cfg.get('alipay') or {}
    if not alipay_cfg.get('app_id'):
        raise PaymentError("支付宝未配置，请在 config.json 中填写 alipay 配置")

    client = AliPay(
        appid=alipay_cfg['app_id'],
        app_notify_url=alipay_cfg.get('notify_url', ''),
        app_private_key_string=_read_key(alipay_cfg['private_key_path']),
        alipay_public_key_string=_read_key(alipay_cfg['public_key_path']),
        sign_type='RSA2',
        debug=alipay_cfg.get('sandbox', False),
    )

    result = client.api_alipay_trade_precreate(
        subject=subject,
        out_trade_no=order_id,
        total_amount=f'{amount_yuan:.2f}',
    )

    if result.get('code') != '10000':
        msg = result.get('sub_msg') or result.get('msg') or str(result)
        raise PaymentError(f"支付宝创建订单失败: {msg}")

    return result['qr_code']


def verify_alipay_notify(data: Dict, cfg) -> bool:
    """验证支付宝异步回调签名"""
    try:
        from alipay import AliPay
    except ImportError:
        return False

    alipay_cfg = cfg.get('alipay') or {}
    client = AliPay(
        appid=alipay_cfg['app_id'],
        app_notify_url=None,
        app_private_key_string=_read_key(alipay_cfg['private_key_path']),
        alipay_public_key_string=_read_key(alipay_cfg['public_key_path']),
        sign_type='RSA2',
        debug=alipay_cfg.get('sandbox', False),
    )
    signature = data.pop('sign', '')
    return client.verify(data, signature)


# ========== 微信支付 ==========

def create_wechat_qr(order_id: str, amount_fen: int, description: str, cfg) -> str:
    """
    创建微信 Native 支付订单，返回 code_url。
    前端用 qrcode.js 将 code_url 渲染成二维码图片。
    """
    try:
        from wechatpayv3 import WeChatPay, WeChatPayType
    except ImportError:
        raise PaymentError("缺少 wechatpayv3，请执行 pip install wechatpayv3")

    wechat_cfg = cfg.get('wechat_pay') or {}
    if not wechat_cfg.get('mchid'):
        raise PaymentError("微信支付未配置，请在 config.json 中填写 wechat_pay 配置")

    client = WeChatPay(
        wechatpay_type=WeChatPayType.NATIVE,
        mchid=wechat_cfg['mchid'],
        private_key=_read_key(wechat_cfg['private_key_path']),
        cert_serial_no=wechat_cfg['cert_serial_no'],
        apiv3_key=wechat_cfg['api_v3_key'],
        appid=wechat_cfg['appid'],
        notify_url=wechat_cfg.get('notify_url', ''),
    )

    code, msg = client.pay(
        description=description,
        out_trade_no=order_id,
        amount={'total': amount_fen},
    )

    if code != 200:
        raise PaymentError(f"微信支付创建订单失败: {msg}")

    import json
    return json.loads(msg)['code_url']


def verify_wechat_notify(headers: Dict, body: bytes, cfg) -> Optional[Dict]:
    """
    解密微信支付回调，支付成功时返回资源数据 dict，否则返回 None。
    """
    try:
        from wechatpayv3 import WeChatPay, WeChatPayType
    except ImportError:
        return None

    wechat_cfg = cfg.get('wechat_pay') or {}
    client = WeChatPay(
        wechatpay_type=WeChatPayType.NATIVE,
        mchid=wechat_cfg['mchid'],
        private_key=_read_key(wechat_cfg['private_key_path']),
        cert_serial_no=wechat_cfg['cert_serial_no'],
        apiv3_key=wechat_cfg['api_v3_key'],
        appid=wechat_cfg['appid'],
        notify_url=wechat_cfg.get('notify_url', ''),
    )

    result = client.callback(headers=dict(headers), body=body)
    if result and result.get('event_type') == 'TRANSACTION.SUCCESS':
        return result.get('resource', {})
    return None
