"""邮件发送工具"""
import smtplib
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Optional

_logger = logging.getLogger(__name__)

def send_email(smtp_cfg: dict, to: str, subject: str, body_html: str) -> bool:
    """
    发送邮件。
    smtp_cfg 字段: host, port, user, password, from_addr, use_ssl(bool)
    返回 True 成功，False 失败。
    """
    host      = smtp_cfg.get('host', '')
    port      = int(smtp_cfg.get('port', 465))
    user      = smtp_cfg.get('user', '')
    password  = smtp_cfg.get('password', '')
    from_addr = smtp_cfg.get('from_addr') or user
    use_ssl   = smtp_cfg.get('use_ssl', True)

    if not host or not user or not password:
        _logger.error("SMTP配置不完整")
        return False

    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From']    = from_addr
    msg['To']      = to
    msg.attach(MIMEText(body_html, 'html', 'utf-8'))

    try:
        if use_ssl:
            server = smtplib.SMTP_SSL(host, port, timeout=15)
        else:
            server = smtplib.SMTP(host, port, timeout=15)
            server.starttls()
        server.login(user, password)
        server.sendmail(from_addr, [to], msg.as_string())
        server.quit()
        _logger.info("邮件已发送至 %s", to)
        return True
    except Exception as e:
        _logger.error("发送邮件失败: %s", e)
        return False
