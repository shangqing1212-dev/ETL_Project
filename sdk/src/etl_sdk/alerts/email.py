"""邮件通道: 标准库 smtplib,465=SMTP_SSL,587=STARTTLS。"""

from __future__ import annotations

import smtplib
from email.message import EmailMessage

from etl_sdk.alerts.base import AlertChannel


class EmailChannel(AlertChannel):
    name = "email"

    def __init__(
        self,
        *,
        host: str,
        port: int = 465,
        user: str = "",
        password: str = "",
        sender: str,
        recipients: list[str],
    ) -> None:
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._sender = sender
        self._recipients = recipients

    def send(self, title: str, text: str, *, level: str) -> None:
        msg = EmailMessage()
        msg["Subject"] = f"[ETL:{level.upper()}] {title}"
        msg["From"] = self._sender
        msg["To"] = ", ".join(self._recipients)
        msg.set_content(text)
        # 465 走隐式 SSL;587 先建明文连接再 STARTTLS(端口即模式,不再另设开关)
        if self._port == 587:
            with smtplib.SMTP(self._host, self._port, timeout=15) as smtp:
                smtp.starttls()
                self._login_and_send(smtp, msg)
        else:
            with smtplib.SMTP_SSL(self._host, self._port, timeout=15) as smtp:
                self._login_and_send(smtp, msg)

    def _login_and_send(self, smtp: smtplib.SMTP, msg: EmailMessage) -> None:
        if self._user:
            smtp.login(self._user, self._password)
        smtp.send_message(msg)
