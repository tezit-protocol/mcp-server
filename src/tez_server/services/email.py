"""Email service for Tez sharing notifications via SendGrid."""

from __future__ import annotations

from typing import Protocol

from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import (
    Content,
    From,
    Mail,
    Subject,
    To,
)


class EmailClient(Protocol):
    """Protocol for sending emails, enabling test doubles."""

    def send(self, message: Mail) -> object: ...


class EmailService:
    """Sends sharing notification emails via SendGrid."""

    def __init__(
        self,
        client: EmailClient,
        from_email: str = "noreply@tezit.com",
    ) -> None:
        self._client = client
        self._from_email = from_email

    @classmethod
    def from_api_key(
        cls,
        api_key: str,
        from_email: str = "noreply@tezit.com",
    ) -> EmailService:
        """Create an EmailService with a real SendGrid client."""
        return cls(client=SendGridAPIClient(api_key), from_email=from_email)

    def send_share_notification(
        self,
        *,
        recipient_email: str,
        sharer_name: str,
        tez_name: str,
        tez_id: str,
        message: str | None = None,
    ) -> int:
        """Send a notification that a Tez has been shared.

        Returns the HTTP status code from SendGrid.
        """
        mail = Mail()
        mail.from_email = From(self._from_email, "Tez")
        mail.to = To(recipient_email)
        mail.subject = Subject(f"{sharer_name} shared a Tez with you")

        plain = build_plain_text(
            sharer_name=sharer_name,
            tez_name=tez_name,
            tez_id=tez_id,
            message=message,
        )
        html = build_html(
            sharer_name=sharer_name,
            tez_name=tez_name,
            tez_id=tez_id,
            message=message,
        )

        mail.add_content(Content("text/plain", plain))
        mail.add_content(Content("text/html", html))

        response = self._client.send(mail)
        return response.status_code  # type: ignore[attr-defined, no-any-return]


def build_plain_text(
    *,
    sharer_name: str,
    tez_name: str,
    tez_id: str,
    message: str | None,
) -> str:
    lines = [
        f"{sharer_name} shared a Tez with you.",
        "",
        f"  Name: {tez_name}",
        f"  ID:   {tez_id}",
    ]
    if message:
        lines += ["", f'"{message}"']
    lines += [
        "",
        f"Open in browser: https://tez.it/{tez_id}",
        "",
        "Or retrieve via CLI:",
        f"  tez download {tez_id}",
        "",
        "---",
        "Tez - scoped, shareable context packages.",
    ]
    return "\n".join(lines)


def build_html(
    *,
    sharer_name: str,
    tez_name: str,
    tez_id: str,
    message: str | None,
) -> str:
    message_block = ""
    if message:
        message_block = f"""
            <tr>
              <td style="padding: 16px 24px 0;">
                <p style="margin: 0; padding: 12px 16px; background: #faf5ff; border-left: 3px solid #8b5cf6; border-radius: 0 6px 6px 0; color: #4c1d95; font-size: 14px; font-style: italic;">
                  "{message}"
                </p>
              </td>
            </tr>"""

    tez_url = f"https://tez.it/{tez_id}"

    return f"""\
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
</head>
<body style="margin: 0; padding: 0; background-color: #f0f0f5; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;">
  <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="padding: 40px 0;">
    <tr>
      <td align="center">
        <table role="presentation" width="480" cellpadding="0" cellspacing="0" style="background: #ffffff; border-radius: 12px; overflow: hidden; box-shadow: 0 2px 8px rgba(0,0,0,0.06);">

          <!-- Colour bar -->
          <tr>
            <td style="height: 4px; background: linear-gradient(90deg, #6366f1, #8b5cf6, #a78bfa);"></td>
          </tr>

          <!-- Header -->
          <tr>
            <td style="padding: 32px 24px 8px; text-align: center;">
              <p style="margin: 0 0 8px; font-size: 14px; color: #6b7280;">
                <strong style="color: #4f46e5;">{sharer_name}</strong> shared a Tez with you
              </p>
            </td>
          </tr>

          <!-- Tez details card -->
          <tr>
            <td style="padding: 8px 24px 0;">
              <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background: linear-gradient(135deg, #eef2ff, #f5f3ff); border-radius: 8px; border: 1px solid #e0e7ff;">
                <tr>
                  <td style="padding: 20px;">
                    <p style="margin: 0 0 6px; font-size: 18px; font-weight: 700; color: #1e1b4b;">
                      {tez_name}
                    </p>
                    <p style="margin: 0; font-size: 13px; color: #6366f1; font-family: monospace;">
                      {tez_id}
                    </p>
                  </td>
                </tr>
              </table>
            </td>
          </tr>

          <!-- Optional message -->{message_block}

          <!-- CTA button -->
          <tr>
            <td style="padding: 24px; text-align: center;">
              <a href="{tez_url}" style="display: inline-block; padding: 12px 32px; background: #4f46e5; color: #ffffff; text-decoration: none; border-radius: 6px; font-size: 14px; font-weight: 600;">
                Open Tez
              </a>
            </td>
          </tr>

          <!-- CLI alternative -->
          <tr>
            <td style="padding: 0 24px 24px;">
              <p style="margin: 0 0 8px; font-size: 13px; color: #6b7280; text-align: center;">
                Or via CLI:
              </p>
              <code style="display: block; padding: 10px 16px; background: #1e1b4b; color: #c7d2fe; border-radius: 6px; font-size: 13px; text-align: center;">
                tez download {tez_id}
              </code>
            </td>
          </tr>

          <!-- Footer -->
          <tr>
            <td style="padding: 16px 24px; border-top: 1px solid #e5e7eb; text-align: center;">
              <p style="margin: 0; font-size: 12px; color: #9ca3af;">
                <strong style="color: #6366f1;">tez</strong> &mdash; scoped, shareable context packages
              </p>
            </td>
          </tr>

        </table>
      </td>
    </tr>
  </table>
</body>
</html>"""
