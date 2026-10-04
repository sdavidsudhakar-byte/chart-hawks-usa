"""
notifier.py — Send email summary after price refresh + scanner run.

Uses Gmail SMTP with an App Password (set GMAIL_APP_PASSWORD in config.py
or as a GitHub Actions secret).  The sender is smartgeniebot@gmail.com.
"""

import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

logger = logging.getLogger(__name__)

RECIPIENT = "smartgeniebot@gmail.com"
SENDER    = "smartgeniebot@gmail.com"


def _get_credentials() -> tuple[str, str] | tuple[None, None]:
    try:
        import config
        pwd = getattr(config, "GMAIL_APP_PASSWORD", None)
        if pwd and pwd.strip():
            return SENDER, pwd.strip()
    except ImportError:
        pass
    import os
    pwd = os.getenv("GMAIL_APP_PASSWORD", "")
    if pwd.strip():
        return SENDER, pwd.strip()
    return None, None


def send_summary(
    price_total: int,
    price_success: int,
    price_failed: list[str],
    scanner_result: dict | None,
):
    """
    Send a daily summary email to RECIPIENT.

    price_total   — total symbols attempted
    price_success — symbols with data loaded
    price_failed  — list of symbol strings that had no data
    scanner_result — dict returned by scanner.run_full_scan(), or None if it didn't run
    """
    sender, password = _get_credentials()
    if not sender:
        logger.warning("GMAIL_APP_PASSWORD not set — skipping summary email")
        return

    failed_count = len(price_failed)
    subject = (
        f"Daily NSE Data Update — {price_success}/{price_total} loaded"
        + (f", {failed_count} failed" if failed_count else "")
    )

    # ── Build HTML body ───────────────────────────────────────────────────────
    failed_section = ""
    if price_failed:
        symbols_html = ", ".join(price_failed[:50])
        if len(price_failed) > 50:
            symbols_html += f" … and {len(price_failed) - 50} more"
        failed_section = f"""
        <tr>
          <td style="padding:6px 12px;color:#94a3b8;">Failed symbols</td>
          <td style="padding:6px 12px;color:#f87171;font-size:.85em;">{symbols_html}</td>
        </tr>"""

    scanner_section = ""
    if scanner_result:
        ok        = scanner_result.get("ok", False)
        scanned   = scanner_result.get("scanned", 0)
        hits_bull = scanner_result.get("hits_bull", 0)
        hits_bear = scanner_result.get("hits_bear", 0)
        errors    = scanner_result.get("errors", 0)
        status_color = "#4ade80" if ok else "#f87171"
        status_text  = "Done" if ok else "Error"
        scanner_section = f"""
        <tr><td colspan="2" style="padding:14px 12px 4px;color:#94a3b8;font-weight:600;border-top:1px solid #1e293b;">
          Daily Hunt Results
        </td></tr>
        <tr>
          <td style="padding:6px 12px;color:#94a3b8;">Status</td>
          <td style="padding:6px 12px;color:{status_color};">{status_text}</td>
        </tr>
        <tr>
          <td style="padding:6px 12px;color:#94a3b8;">Symbols scanned</td>
          <td style="padding:6px 12px;color:#e2e8f0;">{scanned:,}</td>
        </tr>
        <tr>
          <td style="padding:6px 12px;color:#94a3b8;">Bull hits</td>
          <td style="padding:6px 12px;color:#4ade80;">{hits_bull}</td>
        </tr>
        <tr>
          <td style="padding:6px 12px;color:#94a3b8;">Bear hits</td>
          <td style="padding:6px 12px;color:#f87171;">{hits_bear}</td>
        </tr>"""
        if errors:
            scanner_section += f"""
        <tr>
          <td style="padding:6px 12px;color:#94a3b8;">Errors</td>
          <td style="padding:6px 12px;color:#f87171;">{errors}</td>
        </tr>"""
    else:
        scanner_section = """
        <tr><td colspan="2" style="padding:14px 12px 4px;color:#94a3b8;border-top:1px solid #1e293b;">
          Scanner did not run (price refresh may have failed).
        </td></tr>"""

    html = f"""<!DOCTYPE html>
<html>
<body style="margin:0;padding:0;background:#0f172a;font-family:system-ui,sans-serif;color:#e2e8f0;">
  <div style="max-width:520px;margin:32px auto;background:#1e293b;border-radius:10px;overflow:hidden;border:1px solid #334155;">
    <div style="background:#0f172a;padding:18px 24px;border-bottom:1px solid #334155;">
      <span style="font-size:1.1rem;font-weight:700;color:#60a5fa;">NSE Daily Update</span>
    </div>
    <div style="padding:8px 0;">
      <table style="width:100%;border-collapse:collapse;">
        <tr><td colspan="2" style="padding:14px 12px 4px;color:#94a3b8;font-weight:600;">
          Price Data Load
        </td></tr>
        <tr>
          <td style="padding:6px 12px;color:#94a3b8;">Total symbols</td>
          <td style="padding:6px 12px;color:#e2e8f0;">{price_total:,}</td>
        </tr>
        <tr>
          <td style="padding:6px 12px;color:#94a3b8;">Loaded</td>
          <td style="padding:6px 12px;color:#4ade80;">{price_success:,}</td>
        </tr>
        <tr>
          <td style="padding:6px 12px;color:#94a3b8;">Failed</td>
          <td style="padding:6px 12px;color:{'#f87171' if failed_count else '#4ade80'};">{failed_count}</td>
        </tr>
        {failed_section}
        {scanner_section}
      </table>
    </div>
    <div style="padding:12px 24px;border-top:1px solid #334155;font-size:.75rem;color:#475569;">
      Sent by ChartHawks · smartgeniebot@gmail.com
    </div>
  </div>
</body>
</html>"""

    # ── Send via Gmail SMTP ───────────────────────────────────────────────────
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = sender
    msg["To"]      = RECIPIENT
    msg.attach(MIMEText(html, "html"))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=30) as smtp:
            smtp.login(sender, password)
            smtp.sendmail(sender, RECIPIENT, msg.as_string())
        logger.info("Summary email sent to %s", RECIPIENT)
    except Exception as e:
        logger.error("Failed to send summary email: %s", e)
