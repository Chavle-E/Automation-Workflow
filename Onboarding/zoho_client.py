"""
Minimal Zoho Mail admin client for provisioning (create the work mailbox).

Auth is a Self Client created by the org admin (hello@thirstysprout.ai) at
api-console.zoho.com: a long-lived refresh token (Secret Manager) is exchanged
for a ~1h access token on demand. Scope is ZohoMail.organization.accounts.ALL —
enough to list/create org mailboxes, deliberately nothing wider.

US data center (accounts.zoho.com / mail.zoho.com — where the org lives).
The documented create endpoint is /api/organization/{zoid}/accounts, but the
zoid-less form resolves to the token's own org for both GET and POST (verified
live), which saves needing the organization.READ scope just to look up zoid.
"""
import secrets
import string
import time

import requests

ACCOUNTS_BASE = "https://accounts.zoho.com"
MAIL_BASE = "https://mail.zoho.com"

_PASSWORD_SPECIALS = "!@#$%^&*"


def generate_password(length: int = 16) -> str:
    """Zoho rules: ≥8 chars with upper + lower + digit + special (David's playbook)."""
    alphabet = string.ascii_letters + string.digits + _PASSWORD_SPECIALS
    while True:
        pwd = "".join(secrets.choice(alphabet) for _ in range(length))
        if (any(c.islower() for c in pwd) and any(c.isupper() for c in pwd)
                and any(c.isdigit() for c in pwd) and any(c in _PASSWORD_SPECIALS for c in pwd)):
            return pwd


class ZohoMailClient:
    def __init__(self, client_id: str, client_secret: str, refresh_token: str):
        # .strip(): header/form values reject the trailing newline `echo`-created
        # secret versions carry.
        self.client_id = (client_id or "").strip()
        self.client_secret = (client_secret or "").strip()
        self.refresh_token = (refresh_token or "").strip()
        self._access_token = None
        self._expires_at = 0.0

    def _token(self) -> str:
        if self._access_token and time.time() < self._expires_at - 60:
            return self._access_token
        resp = requests.post(f"{ACCOUNTS_BASE}/oauth/v2/token", data={
            "grant_type": "refresh_token",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "refresh_token": self.refresh_token,
        }, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if "access_token" not in data:
            raise RuntimeError(f"Zoho token refresh failed: {data.get('error', 'no access_token')}")
        self._access_token = data["access_token"]
        self._expires_at = time.time() + int(data.get("expires_in", 3600))
        return self._access_token

    def _headers(self) -> dict:
        return {"Authorization": f"Zoho-oauthtoken {self._token()}"}

    def list_users(self):
        users, start, limit = [], 1, 200
        while True:
            resp = requests.get(f"{MAIL_BASE}/api/organization/accounts",
                                headers=self._headers(),
                                params={"start": start, "limit": limit}, timeout=30)
            resp.raise_for_status()
            batch = resp.json().get("data") or []
            users.extend(batch)
            if len(batch) < limit:
                return users
            start += len(batch)

    def find_user_by_email(self, email: str):
        email = (email or "").lower()
        for user in self.list_users():
            addresses = {
                (user.get("primaryEmailAddress") or "").lower(),
                (user.get("mailboxAddress") or "").lower(),
                (user.get("accountName") or "").lower(),
            }
            for entry in user.get("emailAddress") or []:
                if isinstance(entry, dict):
                    addresses.add((entry.get("mailId") or "").lower())
            if email in addresses:
                return user
        return None

    def create_user(self, email: str, first_name: str, last_name: str, password: str):
        """
        Create the org mailbox. oneTimePassword forces a password change at first
        login (playbook rule). Raises on any non-2xx — the caller falls back to
        the notify-a-human path.
        """
        body = {
            "primaryEmailAddress": email,
            "password": password,
            "firstName": first_name,
            "lastName": last_name,
            "displayName": f"{first_name} {last_name}".strip(),
            "role": "member",  # never admin for onboarded contractors
            "oneTimePassword": True,
        }
        resp = requests.post(f"{MAIL_BASE}/api/organization/accounts",
                             headers=self._headers(), json=body, timeout=60)
        if resp.status_code not in (200, 201):
            raise RuntimeError(f"Zoho create_user failed ({resp.status_code}): {resp.text[:300]}")
        return (resp.json() or {}).get("data") or {}
