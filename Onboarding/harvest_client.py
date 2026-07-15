"""
Minimal Harvest v2 client for provisioning (invite user, assign to project).

Mirrors the raw-requests style used by Payroll/Invoicing rather than pulling in a
new dependency. Provisioning-specific concerns:

  - SEAT CAP: the automation can never change the plan's seat count (billing).
    Harvest rejects a user create with 422 when every seat is taken; that is
    surfaced as SeatLimitError so the caller can notify humans instead of failing.
  - Playbook defaults (David): Type=Contractor, capacity 40h/week, Member
    permissions. Billable rate comes from the approval form (NOT Deel) and is set
    as the user's default rate; the project assignment then uses default rates.
"""
import logging

import requests

BASE = "https://api.harvestapp.com/v2"
FORTY_HOURS_SECONDS = 40 * 60 * 60  # weekly_capacity is expressed in seconds

# 422 messages Harvest uses when the plan is out of seats.
_SEAT_LIMIT_MARKERS = ("seat", "upgrade", "plan", "limit")


class SeatLimitError(Exception):
    """Every paid seat is taken — a human must add one before this hire proceeds."""


class HarvestClient:
    def __init__(self, api_key: str, account_id: str):
        # .strip(): header values reject the trailing newline `echo`-created
        # secret versions carry.
        self.headers = {
            "Harvest-Account-Id": str(account_id).strip(),
            "Authorization": f"Bearer {str(api_key).strip()}",
            "User-Agent": "OnboardingProvisioning",
        }

    def _get_paginated(self, path: str, key: str, params=None):
        items, url, params = [], f"{BASE}{path}", dict(params or {})
        while url:
            resp = requests.get(url, headers=self.headers, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            items.extend(data[key])
            url = data.get("links", {}).get("next")
            params = {}
        return items

    def list_projects(self, active_only: bool = True):
        params = {"is_active": "true"} if active_only else None
        return self._get_paginated("/projects", "projects", params)

    def list_users(self, active_only: bool = True):
        params = {"is_active": "true"} if active_only else None
        return self._get_paginated("/users", "users", params)

    def find_user_by_email(self, email: str):
        email = (email or "").lower()
        for user in self.list_users(active_only=False):
            if (user.get("email") or "").lower() == email:
                return user
        return None

    def create_contractor(self, first_name: str, last_name: str, email: str,
                          billable_rate: float, cost_rate=None):
        """
        Create (and thereby invite) a contractor. Raises SeatLimitError when the
        plan has no free seat; re-raises anything else.
        """
        body = {
            "first_name": first_name,
            "last_name": last_name,
            "email": email,
            "is_contractor": True,
            "access_roles": ["member"],
            "weekly_capacity": FORTY_HOURS_SECONDS,
            "default_hourly_rate": float(billable_rate),
        }
        if cost_rate is not None:
            body["cost_rate"] = float(cost_rate)

        resp = requests.post(f"{BASE}/users", headers=self.headers, json=body, timeout=30)
        if resp.status_code == 422:
            message = (resp.json().get("message") or resp.text or "").lower()
            if any(marker in message for marker in _SEAT_LIMIT_MARKERS):
                raise SeatLimitError(message)
        resp.raise_for_status()
        user = resp.json()
        logging.info(f"Harvest user created: {user.get('id')} <{email}>")
        return user

    def assign_user_to_project(self, project_id, user_id):
        """Add the user to the project using their default (approval-form) rates."""
        body = {"user_id": int(user_id), "use_default_rates": True}
        resp = requests.post(f"{BASE}/projects/{project_id}/user_assignments",
                             headers=self.headers, json=body, timeout=30)
        if resp.status_code == 422 and "already" in (resp.text or "").lower():
            logging.info(f"Harvest user {user_id} already assigned to project {project_id}")
            return None
        resp.raise_for_status()
        return resp.json()

    def seat_usage(self):
        """(active_users, note) — for flagging empty seats; Harvest exposes no
        plan-size endpoint, so the cap itself only surfaces via the 422 on create."""
        users = self.list_users(active_only=True)
        return len(users), f"{len(users)} active Harvest users"
