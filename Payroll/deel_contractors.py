# Payroll/find_contracts.py
import os
from dotenv import load_dotenv

load_dotenv(dotenv_path='../.env')

import requests

DEEL_API_KEY = os.getenv('DEEL_API_KEY')
url = "https://api.letsdeel.com/rest/v2/contracts"
headers = {"Authorization": f"Bearer {DEEL_API_KEY}", "accept": "application/json"}

# Search terms
search_names = ['aka', 'dania', 'konstantine', 'kherkeladze', 'butt', 'biganashvili']

response = requests.get(url, headers=headers)
contracts = response.json().get('data', [])

print("=== SEARCHING FOR: Aka, Dania, Konstantine ===\n")

found = []
for c in contracts:
    title = c.get('title', '').lower()
    status = c.get('status', '')
    ctype = c.get('type', '')
    cid = c.get('id', '')

    for name in search_names:
        if name in title:
            found.append(c)
            print(f"✓ {c.get('title')} | ID: {cid} | Type: {ctype} | Status: {status}")
            break

if not found:
    print("No contracts found matching Aka, Dania, or Konstantine")
    print("\n=== ALL IN_PROGRESS CONTRACTS ===\n")
    for c in contracts:
        if c.get('status') == 'in_progress':
            print(f"{c.get('title')} | ID: {c.get('id')} | Type: {c.get('type')}")