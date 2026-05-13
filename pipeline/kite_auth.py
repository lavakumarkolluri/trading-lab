#!/usr/bin/env python3
"""
kite_auth.py
────────────
Daily Kite Connect authentication helper.
Kite access tokens expire at ~06:00 IST every day.

Usage:
  python kite_auth.py
  # Opens login URL → paste request_token → prints access_token
  # Set the token in the environment before starting intraday_monitor:
  #   export KITE_ACCESS_TOKEN=<token>

Environment:
  KITE_API_KEY    — from Kite Connect developer app
  KITE_API_SECRET — from Kite Connect developer app
"""

import hashlib
import os
import sys

KITE_LOGIN_URL = "https://kite.zerodha.com/connect/login?api_key={api_key}&v=3"


def main():
    api_key    = os.getenv("KITE_API_KEY", "").strip()
    api_secret = os.getenv("KITE_API_SECRET", "").strip()

    if not api_key or not api_secret:
        print("ERROR: Set KITE_API_KEY and KITE_API_SECRET environment variables.")
        sys.exit(1)

    try:
        from kiteconnect import KiteConnect
    except ImportError:
        print("ERROR: kiteconnect not installed. Run: pip install kiteconnect")
        sys.exit(1)

    kite = KiteConnect(api_key=api_key)
    login_url = KITE_LOGIN_URL.format(api_key=api_key)

    print(f"\nStep 1: Open this URL in your browser and log in:\n  {login_url}\n")
    print("Step 2: After login, you'll be redirected to your redirect URL.")
    print("        Copy the 'request_token' from the URL query string.\n")

    request_token = input("Paste request_token here: ").strip()
    if not request_token:
        print("No token provided. Exiting.")
        sys.exit(1)

    try:
        session = kite.generate_session(request_token, api_secret=api_secret)
        access_token = session["access_token"]
        print(f"\n✓ Access token obtained successfully.")
        print(f"\nexport KITE_ACCESS_TOKEN={access_token}")
        print(f"\nAdd to docker-compose .env file:")
        print(f"KITE_ACCESS_TOKEN={access_token}")
    except Exception as e:
        print(f"ERROR generating session: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
