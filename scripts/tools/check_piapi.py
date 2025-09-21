# -*- coding: utf-8 -*-
import os
import requests

def main():
    api_key = os.environ.get("PIAPI_API_KEY", "").strip()
    if not api_key:
        raise SystemExit("PIAPI_API_KEY ausente. Configure o Secret no GitHub.")
    url = "https://api.piapi.ai/account/info"
    r = requests.get(url, headers={"Authorization": f"Bearer {api_key}"}, timeout=30)
    r.raise_for_status()
    print("PiAPI OK:", r.json())

if __name__ == "__main__":
    main()
