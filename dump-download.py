import requests
import os
from datetime import datetime, UTC

# Create data directory if not exists
os.makedirs("data", exist_ok=True)

# Try today's date first
today = datetime.now(UTC).strftime("%Y%m%d")
url_template = "https://dumps.wikimedia.org/wikifunctionswiki/{date}/wikifunctionswiki-{date}-pages-meta-current.xml.bz2"
url = url_template.format(date=today)

local_file = os.path.join("data", f"wikifunctionswiki-{today}-pages-meta-current.xml.bz2")

print(f"Trying to download {url} ...")

response = requests.get(url, stream=True)

if response.status_code == 200:
    with open(local_file, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    print(f"Downloaded to {local_file}")
else:
    print(f"File not found (HTTP {response.status_code})")
