"""Delete regbench_* dataset keys from the remote server (post-run cleanup).

Only touches keys matching the regbench_ prefix — the harness's throwaway,
timestamped dataset keys. Verifies none remain. Env: TILED_URL, TILED_API_KEY.
"""
import os
import sys

from tiled.client import from_uri

client = from_uri(os.environ["TILED_URL"], api_key=os.environ["TILED_API_KEY"])
keys = [k for k in client.keys() if k.startswith("regbench_")]
print(f"[cleanup] {len(keys)} regbench_* datasets on {os.environ['TILED_URL']}")
for k in keys:
    client.delete_contents(k, recursive=True)
    print(f"[cleanup] deleted {k}")

left = [k for k in client.keys() if k.startswith("regbench_")]
if left:
    print(f"[cleanup] WARNING: {len(left)} keys remain: {left[:10]}")
    sys.exit(1)
print("[cleanup] clean")
