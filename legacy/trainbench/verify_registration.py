"""Verify a registered bench dataset entity-by-entity, artifact-by-artifact.

The known registration race leaves artifact nodes that exist but have no structure
(they 500 on any data touch) while len(dataset) still looks right — so counting
entities is not verification. This sweeps every entity (paginated items()), checks
its child artifact names against the expected set, and for a spot subset reads one
value from every artifact through the server.

Usage: verify_registration.py KEY n_entities artifact,names,csv [--spot 199]
Exit 0 = clean; prints a summary line either way; broken entity keys land in
<key>.broken.txt next to the CWD for the resume test to consume.
"""

import argparse
import os
import sys
from concurrent.futures import ThreadPoolExecutor

from tiled.client import from_uri


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("key")
    ap.add_argument("n_entities", type=int)
    ap.add_argument("artifacts", help="comma-separated expected artifact names")
    ap.add_argument("--spot", type=int, default=199,
                    help="deep-read every artifact of every spot-th entity")
    ap.add_argument("--threads", type=int, default=16)
    ap.add_argument("--shared", default="",
                    help="comma-separated shared-axis child names to skip")
    args = ap.parse_args()
    expected = set(args.artifacts.split(","))
    shared = set(args.shared.split(",")) if args.shared else set()

    client = from_uri(os.environ["TILED_URL"], api_key=os.environ.get("TILED_API_KEY"))
    node = client[args.key]

    all_items = list(node.items())  # paginated sweep; metadata rides along
    ents = [(k, v) for k, v in all_items if k not in shared]
    n = len(ents)
    n_shared_found = len(all_items) - n

    def check(i_ent):
        i, (k, ent) = i_ent
        problems = []
        try:
            names = set(ent)
            if names != expected:
                problems.append(f"{k}: children {sorted(names)} != expected")
            elif i % args.spot == 0:
                for a in expected:
                    _ = ent[a].read()  # full artifact through the server
        except Exception as e:
            problems.append(f"{k}: {e!r}")
        return problems

    broken = []
    with ThreadPoolExecutor(max_workers=args.threads) as ex:
        for probs in ex.map(check, enumerate(ents)):
            broken.extend(probs)

    ok = n == args.n_entities and not broken
    print(f"VERIFY {args.key}: entities={n}/{args.n_entities} shared={n_shared_found} "
          f"broken={len(broken)} spot_every={args.spot} -> {'OK' if ok else 'FAIL'}")
    if broken:
        out = f"{args.key}.broken.txt"
        with open(out, "w") as f:
            f.write("\n".join(broken) + "\n")
        print(f"details: {out} (first 5)")
        for b in broken[:5]:
            print(" ", b)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
