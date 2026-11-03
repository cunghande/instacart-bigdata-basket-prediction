"""Download the six original Instacart CSV files and verify Zenodo MD5 hashes."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from urllib.request import urlopen

RECORD_API = "https://zenodo.org/api/records/2654444"
FILES = {
    "aisles.csv",
    "departments.csv",
    "products.csv",
    "orders.csv",
    "order_products__prior.csv",
    "order_products__train.csv",
}


def md5_file(path: Path) -> str:
    digest = hashlib.md5()  # nosec: compare against the dataset publisher's checksum
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def download(destination: Path) -> None:
    destination.mkdir(parents=True, exist_ok=True)
    with urlopen(RECORD_API, timeout=60) as response:
        metadata = json.load(response)
    files = {item["key"]: item for item in metadata["files"]}
    if not FILES.issubset(files):
        raise RuntimeError(f"Missing files in Zenodo record: {FILES - files.keys()}")

    for name in sorted(FILES):
        item = files[name]
        expected = item["checksum"].split(":", 1)[-1]
        target = destination / name
        if target.exists() and md5_file(target) == expected:
            print(f"OK {name} (already verified)", flush=True)
            continue
        url = item["links"].get("self") or item["links"].get("content")
        if not url:
            raise RuntimeError(f"No download URL for {name}")
        partial = target.with_suffix(target.suffix + ".partial")
        print(f"Downloading {name} ({item['size'] / 1_000_000:.1f} MB)", flush=True)
        try:
            with urlopen(url, timeout=120) as response, partial.open("wb") as output:
                for block in iter(lambda: response.read(1024 * 1024), b""):
                    output.write(block)
            if md5_file(partial) != expected:
                raise RuntimeError(f"Checksum mismatch: {name}")
            partial.replace(target)
            print(f"OK {name}", flush=True)
        finally:
            partial.unlink(missing_ok=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("data/raw"))
    args = parser.parse_args()
    download(args.output)
