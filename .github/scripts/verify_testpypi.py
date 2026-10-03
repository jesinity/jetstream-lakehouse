"""Verify TestPyPI artifacts against this build and download the matching wheel."""

import hashlib
import json
import sys
import time
import zipfile
from email.parser import BytesParser
from pathlib import Path
from urllib.error import URLError
from urllib.parse import quote
from urllib.request import urlopen


def _download_verified_wheel(dist: Path, destination: Path) -> None:
    """Check both distribution hashes, then save the wheel fetched from TestPyPI."""
    wheels = list(dist.glob("*.whl"))
    sdists = list(dist.glob("*.tar.gz"))
    if len(wheels) != 1 or len(sdists) != 1:
        raise SystemExit("Expected exactly one wheel and one source archive")
    wheel = wheels[0]
    with zipfile.ZipFile(wheel) as archive:
        metadata_path = next(name for name in archive.namelist() if name.endswith("/METADATA"))
        metadata = BytesParser().parsebytes(archive.read(metadata_path))
    if metadata["Name"] != "jetstream-lakehouse":
        raise SystemExit("Unexpected package name in the built wheel")
    version = metadata["Version"]
    endpoint = f"https://test.pypi.org/pypi/jetstream-lakehouse/{quote(version, safe='')}/json"
    expected = {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in wheels + sdists}

    # Allow time for the index and file CDN to expose a just-published release.
    for attempt in range(6):
        try:
            with urlopen(endpoint, timeout=10) as response:
                files = {entry["filename"]: entry for entry in json.load(response)["urls"]}
            if expected.keys() <= files.keys():
                for name, digest in expected.items():
                    if files[name]["digests"]["sha256"] != digest:
                        raise SystemExit(
                            f"TestPyPI already has different bytes for {name}; "
                            "bump the version before publishing changed artifacts"
                        )
                    if files[name].get("yanked"):
                        raise SystemExit(f"TestPyPI artifact is yanked: {name}")
                with urlopen(files[wheel.name]["url"], timeout=10) as response:
                    data = response.read()
                if hashlib.sha256(data).hexdigest() != expected[wheel.name]:
                    raise SystemExit("Downloaded TestPyPI wheel does not match the build")
                destination.mkdir(parents=True, exist_ok=True)
                (destination / wheel.name).write_bytes(data)
                print(f"Verified both TestPyPI artifact hashes for version {version}")
                print(f"Downloaded matching wheel: {wheel.name}")
                return
        except (URLError, TimeoutError):
            pass
        if attempt < 5:
            print("Waiting for TestPyPI artifacts to become available...", flush=True)
            time.sleep(10)
    raise SystemExit("TestPyPI artifacts unavailable after retries; production remains blocked")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        raise SystemExit("Usage: verify_testpypi.py DIST_DIRECTORY DOWNLOAD_DIRECTORY")
    _download_verified_wheel(Path(sys.argv[1]), Path(sys.argv[2]))
