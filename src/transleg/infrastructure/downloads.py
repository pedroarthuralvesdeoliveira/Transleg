from __future__ import annotations

import time
from pathlib import Path


class DownloadWatcher:
    def __init__(self, download_dir: Path) -> None:
        self.download_dir = download_dir
        self._baseline = {path.name for path in download_dir.glob("*")}

    def wait_for_new_file(self, prefix: str, timeout: int) -> Path | None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            files = [
                path
                for path in self.download_dir.glob(f"{prefix}*")
                if path.is_file()
                and path.name not in self._baseline
                and not path.name.endswith(".crdownload")
                and not path.name.endswith(".tmp")
            ]
            if files:
                return max(files, key=lambda item: item.stat().st_mtime)
            time.sleep(1)
        return None

