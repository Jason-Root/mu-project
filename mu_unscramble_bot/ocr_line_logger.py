from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import re
import threading
import time

from mu_unscramble_bot.models import normalize_lookup_text, normalize_spacing


CSV_FIELDS = (
    "logged_at",
    "line_text",
    "normalized_text",
    "contains_coordinates",
    "coordinates",
    "region_left",
    "region_top",
    "region_width",
    "region_height",
)

COORDINATE_PATTERN = re.compile(r"\b\d{1,3}\s*,\s*\d{1,3}\b")


@dataclass(slots=True)
class OCRLineRecord:
    logged_at: str
    line_text: str
    normalized_text: str
    contains_coordinates: bool
    coordinates: str
    region_left: int
    region_top: int
    region_width: int
    region_height: int

    def to_row(self) -> dict[str, str]:
        return {
            "logged_at": self.logged_at,
            "line_text": self.line_text,
            "normalized_text": self.normalized_text,
            "contains_coordinates": "true" if self.contains_coordinates else "false",
            "coordinates": self.coordinates,
            "region_left": str(self.region_left),
            "region_top": str(self.region_top),
            "region_width": str(self.region_width),
            "region_height": str(self.region_height),
        }


class OCRLineLogger:
    def __init__(
        self,
        path: str | Path,
        *,
        enabled: bool = True,
        dedupe_seconds: float = 10.0,
    ) -> None:
        self.path = Path(path)
        self.enabled = enabled
        self.dedupe_seconds = max(0.0, float(dedupe_seconds))
        self._lock = threading.Lock()
        self._last_logged_at_by_key: dict[str, float] = {}

    def log_lines(self, lines: list[str], region: dict[str, int]) -> int:
        if not self.enabled or not lines:
            return 0

        now = time.monotonic()
        timestamp = datetime.now().isoformat(timespec="seconds")
        new_records: list[OCRLineRecord] = []
        batch_keys: set[str] = set()

        for raw_line in lines:
            line_text = normalize_spacing(raw_line)
            if not line_text:
                continue

            normalized_text = normalize_lookup_text(line_text)
            dedupe_key = normalized_text or line_text.casefold()
            if not dedupe_key or dedupe_key in batch_keys:
                continue

            last_logged_at = self._last_logged_at_by_key.get(dedupe_key)
            if last_logged_at is not None and (now - last_logged_at) < self.dedupe_seconds:
                continue

            batch_keys.add(dedupe_key)
            coordinates = "; ".join(match.group(0).replace(" ", "") for match in COORDINATE_PATTERN.finditer(line_text))
            new_records.append(
                OCRLineRecord(
                    logged_at=timestamp,
                    line_text=line_text,
                    normalized_text=normalized_text,
                    contains_coordinates=bool(coordinates),
                    coordinates=coordinates,
                    region_left=int(region.get("left", 0)),
                    region_top=int(region.get("top", 0)),
                    region_width=int(region.get("width", 0)),
                    region_height=int(region.get("height", 0)),
                )
            )

        if not new_records:
            self._prune_recent_cache(now)
            return 0

        with self._lock:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            write_header = not self.path.exists() or self.path.stat().st_size == 0
            with self.path.open("a", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS)
                if write_header:
                    writer.writeheader()
                for record in new_records:
                    writer.writerow(record.to_row())
                    self._last_logged_at_by_key[record.normalized_text or record.line_text.casefold()] = now

        self._prune_recent_cache(now)
        return len(new_records)

    def _prune_recent_cache(self, now: float) -> None:
        if not self._last_logged_at_by_key:
            return
        cutoff = self.dedupe_seconds * 4 if self.dedupe_seconds > 0 else 0.0
        if cutoff <= 0:
            self._last_logged_at_by_key.clear()
            return
        stale_keys = [key for key, logged_at in self._last_logged_at_by_key.items() if (now - logged_at) > cutoff]
        for key in stale_keys:
            self._last_logged_at_by_key.pop(key, None)
