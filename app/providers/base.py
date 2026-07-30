from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol


@dataclass
class ProviderSyncResult:
    provider_name: str
    synced_count: int
    updated_count: int
    skipped_count: int
    errors: list[str]


class ProviderAdapter(Protocol):
    name: str

    def fetch_updated_since(self, cursor: str | None = None) -> tuple[list[dict[str, Any]], str | None]:
        """Return a batch of raw provider records and a next cursor."""

    def normalize(self, raw_listing: dict[str, Any]) -> dict[str, Any]:
        """Normalize a raw provider listing into the internal listing shape."""
