from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    """运行时配置。均可通过环境变量覆盖。"""

    db_path: str
    window_size_seconds: float = 60.0
    allowed_lateness_seconds: float = 30.0

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            db_path=os.getenv("DB_PATH", "telemetry.db"),
            window_size_seconds=float(os.getenv("WINDOW_SIZE_SECONDS", "60")),
            allowed_lateness_seconds=float(os.getenv("ALLOWED_LATENESS_SECONDS", "30")),
        )
