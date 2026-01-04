from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
import json
from typing import List, Dict, Any, Optional, Tuple

CONFIG_FILE_DEFAULT = Path.home() / ".merra2_downloader.json"


def _parse_date(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d")


def validate_coords(north: float, south: float, west: float, east: float) -> Tuple[float, float, float, float]:
    if south >= north:
        south, north = min(south, north), max(south, north)
    if west >= east:
        west, east = min(west, east), max(west, east)
    return north, south, west, east


def validate_dates(inicio: str, fin: str) -> Tuple[str, str]:
    d0 = _parse_date(inicio)
    d1 = _parse_date(fin)
    if d1 < d0:
        inicio, fin = fin, inicio
    return inicio, fin


@dataclass(frozen=True)
class Merra2Config:
    north: float = 5.0
    south: float = -20.0
    west: float = -90.0
    east: float = -70.0

    inicio: str = "2023-10-29"
    fin: str = "2024-12-31"

    producto: str = "M2T1NXAER.5.12.4"
    variables: List[str] = None

    directorio: str = str(Path.cwd() / "datos_merra2")
    max_workers: int = 3

    def __post_init__(self):
        n, s, w, e = validate_coords(self.north, self.south, self.west, self.east)
        object.__setattr__(self, "north", n)
        object.__setattr__(self, "south", s)
        object.__setattr__(self, "west", w)
        object.__setattr__(self, "east", e)

        i, f = validate_dates(self.inicio, self.fin)
        object.__setattr__(self, "inicio", i)
        object.__setattr__(self, "fin", f)

        if self.variables is None:
            object.__setattr__(self, "variables", [])

        if not (1 <= self.max_workers <= 32):
            raise ValueError("max_workers debe estar entre 1 y 32")

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "Merra2Config":
        return Merra2Config(**d)


def load_config(path: Optional[str] = None) -> Merra2Config:
    p = Path(path) if path else CONFIG_FILE_DEFAULT
    if p.exists():
        data = json.loads(p.read_text(encoding="utf-8"))
        return Merra2Config.from_dict(data)
    return Merra2Config()


def save_config(config: Merra2Config, path: Optional[str] = None) -> Path:
    p = Path(path) if path else CONFIG_FILE_DEFAULT
    p.write_text(json.dumps(config.to_dict(), indent=2, ensure_ascii=False), encoding="utf-8")
    return p
