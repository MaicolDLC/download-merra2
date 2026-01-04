from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Dict, Any

from .config import Merra2Config


def metadata_dir(output_dir: Path) -> Path:
    return output_dir / "_metadatos"


def metadata_path(file_path: Path) -> Path:
    return metadata_dir(file_path.parent) / f"{file_path.stem}.metadata.json"


def write_metadata(file_path: Path, config: Merra2Config) -> Path:
    md_dir = metadata_dir(file_path.parent)
    md_dir.mkdir(parents=True, exist_ok=True)

    data: Dict[str, Any] = {
        "configuracion": {
            "north": config.north,
            "south": config.south,
            "west": config.west,
            "east": config.east,
            "variables": sorted(config.variables),
            "producto": config.producto,
        },
        "fecha_descarga_utc": datetime.now(timezone.utc).isoformat(),
        "tamaño_archivo": file_path.stat().st_size if file_path.exists() else 0,
        "archivo_original": file_path.name,
    }

    p = metadata_path(file_path)
    p.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    return p


def read_metadata(file_path: Path) -> Optional[Dict[str, Any]]:
    p = metadata_path(file_path)
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


def delete_metadata(file_path: Path) -> None:
    p = metadata_path(file_path)
    try:
        if p.exists():
            p.unlink()
    except Exception:
        pass


def config_matches(file_path: Path, config: Merra2Config) -> bool:
    md = read_metadata(file_path)
    if not md or "configuracion" not in md:
        return False
    c = md["configuracion"]
    return (
        float(c["north"]) == float(config.north)
        and float(c["south"]) == float(config.south)
        and float(c["west"]) == float(config.west)
        and float(c["east"]) == float(config.east)
        and sorted(c["variables"]) == sorted(config.variables)
        and c["producto"] == config.producto
    )


def safe_remove_file_and_metadata(file_path: Path) -> None:
    try:
        if file_path.exists():
            file_path.unlink()
    finally:
        delete_metadata(file_path)
