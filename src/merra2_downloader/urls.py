# src/merra2_downloader/urls.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Sequence

import re
import requests

from .config import Merra2Config

# THREDDS base (NCSS y OPeNDAP viven en el mismo host)
THREDDS_HOST = "https://goldsmr4.gesdisc.eosdis.nasa.gov/thredds"
DEFAULT_ACCEPT = "netcdf4-classic"

# Variables típicas que NO queremos pedir como "var="
# (coordenadas / dims / helpers)
EXCLUDE_VARS = {
    "time", "lat", "lon", "lev", "level",
    "latitude", "longitude",
    "crs", "Lambert_Conformal", "projection",
}


def merra2_bloque(year: int) -> int:
    """Bloque MERRA2 por año (100/200/300/400)."""
    if 1980 <= year <= 1991:
        return 100
    if 1992 <= year <= 2000:
        return 200
    if 2001 <= year <= 2010:
        return 300
    return 400


def _shortname(producto: str) -> str:
    """
    producto puede venir como 'M2T1NXAER.5.12.4'.
    Retornamos el ShortName base: 'M2T1NXAER'
    """
    return producto.split(".")[0].strip()


def collection_from_esdt(producto: str) -> str:
    """
    Deriva el nombre de colección (p.ej. 'tavg1_2d_aer_Nx') desde el ESDT M2TFHVGGG.
    - ESDT: M2TFHVGGG :contentReference[oaicite:3]{index=3}
    - collection name: freq_dims_group_HV :contentReference[oaicite:4]{index=4}
    """
    esdt = _shortname(producto)
    if len(esdt) != 9 or not esdt.startswith("M2"):
        raise ValueError(f"ESDT inválido: {esdt} (esperaba algo tipo M2T1NXAER)")

    T = esdt[2]  # I/T/C/S
    F = esdt[3]  # 1/3/6/M/D/U/0
    H = esdt[4]  # N
    V = esdt[5]  # X/P/V/E
    GGG = esdt[6:9]  # AER, FLX, SLV, ...

    time_map = {"I": "inst", "T": "tavg", "C": "const", "S": "stat"}
    if T not in time_map:
        raise ValueError(f"ESDT '{esdt}': T desconocido '{T}'")

    # dims: si V es X => 2D; si no => 3D
    dims = "2d" if V.upper() == "X" else "3d"

    # group: minúscula
    group = GGG.lower()

    # HV: H + v_minúscula (ej: N + x => Nx, N + p => Np)
    hv = f"{H}{V.lower()}"

    # freq: se mantiene tal cual en el nombre de colección (ej: tavg1, inst3, const_2d..., etc.)
    # para const suele ser const_... (en doc aparece const_2d_*), pero el ESDT trae F=0.
    # Aquí lo dejamos consistente: const0_... NO existe, así que si es const, omitimos F.
    time_prefix = time_map[T]
    if time_prefix == "const":
        return f"const_{dims}_{group}_{hv}"
    if time_prefix == "stat":
        return f"stat{F}_{dims}_{group}_{hv}"
    return f"{time_prefix}{F}_{dims}_{group}_{hv}"


def filename_for_date(producto: str, yyyymmdd: str) -> str:
    """
    Nombre de archivo: MERRA2_{bloque}.{collection}.{yyyymmdd}.nc4
    (runid.collection.timestamp) :contentReference[oaicite:5]{index=5}
    """
    year = int(yyyymmdd[:4])
    bloque = merra2_bloque(year)
    collection = collection_from_esdt(producto)
    return f"MERRA2_{bloque}.{collection}.{yyyymmdd}.nc4"


def _dds_url(producto: str, y: str, m: str, fname: str) -> str:
    # OPeNDAP dataset descriptor: .../dodsC/<producto>/<y>/<m>/<fname>.dds
    return f"{THREDDS_HOST}/dodsC/{producto}/{y}/{m}/{fname}.dds"


_DDS_VAR_RE = re.compile(
    r"^\s*(?:Byte|Int16|UInt16|Int32|UInt32|Int64|UInt64|Float32|Float64|String)\s+"
    r"([A-Za-z_][A-Za-z0-9_]*)\s*(?:\[[^\]]+\]\s*)*;",
    re.MULTILINE,
)

def variables_from_dds(producto: str, y: str, m: str, yyyymmdd: str) -> List[str]:
    fname = filename_for_date(producto, yyyymmdd)
    url = _dds_url(producto, y, m, fname)

    s = requests.Session()
    s.trust_env = True
    r = s.get(url, timeout=60)

    if r.status_code != 200:
        raise RuntimeError(
            f"No pude leer DDS (HTTP {r.status_code}). URL: {url}\n"
            "Revisa tu .netrc y que exista ese día."
        )

    text = r.text

    vars_found = []
    for name in _DDS_VAR_RE.findall(text):
        low = name.lower()
        if low in EXCLUDE_VARS or name in EXCLUDE_VARS:
            continue
        vars_found.append(name)

    # únicos preservando orden
    out, seen = [], set()
    for v in vars_found:
        if v not in seen:
            seen.add(v)
            out.append(v)

    if not out:
        head = "\n".join(text.splitlines()[:60])
        raise RuntimeError(
            "Pude leer el DDS pero no extraje variables.\n"
            "Primeras 60 líneas del DDS:\n"
            f"{head}"
        )
    return out



def resolve_variables(config: Merra2Config) -> List[str]:
    """
    - Si el usuario pasó variables, se usan.
    - Si variables está vacío, consultamos el servidor y usamos todas las variables del dataset para la fecha inicio.
    """
    if config.variables and len(config.variables) > 0:
        return list(config.variables)

    d0 = datetime.strptime(config.inicio, "%Y-%m-%d")
    y = d0.strftime("%Y")
    m = d0.strftime("%m")
    yyyymmdd = d0.strftime("%Y%m%d")

    return variables_from_dds(config.producto, y=y, m=m, yyyymmdd=yyyymmdd)


def generar_urls_merra_rango(config: Merra2Config) -> List[str]:
    d0 = datetime.strptime(config.inicio, "%Y-%m-%d")
    d1 = datetime.strptime(config.fin, "%Y-%m-%d")

    # ✅ aquí está la magia: si variables=[] => se detectan todas para ESTE producto
    vars_list = resolve_variables(config)

    urls: List[str] = []
    for i in range((d1 - d0).days + 1):
        d = d0 + timedelta(days=i)
        y = d.strftime("%Y")
        m = d.strftime("%m")
        dd = d.strftime("%d")
        yyyymmdd = d.strftime("%Y%m%d")

        fname = filename_for_date(config.producto, yyyymmdd)
        base_url = f"{THREDDS_HOST}/ncss/grid/{config.producto}/{y}/{m}/{fname}"

        params = [f"var={v}" for v in vars_list]
        params += [
            f"north={config.north}",
            f"west={config.west}",
            f"east={config.east}",
            f"south={config.south}",
            "horizStride=1",
            f"time_start={y}-{m}-{dd}T00:30:00Z",
            f"time_end={y}-{m}-{dd}T23:30:00Z",
            f"accept={DEFAULT_ACCEPT}",
        ]
        query = "&".join(params)
        urls.append(f"{base_url}?{query}")

    return urls


__all__ = [
    "merra2_bloque",
    "collection_from_esdt",
    "filename_for_date",
    "variables_from_dds",
    "resolve_variables",
    "generar_urls_merra_rango",
]



