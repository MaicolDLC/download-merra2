# src/merra2_downloader/urls.py
from __future__ import annotations

from datetime import datetime, timedelta
from typing import List, Tuple, Iterable
import re
import requests

from .config import Merra2Config

THREDDS_HOST = "https://goldsmr4.gesdisc.eosdis.nasa.gov/thredds"
DEFAULT_ACCEPT = "netcdf4-classic"

EXCLUDE_VARS = {
    "time", "lat", "lon", "lev", "level",
    "latitude", "longitude",
    "crs", "Lambert_Conformal", "projection",
}

# ----------------------------
# ESDT helpers
# ----------------------------

def merra2_bloque(year: int) -> int:
    if 1980 <= year <= 1991:
        return 100
    if 1992 <= year <= 2000:
        return 200
    if 2001 <= year <= 2010:
        return 300
    return 400


def _shortname(producto: str) -> str:
    return producto.split(".")[0].strip()


def _esdt_parts(producto: str) -> Tuple[str, str, str, str, str]:
    esdt = _shortname(producto)
    if len(esdt) != 9 or not esdt.startswith("M2"):
        raise ValueError(f"ESDT inválido: {esdt} (esperaba algo tipo M2T1NXAER)")
    T = esdt[2]   # I/T/C/S
    F = esdt[3]   # 1/3/6/M/D/U/0
    H = esdt[4]   # N
    V = esdt[5]   # X/P/V/E
    GGG = esdt[6:9]
    return T, F, H, V, GGG


def collection_from_esdt(producto: str) -> str:
    T, F, H, V, GGG = _esdt_parts(producto)

    time_map = {"I": "inst", "T": "tavg", "C": "const", "S": "stat"}
    if T not in time_map:
        raise ValueError(f"ESDT '{_shortname(producto)}': T desconocido '{T}'")

    dims = "2d" if V.upper() == "X" else "3d"
    group = GGG.lower()
    hv = f"{H}{V.lower()}"

    prefix = time_map[T]
    if prefix == "const":
        return f"const_{dims}_{group}_{hv}"
    if prefix == "stat":
        return f"stat{F}_{dims}_{group}_{hv}"
    return f"{prefix}{F}_{dims}_{group}_{hv}"


def _freq_code(producto: str) -> str:
    _, F, _, _, _ = _esdt_parts(producto)
    return F


def _root_folder(producto: str) -> str:
    """
    SOLO 3 carpetas:
      - F=U => MERRA2_DIURNAL
      - F=M => MERRA2_MONTHLY
      - resto => MERRA2
    """
    F = _freq_code(producto)
    if F == "U":
        return "MERRA2_DIURNAL"
    if F == "M":
        return "MERRA2_MONTHLY"
    return "MERRA2"


def _timestamp_for_period(producto: str, d: datetime) -> str:
    """
    - diario/horario: yyyymmdd
    - mensual (F=M) y diurnal (F=U): yyyymm
    """
    F = _freq_code(producto)
    if F in ("M", "U"):
        return d.strftime("%Y%m")
    return d.strftime("%Y%m%d")


def filename_for_date(producto: str, d: datetime) -> str:
    runid = merra2_bloque(d.year)
    collection = collection_from_esdt(producto)
    stamp = _timestamp_for_period(producto, d)
    return f"MERRA2_{runid}.{collection}.{stamp}.nc4"


# ----------------------------
# Period iterator
# ----------------------------

def _iter_days(d0: datetime, d1: datetime) -> Iterable[datetime]:
    n = (d1 - d0).days
    for i in range(n + 1):
        yield d0 + timedelta(days=i)


def _iter_months(d0: datetime, d1: datetime) -> Iterable[datetime]:
    y, m = d0.year, d0.month
    end_y, end_m = d1.year, d1.month
    while (y < end_y) or (y == end_y and m <= end_m):
        yield datetime(y, m, 1)
        m += 1
        if m == 13:
            m = 1
            y += 1


def iter_periods(config: Merra2Config) -> List[datetime]:
    d0 = datetime.strptime(config.inicio, "%Y-%m-%d")
    d1 = datetime.strptime(config.fin, "%Y-%m-%d")
    F = _freq_code(config.producto)
    if F in ("M", "U"):
        return list(_iter_months(d0, d1))
    return list(_iter_days(d0, d1))


# ----------------------------
# DDS var discovery
# ----------------------------

_DDS_FLAT_VAR_RE = re.compile(
    r"^\s*(Byte|Int16|Int32|Int64|Float32|Float64|String)\s+([A-Za-z0-9_]+)\s*(\[[^\]]+\])?\s*;",
    re.MULTILINE,
)

_DDS_GRID_NAME_RE = re.compile(
    r"\}\s*([A-Za-z0-9_]+)\s*;",
    re.MULTILINE,
)


def _parse_vars_from_dds(text: str) -> List[str]:
    vars_found: List[str] = []

    # Grid-style: } VAR;
    for name in _DDS_GRID_NAME_RE.findall(text):
        if name not in EXCLUDE_VARS:
            vars_found.append(name)

    # Flat-style: Float32 VAR[...];
    for _typ, name, _dims in _DDS_FLAT_VAR_RE.findall(text):
        if name not in EXCLUDE_VARS:
            vars_found.append(name)

    seen = set()
    out: List[str] = []
    for v in vars_found:
        if v not in seen:
            seen.add(v)
            out.append(v)
    return out


def variables_from_dds(producto: str, d: datetime) -> List[str]:
    root = _root_folder(producto)
    fname = filename_for_date(producto, d)
    y = d.strftime("%Y")
    m = d.strftime("%m")

    url = f"{THREDDS_HOST}/dodsC/{root}/{producto}/{y}/{m}/{fname}.dds"

    s = requests.Session()
    s.trust_env = True  # usa ~/.netrc
    r = s.get(url, timeout=60)

    if r.status_code != 200:
        raise RuntimeError(
            f"No pude leer DDS para listar variables (HTTP {r.status_code}).\n"
            f"URL: {url}\n"
            "Verifica que exista el granule para ese periodo y que tu .netrc esté activo."
        )

    out = _parse_vars_from_dds(r.text)
    if not out:
        head = "\n".join(r.text.splitlines()[:80])
        raise RuntimeError(
            "Pude leer el DDS pero no extraje variables.\n"
            "Primeras 80 líneas del DDS:\n"
            f"{head}"
        )
    return out


def resolve_variables(config: Merra2Config) -> List[str]:
    if config.variables and len(config.variables) > 0:
        return list(config.variables)

    periods = iter_periods(config)
    return variables_from_dds(config.producto, periods[0])


# ----------------------------
# URL generator (NCSS)
# ----------------------------

def generar_urls_merra_rango(config: Merra2Config) -> List[str]:
    root = _root_folder(config.producto)
    vars_list = resolve_variables(config)
    periods = iter_periods(config)

    urls: List[str] = []
    for d in periods:
        fname = filename_for_date(config.producto, d)
        y = d.strftime("%Y")
        m = d.strftime("%m")

        base_url = f"{THREDDS_HOST}/ncss/grid/{root}/{config.producto}/{y}/{m}/{fname}"

        params = [f"var={v}" for v in vars_list]
        params += [
            f"north={config.north}",
            f"west={config.west}",
            f"east={config.east}",
            f"south={config.south}",
            "horizStride=1",
            f"accept={DEFAULT_ACCEPT}",
        ]
        urls.append(f"{base_url}?{'&'.join(params)}")

    return urls


__all__ = [
    "merra2_bloque",
    "collection_from_esdt",
    "filename_for_date",
    "variables_from_dds",
    "resolve_variables",
    "generar_urls_merra_rango",
]





