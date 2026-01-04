# src/merra2_downloader/urls.py
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Iterable, List, Tuple
import re
import requests

from .config import Merra2Config

THREDDS_HOST = "https://goldsmr4.gesdisc.eosdis.nasa.gov/thredds"
DEFAULT_ACCEPT = "netcdf4-classic"

# Variables típicas que NO queremos pedir como "var="
EXCLUDE_VARS = {
    "time", "lat", "lon", "lev", "level",
    "latitude", "longitude",
    "crs", "lambert_conformal", "projection",
}

# ----------------------------
# ESDT helpers
# ----------------------------

def merra2_bloque(year: int) -> int:
    """Stream MERRA2 por año (100/200/300/400)."""
    if 1980 <= year <= 1991:
        return 100
    if 1992 <= year <= 2000:
        return 200
    if 2001 <= year <= 2010:
        return 300
    return 400


def _shortname(producto: str) -> str:
    """'M2T1NXFLX.5.12.4' -> 'M2T1NXFLX'"""
    return producto.split(".")[0].strip()


def _esdt_parts(producto: str) -> Tuple[str, str, str, str, str]:
    """
    ESDT: M2TFHVGGG (9 chars)
    T: I/T/C/S
    F: 1/3/6/D/M/U/0...
    H: N
    V: X/P/V/E
    GGG: AER, FLX, INT, ...
    """
    esdt = _shortname(producto)
    if len(esdt) != 9 or not esdt.startswith("M2"):
        raise ValueError(f"ESDT inválido: {esdt} (esperaba algo tipo M2T1NXAER)")
    T = esdt[2]
    F = esdt[3]
    H = esdt[4]
    V = esdt[5]
    GGG = esdt[6:9]
    return T, F, H, V, GGG


def _is_monthly_like(producto: str) -> bool:
    """Mensual (M) o mensual-diurnal (U) usan timestamp yyyymm."""
    _, F, _, _, _ = _esdt_parts(producto)
    return F in ("M", "U")


def _root_folder_hint(producto: str) -> str:
    """
    Hint lógico de carpeta (tus 3 primeras), pero NO siempre aparece en el path real,
    por eso probamos con y sin carpeta.
    """
    _, F, _, _, _ = _esdt_parts(producto)
    if F == "U":
        return "MERRA2_DIURNAL"
    if F == "M":
        return "MERRA2_MONTHLY"
    return "MERRA2"


def collection_from_esdt(producto: str) -> str:
    """
    Colección: freq_dims_group_HV
    ej:
      - tavg1_2d_flx_Nx
      - inst1_2d_int_Nx
      - instU_2d_int_Nx
    """
    T, F, H, V, GGG = _esdt_parts(producto)

    time_map = {"I": "inst", "T": "tavg", "C": "const", "S": "stat"}
    if T not in time_map:
        raise ValueError(f"ESDT '{_shortname(producto)}': T desconocido '{T}'")

    dims = "2d" if V.upper() == "X" else "3d"
    group = GGG.lower()
    hv = f"{H}{V.lower()}"

    pref = time_map[T]
    if pref == "const":
        return f"const_{dims}_{group}_{hv}"
    if pref == "stat":
        return f"stat{F}_{dims}_{group}_{hv}"
    return f"{pref}{F}_{dims}_{group}_{hv}"


def filename_for_date(producto: str, d: datetime) -> str:
    """
    MERRA2_{stream}.{collection}.{timestamp}.nc4
    timestamp:
      - daily/hourly: YYYYMMDD
      - monthly/diurnal: YYYYMM
    """
    stream = merra2_bloque(d.year)
    collection = collection_from_esdt(producto)
    stamp = d.strftime("%Y%m") if _is_monthly_like(producto) else d.strftime("%Y%m%d")
    return f"MERRA2_{stream}.{collection}.{stamp}.nc4"


# ----------------------------
# Period iterator
# ----------------------------

def _iter_days(d0: datetime, d1: datetime) -> Iterable[datetime]:
    n = (d1 - d0).days
    for i in range(n + 1):
        yield d0 + timedelta(days=i)


def _iter_months(d0: datetime, d1: datetime) -> Iterable[datetime]:
    """Itera meses inclusive: (YYYY,MM)"""
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
    if _is_monthly_like(config.producto):
        return list(_iter_months(d0, d1))
    return list(_iter_days(d0, d1))


# ----------------------------
# DDS parsing
# ----------------------------

# Flat: Float32 VAR[...];
_DDS_FLAT_VAR_RE = re.compile(
    r"^\s*(?:Byte|Int16|UInt16|Int32|UInt32|Int64|UInt64|Float32|Float64|String)\s+"
    r"([A-Za-z_][A-Za-z0-9_]*)\s*(?:\[[^\]]+\]\s*)*;",
    re.MULTILINE,
)

# Grid: } NAME;
_DDS_GRID_NAME_RE = re.compile(
    r"\}\s*([A-Za-z_][A-Za-z0-9_]*)\s*;",
    re.MULTILINE,
)


def _parse_vars_from_dds(text: str) -> List[str]:
    vars_found: List[str] = []

    for name in _DDS_GRID_NAME_RE.findall(text):
        if name.lower() in EXCLUDE_VARS:
            continue
        vars_found.append(name)

    for name in _DDS_FLAT_VAR_RE.findall(text):
        if name.lower() in EXCLUDE_VARS:
            continue
        vars_found.append(name)

    out: List[str] = []
    seen = set()
    for v in vars_found:
        if v not in seen:
            seen.add(v)
            out.append(v)
    return out


# ----------------------------
# Candidate URLs (con/sin carpeta) + layout detection
# ----------------------------

def _dds_candidates(producto: str, d: datetime, fname: str) -> List[Tuple[str, str, str]]:
    """
    Returns list of (mode, layout, url_dds)
      mode: "no_root" or "with_root"
      layout: "ym" (/YYYY/MM), "y" (/YYYY), "flat"
    """
    y = d.strftime("%Y")
    m = d.strftime("%m")
    root = _root_folder_hint(producto)

    bases = [
        ("no_root",  f"{THREDDS_HOST}/dodsC/{producto}"),
        ("with_root", f"{THREDDS_HOST}/dodsC/{root}/{producto}"),
    ]

    monthly_like = _is_monthly_like(producto)

    out: List[Tuple[str, str, str]] = []
    for mode, base in bases:
        if monthly_like:
            # PRIORIDAD: /YYYY/ (como tu URL manual)
            out.append((mode, "y",    f"{base}/{y}/{fname}.dds"))
            out.append((mode, "ym",   f"{base}/{y}/{m}/{fname}.dds"))   # fallback
            out.append((mode, "flat", f"{base}/{fname}.dds"))
        else:
            # PRIORIDAD: /YYYY/MM/
            out.append((mode, "ym",   f"{base}/{y}/{m}/{fname}.dds"))
            out.append((mode, "y",    f"{base}/{y}/{fname}.dds"))
            out.append((mode, "flat", f"{base}/{fname}.dds"))

    return out


def _ncss_base(producto: str, mode: str) -> str:
    if mode == "with_root":
        root = _root_folder_hint(producto)
        return f"{THREDDS_HOST}/ncss/grid/{root}/{producto}"
    return f"{THREDDS_HOST}/ncss/grid/{producto}"


def _ncss_url(ncss_base: str, layout: str, d: datetime, fname: str) -> str:
    y = d.strftime("%Y")
    m = d.strftime("%m")
    if layout == "ym":
        return f"{ncss_base}/{y}/{m}/{fname}"
    if layout == "y":
        return f"{ncss_base}/{y}/{fname}"
    return f"{ncss_base}/{fname}"


def variables_and_endpoint_from_dds(producto: str, d: datetime) -> Tuple[List[str], str, str]:
    """
    Lee DDS para:
      - descubrir variables
      - detectar mode (con/sin carpeta) y layout (ym/y/flat)
    Retorna: (vars_list, ncss_base, layout)
    """
    fname = filename_for_date(producto, d)

    s = requests.Session()
    s.trust_env = True

    tried: List[str] = []
    for mode, layout, url in _dds_candidates(producto, d, fname):
        tried.append(url)
        r = s.get(url, timeout=60)

        if r.status_code in (401, 403):
            raise RuntimeError(
                f"Auth falló (HTTP {r.status_code}) al leer DDS.\nURL: {url}\n"
                "Asegúrate de tener .netrc activo en el runtime."
            )

        if r.status_code != 200:
            continue

        vars_list = _parse_vars_from_dds(r.text)
        if not vars_list:
            head = "\n".join(r.text.splitlines()[:80])
            raise RuntimeError(
                "Pude leer el DDS pero no extraje variables.\n"
                "Primeras 80 líneas del DDS:\n" + head
            )

        return vars_list, _ncss_base(producto, mode), layout

    raise RuntimeError(
        "No pude leer DDS para listar variables.\n"
        "Probé:\n- " + "\n- ".join(tried) + "\n"
        "Verifica que el granule exista para ese periodo y que el .netrc esté activo."
    )


def resolve_variables_and_endpoint(config: Merra2Config) -> Tuple[List[str], str, str]:
    """
    Devuelve: (vars_list, ncss_base, layout)
    - variables=[] o ["todas"/"all"/*] => autodetecta via DDS
    - variables explícitas => se usan, pero igual detectamos endpoint/layout via DDS
      para construir bien las URLs.
    """
    config_vars: List[str] = []
    if config.variables:
        if len(config.variables) == 1 and str(config.variables[0]).strip().lower() in ("todas", "todo", "all", "*"):
            config_vars = []
        else:
            config_vars = list(config.variables)

    d0 = iter_periods(config)[0]
    all_vars, ncss_base, layout = variables_and_endpoint_from_dds(config.producto, d0)

    return (config_vars if config_vars else all_vars), ncss_base, layout


# ----------------------------
# URL generator (NCSS)
# ----------------------------

def generar_urls_merra_rango(config: Merra2Config) -> List[str]:
    vars_list, ncss_base, layout = resolve_variables_and_endpoint(config)
    periods = iter_periods(config)

    urls: List[str] = []
    for d in periods:
        fname = filename_for_date(config.producto, d)
        base_url = _ncss_url(ncss_base, layout, d, fname)

        params = [f"var={v}" for v in vars_list]
        params += [
            f"north={config.north}",
            f"west={config.west}",
            f"east={config.east}",
            f"south={config.south}",
            "horizStride=1",
            f"accept={DEFAULT_ACCEPT}",
        ]

        # Para no-mensual: añade time_start/time_end del día
        if not _is_monthly_like(config.producto):
            y = d.strftime("%Y")
            m = d.strftime("%m")
            dd = d.strftime("%d")
            params += [
                f"time_start={y}-{m}-{dd}T00:30:00Z",
                f"time_end={y}-{m}-{dd}T23:30:00Z",
            ]

        urls.append(base_url + "?" + "&".join(params))

    return urls


__all__ = [
    "merra2_bloque",
    "collection_from_esdt",
    "filename_for_date",
    "generar_urls_merra_rango",
]

