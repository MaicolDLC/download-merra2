# src/merra2_downloader/urls.py
from __future__ import annotations

from datetime import datetime, timedelta
from typing import List, Tuple, Iterable, Optional
import re
import requests

from .config import Merra2Config

THREDDS_HOST = "https://goldsmr4.gesdisc.eosdis.nasa.gov/thredds"
DEFAULT_ACCEPT = "netcdf4-classic"

# Variables típicas que NO queremos pedir como "var="
EXCLUDE_VARS = {
    "time", "lat", "lon", "lev", "level",
    "latitude", "longitude",
    "crs", "Lambert_Conformal", "projection",
}

# ----------------------------
# Helpers: ESDT / naming
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


def _root_folder_from_producto(producto: str) -> str:
    """
    Solo soportamos tus 3 carpetas:
    - MERRA2
    - MERRA2_DIURNAL   (F = U  => Monthly-Diurnal mean)
    - MERRA2_MONTHLY   (F = M  => Monthly mean)
    """
    _, F, _, _, _ = _esdt_parts(producto)
    if F == "U":
        return "MERRA2_DIURNAL"
    if F == "M":
        return "MERRA2_MONTHLY"
    return "MERRA2"


def _is_monthly_like(producto: str) -> bool:
    """Mensual (M) o mensual-diurnal (U) usan timestamp yyyymm."""
    _, F, _, _, _ = _esdt_parts(producto)
    return F in ("M", "U")


def collection_from_esdt(producto: str) -> str:
    """
    Colección: freq_dims_group_HV  (ej: tavg1_2d_flx_Nx, instU_2d_int_Nx, etc.)
    """
    T, F, H, V, GGG = _esdt_parts(producto)

    time_map = {"I": "inst", "T": "tavg", "C": "const", "S": "stat"}
    if T not in time_map:
        raise ValueError(f"ESDT '{_shortname(producto)}': T desconocido '{T}'")

    dims = "2d" if V.upper() == "X" else "3d"
    group = GGG.lower()
    hv = f"{H}{V.lower()}"

    time_prefix = time_map[T]
    if time_prefix == "const":
        # (en tu caso no lo estás usando, pero lo dejamos consistente)
        return f"const_{dims}_{group}_{hv}"
    if time_prefix == "stat":
        return f"stat{F}_{dims}_{group}_{hv}"
    return f"{time_prefix}{F}_{dims}_{group}_{hv}"


def _timestamp_for_period(producto: str, d: datetime) -> str:
    """
    - diarios/horarios: yyyymmdd
    - monthly + monthly-diurnal: yyyymm
    """
    if _is_monthly_like(producto):
        return d.strftime("%Y%m")
    return d.strftime("%Y%m%d")


def filename_for_date(producto: str, d: datetime) -> str:
    """
    MERRA2_{stream}.{collection}.{timestamp}.nc4
    """
    stream = merra2_bloque(d.year)
    collection = collection_from_esdt(producto)
    stamp = _timestamp_for_period(producto, d)
    return f"MERRA2_{stream}.{collection}.{stamp}.nc4"


# ----------------------------
# Period iterator
# ----------------------------

def _iter_days(d0: datetime, d1: datetime) -> Iterable[datetime]:
    n = (d1 - d0).days
    for i in range(n + 1):
        yield d0 + timedelta(days=i)


def _iter_months(d0: datetime, d1: datetime) -> Iterable[datetime]:
    """Itera meses inclusive."""
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
# DDS variable discovery + layout detection
# ----------------------------

# Caso 1: variables “planas” (Float32 VAR[...];)
_DDS_FLAT_VAR_RE = re.compile(
    r"^\s*(Byte|Int16|Int32|Int64|Float32|Float64|String)\s+([A-Za-z0-9_]+)\s*(\[[^\]]+\])?\s*;",
    re.MULTILINE,
)

# Caso 2: variables como “Grid { ... } NAME;”
_DDS_GRID_NAME_RE = re.compile(
    r"\}\s*([A-Za-z0-9_]+)\s*;",
    re.MULTILINE,
)


def _parse_vars_from_dds(text: str) -> List[str]:
    vars_found: List[str] = []

    # Grid-style primero (tu caso típico)
    for name in _DDS_GRID_NAME_RE.findall(text):
        if name in EXCLUDE_VARS:
            continue
        vars_found.append(name)

    # Flat-style adicional
    for _typ, name, _dims in _DDS_FLAT_VAR_RE.findall(text):
        if name in EXCLUDE_VARS:
            continue
        vars_found.append(name)

    # únicos preservando orden
    seen = set()
    out: List[str] = []
    for v in vars_found:
        if v not in seen:
            seen.add(v)
            out.append(v)
    return out


def _candidate_layout_urls_dds(root: str, producto: str, d: datetime, fname: str) -> List[Tuple[str, str]]:
    """
    Retorna [(layout, url_dds), ...]
    layout:
      - "ym": /YYYY/MM/
      - "y" : /YYYY/
      - "flat": sin fecha
    """
    y = d.strftime("%Y")
    m = d.strftime("%m")
    base = f"{THREDDS_HOST}/dodsC/{root}/{producto}"

    return [
        ("ym",   f"{base}/{y}/{m}/{fname}.dds"),
        ("y",    f"{base}/{y}/{fname}.dds"),
        ("flat", f"{base}/{fname}.dds"),
    ]


def variables_and_layout_from_dds(producto: str, d: datetime) -> Tuple[List[str], str]:
    """
    Lee el .dds para:
      1) descubrir todas las variables
      2) detectar qué layout usa el servidor para ESTE producto (ym / y / flat)
    """
    root = _root_folder_from_producto(producto)
    fname = filename_for_date(producto, d)

    s = requests.Session()
    s.trust_env = True  # usa ~/.netrc si existe

    tried: List[str] = []
    for layout, url in _candidate_layout_urls_dds(root, producto, d, fname):
        tried.append(url)
        r = s.get(url, timeout=60)

        # 401/403 suelen ser auth (.netrc)
        if r.status_code in (401, 403):
            raise RuntimeError(
                f"Auth falló (HTTP {r.status_code}) al leer DDS.\n"
                f"URL: {url}\n"
                "Asegúrate de tener .netrc activo (Earthdata) en el HOME del runtime."
            )

        if r.status_code != 200:
            continue

        text = r.text
        vars_list = _parse_vars_from_dds(text)
        if not vars_list:
            head = "\n".join(text.splitlines()[:80])
            raise RuntimeError(
                "Pude leer el DDS pero no extraje variables.\n"
                "Primeras 80 líneas del DDS (para debug):\n"
                f"{head}"
            )

        return vars_list, layout

    raise RuntimeError(
        "No pude leer DDS para listar variables.\n"
        "Probé:\n- " + "\n- ".join(tried) + "\n"
        "Verifica que el granule exista para ese periodo y que el .netrc esté activo."
    )


def resolve_variables_and_layout(config: Merra2Config) -> Tuple[List[str], str]:
    """
    - Si el usuario pasó variables, se usan tal cual.
    - Si variables=[] o variables=['todas'/'all'/'*'] => se consultan todas desde DDS.
    Devuelve: (vars_list, layout)
    """
    # Caso: el usuario escribió variables=["todas"]
    if config.variables and len(config.variables) == 1:
        v0 = str(config.variables[0]).strip().lower()
        if v0 in ("todas", "todo", "all", "*"):
            config_vars = []
        else:
            config_vars = list(config.variables)
    else:
        config_vars = list(config.variables) if config.variables else []

    # Si ya vienen variables explícitas, igual necesitamos layout.
    periods = iter_periods(config)
    d0 = periods[0]
    all_vars, layout = variables_and_layout_from_dds(config.producto, d0)

    if config_vars:
        return config_vars, layout
    return all_vars, layout


# ----------------------------
# URL generator (NCSS)
# ----------------------------

def _ncss_base(root: str, producto: str) -> str:
    return f"{THREDDS_HOST}/ncss/grid/{root}/{producto}"


def _ncss_url_for_layout(root: str, producto: str, layout: str, d: datetime, fname: str) -> str:
    y = d.strftime("%Y")
    m = d.strftime("%m")
    base = _ncss_base(root, producto)
    if layout == "ym":
        return f"{base}/{y}/{m}/{fname}"
    if layout == "y":
        return f"{base}/{y}/{fname}"
    return f"{base}/{fname}"


def generar_urls_merra_rango(config: Merra2Config) -> List[str]:
    root = _root_folder_from_producto(config.producto)
    vars_list, layout = resolve_variables_and_layout(config)
    periods = iter_periods(config)

    urls: List[str] = []
    for d in periods:
        fname = filename_for_date(config.producto, d)
        base_url = _ncss_url_for_layout(root, config.producto, layout, d, fname)

        params = [f"var={v}" for v in vars_list]
        params += [
            f"north={config.north}",
            f"west={config.west}",
            f"east={config.east}",
            f"south={config.south}",
            "horizStride=1",
            f"accept={DEFAULT_ACCEPT}",
        ]

        # Si es diario/horario, normalmente conviene acotar time_start/time_end
        if not _is_monthly_like(config.producto):
            y = d.strftime("%Y")
            m = d.strftime("%m")
            dd = d.strftime("%d")
            params += [
                f"time_start={y}-{m}-{dd}T00:30:00Z",
                f"time_end={y}-{m}-{dd}T23:30:00Z",
            ]

        query = "&".join(params)
        urls.append(f"{base_url}?{query}")

    return urls


__all__ = [
    "merra2_bloque",
    "collection_from_esdt",
    "filename_for_date",
    "generar_urls_merra_rango",
]






