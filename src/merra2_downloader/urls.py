# src/merra2_downloader/urls.py
from __future__ import annotations

from datetime import datetime, timedelta
from typing import List, Tuple, Iterable
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
    """Bloque MERRA2 por año (100/200/300/400)."""
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
    """
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
    """
    Colección: freq_dims_group_HV
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
        # En el spec las colecciones const se nombran 'const_2d_*'
        return f"const_{dims}_{group}_{hv}"
    if time_prefix == "stat":
        return f"stat{F}_{dims}_{group}_{hv}"
    return f"{time_prefix}{F}_{dims}_{group}_{hv}"


def _is_monthly(producto: str) -> bool:
    _, F, _, _, _ = _esdt_parts(producto)
    return F in ("M", "U")


def _is_constant(producto: str) -> bool:
    T, _, _, _, _ = _esdt_parts(producto)
    return T == "C"


def _timestamp_for_period(producto: str, d: datetime) -> str:
    """
    Spec:
    - yyyymmdd para diarios/horarios
    - yyyymm para mensuales (M/U)
    """
    if _is_monthly(producto):
        return d.strftime("%Y%m")
    if _is_constant(producto):
        # En THREDDS típicamente aparecen como ...00000000.nc4
        # (si en tu caso fuese distinto, se ajusta aquí)
        return "00000000"
    return d.strftime("%Y%m%d")


def filename_for_date(producto: str, d: datetime) -> str:
    """
    Nombre:
    MERRA2_{bloque}.{collection}.{timestamp}.nc4
    """
    collection = collection_from_esdt(producto)
    stamp = _timestamp_for_period(producto, d)

    if _is_constant(producto):
        # Constantes no dependen del año-stream: se sirven como un solo granule “ancillary”.
        # Si alguna colección const tuya usa otro runid, se puede parametrizar.
        runid = 101
    else:
        runid = merra2_bloque(d.year)

    return f"MERRA2_{runid}.{collection}.{stamp}.nc4"


# ----------------------------
# Iteradores (día vs mes)
# ----------------------------

def _iter_days(d0: datetime, d1: datetime) -> Iterable[datetime]:
    n = (d1 - d0).days
    for i in range(n + 1):
        yield d0 + timedelta(days=i)


def _iter_months(d0: datetime, d1: datetime) -> Iterable[datetime]:
    """
    Itera meses inclusive: d0..d1 por (YYYY,MM)
    """
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

    if _is_constant(config.producto):
        return [d0]  # un solo granule
    if _is_monthly(config.producto):
        return list(_iter_months(d0, d1))
    return list(_iter_days(d0, d1))


# ----------------------------
# DDS variable discovery
# ----------------------------

def _dds_url_dated(producto: str, y: str, m: str, fname: str) -> str:
    return f"{THREDDS_HOST}/dodsC/{producto}/{y}/{m}/{fname}.dds"


def _dds_url_flat(producto: str, fname: str) -> str:
    return f"{THREDDS_HOST}/dodsC/{producto}/{fname}.dds"


# Caso 1: variables declaradas “planas”
_DDS_FLAT_VAR_RE = re.compile(
    r"^\s*(Byte|Int16|Int32|Int64|Float32|Float64|String)\s+([A-Za-z0-9_]+)\s*(\[[^\]]+\])?\s*;",
    re.MULTILINE,
)

# Caso 2 (tu DDS): variables como “Grid { ... } NAME;”
_DDS_GRID_NAME_RE = re.compile(
    r"\}\s*([A-Za-z0-9_]+)\s*;",
    re.MULTILINE,
)

def _parse_vars_from_dds(text: str) -> List[str]:
    vars_found: List[str] = []

    # 1) Grid-style: } BSTAR;
    for name in _DDS_GRID_NAME_RE.findall(text):
        if name in EXCLUDE_VARS:
            continue
        vars_found.append(name)

    # 2) Flat-style: Float32 VAR[...];
    for _typ, name, _dims in _DDS_FLAT_VAR_RE.findall(text):
        if name in EXCLUDE_VARS:
            continue
        vars_found.append(name)

    # únicos preservando orden
    seen = set()
    out = []
    for v in vars_found:
        if v not in seen:
            seen.add(v)
            out.append(v)
    return out


def variables_from_dds(producto: str, d: datetime) -> List[str]:
    """
    Descubre variables reales del archivo consultando el .dds (requiere auth vía .netrc).
    Probamos layout con /YYYY/MM/ y layout plano (para constantes u otros casos).
    """
    fname = filename_for_date(producto, d)

    y = d.strftime("%Y")
    m = d.strftime("%m")

    s = requests.Session()
    s.trust_env = True  # usa ~/.netrc

    # intentamos primero el layout “normal” (/YYYY/MM/)
    tried = []
    for url in (_dds_url_dated(producto, y, m, fname), _dds_url_flat(producto, fname)):
        tried.append(url)
        r = s.get(url, timeout=60)
        if r.status_code != 200:
            continue

        text = r.text
        out = _parse_vars_from_dds(text)
        if not out:
            head = "\n".join(text.splitlines()[:80])
            raise RuntimeError(
                "Pude leer el DDS pero no extraje variables.\n"
                "Primeras 80 líneas del DDS (para debug):\n"
                f"{head}"
            )
        return out

    raise RuntimeError(
        "No pude leer DDS para listar variables.\n"
        f"Probé:\n- " + "\n- ".join(tried) + "\n"
        "Asegúrate de tener autenticación Earthdata activa (.netrc) y que el granule exista."
    )


def resolve_variables(config: Merra2Config) -> List[str]:
    """
    - Si el usuario pasó variables, se usan tal cual.
    - Si variables=[] => se consultan todas las variables del dataset vía DDS (para el primer periodo).
    """
    if config.variables and len(config.variables) > 0:
        return list(config.variables)

    periods = iter_periods(config)
    return variables_from_dds(config.producto, periods[0])


# ----------------------------
# URL generator
# ----------------------------

def _ncss_base_dated(producto: str, y: str, m: str, fname: str) -> str:
    return f"{THREDDS_HOST}/ncss/grid/{producto}/{y}/{m}/{fname}"


def _ncss_base_flat(producto: str, fname: str) -> str:
    return f"{THREDDS_HOST}/ncss/grid/{producto}/{fname}"


def generar_urls_merra_rango(config: Merra2Config) -> List[str]:
    vars_list = resolve_variables(config)
    periods = iter_periods(config)

    urls: List[str] = []
    for d in periods:
        fname = filename_for_date(config.producto, d)

        y = d.strftime("%Y")
        m = d.strftime("%m")

        # layout normal vs plano (constantes)
        if _is_constant(config.producto):
            base_url = _ncss_base_flat(config.producto, fname)
        else:
            base_url = _ncss_base_dated(config.producto, y, m, fname)

        params = [f"var={v}" for v in vars_list]
        params += [
            f"north={config.north}",
            f"west={config.west}",
            f"east={config.east}",
            f"south={config.south}",
            "horizStride=1",
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




