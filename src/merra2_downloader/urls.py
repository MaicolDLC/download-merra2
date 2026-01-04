from __future__ import annotations

from datetime import datetime, timedelta
from typing import List
from .config import Merra2Config

# Productos soportados (mapeo explícito)
PRODUCTOS_MERRA2 = {
    "M2T1NXAER.5.12.4": "tavg1_2d_aer_Nx",
    "M2T1NXFLX.5.12.4": "tavg1_2d_flx_Nx",
    # agrega más si quieres:
    # "M2T1NXSLV.5.12.4": "tavg1_2d_slv_Nx",
}


def merra2_bloque(year: int) -> int:
    if 1980 <= year <= 1991:
        return 100
    if 1992 <= year <= 2000:
        return 200
    if 2001 <= year <= 2010:
        return 300
    return 400


def nombre_archivo_producto(producto: str) -> str:
    if producto in PRODUCTOS_MERRA2:
        return PRODUCTOS_MERRA2[producto]

    # inferencia genérica para otros M2T1NX*** (ASM, SLV, etc.)
    base = producto.split(".")[0]   # e.g. M2T1NXAER
    if len(base) >= 3 and base.startswith("M2") and base[2] in ("T", "I") and "NX" in base:
        tipo = "tavg" if base[2] == "T" else "inst"
        periodo = base[3]  # '1' -> 1-hourly
        sufijo = base[-3:].lower()
        return f"{tipo}{periodo}_2d_{sufijo}_Nx"

    raise ValueError(
        f"No sé construir el nombre de archivo para el producto {producto}. "
        f"Añádelo a PRODUCTOS_MERRA2."
    )


def generar_urls_merra_rango(config: Merra2Config) -> List[str]:
    d0 = datetime.strptime(config.inicio, "%Y-%m-%d")
    d1 = datetime.strptime(config.fin, "%Y-%m-%d")
    urls: List[str] = []

    producto_core = nombre_archivo_producto(config.producto)

    for i in range((d1 - d0).days + 1):
        d = d0 + timedelta(days=i)
        y, m, dd = d.strftime("%Y %m %d").split()
        bloque = merra2_bloque(d.year)

        base_url = f"https://goldsmr4.gesdisc.eosdis.nasa.gov/thredds/ncss/grid/{config.producto}"
        fname = f"MERRA2_{bloque}.{producto_core}.{y}{m}{dd}.nc4"

        var_params = "&".join(f"var={v}" for v in config.variables) if config.variables else ""

        url = (
            f"{base_url}/{y}/{m}/{fname}"_
