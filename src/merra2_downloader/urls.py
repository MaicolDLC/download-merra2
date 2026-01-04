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

# Lista completa de variables (solo para el producto de Aerosoles AER)
AER_ALL_VARIABLES = [
    # Black Carbon (BC)
    "BCANGSTR", "BCCMASS", "BCEXTTAU", "BCFLUXU", "BCFLUXV", "BCSCATAU", "BCSMASS",
    # Dust (DU)
    "DUANGSTR", "DUCMASS", "DUCMASS25", "DUEXTT25", "DUEXTTAU", "DUFLUXU", "DUFLUXV",
    "DUSCAT25", "DUSCATAU", "DUSMASS", "DUSMASS25",
    # Organic Carbon (OC)
    "OCANGSTR", "OCCMASS", "OCEXTTAU", "OCFLUXU", "OCFLUXV", "OCSCATAU", "OCSMASS",
    # Sulfates (SO / SU)
    "SO2CMASS", "SO2SMASS", "SO4CMASS", "SO4SMASS",
    "SUANGSTR", "SUEXTTAU", "SUFLUXU", "SUFLUXV", "SUSCATAU",
    # Sea Salt (SS)
    "SSANGSTR", "SSCMASS", "SSCMASS25", "SSEXTT25", "SSEXTTAU",
    "SSFLUXU", "SSFLUXV", "SSSCAT25", "SSSCATAU", "SSSMASS", "SSSMASS25",
    # Total Aerosol
    "TOTANGSTR", "TOTEXTTAU", "TOTSCATAU",
    # DMS
    "DMSCMASS", "DMSSMASS",
]


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
    base = producto.split(".")[0]  # e.g. M2T1NXAER
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

    producto_core = nombre_archivo_producto(config.producto)

    # ✅ Si no vienen variables, usamos TODAS (solo AER). Para otros productos, error claro.
    vars_list = list(config.variables) if config.variables else []
    if not vars_list:
        if config.producto.startswith("M2T1NXAER"):
            vars_list = AER_ALL_VARIABLES
        else:
            raise ValueError(
                "No pasaste variables y este producto no es AER. "
                "El servicio NCSS requiere al menos una variable (var=...)."
            )

    urls: List[str] = []
    for i in range((d1 - d0).days + 1):
        d = d0 + timedelta(days=i)
        y, m, dd = d.strftime("%Y %m %d").split()
        bloque = merra2_bloque(d.year)

        base_url = f"https://goldsmr4.gesdisc.eosdis.nasa.gov/thredds/ncss/grid/{config.producto}"
        fname = f"MERRA2_{bloque}.{producto_core}.{y}{m}{dd}.nc4"

        # ✅ Construcción robusta del querystring
        params = [f"var={v}" for v in vars_list]
        params += [
            f"north={config.north}",
            f"west={config.west}",
            f"east={config.east}",
            f"south={config.south}",
            "horizStride=1",
            f"time_start={y}-{m}-{dd}T00:30:00Z",
            f"time_end={y}-{m}-{dd}T23:30:00Z",
            "accept=netcdf4-classic",
        ]
        query = "&".join(params)

        url = f"{base_url}/{y}/{m}/{fname}?{query}"
        urls.append(url)

    return urls

