from __future__ import annotations

import threading
import time
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Callable, Dict

import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

from .config import Merra2Config
from .urls import generar_urls_merra_rango
from .metadata import (
    config_matches,
    write_metadata,
    safe_remove_file_and_metadata,
)

# Barra de progreso (opcional)
try:
    from tqdm.auto import tqdm
except Exception:
    tqdm = None


@dataclass(frozen=True)
class DownloadResult:
    exitosos: int
    fallidos: int


def verify_connection(timeout: int = 10) -> bool:
    try:
        r = requests.get(
            "https://goldsmr4.gesdisc.eosdis.nasa.gov/thredds/catalog.xml",
            timeout=timeout,
        )
        return r.status_code == 200
    except Exception:
        return False


class Merra2Client:
    """
    Cliente para descargas MERRA-2 (GES DISC).
    - Usa .netrc si existe (requests lo respeta con trust_env=True).
    - Crea una Session por thread (thread-local) para descargas en paralelo.
    """

    def __init__(self, timeout: int = 120):
        self.timeout = timeout
        self._local = threading.local()

    def _session(self) -> requests.Session:
        sess = getattr(self._local, "session", None)
        if sess is None:
            sess = requests.Session()
            sess.trust_env = True  # permite .netrc
            self._local.session = sess
        return sess

    def download_file(
        self,
        url: str,
        dest: Path,
        config: Merra2Config,
        intentos_max: int = 3,
        backoff_base: int = 5,
    ) -> str:
        # Existe y coincide => no descargar
        if dest.exists() and dest.stat().st_size > 0:
            if config_matches(dest, config):
                return "existente"
            # Existe pero con config distinta => limpiar
            safe_remove_file_and_metadata(dest)

        for intento in range(1, intentos_max + 1):
            try:
                sess = self._session()
                r = sess.get(url, stream=True, timeout=self.timeout)
                r.raise_for_status()

                dest.parent.mkdir(parents=True, exist_ok=True)

                with dest.open("wb") as f:
                    for chunk in r.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            f.write(chunk)

                expected = int(r.headers.get("content-length", 0))
                if expected > 0 and dest.stat().st_size != expected:
                    raise RuntimeError("Tamaño del archivo incorrecto (content-length mismatch)")

                write_metadata(dest, config)
                return "descargado"

            except Exception as e:
                print(f"[{intento}/{intentos_max}] Error en {dest.name}: {e}", file=sys.stderr)
                if intento < intentos_max:
                    espera = backoff_base * (2 ** (intento - 1))
                    time.sleep(espera)
                else:
                    safe_remove_file_and_metadata(dest)
                    return "fallido"

        return "fallido"

    def download_range(
        self,
        config: Merra2Config,
        dry_run: bool = False,
        progress_cb: Optional[Callable[[str, str], None]] = None,
        show_progress: bool = True,   # ✅ NUEVO: permitir activar/desactivar barra
    ) -> DownloadResult:
        urls = generar_urls_merra_rango(config)

        if dry_run:
            for u in urls:
                fname = u.split("/")[-1].split("?")[0]
                if progress_cb:
                    progress_cb(fname, "url")
            return DownloadResult(exitosos=0, fallidos=0)

        lock = threading.Lock()
        resultados: Dict[str, int] = {"exitosos": 0, "fallidos": 0}

        def worker(url: str) -> str:
            fname = url.split("/")[-1].split("?")[0]
            dest = Path(config.directorio) / fname
            estado = self.download_file(url, dest, config)

            with lock:
                if estado in ("descargado", "existente"):
                    resultados["exitosos"] += 1
                else:
                    resultados["fallidos"] += 1

            if progress_cb:
                progress_cb(fname, estado)

            return estado

        # ✅ Barra de progreso por archivo completado
        use_bar = bool(show_progress and tqdm is not None)
        pbar = tqdm(total=len(urls), desc="Descargando", unit="archivo") if use_bar else None

        with ThreadPoolExecutor(max_workers=config.max_workers) as ex:
            futures = [ex.submit(worker, u) for u in urls]

            for fut in as_completed(futures):
                _ = fut.result()  # levanta error si ocurre
                if pbar:
                    pbar.update(1)
                    pbar.set_postfix(ok=resultados["exitosos"], fail=resultados["fallidos"])

        if pbar:
            pbar.close()

        return DownloadResult(exitosos=resultados["exitosos"], fallidos=resultados["fallidos"])

