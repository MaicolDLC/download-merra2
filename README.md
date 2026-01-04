# Download MERRA2

Librería + CLI para descargar datos MERRA-2 desde GES DISC (Earthdata).

## Install
```bash
pip install git+https://github.com/MaicolDLC/download-merra2.git

merra2-download --help


4. Commit message: `Add README`
5. Commit.

---

## Paso 3: crear el paquete Python (carpetas + `__init__.py`)
Ahora crearemos las carpetas `src/merra2_downloader/` creando un archivo con esa ruta:

1. **Add file → Create new file**
2. Nombre del archivo (muy importante que sea EXACTO):
   **`src/merra2_downloader/__init__.py`**
3. Contenido:

```python
__version__ = "0.1.0"

from .config import Merra2Config, load_config, save_config
from .client import Merra2Client, DownloadResult

__all__ = ["Merra2Config", "load_config", "save_config", "Merra2Client", "DownloadResult"]
