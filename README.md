# Download MERRA2

Librería (Python) + CLI para descargar datos MERRA-2 desde GES DISC (Earthdata) vía THREDDS/NCSS.

---

## 1) Instalación desde GitHub

### PC local (Spyder / terminal)
```bash
pip install --upgrade --no-cache-dir git+https://github.com/MaicolDLC/download-merra2.git
```
Si usas Google Colab: 
```bash
!pip -q install --upgrade --no-cache-dir git+https://github.com/MaicolDLC/download-merra2.git
```
## 2) Creación de archivo .netrc (Autenticación)

Para descargar datos de NASA GES DISC, necesitas una cuenta de [Earthdata](https://urs.earthdata.nasa.gov/). Debes crear un archivo `.netrc` en tu directorio raíz o carpeta de usuario con tus credenciales:

```bash
# Tu carpeta en Drive
drive_netrc = Path(path + ".netrc")

user = input("Usuario Earthdata: ").strip()
pwd = getpass.getpass("Password Earthdata: ")

drive_netrc.write_text(
    f"machine urs.earthdata.nasa.gov login {user} password {pwd}\n"
    f"machine goldsmr4.gesdisc.eosdis.nasa.gov login {user} password {pwd}\n",
    encoding="utf-8",
)

# Permisos en drive (si es que aplica)
try:
    drive_netrc.chmod(stat.S_IRUSR | stat.S_IWUSR)
except Exception:
    pass

root_netrc = Path("/root/.netrc")
root_netrc.write_text(drive_netrc.read_text(encoding="utf-8"), encoding="utf-8")
root_netrc.chmod(stat.S_IRUSR | stat.S_IWUSR)

print("Guardado en:", drive_netrc)
print("Activo en:", root_netrc)
```


## 3) Guía de Productos y Especificaciones
La descarga se realiza de acuerdo a los productos disponibles en el Catálogo oficial de GES DISC: 
* [Catálogo THREDDS MERRA-2](https://goldsmr4.gesdisc.eosdis.nasa.gov/thredds/catalog/catalog.html)
El catálogo incluye productos con resolución horaria, diurno y mensual. Para conocer los nombres exactos de las variables, dimensiones y nombres de archivos, consulta el manual técnico incluido en este repositorio:
* MERRA2_Specification.pdf 


