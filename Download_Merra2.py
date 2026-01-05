
"""
AUTOR : Michael De La Cruz La Rosa 
20180176@lamolina.edu.pe 
Michael.dlc.lr@gmail.com 
"""

#%% 
# omitir warnings :
import warnings
warnings.filterwarnings('ignore')
#%% 
import os
path = '/home/Python_Ocean/' #
os.chdir(path)
#%% 
from pathlib import Path
import stat, getpass

user = input("Usuario Earthdata: ").strip()
pwd = getpass.getpass("Password Earthdata: ")

p = Path.home() / ".netrc"
p.write_text(
    f"machine urs.earthdata.nasa.gov login {user} password {pwd}\n"
    f"machine goldsmr4.gesdisc.eosdis.nasa.gov login {user} password {pwd}\n",
    encoding="utf-8"
)

# En Windows esto no aplica igual que chmod 600, pero lo dejamos como intento suave:
try:
    p.chmod(stat.S_IREAD | stat.S_IWRITE)
except Exception:
    pass

print("Creado:", p)

#%%
from pathlib import Path
print("HOME:", Path.home())
print("netrc exists:", (Path.home() / ".netrc").exists())
print("netrc path:", Path.home() / ".netrc")
#%% 
# Importamos la librería
from merra2_downloader import Merra2Config, Merra2Client

cfg_h = Merra2Config(
    north=5, south=-20, west=-90, east=-70, # Ingresar zona de interés
    inicio="2023-10-01", # Ingresar fecha en formato YYYY-MM-DD
    fin="2023-11-30",
    producto="M2T1NXAER.5.12.4",  # Ejemplo de producto horario
    variables=[],                 # Vacío: descarga todas las variables
    # variables=["BCEXTTAU", "BCSCATAU"] # Especificando variables
    directorio= path + "merra2_horario", # Directorio de guardado
    max_workers=5, # Hilos de descarga
)

client = Merra2Client()
result = client.download_range(cfg_h)
print(result)
print(f"Se guardó en: {cfg_h.directorio}")

#%% 
