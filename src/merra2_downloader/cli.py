from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn

from .config import Merra2Config, load_config, save_config
from .client import Merra2Client, verify_connection

app = typer.Typer(add_completion=False, help="Descargador MERRA-2 (GES DISC)")
console = Console()


@app.command("config-show")
def config_show(config_path: Optional[Path] = typer.Option(None, "--config", help="Ruta a config JSON")):
    """Muestra la configuración actual (por defecto ~/.merra2_downloader.json)."""
    cfg = load_config(str(config_path) if config_path else None)
    console.print(cfg.to_dict())


@app.command("config-save")
def config_save(
    config_path: Optional[Path] = typer.Option(None, "--config", help="Ruta a config JSON (default global)"),
    # bbox
    north: float = typer.Option(5.0),
    south: float = typer.Option(-20.0),
    west: float = typer.Option(-90.0),
    east: float = typer.Option(-70.0),
    # fechas
    inicio: str = typer.Option("2023-10-29"),
    fin: str = typer.Option("2024-12-31"),
    # dataset
    producto: str = typer.Option("M2T1NXAER.5.12.4"),
    variables: Optional[str] = typer.Option(None, help="CSV: VAR1,VAR2,..."),
    # output
    directorio: str = typer.Option(str(Path.cwd() / "datos_merra2")),
    max_workers: int = typer.Option(3),
):
    """Guarda una configuración en JSON para usarla luego en 'download'."""
    vars_list = [v.strip().upper() for v in variables.split(",")] if variables else []
    cfg = Merra2Config(
        north=north, south=south, west=west, east=east,
        inicio=inicio, fin=fin,
        producto=producto,
        variables=vars_list,
        directorio=directorio,
        max_workers=max_workers,
    )
    p = save_config(cfg, str(config_path) if config_path else None)
    console.print(f"[green]✓[/green] Config guardada en: {p}")


@app.command("download")
def download(
    config_path: Optional[Path] = typer.Option(None, "--config", help="Ruta a config JSON"),
    dry_run: bool = typer.Option(False, "--dry-run", help="No descarga, solo simula"),
    check: bool = typer.Option(True, "--check/--no-check", help="Verificar conexión antes de descargar"),
):
    """Descarga el rango configurado (usa .netrc para auth)."""
    cfg = load_config(str(config_path) if config_path else None)

    if check and not verify_connection():
        console.print("[red]✖[/red] No hay conexión a Earthdata / GES DISC.")
        raise typer.Exit(code=1)

    client = Merra2Client()

    if dry_run:
        console.print("[yellow]DRY RUN[/yellow] (no se descargará nada)")
        result = client.download_range(cfg, dry_run=True)
        console.print(result)
        raise typer.Exit(code=0)

    total = (Path(cfg.directorio)).exists()
    console.print(f"Producto: [bold]{cfg.producto}[/bold]")
    console.print(f"Fechas: {cfg.inicio} a {cfg.fin}")
    console.print(f"Directorio: {cfg.directorio}")
    console.print(f"Hilos: {cfg.max_workers}")

    with Progress(
        SpinnerColumn(),
        TextColumn("{task.description}"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total}"),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        # Para mostrar progreso real, calculamos cuántos días son
        from datetime import datetime
        d0 = datetime.strptime(cfg.inicio, "%Y-%m-%d")
        d1 = datetime.strptime(cfg.fin, "%Y-%m-%d")
        total_files = (d1 - d0).days + 1

        task = progress.add_task("Descargando", total=total_files)

        def cb(_fname: str, _estado: str):
            progress.advance(task, 1)

        result = client.download_range(cfg, dry_run=False, progress_cb=cb)

    console.print(f"\n[green]✓[/green] Exitosos: {result.exitosos}  |  [red]✖[/red] Fallidos: {result.fallidos}")

