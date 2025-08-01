import shutil
import subprocess
from pathlib import Path
from typing import Iterable, Union

from tqdm import tqdm

PathLike = Union[str, Path]


def _run_ogr2ogr(
    args: list[str],
    label: str,
    log_file,
    raise_on_error: bool,
    log_path: Path,
) -> None:
    """
    Run an ogr2ogr subprocess and write output to the provided log file.

    This function executes the given ogr2ogr command, logs its stdout and
    stderr, and optionally raises a RuntimeError on failure.

    Parameters
    ----------
    args : list of str
        Command-line arguments to pass to ogr2ogr.
    label : str
        Label to include in the log for identifying this operation.
    log_file : file-like
        Open file object for logging command output.
    raise_on_error : bool
        If True, raise a RuntimeError on non-zero exit code.
    log_path : Path
        Path to the log file, used in error messages.

    Raises
    ------
    RuntimeError
        If the subprocess exits with a non-zero code and raise_on_error is True.
    """
    log_file.write(f"=== Exporting {label} ===\n")

    result = subprocess.run(
        args,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
        encoding="utf-8",
        errors="replace",
    )

    if result.stdout:
        log_file.write(f"[STDOUT]\n{result.stdout}\n")
    if result.stderr:
        log_file.write(f"[STDERR]\n{result.stderr}\n")
    if result.returncode != 0:
        log_file.write(f"[ERROR] Exit code {result.returncode}\n\n")
        if raise_on_error:
            raise RuntimeError(
                f"ogr2ogr failed for {label} with exit code {result.returncode}. "
                f"See log at {log_path}"
            )


def extract_geometries(
    dxf_path: PathLike,
    output_root: PathLike,
    geometry_types: Iterable[str] = (
        "POINT",
        "LINESTRING",
        "POLYGON",
        "MULTILINESTRING",
        "MULTIPOLYGON",
    ),
    raise_on_error: bool = False,
    flatten: bool = False,
    output_format: str = "ESRI Shapefile",
) -> None:
    """
    Extract geometries from a DXF file into GIS output files using ogr2ogr.

    Supports Shapefile or GeoPackage output. Outputs are written to the
    specified directory, either as separate files/layers per geometry type,
    or as a single flattened output.

    Parameters
    ----------
    dxf_path : PathLike
        Path to the input DXF file.
    output_root : PathLike
        Root directory where output files will be saved.
    geometry_types : iterable of str, optional
        Geometry types to extract. Only used if flatten is False.
        Defaults to common OGR types.
    raise_on_error : bool, optional
        If True, raise a RuntimeError if ogr2ogr fails. Default is False.
    flatten : bool, optional
        If True, extract all geometries into a single file. If False,
        create one output per geometry type. Default is False.
    output_format : str, optional
        GDAL output format. Must be 'ESRI Shapefile' or 'GPKG'.
        Default is 'ESRI Shapefile'.

    Raises
    ------
    EnvironmentError
        If `ogr2ogr` is not found in the system PATH.
    RuntimeError
        If ogr2ogr fails and `raise_on_error` is True.
    ValueError
        If an unsupported output format is provided.
    """
    dxf_path = Path(dxf_path).expanduser().resolve()
    output_root = Path(output_root).expanduser().resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    log_path = output_root / "export.log"

    if shutil.which("ogr2ogr") is None:
        raise EnvironmentError(
            "Required executable 'ogr2ogr' not found in system PATH."
        )

    fmt = output_format.upper()
    if fmt not in ("ESRI SHAPEFILE", "GPKG"):
        raise ValueError("Unsupported output format. Use 'ESRI Shapefile' or 'GPKG'.")

    if flatten and fmt == "ESRI SHAPEFILE":
        raise ValueError(
            "Flattened output to a single Shapefile is not supported "
            "(mixed geometry types). Use output_format='GPKG' or set flatten=False."
        )

    with log_path.open("w", encoding="utf-8") as log_file:
        if flatten:
            ext = ".shp" if fmt == "ESRI SHAPEFILE" else ".gpkg"
            output_file = output_root / f"all_geometries{ext}"
            args = [
                "ogr2ogr",
                "-f",
                fmt,
                str(output_file),
                str(dxf_path),
                "-skipfailures",
            ]
            _run_ogr2ogr(args, "all geometries", log_file, raise_on_error, log_path)
        else:
            for gtype in tqdm(geometry_types, desc="Iterating over geometries"):
                if fmt == "ESRI SHAPEFILE":
                    out_dir = output_root / gtype.lower()
                    out_dir.mkdir(parents=True, exist_ok=True)
                    output_file = out_dir / f"{gtype.lower()}.shp"
                else:  # GPKG
                    output_file = output_root / f"{gtype.lower()}.gpkg"

                args = [
                    "ogr2ogr",
                    "-f",
                    fmt,
                    str(output_file),
                    str(dxf_path),
                    "-nlt",
                    gtype,
                    "-where",
                    f"OGR_GEOMETRY='{gtype}'",
                    "-skipfailures",
                ]

                if fmt == "GPKG":
                    args += ["-nln", gtype.lower()]

                _run_ogr2ogr(args, gtype, log_file, raise_on_error, log_path)
