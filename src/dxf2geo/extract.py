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
) -> None:
    """
    Extract geometries from a DXF file into shapefiles using ogr2ogr.

    Supports either exporting one shapefile per geometry type, or exporting
    all geometries into a single shapefile. Outputs are saved under the
    specified root directory, and process logs are written to `export.log`.

    Parameters
    ----------
    dxf_path : PathLike
        Path to the input DXF file.
    output_root : PathLike
        Root directory where output shapefiles will be saved.
    geometry_types : iterable of str, optional
        Geometry types to extract. Only used if flatten is False.
        Defaults to common OGR types.
    raise_on_error : bool, optional
        If True, raise a RuntimeError if ogr2ogr fails. Default is False.
    flatten : bool, optional
        If True, extract all geometries into a single shapefile named
        'all_geometries.shp'. If False, create one shapefile per geometry type.

    Raises
    ------
    EnvironmentError
        If `ogr2ogr` is not found in the system PATH.
    RuntimeError
        If ogr2ogr fails and `raise_on_error` is True.
    """

    dxf_path = Path(dxf_path).expanduser().resolve()
    output_root = Path(output_root).expanduser().resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    log_path = output_root / "export.log"

    if shutil.which("ogr2ogr") is None:
        raise EnvironmentError(
            "Required executable 'ogr2ogr' not found in system PATH."
        )

    with log_path.open("w", encoding="utf-8") as log_file:
        if flatten:
            shp_path = output_root / "all_geometries.shp"
            args = [
                "ogr2ogr",
                "-f",
                "ESRI Shapefile",
                str(shp_path),
                str(dxf_path),
                "-skipfailures",
            ]
            _run_ogr2ogr(args, "all geometries", log_file, raise_on_error, log_path)
        else:
            for gtype in tqdm(geometry_types, desc="Iterating over geometries"):
                out_dir = output_root / gtype.lower()
                out_dir.mkdir(parents=True, exist_ok=True)
                shp_path = out_dir / f"{gtype.lower()}.shp"
                args = [
                    "ogr2ogr",
                    "-f",
                    "ESRI Shapefile",
                    str(shp_path),
                    str(dxf_path),
                    "-nlt",
                    gtype,
                    "-where",
                    f"OGR_GEOMETRY='{gtype}'",
                    "-skipfailures",
                ]
                _run_ogr2ogr(args, gtype, log_file, raise_on_error, log_path)
