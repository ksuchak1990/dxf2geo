import shutil
import subprocess
from pathlib import Path
from typing import Iterable, Union

from tqdm import tqdm

PathLike = Union[str, Path]


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
) -> None:
    """
    Extract specified geometry types from a DXF file using ogr2ogr.

    For each geometry type, this function invokes `ogr2ogr` to extract matching
    features from the input DXF file and writes them to a shapefile in a
    corresponding subdirectory under the specified output root. Standard output
    and errors from the process are written to `export.log`.

    Parameters
    ----------
    dxf_path : PathLike
        Path to the input DXF file.
    output_root : PathLike
        Root directory where output shapefiles will be saved.
    geometry_types : iterable of str, optional
        Geometry types to extract (e.g. "POINT", "POLYGON"). Defaults to a
        standard list of common OGR types.
    raise_on_error : bool, optional
        If True, raises a RuntimeError on ogr2ogr failure. If False, logs
        the error and continues. Default is False.

    Raises
    ------
    EnvironmentError
        If `ogr2ogr` is not found in the system PATH.
    RuntimeError
        If a subprocess fails and `raise_on_error` is True.
    """
    dxf_path = Path(dxf_path).expanduser().resolve()
    output_root = Path(output_root).expanduser().resolve()

    if shutil.which("ogr2ogr") is None:
        raise EnvironmentError(
            "Required executable 'ogr2ogr' not found in system PATH."
        )

    output_root.mkdir(parents=True, exist_ok=True)
    log_path = output_root / "export.log"

    with log_path.open("w", encoding="utf-8") as log_file:
        for gtype in tqdm(geometry_types, desc="Iterating over geometries"):
            out_dir = output_root / gtype.lower()
            out_dir.mkdir(parents=True, exist_ok=True)
            shp_path = out_dir / f"{gtype.lower()}.shp"

            log_file.write(f"=== Exporting {gtype} to {shp_path} ===\n")

            result = subprocess.run(
                [
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
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=False,
                encoding="utf-8",
                errors="replace",
            )

            if result.returncode != 0:
                log_file.write(f"[ERROR] Exit code {result.returncode}\n\n")
                if raise_on_error:
                    raise RuntimeError(
                        f"ogr2ogr failed for geometry type '{gtype}' "
                        f"with exit code {result.returncode}. See {log_path}"
                    )

            log_file.write(
                f"[STDOUT]\n{result.stdout}\n" if result.stdout else "")
            log_file.write(
                f"[STDERR]\n{result.stderr}\n" if result.stderr else "")
            if result.returncode != 0:
                log_file.write(f"[ERROR] Exit code {result.returncode}\n\n")
                if raise_on_error:
                    raise RuntimeError(
                        f"ogr2ogr failed for geometry type '{gtype}' "
                        f"with exit code {result.returncode}. See {log_path}"
                    )
