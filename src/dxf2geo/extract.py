"""
Extract geometries from DXF files into GIS formats using GeoPandas/Pyogrio.

This module converts DXF vector data into structured GIS outputs (Shapefile or
GeoPackage), with optional filtering by geometry type, layer name (exact or
regex), spatial extent, geometry size, and attribute values.

The main entry point is `extract_geometries`. The module is designed for batch
processing and supports flattened or partitioned output modes. Error handling,
field sanitisation, and logging are implemented to support robust automated use.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
import logging
from pathlib import Path
import re

import geopandas as gpd
from tqdm.auto import tqdm

PathLike = str | Path


# Exceptions
class ExtractError(Exception):
    """Base exception for all extract-related errors."""


class InputOpenError(ExtractError):
    """Raised when a DXF file cannot be opened or contains no usable layers."""


class OutputCreateError(ExtractError):
    """Raised when an output file or layer cannot be created."""


# Data structures
@dataclass(frozen=True)
class FilterOptions:
    """
    Filtering options applied to features prior to export.

    Parameters
    ----------
    include_layers : tuple of str, optional
        Exact layer names to include. Matching is case-insensitive. If any
        include names or patterns are provided, a layer must match at least
        one to pass.
    exclude_layers : tuple of str, optional
        Exact layer names to exclude. Matching is case-insensitive. Any match
        vetoes inclusion.
    include_layer_patterns : tuple of str, optional
        Regular expressions applied to the original layer name with
        ``re.IGNORECASE``. If any include names or patterns are provided,
        a layer must match at least one to pass.
    exclude_layer_patterns : tuple of str, optional
        Regular expressions applied to the original layer name with
        ``re.IGNORECASE``. Any match vetoes inclusion.
    drop_empty : bool, optional
        If ``True``, drop features whose geometry is empty.
    drop_zero_geom : bool, optional
        If ``True``, drop features whose polygon area or line length evaluates
        to zero.
    min_area : float, optional
        Minimum polygon area. Polygons (and multipolygons) with area below this
        threshold are dropped.
    min_length : float, optional
        Minimum line length. Lines (and multilines) with length below this
        threshold are dropped.
    bbox : tuple of float, optional
        Axis-aligned bounding box as ``(minx, miny, maxx, maxy)`` in the
        dataset’s coordinate reference system. Used for fast, server-side
        filtering during read, and re-applied defensively after read.
    exclude_field_values : dict[str, set[str]], optional
        Mapping of field name to a set of disallowed string values. Features
        with matching field values are dropped.
    """

    include_layers: tuple[str, ...] | None = None
    exclude_layers: tuple[str, ...] | None = None
    include_layer_patterns: tuple[str, ...] | None = None
    exclude_layer_patterns: tuple[str, ...] | None = None
    min_area: float | None = None
    min_length: float | None = None
    drop_empty: bool = True
    drop_zero_geom: bool = True
    # (minx, miny, maxx, maxy)
    bbox: tuple[float, float, float, float] | None = None
    # Exact-match field exclusions, e.g. {"EntityType": {"TEXT", "MTEXT"}}
    exclude_field_values: dict[str, set[str]] | None = None


@dataclass(frozen=True)
class ExtractOptions:
    """
    Configuration for a single DXF-to-GIS extraction task.
    """

    dxf_path: Path
    output_root: Path
    flatten: bool
    driver_name: str  # "ESRI Shapefile" or "GPKG"
    geometry_types: tuple[str, ...]
    raise_on_error: bool
    filter_options: FilterOptions | None = None


# Public API
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
    filter_options: FilterOptions | None = None,
    assume_crs: int | str | None = None,
) -> None:
    """
    Extract geometries from a DXF file and write them to GIS format outputs.

    Parameters
    ----------
    dxf_path : PathLike
        Path to the input DXF file.
    output_root : PathLike
        Directory where output files and logs will be written.
    geometry_types : Iterable[str], optional
        Geometry types to extract (e.g. "POINT", "LINESTRING"). Defaults to
        common types.
    raise_on_error : bool, optional
        If True, raise an exception when no features are written for any output.
    flatten : bool, optional
        If True, export all geometries into a single GeoPackage layer. Must be
        False for Shapefile output.
    output_format : str, optional
        Output format: either "ESRI Shapefile" or "GPKG" (case-insensitive).
    filter_options : FilterOptions, optional
        Optional filters for layer names, geometry size, bounding box, or field
        values.
    assume_crs : int | str | None, optional
        If provided and the input DXF lacks a CRS, assign this CRS to the
        GeoDataFrame prior to writing (e.g. 3857, "EPSG:27700"). The function
        does not override an existing CRS; it logs and ignores the hint.
    """
    output_format_upper = output_format.upper()
    if output_format_upper not in ("ESRI SHAPEFILE", "GPKG"):
        raise ValueError("Unsupported output format. Use 'ESRI Shapefile' or 'GPKG'.")
    if flatten and output_format_upper == "ESRI SHAPEFILE":
        raise ValueError(
            "Flattened shapefile not supported; use GPKG or set flatten=False."
        )

    dxf_path = Path(dxf_path).expanduser().resolve()
    output_root = Path(output_root).expanduser().resolve()
    output_root.mkdir(parents=True, exist_ok=True)

    log_path = output_root / "export.log"
    _configure_logging(log_path)
    logger = logging.getLogger("dxf2geo.extract")

    options = ExtractOptions(
        dxf_path=dxf_path,
        output_root=output_root,
        flatten=flatten,
        driver_name=(
            "ESRI Shapefile" if output_format_upper == "ESRI SHAPEFILE" else "GPKG"
        ),
        geometry_types=tuple(geometry_types),
        raise_on_error=raise_on_error,
        filter_options=filter_options,
    )

    logger.info("Opening DXF with GeoPandas/Pyogrio: %s", dxf_path)
    try:
        gdf = _read_dxf(dxf_path, options.filter_options)
    except Exception as e:
        raise InputOpenError(f"Failed to read DXF: {dxf_path} ({e})") from e

    # Assign a CRS if the source lacks one and the caller supplied a hint
    if assume_crs is not None:
        if gdf.crs is None:
            try:
                gdf = gdf.set_crs(assume_crs, allow_override=True)
                logger.info("Assigned CRS %r to dataset with no CRS.", assume_crs)
            except Exception as e:
                if options.raise_on_error:
                    raise ExtractError(
                        f"Failed to set CRS to {assume_crs!r}: {e}"
                    ) from e
                logger.warning("Failed to set CRS to %r: %s", assume_crs, e)
        else:
            logger.info("CRS already present (%s); 'assume_crs' hint ignored.", gdf.crs)

    if gdf.empty:
        if options.raise_on_error:
            raise ExtractError("No features read from DXF.")
        logger.warning("No features read from DXF; nothing to write.")
        return

    # Apply filters
    gdf = _apply_filters(gdf, options.filter_options)
    if gdf.empty:
        if options.raise_on_error:
            raise ExtractError("No features remaining after filtering.")
        logger.warning("No features remaining after filtering; nothing to write.")
        return

    # Export
    if options.flatten:
        _export_flattened_gpkg(gdf, options, logger)
    else:
        _export_partitioned(gdf, options, logger)


# ---- Private helpers ----


# Setup / IO helpers
def _configure_logging(
    log_path: Path, *, console_level: int | None = logging.INFO
) -> None:
    """
    Configure logging to both file and console.
    """
    logger = logging.getLogger("dxf2geo.extract")
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()

    file_h = logging.FileHandler(log_path, encoding="utf-8")
    file_h.setLevel(logging.DEBUG)
    file_h.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(file_h)

    if console_level is not None:
        console_h = logging.StreamHandler()
        console_h.setLevel(console_level)
        console_h.setFormatter(
            logging.Formatter(
                "%(asctime)s | %(filename)s > %(levelname)s > %(message)s",
                datefmt="%H:%M:%S",
            )
        )
        logger.addHandler(console_h)


def _read_dxf(dxf_path: Path, fo: FilterOptions | None) -> gpd.GeoDataFrame:
    """
    Read DXF into a GeoDataFrame, applying bbox early when provided.
    """
    kwargs = {"engine": "pyogrio"}
    if fo and fo.bbox:
        kwargs["bbox"] = fo.bbox
    gdf = gpd.read_file(dxf_path, **kwargs)
    # Ensure a geometry column exists and drop obviously invalid rows
    if "geometry" not in gdf.columns:
        gdf = gpd.GeoDataFrame(gdf, geometry=None, crs=None)
    return gdf


# Filtering helpers
def _apply_filters(gdf: gpd.GeoDataFrame, fo: FilterOptions | None) -> gpd.GeoDataFrame:
    """
    Apply layer/regex, emptiness, size, bbox (defensive), and field value filters.
    """
    if fo is None:
        return gdf

    out = gdf

    # Layer name filtering (exact, case-insensitive; and regex on raw values)
    layer_series = out.get("Layer")
    if layer_series is not None:
        # Inclusion gate
        inc_names = set(map(str.lower, fo.include_layers or ()))
        inc_pats = [re.compile(p, re.I) for p in (fo.include_layer_patterns or ())]
        if inc_names or inc_pats:
            mask_name = layer_series.astype(str).str.lower().isin(inc_names)
            mask_pat = False
            if inc_pats:
                # Combine with OR across patterns
                pat = re.compile("|".join(p.pattern for p in inc_pats), re.I)
                mask_pat = layer_series.astype(str).str.contains(pat, na=False)
            out = out[mask_name | mask_pat]

        # Exclusion veto
        exc_names = set(map(str.lower, fo.exclude_layers or ()))
        if exc_names:
            out = out[~layer_series.astype(str).str.lower().isin(exc_names)]
        if fo.exclude_layer_patterns:
            pat_exc = re.compile("|".join(fo.exclude_layer_patterns), re.I)
            out = out[~layer_series.astype(str).str.contains(pat_exc, na=False)]

    # Drop empties
    if fo.drop_empty:
        out = out[~out.geometry.is_empty]

    # Zero-size / min thresholds
    # Note: area() is meaningful for areal geometries; length for linear.
    if fo.drop_zero_geom or fo.min_area is not None:
        # For polygons only
        is_poly = out.geom_type.str.contains("POLYGON", case=False, na=False)
        if fo.min_area is not None:
            out = out[~is_poly | (out.geometry.area >= float(fo.min_area))]
        if fo.drop_zero_geom:
            out = out[~is_poly | (out.geometry.area > 0.0)]

    if fo.drop_zero_geom or fo.min_length is not None:
        # For lineal geometries only
        is_line = out.geom_type.str.contains("LINE", case=False, na=False)
        if fo.min_length is not None:
            out = out[~is_line | (out.geometry.length >= float(fo.min_length))]
        if fo.drop_zero_geom:
            out = out[~is_line | (out.geometry.length > 0.0)]

    # Defensive bbox post-filter (Pyogrio already applied on read if provided)
    if fo.bbox:
        minx, miny, maxx, maxy = fo.bbox
        # envelope intersects box
        bounds = out.geometry.bounds  # DataFrame: minx, miny, maxx, maxy
        mask_bbox = (
            (bounds["maxx"] >= minx)
            & (bounds["minx"] <= maxx)
            & (bounds["maxy"] >= miny)
            & (bounds["miny"] <= maxy)
        )
        out = out[mask_bbox]

    # Field value exclusions
    if fo.exclude_field_values:
        tmp = out
        for col, disallowed in fo.exclude_field_values.items():
            if col in tmp.columns:
                tmp = tmp[~tmp[col].isin(disallowed)]
        out = tmp

    return out


# Export helpers
_GEOMS_CANON = ("POINT", "LINESTRING", "POLYGON", "MULTILINESTRING", "MULTIPOLYGON")


def _export_flattened_gpkg(
    gdf: gpd.GeoDataFrame, options: ExtractOptions, logger: logging.Logger
) -> None:
    """
    Export all geometries into a single flattened GeoPackage layer: 'all_geometries'.
    """
    assert options.driver_name == "GPKG", "Flattened output only supported for GPKG."
    output_path = options.output_root / "all_geometries.gpkg"
    _prepare_output_path(output_path, is_shapefile=False)
    logger.info("Exporting all geometries to %s", output_path)

    try:
        gdf.to_file(
            output_path, layer="all_geometries", driver="GPKG", engine="pyogrio"
        )
    except Exception as e:
        raise OutputCreateError(f"Failed to write flattened GPKG: {e}") from e

    if options.raise_on_error and len(gdf) == 0:
        raise ExtractError("No features written for 'all_geometries'.")


def _export_partitioned(
    gdf: gpd.GeoDataFrame, options: ExtractOptions, logger: logging.Logger
) -> None:
    """
    Export features into separate layers or files by geometry type.
    """
    is_shapefile = options.driver_name == "ESRI Shapefile"

    # Normalise requested geometry names
    requested = tuple(n.upper() for n in options.geometry_types)
    # Keep order and only those we know how to detect
    requested = tuple(n for n in requested if n in _GEOMS_CANON)

    for geometry_name in tqdm(
        requested,
        desc="Iterating over geometries",
        dynamic_ncols=True,
        leave=True,
        mininterval=0.2,
    ):
        part = _filter_by_geom_name(gdf, geometry_name)
        if part.empty:
            continue

        if is_shapefile:
            # One folder per type, one shapefile per folder
            out_dir = options.output_root / geometry_name.lower()
            out_dir.mkdir(parents=True, exist_ok=True)
            output_path = out_dir / f"{geometry_name.lower()}.shp"
            layer_kw = dict()
            # Shapefile field-name constraints
            part_to_write = _apply_shapefile_field_rules(part)
        else:
            # One GPKG file per type, layer name is the type
            output_path = options.output_root / f"{geometry_name.lower()}.gpkg"
            layer_kw = dict(layer=geometry_name.lower())
            part_to_write = part

        _prepare_output_path(output_path, is_shapefile=is_shapefile)
        try:
            part_to_write.to_file(
                output_path,
                driver=("ESRI Shapefile" if is_shapefile else "GPKG"),
                engine="pyogrio",
                **layer_kw,
            )
        except Exception as e:
            raise OutputCreateError(f"Failed to write '{geometry_name}': {e}") from e

        if options.raise_on_error and part_to_write.empty:
            raise ExtractError(f"No features written for '{geometry_name}'.")


def _filter_by_geom_name(gdf: gpd.GeoDataFrame, target: str) -> gpd.GeoDataFrame:
    """
    Return subset of gdf whose geometry type matches target (case-insensitive).
    """
    # GeoPandas/Shapely report geometry types like 'LineString', 'MultiPolygon'
    mask = gdf.geom_type.str.upper() == target.upper()
    return gdf[mask]


def _prepare_output_path(output_path: Path, *, is_shapefile: bool) -> None:
    """
    Remove existing outputs to emulate ogr2ogr overwrite semantics.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.exists():
        if is_shapefile:
            stem = output_path.stem
            for sidecar in output_path.parent.glob(stem + ".*"):
                sidecar.unlink(missing_ok=True)
        else:
            output_path.unlink(missing_ok=True)


# Field/Schema helpers for Shapefile
def _normalise_field_name(name: str) -> str:
    """Alphanumeric/underscore only; conservative for cross-compat."""
    return re.sub(r"[^A-Za-z0-9_]", "_", name)


def _make_shapefile_field_names(source_names: list[str]) -> list[str]:
    """
    Create valid, unique field names for Shapefiles from source field names.
    Ensures names are uppercase, ≤10 characters, and unique.
    """
    used = set()
    result: list[str] = []
    for raw in source_names:
        base = _normalise_field_name(raw) or "F"
        base10 = base[:10]
        candidate = base10.upper()
        i = 1
        while candidate in used or candidate == "":
            suffix = f"_{i}"
            candidate = (base10[: max(0, 10 - len(suffix))] + suffix).upper()
            i += 1
        used.add(candidate)
        result.append(candidate)
    return result


def _apply_shapefile_field_rules(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Return a copy of gdf with columns renamed to satisfy Shapefile constraints.
    Geometry column is preserved.
    """
    cols = [c for c in gdf.columns if c != gdf.geometry.name]
    new_cols = _make_shapefile_field_names(cols)
    rename_map = dict(zip(cols, new_cols, strict=False))
    if not rename_map:
        return gdf
    return gdf.rename(columns=rename_map, copy=True)
