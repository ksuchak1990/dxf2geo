"""
Extract geometries from DXF files into GIS formats using GDAL/OGR.

This module provides tools for converting DXF vector data into structured GIS
outputs (Shapefile or GeoPackage), with optional filtering by geometry type,
layer name, spatial extent, and attribute values.

The main entry point is `extract_geometries`. The module is designed for batch
processing and supports flattened or partitioned output modes. Error handling,
field sanitisation, and logging are implemented to support robust automated use.
"""

from __future__ import annotations

import logging
import re
from contextlib import contextmanager
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from typing import Iterable, Optional, Union

from osgeo import gdal, ogr
from tqdm import tqdm

PathLike = Union[str, Path]


# Exceptions
class ExtractError(Exception):
    """Base exception for all extract-related errors."""


class InputOpenError(ExtractError):
    """Raised when a DXF file cannot be opened or contains no usable layers."""


class DriverNotFoundError(ExtractError):
    """Raised when the requested GDAL/OGR output driver is not available."""


class OutputCreateError(ExtractError):
    """Raised when an output data source or layer cannot be created."""


# Data structures
@dataclass(frozen=True)
class FilterOptions:
    """
    Options for filtering DXF features prior to export.

    Filters may be applied by layer name, geometry size, bounding box, or
    field values. All filters are optional and independently configurable.
    """

    include_layers: Optional[tuple[str, ...]] = None
    exclude_layers: Optional[tuple[str, ...]] = None
    min_area: Optional[float] = None
    min_length: Optional[float] = None
    drop_empty: bool = True
    drop_zero_geom: bool = True
    # (minx, miny, maxx, maxy)
    bbox: Optional[tuple[float, float, float, float]] = None
    # Exact-match field exclusions, e.g. {"EntityType": {"TEXT", "MTEXT"}}
    exclude_field_values: Optional[dict[str, set[str]]] = None


@dataclass(frozen=True)
class ExtractOptions:
    """
    Configuration for a single DXF-to-GIS extraction task.

    Encapsulates all parameters required for processing and exporting
    geometries from a DXF file, including input/output paths, format settings,
    geometry types to extract, and optional feature-level filters.
    """

    dxf_path: Path
    output_root: Path
    flatten: bool
    driver_name: str  # "ESRI Shapefile" or "GPKG"
    geometry_types: tuple[str, ...]
    raise_on_error: bool
    filter_options: Optional[FilterOptions] = None


@dataclass(frozen=True)
class SourceData:
    """
    Represents an open DXF source and its first layer.

    Contains the OGR dataset, primary layer, and associated spatial reference
    object (if available). Used internally during extraction.
    """

    dataset: ogr.DataSource
    layer: ogr.Layer
    spatial_ref: "ogr.osr.SpatialReference | None"


# Public functions
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
    filter_options: Optional[FilterOptions] = None,
) -> None:
    """
    Extract geometries from a DXF file and write them to GIS format outputs.

    Supports selective export by geometry type, optional flattening to a single
    layer, and configurable filtering. Outputs are written as Shapefiles or
    GeoPackages, depending on the specified format.

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
        Default is False.
    flatten : bool, optional
        If True, export all geometries into a single GeoPackage layer. Must be
        False for Shapefile output. Default is False.
    output_format : str, optional
        Output format: either "ESRI Shapefile" or "GPKG" (case-insensitive).
        Default is "ESRI Shapefile".
    filter_options : FilterOptions, optional
        Optional filters for layer names, geometry size, bounding box, or field
        values.

    Returns
    -------
    None
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

    # Make GDAL raise Python exceptions instead of silent error codes
    gdal.UseExceptions()

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

    logger.info("Opening DXF: %s", dxf_path)
    with gdal_log_to_logger(logger):
        source = _open_source(dxf_path)

        try:
            if options.flatten:
                _export_flattened(source.layer, source.spatial_ref, options, logger)
            else:
                _export_partitioned(source.layer, source.spatial_ref, options, logger)
        finally:
            # Explicitly drop GDAL handles
            source_layer = None  # noqa: F841
            source_dataset = None  # noqa: F841


# Private functions


# Setup / IO helpers
def _configure_logging(log_path: Path) -> None:
    """
    Configure file-based logging for the extraction process.

    Creates or overwrites a log file at the specified path and attaches a
    timestamped logging handler to the module-level logger.
    """
    logger = logging.getLogger("dxf2geo.extract")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    handler = logging.FileHandler(log_path, encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(handler)


def _open_source(dxf_path: Path) -> SourceData:
    """
    Open a DXF file and retrieve its first layer and spatial reference.

    Parameters
    ----------
    dxf_path : Path
        Path to the input DXF file.

    Returns
    -------
    SourceData
        An object containing the dataset, layer, and spatial reference.

    Raises
    ------
    InputOpenError
        If the DXF file cannot be opened or contains no layers.
    """
    dataset = ogr.Open(str(dxf_path), 0)  # read-only
    if dataset is None:
        raise InputOpenError(f"Failed to open DXF dataset: {dxf_path}")

    layer = dataset.GetLayer(0)
    if layer is None:
        raise InputOpenError("No layers found in DXF")

    spatial_ref = layer.GetSpatialRef()
    return SourceData(dataset=dataset, layer=layer, spatial_ref=spatial_ref)


def _get_driver(driver_name: str) -> ogr.Driver:
    """
    Retrieve an OGR driver by name.

    Parameters
    ----------
    driver_name : str
        Name of the desired GDAL/OGR driver (e.g. "GPKG", "ESRI Shapefile").

    Returns
    -------
    ogr.Driver
        The corresponding driver instance.

    Raises
    ------
    DriverNotFoundError
        If no matching driver is found.
    """
    driver = ogr.GetDriverByName(driver_name)
    if driver is None:
        raise DriverNotFoundError(f"OGR driver not found: {driver_name}")
    return driver


def _create_output_dataset(
    driver: ogr.Driver, output_path: Path, is_shapefile: bool
) -> ogr.DataSource:
    """
    Create a new OGR output data source, overwriting if it already exists.

    Parameters
    ----------
    driver : ogr.Driver
        The driver used to create the data source.
    output_path : Path
        Target file path for the output.
    is_shapefile : bool
        If True, remove sidecar files (e.g. .dbf, .shx) before creation.

    Returns
    -------
    ogr.DataSource
        A writable OGR data source.

    Raises
    ------
    OutputCreateError
        If the output file cannot be created.
    """
    # Emulate ogr2ogr overwrite behaviour
    if output_path.exists():
        if is_shapefile:
            for sidecar in output_path.parent.glob(output_path.stem + ".*"):
                sidecar.unlink(missing_ok=True)
        else:
            output_path.unlink(missing_ok=True)

    output_dataset = driver.CreateDataSource(str(output_path))
    if output_dataset is None:
        raise OutputCreateError(f"Failed to create output datasource: {output_path}")
    return output_dataset


def _copy_layer_schema(source_layer: ogr.Layer, output_layer: ogr.Layer) -> None:
    """
    Copy the field schema from a source layer to an output layer.

    Field types, widths, and precisions are preserved. Fails on first error.

    Parameters
    ----------
    source_layer : ogr.Layer
        The input layer to read field definitions from.
    output_layer : ogr.Layer
        The target layer to which fields will be added.

    Raises
    ------
    OutputCreateError
        If any field fails to be created on the output layer.
    """
    source_definition = source_layer.GetLayerDefn()
    for i in range(source_definition.GetFieldCount()):
        field_defn = source_definition.GetFieldDefn(i)
        new_field = ogr.FieldDefn(field_defn.GetName(), field_defn.GetType())
        new_field.SetWidth(field_defn.GetWidth())
        new_field.SetPrecision(field_defn.GetPrecision())
        if output_layer.CreateField(new_field) != 0:
            raise OutputCreateError(f"Failed to create field '{field_defn.GetName()}'")


# Export helpers
_GEOMETRY_NAME_TO_WKB = {
    "POINT": ogr.wkbPoint,
    "MULTIPOINT": ogr.wkbMultiPoint,
    "LINESTRING": ogr.wkbLineString,
    "MULTILINESTRING": ogr.wkbMultiLineString,
    "POLYGON": ogr.wkbPolygon,
    "MULTIPOLYGON": ogr.wkbMultiPolygon,
}


def _export_flattened(
    source_layer: ogr.Layer,
    spatial_ref,
    options: ExtractOptions,
    logger: logging.Logger,
) -> None:
    """
    Export all features into a single flattened GeoPackage layer.

    Used when `flatten=True` and the output format is GPKG. All geometries are
    written to one layer named "all_geometries".

    Parameters
    ----------
    source_layer : ogr.Layer
        The source layer containing features to export.
    spatial_ref
        The spatial reference object for the output layer.
    options : ExtractOptions
        Extraction settings including filters and paths.
    logger : logging.Logger
        Logger instance for output and error reporting.

    Raises
    ------
    OutputCreateError
        If the output layer or file cannot be created.
    ExtractError
        If no features are written and `raise_on_error` is set.
    """
    assert options.driver_name == "GPKG", "flattened output only supported for GPKG"
    driver = _get_driver(options.driver_name)

    output_path = options.output_root / "all_geometries.gpkg"
    logger.info("Exporting all geometries to %s", output_path)

    output_dataset = _create_output_dataset(driver, output_path, is_shapefile=False)
    try:
        output_layer = output_dataset.CreateLayer(
            "all_geometries", srs=spatial_ref, geom_type=ogr.wkbUnknown
        )
        if output_layer is None:
            raise OutputCreateError("Failed to create output layer 'all_geometries'")

        field_index_mapping = _copy_layer_schema_with_mapping(
            source_layer, output_layer, for_shapefile=False
        )

        written, skipped = _stream_features(
            source_layer,
            output_layer,
            logger=logger,
            filter_geometry_name=None,
            field_index_mapping=field_index_mapping,
            filter_options=options.filter_options,
        )

        logger.info("Written: %d, Skipped: %d", written, skipped)

        if options.raise_on_error and written == 0:
            raise ExtractError("No features written for 'all geometries'")
    finally:
        output_dataset = None  # flush


def _export_partitioned(
    source_layer: ogr.Layer,
    spatial_ref,
    options: ExtractOptions,
    logger: logging.Logger,
) -> None:
    """
    Export features into separate layers or files by geometry type.

    Each geometry type (e.g. "POINT", "LINESTRING") is written to a separate
    Shapefile or GeoPackage, depending on `options.driver_name`.

    Parameters
    ----------
    source_layer : ogr.Layer
        The input DXF layer.
    spatial_ref
        The spatial reference object for all outputs.
    options : ExtractOptions
        Settings controlling geometry types, filters, and output format.
    logger : logging.Logger
        Logger instance for messages and warnings.

    Raises
    ------
    OutputCreateError
        If any output layer fails to be created.
    ExtractError
        If no features are written for any geometry and `raise_on_error` is set.
    """
    driver = _get_driver(options.driver_name)
    is_shapefile = options.driver_name == "ESRI Shapefile"

    for geometry_name in tqdm(options.geometry_types, desc="Iterating over geometries"):
        geometry_wkb = _GEOMETRY_NAME_TO_WKB.get(geometry_name.upper(), ogr.wkbUnknown)

        if is_shapefile:
            output_dir = options.output_root / geometry_name.lower()
            output_dir.mkdir(parents=True, exist_ok=True)
            output_path = output_dir / f"{geometry_name.lower()}.shp"
            output_layer_name = geometry_name.lower()
        else:
            output_path = options.output_root / f"{geometry_name.lower()}.gpkg"
            output_layer_name = geometry_name.lower()

        logger.info("Exporting %s to %s", geometry_name, output_path)

        output_dataset = _create_output_dataset(
            driver, output_path, is_shapefile=is_shapefile
        )
        try:
            output_layer = output_dataset.CreateLayer(
                output_layer_name, srs=spatial_ref, geom_type=geometry_wkb
            )
            if output_layer is None:
                raise OutputCreateError(
                    f"Failed to create output layer '{output_layer_name}'"
                )

            field_index_mapping = _copy_layer_schema_with_mapping(
                source_layer,
                output_layer,
                for_shapefile=is_shapefile,
            )

            written, skipped = _stream_features(
                source_layer,
                output_layer,
                logger=logger,
                filter_geometry_name=geometry_name.upper(),
                field_index_mapping=field_index_mapping,
                filter_options=options.filter_options,
            )

            logger.info("Written %s: %d, Skipped: %d", geometry_name, written, skipped)

            if options.raise_on_error and written == 0:
                raise ExtractError(f"No features written for '{geometry_name}'")
        finally:
            output_dataset = None  # flush


# Data streaming
def _stream_features(
    source_layer: ogr.Layer,
    output_layer: ogr.Layer,
    *,
    logger: logging.Logger,
    filter_geometry_name: Optional[str],
    field_index_mapping: list[Optional[int]],
    filter_options: Optional[FilterOptions] = None,
) -> tuple[int, int]:
    """
    Stream features from the source layer to the output layer with optional filtering.

    Copies each feature's geometry and selected fields. Skips features that do
    not match the specified geometry type or filter criteria.

    Parameters
    ----------
    source_layer : ogr.Layer
        Layer to read features from.
    output_layer : ogr.Layer
        Layer to write accepted features to.
    logger : logging.Logger
        Logger for warning messages when features are skipped.
    filter_geometry_name : str, optional
        Geometry type name to filter on (e.g. "POLYGON"), or None to accept all.
    field_index_mapping : list of Optional[int]
        Mapping from source field indices to output field indices.
    filter_options : FilterOptions, optional
        Additional per-feature filters.

    Returns
    -------
    tuple[int, int]
        A tuple (written, skipped) indicating the number of features processed.
    """
    written = 0
    skipped = 0

    source_def = source_layer.GetLayerDefn()
    source_layer.ResetReading()

    for source_feature in source_layer:
        geometry = source_feature.GetGeometryRef()
        if filter_geometry_name and not _geometry_name_equals(
            geometry, filter_geometry_name
        ):
            continue
        if not _feature_allowed(source_feature, filter_options):
            continue

        output_feature = ogr.Feature(output_layer.GetLayerDefn())
        try:
            # Copy attributes using index mapping
            for i in range(source_def.GetFieldCount()):
                dest_idx = field_index_mapping[i]
                if dest_idx is None:
                    continue
                output_feature.SetField(dest_idx, source_feature.GetField(i))

            if geometry is not None:
                output_feature.SetGeometry(geometry.Clone())

            output_layer.CreateFeature(output_feature)
            written += 1
        except Exception as feature_error:
            skipped += 1
            logger.warning("Skipping a feature: %s", feature_error)
        finally:
            output_feature = None  # release handle

    return written, skipped


def _geometry_name_equals(geometry: Optional[ogr.Geometry], target_name: str) -> bool:
    """
    Check whether a geometry matches the target geometry name.

    Comparison is case-insensitive.

    Parameters
    ----------
    geometry : ogr.Geometry or None
        Geometry to test.
    target_name : str
        Expected geometry name.

    Returns
    -------
    bool
        True if names match, False otherwise.
    """
    return bool(geometry) and geometry.GetGeometryName().upper() == target_name.upper()


def _normalise_field_name(name: str) -> str:
    """
    Sanitise a field name for use in output formats.

    Replaces non-alphanumeric characters with underscores to improve
    compatibility across GIS formats.

    Parameters
    ----------
    name : str
        Raw field name.

    Returns
    -------
    str
        Sanitised field name.
    """
    # Alphanumeric + underscore, no spaces, conservative for cross-compat
    norm = re.sub(r"[^A-Za-z0-9_]", "_", name)
    return norm


def _make_shapefile_field_names(source_names: list[str]) -> list[str]:
    """
    Create valid, unique field names for Shapefiles from source field names.

    Ensures all names are uppercase, ≤10 characters, and conflict-free.
    Appends numeric suffixes if required to maintain uniqueness.

    Parameters
    ----------
    source_names : list of str
        Original field names from the source layer.

    Returns
    -------
    list of str
        Sanitised and truncated field names suitable for Shapefiles.
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


def _copy_layer_schema_with_mapping(
    source_layer: ogr.Layer,
    output_layer: ogr.Layer,
    *,
    for_shapefile: bool,
) -> list[Optional[int]]:
    """
    Copy the field schema from a source layer to an output layer.

    Produces a mapping from source field indices to destination indices,
    accounting for field name constraints. Shapefiles are restricted to
    10-character uppercase field names and must be made unique.

    Parameters
    ----------
    source_layer : ogr.Layer
        The input layer from which to read field definitions.
    output_layer : ogr.Layer
        The output layer to receive new fields.
    for_shapefile : bool
        If True, enforce Shapefile naming rules.

    Returns
    -------
    list of Optional[int]
        A mapping from source field index to output field index, or None if
        the field could not be created.

    Raises
    ------
    OutputCreateError
        If any output field fails to be created.
    """
    source_def = source_layer.GetLayerDefn()
    source_names = [
        source_def.GetFieldDefn(i).GetName() for i in range(source_def.GetFieldCount())
    ]

    if for_shapefile:
        dest_names = _make_shapefile_field_names(source_names)
    else:
        # Other drivers usually preserve names
        dest_names = source_names

    # Create fields on the output layer
    for i, src_name in enumerate(source_names):
        src_fdef = source_def.GetFieldDefn(i)
        dest_name = dest_names[i]
        new_fdef = ogr.FieldDefn(dest_name, src_fdef.GetType())
        new_fdef.SetWidth(src_fdef.GetWidth())
        new_fdef.SetPrecision(src_fdef.GetPrecision())
        if output_layer.CreateField(new_fdef) != 0:
            raise OutputCreateError(f"Failed to create field '{dest_name}'")

    # Build destination name->index after creation (driver may still adjust case)
    dest_def = output_layer.GetLayerDefn()
    dest_index_by_name = {
        dest_def.GetFieldDefn(j).GetName(): j for j in range(dest_def.GetFieldCount())
    }

    # Map source index -> destination index by our chosen dest_names (robust)
    mapping: list[Optional[int]] = []
    for dest_name in dest_names:
        mapping.append(dest_index_by_name.get(dest_name))
    return mapping


# Filtering helpers (top-level; no inner functions)
def _norm_layer(name: Optional[str]) -> str:
    """
    Normalise a layer name to lowercase with surrounding whitespace removed.

    Parameters
    ----------
    name : str or None
        Original layer name.

    Returns
    -------
    str
        Normalised layer name, or empty string if None.
    """
    return (name or "").strip().lower()


def _layer_allowed(layer_name: Optional[str], opts: Optional[FilterOptions]) -> bool:
    """
    Determine whether a feature's layer passes inclusion/exclusion filters.

    Parameters
    ----------
    layer_name : str or None
        Name of the layer the feature belongs to.
    opts : FilterOptions or None
        Filtering criteria to apply.

    Returns
    -------
    bool
        True if the layer is allowed under the specified filters.
    """
    if not opts:
        return True
    ln = _norm_layer(layer_name)
    inc = tuple(map(str.lower, opts.include_layers or ()))
    exc = tuple(map(str.lower, opts.exclude_layers or ()))
    if inc:
        if ln not in inc:
            return False
    if exc and ln in exc:
        return False
    return True


def _geom_allowed(g: Optional[ogr.Geometry], opts: Optional[FilterOptions]) -> bool:
    """
    Determine whether a geometry passes spatial and structural filters.

    Evaluates emptiness, area, length, and bounding box constraints. Rules vary
    depending on geometry type.

    Parameters
    ----------
    g : ogr.Geometry or None
        Geometry to test.
    opts : FilterOptions or None
        Filtering criteria.

    Returns
    -------
    bool
        True if the geometry satisfies all filter conditions.
    """
    if not opts:
        return True
    if not g:
        return not opts.drop_empty
    if opts.drop_empty and g.IsEmpty():
        return False

    name = (g.GetGeometryName() or "").upper()

    if "POLYGON" in name:
        area = g.GetArea()
        if opts.min_area is not None and area < opts.min_area:
            return False
        if opts.drop_zero_geom and opts.min_area is None and area == 0.0:
            return False

    if "LINE" in name:
        length = g.Length()
        if opts.min_length is not None and length < opts.min_length:
            return False
        if opts.drop_zero_geom and opts.min_length is None and length == 0.0:
            return False

    if opts.bbox:
        minx, miny, maxx, maxy = opts.bbox
        gxmin, gxmax, gymin, gymax = g.GetEnvelope()  # (minx, maxx, miny, maxy)
        if gxmax < minx or gxmin > maxx or gymax < miny or gymin > maxy:
            return False

    return True


def _fields_allowed(feat: ogr.Feature, opts: Optional[FilterOptions]) -> bool:
    """
    Determine whether a feature passes field value filters.

    Excludes features based on specific disallowed values in named fields.

    Parameters
    ----------
    feat : ogr.Feature
        Feature to test.
    opts : FilterOptions or None
        Filtering rules for attribute values.

    Returns
    -------
    bool
        True if the feature passes all field-based filters.
    """
    if not opts or not opts.exclude_field_values:
        return True
    for fld, disallowed in opts.exclude_field_values.items():
        try:
            val = feat.GetField(fld)
        except Exception:
            continue
        if val in disallowed:
            return False
    return True


def _feature_allowed(feat: ogr.Feature, opts: Optional[FilterOptions]) -> bool:
    """
    Check whether a feature should be included based on all filters.

    Applies layer name, field value, and geometry-based filtering in sequence.

    Parameters
    ----------
    feat : ogr.Feature
        Feature to evaluate.
    opts : FilterOptions or None
        Filtering options.

    Returns
    -------
    bool
        True if the feature meets all inclusion criteria.
    """
    if not opts:
        return True
    try:
        layer_name = feat.GetField("Layer")
    except Exception:
        layer_name = None
    if not _layer_allowed(layer_name, opts):
        return False
    if not _fields_allowed(feat, opts):
        return False
    return _geom_allowed(feat.GetGeometryRef(), opts)


def _gdal_handler(err_class, err_no, msg, *, logger, suppress_contains):
    """
    Custom GDAL error handler that redirects GDAL messages to the logger.

    Handles debug, warning, and error messages from GDAL. Suppresses selected
    warnings based on substring matching.

    Parameters
    ----------
    err_class : int
        GDAL error class constant (e.g. CE_Warning, CE_Failure).
    err_no : int
        GDAL error number constant (e.g. CPLE_OpenFailed).
    msg : str
        Error message text.
    logger : logging.Logger
        Logger instance to receive the output.
    suppress_contains : Iterable[str]
        Substrings used to suppress specific warning messages.
    """
    # err_class: CE_*
    # err_no:    CPLE_*
    if err_class == gdal.CE_Debug:
        logger.debug("GDAL: %s", msg)
        return

    if err_class == gdal.CE_Warning:
        if any(s in msg for s in suppress_contains):
            logger.debug("Suppressed GDAL warning: %s", msg)
        else:
            logger.warning("GDAL warning: %s", msg)
        return

    if err_class in (gdal.CE_Failure, gdal.CE_Fatal):
        # UseExceptions() will raise; still log context
        logger.error("GDAL error (%s): %s", err_no, msg)
        return

    # Fallback for CE_None or anything unexpected
    logger.info("GDAL: %s", msg)


@contextmanager
def gdal_log_to_logger(logger, suppress_contains=("Block ", "DXF: Skipping")):
    """
    Context manager that redirects GDAL errors to a Python logger.

    Installs a temporary GDAL error handler that logs messages and selectively
    suppresses known noisy warnings.

    Parameters
    ----------
    logger : logging.Logger
        Logger to capture GDAL messages.
    suppress_contains : tuple of str, optional
        Substrings of warning messages to suppress. Default includes common
        DXF and block-related messages.
    """
    handler = partial(
        _gdal_handler, logger=logger, suppress_contains=tuple(suppress_contains)
    )
    gdal.PushErrorHandler(handler)
    try:
        yield
    finally:
        gdal.PopErrorHandler()
