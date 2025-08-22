"""
Visualisation utilities for DXF-derived geometries.

This module provides helpers to load geospatial data written by the extraction
pipeline into GeoPandas ``GeoDataFrame`` objects and render them as interactive
Plotly HTML files. All I/O is routed through **Pyogrio** (the default engine
in GeoPandas ≥ 1.0), ensuring a single, pip-friendly GDAL provider.

Main entry points
-----------------
- :func:`load_geometries`: Load geometries from a single file or an output
  directory structure into a combined ``GeoDataFrame``.
- :func:`filter_modelspace_lines`: Convenience filter to exclude paper space
  entities from CAD-derived data.
- :func:`plot_geometries`: Quick Plotly visualisation grouped by geometry type.

Notes
-----
- Reading and writing are performed with ``engine="pyogrio"`` for determinism.
- Layer discovery for GeoPackage files uses :func:`pyogrio.list_layers`.
"""

from collections.abc import Iterable, Sequence
from pathlib import Path

import geopandas as gpd
import pandas as pd
import plotly.graph_objects as go
from pyogrio import list_layers


def _normalise_geom_labels(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Add an upper-case geometry type label.

    Parameters
    ----------
    gdf : geopandas.GeoDataFrame
        Input frame with a ``geometry`` column.

    Returns
    -------
    geopandas.GeoDataFrame
        Copy of ``gdf`` with a new ``geometry_type`` column containing
        upper-case geometry type names (e.g., ``'LINESTRING'``).
    """
    gdf = gdf.copy()
    gdf["geometry_type"] = gdf.geom_type.str.upper()
    return gdf


def _read_shp(path: Path, source_label: str | None = None) -> gpd.GeoDataFrame:
    """
    Read a Shapefile and annotate with geometry labels and a source tag.

    Parameters
    ----------
    path : pathlib.Path
        Path to a ``.shp`` file.
    source_label : str, optional
        Value for the ``__source__`` column. Defaults to the file stem.

    Returns
    -------
    geopandas.GeoDataFrame
        Loaded frame with ``geometry_type`` and ``__source__`` columns added.
    """
    gdf = gpd.read_file(path, engine="pyogrio")
    gdf = _normalise_geom_labels(gdf)
    gdf["__source__"] = source_label or path.stem
    return gdf


def _read_gpkg_all_layers(path: Path) -> list[gpd.GeoDataFrame]:
    """
    Read all spatial layers from a GeoPackage.

    Non-spatial tables are skipped automatically.

    Parameters
    ----------
    path : pathlib.Path
        Path to a ``.gpkg`` file.

    Returns
    -------
    list of geopandas.GeoDataFrame
        One frame per spatial layer, each annotated with ``geometry_type`` and
        ``__source__`` (layer name).
    """
    gdfs: list[gpd.GeoDataFrame] = []
    for name, gtype in list_layers(path):
        if gtype is None:
            continue  # skip non-spatial tables
        gdf = gpd.read_file(path, layer=name, engine="pyogrio")
        gdf = _normalise_geom_labels(gdf)
        gdf["__source__"] = name
        gdfs.append(gdf)
    return gdfs


def _load_from_file(file_path: Path) -> list[gpd.GeoDataFrame]:
    """
    Load from a single geospatial file (Shapefile or GeoPackage).

    Parameters
    ----------
    file_path : pathlib.Path
        Input path ending with ``.shp`` or ``.gpkg``.

    Returns
    -------
    list of geopandas.GeoDataFrame
        One or more frames read from the file.

    Raises
    ------
    ValueError
        If the file extension is unsupported.
    """
    ext = file_path.suffix.lower()
    if ext == ".shp":
        return [_read_shp(file_path)]
    if ext == ".gpkg":
        return _read_gpkg_all_layers(file_path)
    raise ValueError(f"Unsupported file format: {file_path.name}")


def _load_from_shapefile_dir(
    root: Path, geometry_types: Iterable[str]
) -> list[gpd.GeoDataFrame]:
    """
    Load per-type Shapefiles from a directory structure.

    The expected layout is ``<root>/<type>/<type>.shp`` for each geometry type.

    Parameters
    ----------
    root : pathlib.Path
        Root directory containing per-type Shapefile subdirectories.
    geometry_types : Iterable[str]
        Geometry type names (e.g., ``'POINT'``, ``'LINESTRING'``).

    Returns
    -------
    list of geopandas.GeoDataFrame
        Frames for each found type; missing types are ignored.
    """
    gdfs: list[gpd.GeoDataFrame] = []
    for gtype in geometry_types:
        shp_path = root / gtype.lower() / f"{gtype.lower()}.shp"
        if shp_path.exists():
            gdf = gpd.read_file(shp_path, engine="pyogrio")
            gdf["geometry_type"] = gtype  # explicit for plotting
            gdf["__source__"] = shp_path.stem
            gdfs.append(gdf)
    return gdfs


def _load_from_gpkg_dir(
    root: Path, geometry_types: Iterable[str]
) -> list[gpd.GeoDataFrame]:
    """
    Load per-type GeoPackages from a directory.

    The expected layout is ``<root>/<type>.gpkg`` with one or more layers per
    file. All spatial layers from each file are loaded.

    Parameters
    ----------
    root : pathlib.Path
        Root directory containing per-type GeoPackages.
    geometry_types : Iterable[str]
        Geometry type names (e.g., ``'POLYGON'``, ``'MULTILINESTRING'``).

    Returns
    -------
    list of geopandas.GeoDataFrame
        Frames for all spatial layers found in the per-type GeoPackages.
    """
    gdfs: list[gpd.GeoDataFrame] = []
    for gtype in geometry_types:
        gpkg_path = root / f"{gtype.lower()}.gpkg"
        if gpkg_path.exists():
            gdfs.extend(_read_gpkg_all_layers(gpkg_path))
    return gdfs


def _fallback_scan_top_level(root: Path) -> list[gpd.GeoDataFrame]:
    """
    Fallback loader that scans a directory for top-level ``.gpkg`` and ``.shp`` files.

    Parameters
    ----------
    root : pathlib.Path
        Directory to scan.

    Returns
    -------
    list of geopandas.GeoDataFrame
        Frames read from all discovered files.
    """
    gdfs: list[gpd.GeoDataFrame] = []
    for gpkg in sorted(root.glob("*.gpkg")):
        gdfs.extend(_read_gpkg_all_layers(gpkg))
    for shp in sorted(root.glob("*.shp")):
        gdfs.append(_read_shp(shp))
    return gdfs


def _coords_to_xy(seq: Sequence[Sequence[float]]) -> tuple[list[float], list[float]]:
    """
    Split a coordinate sequence into separate X and Y lists.

    Parameters
    ----------
    seq : Sequence[Sequence[float]]
        Iterable of 2D or 3D coordinates (e.g., ``[(x, y), ...]``).

    Returns
    -------
    (list of float, list of float)
        Two lists: ``(xs, ys)``.
    """
    xs, ys = [], []
    for c in seq:
        xs.append(c[0])
        ys.append(c[1])
    return xs, ys


def format_hovertext(row_entry: pd.Series) -> str:
    """
    Construct a multi-line hover label from non-geometry attributes.

    Parameters
    ----------
    row_entry : pandas.Series
        Row of attributes including a Shapely geometry.

    Returns
    -------
    str
        HTML string with ``<br>`` separators. Geometry/meta columns are omitted.
    """
    return (
        "<br>".join(
            f"{col}: {val}"
            for col, val in row_entry.items()
            if col not in ("geometry", "geometry_type", "__source__")
            and pd.notnull(val)
        )
        or " "
    )


def load_geometries(
    input_path: Path | str, geometry_types: Iterable[str] | None = None
) -> gpd.GeoDataFrame:
    """
    Load geometries from a file or an extraction output directory.

    Supported inputs
    ----------------
    1. Single file:
       - ``.shp`` (Shapefile)
       - ``.gpkg`` (GeoPackage; all spatial layers are loaded)
    2. Directory layouts produced by the extractor:
       - Per-type Shapefiles: ``<root>/<type>/<type>.shp``
       - Per-type GeoPackages: ``<root>/<type>.gpkg``
       - Fallback: any top-level ``.gpkg``/``.shp`` files in ``<root>``

    Parameters
    ----------
    input_path : path-like
        Path to a file or directory as described above.
    geometry_types : Iterable[str], optional
        Geometry type names used to probe per-type layouts. If omitted, a
        standard set is used (``POINT``, ``LINESTRING``, ``POLYGON``,
        ``MULTILINESTRING``, ``MULTIPOLYGON``).

    Returns
    -------
    geopandas.GeoDataFrame
        Combined frame of all loaded features. The first non-null CRS found
        amongst inputs is applied to the result. Contains helper columns
        ``geometry_type`` and ``__source__``.

    Raises
    ------
    RuntimeError
        If no valid input can be found or no geometries are loaded.

    Notes
    -----
    - Reads use ``engine="pyogrio"`` to ensure consistent GDAL handling.
    - ``__source__`` holds the file stem (Shapefile) or layer name (GeoPackage).
    """
    input_path = Path(input_path).expanduser().resolve()
    if geometry_types is None:
        geometry_types = (
            "POINT",
            "LINESTRING",
            "POLYGON",
            "MULTILINESTRING",
            "MULTIPOLYGON",
        )

    gdfs: list[gpd.GeoDataFrame] = []

    if input_path.is_file():
        gdfs = _load_from_file(input_path)
    elif input_path.is_dir():
        gdfs.extend(_load_from_shapefile_dir(input_path, geometry_types))
        gdfs.extend(_load_from_gpkg_dir(input_path, geometry_types))
        if not gdfs:
            gdfs = _fallback_scan_top_level(input_path)
    else:
        raise RuntimeError(f"No valid input found at {input_path}")

    if not gdfs:
        raise RuntimeError(f"No geometries loaded from {input_path}")

    crs = next((g.crs for g in gdfs if g.crs is not None), None)
    gdf = gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True), crs=crs)
    return gdf


def filter_modelspace_lines(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Remove paper-space linework from CAD-derived layers.

    Many DXF/GPKG conversions preserve a ``PaperSpace`` attribute (0/1 or
    equivalent). This helper keeps entities that are **not** in paper space.

    Parameters
    ----------
    gdf : geopandas.GeoDataFrame
        Input frame with a ``PaperSpace`` column (numeric or coercible).

    Returns
    -------
    geopandas.GeoDataFrame
        Subset of ``gdf`` where ``PaperSpace != 1``.
    """
    paper_space_indicator = 1.0
    paper = gdf.get("PaperSpace", pd.Series(0, index=gdf.index))
    paper = pd.to_numeric(paper, errors="coerce").fillna(0).astype(int)
    return gdf.loc[paper != int(bool(paper_space_indicator))]


def plot_geometries(gdf: gpd.GeoDataFrame, output_html: Path | str) -> None:
    """
    Plot geometries into an interactive HTML file using Plotly.

    The output groups features by ``geometry_type`` and draws points, lines,
    and polygon exteriors with simple defaults. Non-geometry columns are
    rendered as hover text.

    Parameters
    ----------
    gdf : geopandas.GeoDataFrame
        Input features. Must contain ``geometry`` and ``geometry_type`` columns.
    output_html : path-like
        Destination path for the generated HTML file.

    Returns
    -------
    None

    Notes
    -----
    - The figure uses an equal-scale axis (``yaxis_scaleanchor='x'``).
    - ``plotly`` is used directly without specifying a theme to keep
      dependencies light.
    """
    fig = go.Figure()
    geometry_types = gdf.geometry_type.unique()

    for geom_type in geometry_types:
        layer = gdf[gdf["geometry_type"] == geom_type]
        if layer.empty:
            continue
        elif geom_type in {"POINT", "MULTIPOINT"}:
            xs, ys, hover = [], [], []
            for _, row in layer.iterrows():
                if geom_type == "POINT":
                    xs.append(row.geometry.x)
                    ys.append(row.geometry.y)
                    hover.append(format_hovertext(row))
                else:
                    for pt in row.geometry.geoms:
                        xs.append(pt.x)
                        ys.append(pt.y)
                        hover.append(format_hovertext(row))
            fig.add_trace(
                go.Scatter(
                    x=xs,
                    y=ys,
                    mode="markers",
                    name=geom_type,
                    marker={"size": 4},
                    text=hover,
                    hoverinfo="text",
                )
            )
        elif geom_type in {"LINESTRING", "MULTILINESTRING"}:
            all_x, all_y, hovertext = [], [], []
            for _, row in layer.iterrows():
                segments = (
                    [row.geometry.coords]
                    if geom_type == "LINESTRING"
                    else [line.coords for line in row.geometry.geoms]
                )
                for seg in segments:
                    xs, ys = _coords_to_xy(seg)
                    all_x.extend(xs + [None])
                    all_y.extend(ys + [None])
                    hovertext.extend([format_hovertext(row)] * (len(xs) + 1))
            fig.add_trace(
                go.Scatter(
                    x=all_x,
                    y=all_y,
                    mode="lines",
                    name=geom_type,
                    text=hovertext,
                    hoverinfo="text",
                    line={"width": 1},
                )
            )
        elif geom_type in {"POLYGON", "MULTIPOLYGON"}:
            all_x, all_y, hovertext = [], [], []
            for _, row in layer.iterrows():
                polys = [row.geometry] if geom_type == "POLYGON" else row.geometry.geoms
                for poly in polys:
                    xs, ys = _coords_to_xy(poly.exterior.coords)
                    all_x.extend(xs + [None])
                    all_y.extend(ys + [None])
                    hovertext.extend([format_hovertext(row)] * (len(xs) + 1))
            fig.add_trace(
                go.Scatter(
                    x=all_x,
                    y=all_y,
                    mode="lines",
                    name=geom_type,
                    text=hovertext,
                    hoverinfo="text",
                    fill="toself",
                    opacity=0.4,
                )
            )

    fig.update_layout(
        xaxis_title="X",
        yaxis_title="Y",
        legend_title="Geometry Type",
        autosize=True,
        showlegend=True,
        yaxis_scaleanchor="x",
    )

    output_html = Path(output_html)
    fig.write_html(str(output_html), include_plotlyjs="cdn")
