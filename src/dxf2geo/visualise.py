from collections.abc import Iterable
from pathlib import Path

import fiona
import geopandas as gpd
import pandas as pd
import plotly.graph_objects as go


def _normalise_geom_labels(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Upper-case geometry labels to match plotting expectations.

    GeoPandas ``.geom_type`` yields labels like "Point", "MultiLineString".
    This function adds/overwrites a ``geometry_type`` column with upper-case
    equivalents ("POINT", "MULTILINESTRING", etc.).
    """
    gdf = gdf.copy()
    gdf["geometry_type"] = gdf.geom_type.str.upper()
    return gdf


def _read_shp(path: Path, source_label: str | None = None) -> gpd.GeoDataFrame:
    gdf = gpd.read_file(path)
    gdf = _normalise_geom_labels(gdf)
    gdf["__source__"] = source_label or path.stem
    return gdf


def _read_gpkg_all_layers(path: Path) -> list[gpd.GeoDataFrame]:
    gdfs: list[gpd.GeoDataFrame] = []
    for layer in fiona.listlayers(str(path)):
        gdf = gpd.read_file(path, layer=layer)
        gdf = _normalise_geom_labels(gdf)
        gdf["__source__"] = layer
        gdfs.append(gdf)
    return gdfs


def _load_from_file(file_path: Path) -> list[gpd.GeoDataFrame]:
    ext = file_path.suffix.lower()
    if ext == ".shp":
        return [_read_shp(file_path)]
    if ext == ".gpkg":
        return _read_gpkg_all_layers(file_path)
    raise ValueError(f"Unsupported file format: {file_path.name}")


def _load_from_shapefile_dir(
    root: Path, geometry_types: Iterable[str]
) -> list[gpd.GeoDataFrame]:
    """Load per-type Shapefiles from <root>/<type>/<type>.shp."""
    gdfs: list[gpd.GeoDataFrame] = []
    for gtype in geometry_types:
        shp_path = root / gtype.lower() / f"{gtype.lower()}.shp"
        if shp_path.exists():
            gdf = gpd.read_file(shp_path)
            # Here the path implies the type; keep it explicit for plotting.
            gdf["geometry_type"] = gtype
            gdf["__source__"] = shp_path.stem
            gdfs.append(gdf)
    return gdfs


def _load_from_gpkg_dir(
    root: Path, geometry_types: Iterable[str]
) -> list[gpd.GeoDataFrame]:
    """Load per-type GeoPackages from <root>/<type>.gpkg (all layers within each)."""
    gdfs: list[gpd.GeoDataFrame] = []
    for gtype in geometry_types:
        gpkg_path = root / f"{gtype.lower()}.gpkg"
        if gpkg_path.exists():
            gdfs.extend(_read_gpkg_all_layers(gpkg_path))
    return gdfs


def _fallback_scan_top_level(root: Path) -> list[gpd.GeoDataFrame]:
    """If nothing matched the patterns, pick up top-level .gpkg and .shp files."""
    gdfs: list[gpd.GeoDataFrame] = []
    for gpkg in sorted(root.glob("*.gpkg")):
        gdfs.extend(_read_gpkg_all_layers(gpkg))
    for shp in sorted(root.glob("*.shp")):
        gdfs.append(_read_shp(shp))
    return gdfs


def _coords_to_xy(seq):
    """Return two lists (xs, ys) from a sequence of 2D/3D coordinates."""
    xs, ys = [], []
    for c in seq:
        xs.append(c[0])
        ys.append(c[1])
    return xs, ys


def format_hovertext(row_entry: pd.Series) -> str:
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
    Load geometries from one of the following layouts:

    1) a single ``.shp`` or ``.gpkg`` file;
    2) a directory of per-type Shapefiles: ``<root>/<type>/<type>.shp``;
    3) a directory of per-type GeoPackages: ``<root>/<type>.gpkg``;
    4) fallback: any top-level ``.gpkg``/``.shp`` files in the directory.

    Returns a combined GeoDataFrame with columns:
    - ``geometry``
    - ``geometry_type`` (upper-cased: POINT, LINESTRING, POLYGON, MULTILINESTRING, MULTIPOLYGON)
    - ``__source__`` (file stem or GPKG layer name)
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
        # Prefer structured extractor layouts first.
        gdfs.extend(_load_from_shapefile_dir(input_path, geometry_types))
        gdfs.extend(_load_from_gpkg_dir(input_path, geometry_types))

        # If patterns found nothing, scan top level.
        if not gdfs:
            gdfs = _fallback_scan_top_level(input_path)

    else:
        raise RuntimeError(f"No valid input found at {input_path}")

    if not gdfs:
        raise RuntimeError(f"No geometries loaded from {input_path}")

    # Choose a CRS (first non-null). Users may reproject afterwards if needed.
    crs = next((g.crs for g in gdfs if g.crs is not None), None)
    gdf = gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True), crs=crs)
    return gdf


def filter_modelspace_lines(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    paper_space_indicator = 1.0
    paper = gdf.get("PaperSpace", pd.Series(0, index=gdf.index))
    paper = pd.to_numeric(paper, errors="coerce").fillna(0).astype(int)
    return gdf.loc[paper != int(bool(paper_space_indicator))]


def plot_geometries(gdf: gpd.GeoDataFrame, output_html: Path | str) -> None:
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
                    # Works for 2D/3D Points
                    xs.append(row.geometry.x)
                    ys.append(row.geometry.y)
                    hover.append(format_hovertext(row))
                else:  # MULTIPOINT
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
