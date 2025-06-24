from pathlib import Path

import geopandas as gpd
import pandas as pd
import plotly.graph_objects as go
from shapely.geometry import LineString


def is_axis_aligned(geom, tol=1e-6):
    if not isinstance(geom, LineString):
        return False
    x0, y0 = geom.coords[0]
    x1, y1 = geom.coords[-1]
    return (abs(x1 - x0) < tol and abs(y1 - y0) > tol) or (
        abs(y1 - y0) < tol and abs(x1 - x0) > tol
    )


def is_short(line_section, max_length=2.0):
    return line_section.length < max_length


def format_hovertext(row_entry):
    return (
        "<br>".join(
            f"{col}: {val}"
            for col, val in row_entry.items()
            if col not in ("geometry", "geometry_type", "__source__")
            and pd.notnull(val)
        )
        or " "
    )


def load_geometries(shapefile_root: Path, geometry_types=None) -> gpd.GeoDataFrame:
    if geometry_types is None:
        geometry_types = [
            "POINT",
            "LINESTRING",
            "POLYGON",
            "MULTILINESTRING",
            "MULTIPOLYGON",
        ]

    gdfs = []
    for gtype in geometry_types:
        shp_path = shapefile_root / gtype.lower() / f"{gtype.lower()}.shp"
        if shp_path.exists():
            gdf = gpd.read_file(shp_path)
            gdf["geometry_type"] = gtype
            gdf["__source__"] = shp_path.stem
            gdfs.append(gdf)

    if not gdfs:
        raise RuntimeError(f"No shapefiles found in {shapefile_root}")

    combined = gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True), crs=None)
    return combined


def filter_modelspace_lines(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    PAPER_SPACE_INDICATOR = 1.0
    return gdf.loc[gdf.get("PaperSpace", 0) != PAPER_SPACE_INDICATOR]


def remove_short_axis_aligned_lines(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    mask = (
        (gdf.geometry_type == "LINESTRING")
        & gdf.geometry.apply(is_axis_aligned)
        & gdf.geometry.apply(is_short)
    )
    return gdf.loc[~mask]


def plot_geometries(gdf: gpd.GeoDataFrame, output_html: Path) -> None:
    fig = go.Figure()
    geometry_types = gdf.geometry_type.unique()

    for geom_type in geometry_types:
        layer = gdf[gdf["geometry_type"] == geom_type]
        if layer.empty:
            continue

        if geom_type == "POINT":
            fig.add_trace(
                go.Scatter(
                    x=layer.geometry.x,
                    y=layer.geometry.y,
                    mode="markers",
                    name=geom_type,
                    marker={"size": 4},
                    text=layer.apply(format_hovertext, axis=1),
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
                    x, y = zip(*seg)
                    all_x.extend(x + (None,))
                    all_y.extend(y + (None,))
                    hovertext.extend([format_hovertext(row)] * (len(x) + 1))
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
                    x, y = zip(*poly.exterior.coords)
                    all_x.extend(x + (None,))
                    all_y.extend(y + (None,))
                    hovertext.extend([format_hovertext(row)] * (len(x) + 1))
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

    fig.write_html(str(output_html), include_plotlyjs="cdn")
