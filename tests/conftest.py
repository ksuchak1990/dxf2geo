from __future__ import annotations

import os
from pathlib import Path
from typing import Callable, Iterable, Tuple

import pytest


# Skip conditions
def _have_gdal_dxf() -> bool:
    try:
        from osgeo import ogr  # type: ignore
    except Exception:
        return False
    return bool(ogr.GetDriverByName("DXF"))


have_gdal_dxf = pytest.mark.skipif(
    not _have_gdal_dxf(), reason="GDAL not available or DXF driver missing"
)


# Import shims to tolerate minor module layout churn
def import_api():
    """
    Try a few plausible locations for the public API mentioned in the README.
    Adjust here if your internal layout differs.
    """
    # extract_geometries + FilterOptions
    Extract = None
    Filt = None
    try:
        from dxf2geo.extract import \
            extract_geometries as Extract  # type: ignore
    except Exception:
        pass
    if Extract is None:
        from dxf2geo import extract  # type: ignore

        Extract = extract.extract_geometries  # type: ignore[attr-defined]

    # FilterOptions may be exported in several places
    try:
        from dxf2geo.extract import FilterOptions as Filt  # type: ignore
    except Exception:
        try:
            from dxf2geo.filters import FilterOptions as Filt  # type: ignore
        except Exception:
            from dxf2geo import FilterOptions as Filt  # type: ignore

    # visualise
    try:
        from dxf2geo.visualise import (load_geometries,  # type: ignore
                                       plot_geometries)
    except Exception:  # pragma: no cover - we only reach this if names moved
        load_geometries = None
        plot_geometries = None

    return Extract, Filt, load_geometries, plot_geometries


# Helper to generate tiny DXF files with OGR


def _create_layer(ds, name: str, geom_type):
    lyr = ds.CreateLayer(name, srs=None, geom_type=geom_type)
    return lyr


def _add_point(lyr, x: float, y: float):
    from osgeo import ogr  # type: ignore

    g = ogr.Geometry(ogr.wkbPoint)
    g.AddPoint(float(x), float(y))
    feat = ogr.Feature(lyr.GetLayerDefn())
    feat.SetGeometry(g)
    lyr.CreateFeature(feat)
    feat = None


def _add_linestring(lyr, coords: Iterable[Tuple[float, float]]):
    from osgeo import ogr  # type: ignore

    g = ogr.Geometry(ogr.wkbLineString)
    for x, y in coords:
        g.AddPoint(float(x), float(y))
    feat = ogr.Feature(lyr.GetLayerDefn())
    feat.SetGeometry(g)
    lyr.CreateFeature(feat)
    feat = None


def _add_polygon(lyr, ring_coords: Iterable[Tuple[float, float]]):
    from osgeo import ogr  # type: ignore

    ring = ogr.Geometry(ogr.wkbLinearRing)
    for x, y in ring_coords:
        ring.AddPoint(float(x), float(y))
    poly = ogr.Geometry(ogr.wkbPolygon)
    poly.AddGeometry(ring)
    feat = ogr.Feature(lyr.GetLayerDefn())
    feat.SetGeometry(poly)
    lyr.CreateFeature(feat)
    feat = None


@pytest.fixture(scope="session")
def make_dxf(tmp_path_factory) -> Callable[..., Path]:
    """
    Factory fixture that creates a minimal DXF with optional extras.

    Parameters (all optional, see defaults below):
      - add_zero_length_line: bool
      - add_far_feature: bool
      - small_poly: bool
    """

    def _factory(
        *,
        add_zero_length_line: bool = False,
        add_far_feature: bool = False,
        small_poly: bool = False,
    ) -> Path:
        from osgeo import ogr  # type: ignore

        d = tmp_path_factory.mktemp("dxfdata")
        dxf_path = d / "input.dxf"
        drv = ogr.GetDriverByName("DXF")
        ds = drv.CreateDataSource(str(dxf_path))  # type: ignore[arg-type]

        # Layers: roads (lines/points), buildings (polygons), tmp (short lines)
        lyr_roads = _create_layer(ds, "roads", ogr.wkbLineString)
        lyr_build = _create_layer(ds, "buildings", ogr.wkbPolygon)
        lyr_tmp = _create_layer(ds, "tmp", ogr.wkbLineString)
        lyr_points = _create_layer(ds, "survey_points", ogr.wkbPoint)

        # Roads: one long horizontal line and one diagonal
        _add_linestring(lyr_roads, [(0, 0), (100, 0)])
        _add_linestring(lyr_roads, [(0, 0), (100, 100)])
        if add_zero_length_line:
            _add_linestring(lyr_roads, [(5, 5), (5, 5)])

        if add_far_feature:
            _add_linestring(lyr_roads, [(10000, 10000), (10100, 10100)])

        # Tmp: short axis-aligned line
        _add_linestring(lyr_tmp, [(0, 0), (1, 0)])

        # Buildings: either one big or one small + one big
        if small_poly:
            _add_polygon(lyr_build, [(0, 0), (2, 0),
                         (2, 2), (0, 2), (0, 0)])  # area 4
        _add_polygon(
            lyr_build, [(10, 10), (15, 10), (15, 15), (10, 15), (10, 10)]
        )  # area 25

        # A point to exercise POINT handling
        _add_point(lyr_points, 0, 0)

        # Flush & close
        ds = None  # type: ignore[assignment]
        return dxf_path

    return _factory


@pytest.fixture
def output_dir(tmp_path) -> Path:
    return tmp_path / "output"


@pytest.fixture(scope="session")
def api():
    return import_api()
