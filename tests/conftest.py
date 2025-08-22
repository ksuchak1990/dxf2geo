from __future__ import annotations

from collections.abc import Callable, Iterable
from pathlib import Path

import pytest

try:
    from osgeo import ogr  # type: ignore
except ImportError:
    ogr = None

try:
    import ezdxf  # type: ignore
except ImportError:
    ezdxf = None


def have_gdal_dxf_driver() -> bool:
    if ogr is None:
        return False
    return ogr.GetDriverByName("DXF") is not None


def gdal_can_write_dxf() -> bool:
    if not have_gdal_dxf_driver():
        return False
    drv = ogr.GetDriverByName("DXF")
    try:
        return bool(drv.TestCapability("CreateDataSource"))
    except Exception:
        return False


have_gdal_dxf = pytest.mark.skipif(
    not have_gdal_dxf_driver(), reason="GDAL not available or DXF driver missing"
)


def _create_layer(ds, name: str, geom_type):
    return ds.CreateLayer(name, srs=None, geom_type=geom_type)


def _add_point(lyr, x: float, y: float):
    g = ogr.Geometry(ogr.wkbPoint)
    g.AddPoint(float(x), float(y))
    feat = ogr.Feature(lyr.GetLayerDefn())
    feat.SetGeometry(g)
    lyr.CreateFeature(feat)


def _add_linestring(lyr, coords: Iterable[tuple[float, float]]):
    g = ogr.Geometry(ogr.wkbLineString)
    for x, y in coords:
        g.AddPoint(float(x), float(y))
    feat = ogr.Feature(lyr.GetLayerDefn())
    feat.SetGeometry(g)
    lyr.CreateFeature(feat)


def _add_polygon(lyr, ring_coords: Iterable[tuple[float, float]]):
    ring = ogr.Geometry(ogr.wkbLinearRing)
    for x, y in ring_coords:
        ring.AddPoint(float(x), float(y))
    poly = ogr.Geometry(ogr.wkbPolygon)
    poly.AddGeometry(ring)
    feat = ogr.Feature(lyr.GetLayerDefn())
    feat.SetGeometry(poly)
    lyr.CreateFeature(feat)


def write_dxf_with_gdal(
    dxf_path: Path,
    *,
    add_zero_length_line: bool = False,
    add_far_feature: bool = False,
    small_poly: bool = False,
) -> None:
    drv = ogr.GetDriverByName("DXF")
    ds = drv.CreateDataSource(str(dxf_path))
    if ds is None:
        raise RuntimeError("GDAL DXF driver returned None from CreateDataSource")

    lyr_roads = _create_layer(ds, "roads", ogr.wkbLineString)
    lyr_build = _create_layer(ds, "buildings", ogr.wkbPolygon)
    lyr_tmp = _create_layer(ds, "tmp", ogr.wkbLineString)
    lyr_points = _create_layer(ds, "survey_points", ogr.wkbPoint)
    if not all([lyr_roads, lyr_build, lyr_tmp, lyr_points]):
        ds = None  # close before bailing
        raise RuntimeError("GDAL DXF driver failed to create one or more layers")

    _add_linestring(lyr_roads, [(0, 0), (100, 0)])
    _add_linestring(lyr_roads, [(0, 0), (100, 100)])
    if add_zero_length_line:
        _add_linestring(lyr_roads, [(5, 5), (5, 5)])
    if add_far_feature:
        _add_linestring(lyr_roads, [(10000, 10000), (10100, 10100)])

    _add_linestring(lyr_tmp, [(0, 0), (1, 0)])

    if small_poly:
        _add_polygon(lyr_build, [(0, 0), (2, 0), (2, 2), (0, 2), (0, 0)])
    _add_polygon(lyr_build, [(10, 10), (15, 10), (15, 15), (10, 15), (10, 10)])

    _add_point(lyr_points, 0, 0)
    ds = None  # flush/close


def write_dxf_with_ezdxf(
    dxf_path: Path,
    *,
    add_zero_length_line=False,
    add_far_feature=False,
    small_poly=False,
) -> None:
    doc = ezdxf.new(setup=True)
    msp = doc.modelspace()

    for name in ("roads", "buildings", "tmp", "survey_points"):
        if name not in doc.layers:
            doc.layers.add(name)

    msp.add_lwpolyline([(0, 0), (100, 0)], dxfattribs={"layer": "roads"})
    msp.add_lwpolyline([(0, 0), (100, 100)], dxfattribs={"layer": "roads"})
    if add_zero_length_line:
        msp.add_lwpolyline([(5, 5), (5, 5)], dxfattribs={"layer": "roads"})
    if add_far_feature:
        msp.add_lwpolyline(
            [(10000, 10000), (10100, 10100)], dxfattribs={"layer": "roads"}
        )

    msp.add_lwpolyline([(0, 0), (1, 0)], dxfattribs={"layer": "tmp"})

    if small_poly:
        msp.add_lwpolyline(
            [(0, 0), (2, 0), (2, 2), (0, 2), (0, 0)],
            close=True,
            dxfattribs={"layer": "buildings"},
        )
    msp.add_lwpolyline(
        [(10, 10), (15, 10), (15, 15), (10, 15), (10, 10)],
        close=True,
        dxfattribs={"layer": "buildings"},
    )

    msp.add_point((0, 0), dxfattribs={"layer": "survey_points"})
    doc.saveas(str(dxf_path))


@pytest.fixture(scope="session")
def make_dxf(tmp_path_factory) -> Callable[..., Path]:
    def _factory(
        *, add_zero_length_line=False, add_far_feature=False, small_poly=False
    ) -> Path:
        d = tmp_path_factory.mktemp("dxfdata")
        dxf_path = d / "input.dxf"

        tried_gdal = False
        if gdal_can_write_dxf():
            tried_gdal = True
            try:
                write_dxf_with_gdal(
                    dxf_path,
                    add_zero_length_line=add_zero_length_line,
                    add_far_feature=add_far_feature,
                    small_poly=small_poly,
                )
                return dxf_path
            except Exception:
                # Clean up partial file so ezdxf can safely overwrite
                try:
                    # Python 3.8+: wrap in try/except if needed
                    dxf_path.unlink(missing_ok=True)
                except Exception:
                    pass  # best effort; ezdxf will overwrite anyway

        if ezdxf is not None:
            write_dxf_with_ezdxf(
                dxf_path,
                add_zero_length_line=add_zero_length_line,
                add_far_feature=add_far_feature,
                small_poly=small_poly,
            )
            return dxf_path

        reason = (
            "GDAL DXF is unusable (read-only or broken) and ezdxf not installed"
            if tried_gdal
            else "GDAL not available and ezdxf not installed"
        )
        pytest.skip(f"Cannot create DXF: {reason}")

    return _factory


@pytest.fixture
def output_dir(tmp_path) -> Path:
    return tmp_path / "output"


def import_api():
    try:
        from dxf2geo.extract import extract_geometries  # type: ignore
    except Exception:
        from dxf2geo import extract as _extract  # type: ignore

        extract_geometries = _extract.extract_geometries

    try:
        from dxf2geo.extract import FilterOptions  # type: ignore
    except Exception:
        try:
            from dxf2geo.filters import FilterOptions  # type: ignore
        except Exception:
            from dxf2geo import FilterOptions  # type: ignore

    try:
        from dxf2geo.visualise import load_geometries, plot_geometries  # type: ignore
    except Exception:
        load_geometries = None
        plot_geometries = None

    return extract_geometries, FilterOptions, load_geometries, plot_geometries


@pytest.fixture(scope="session")
def api():
    return import_api()
