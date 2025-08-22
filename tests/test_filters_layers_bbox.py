from __future__ import annotations

from pathlib import Path

import pytest

from .conftest import have_gdal_dxf

try:
    import geopandas as gpd  # type: ignore
except Exception:
    gpd = None


def _read_any_vector(base: Path):
    shp = base.with_suffix(".shp")
    gpkg = base.with_suffix(".gpkg")
    p = shp if shp.exists() else gpkg
    assert p.exists(), f"Vector dataset not found next to {base}"
    return gpd.read_file(p)  # type: ignore


@have_gdal_dxf
@pytest.mark.skipif(gpd is None, reason="geopandas not available")
def test_include_exclude_layers_exact(make_dxf, output_dir, api):
    extract_geometries, FilterOptions, *_ = api
    dxf_path = make_dxf()

    fo = FilterOptions(include_layers=("roads",), exclude_layers=("tmp",))
    extract_geometries(dxf_path, output_dir, filter_options=fo)

    gdf = _read_any_vector(output_dir / "linestring" / "linestring")
    assert len(gdf) == 2


@have_gdal_dxf
@pytest.mark.skipif(gpd is None, reason="geopandas not available")
def test_include_exclude_layers_regex(make_dxf, output_dir, api):
    extract_geometries, FilterOptions, *_ = api
    dxf_path = make_dxf()

    fo = FilterOptions(
        include_layer_patterns=(r"(?i)^roads?$",),
        exclude_layer_patterns=(r"(?i)^tmp$",),
    )
    extract_geometries(dxf_path, output_dir, filter_options=fo)

    gdf = _read_any_vector(output_dir / "linestring" / "linestring")
    assert len(gdf) == 2


@have_gdal_dxf
@pytest.mark.skipif(gpd is None, reason="geopandas not available")
def test_bbox_filters_out_far_features(make_dxf, output_dir, api):
    extract_geometries, FilterOptions, *_ = api
    dxf_path = make_dxf(add_far_feature=True)

    fo = FilterOptions(
        bbox=(-100.0, -100.0, 200.0, 200.0),
        # focus on the two nearby road lines; far one should drop
        include_layers=("roads",),
    )
    extract_geometries(dxf_path, output_dir, filter_options=fo)

    gdf = _read_any_vector(output_dir / "linestring" / "linestring")
    assert len(gdf) == 2

    # Safety check: all coordinates lie within the bbox
    bounds = gdf.total_bounds  # minx, miny, maxx, maxy
    assert bounds[0] >= -100 and bounds[1] >= -100
    assert bounds[2] <= 200 and bounds[3] <= 200
