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
def test_min_length_filters_short_lines(make_dxf, output_dir, api):
    extract_geometries, FilterOptions, *_ = api
    dxf_path = make_dxf()

    # Keep only lines with length >= 10.
    # Limit to 'roads' and 'tmp' layers to avoid polygon rings.
    fo = FilterOptions(min_length=10.0, include_layers=("roads", "tmp"))
    extract_geometries(dxf_path, output_dir, filter_options=fo)

    gdf = _read_any_vector(output_dir / "linestring" / "linestring")
    # Expect the two road lines; the 1-unit tmp line is filtered out.
    assert len(gdf) == 2


@have_gdal_dxf
@pytest.mark.skipif(gpd is None, reason="geopandas not available")
def test_drop_zero_length_line(make_dxf, output_dir, api):
    extract_geometries, FilterOptions, *_ = api
    dxf_path = make_dxf(add_zero_length_line=True)

    # Drop zero-length geometries. Focus on 'roads' so counts are stable.
    fo = FilterOptions(drop_zero_geom=True, include_layers=("roads",))
    extract_geometries(dxf_path, output_dir, filter_options=fo)

    gdf = _read_any_vector(output_dir / "linestring" / "linestring")
    # Two valid road lines remain; the zero-length one is removed.
    assert len(gdf) == 2


@have_gdal_dxf
@pytest.mark.skipif(gpd is None, reason="geopandas not available")
def test_min_area_polygons_if_present(make_dxf, output_dir, api):
    extract_geometries, FilterOptions, *_ = api
    dxf_path = make_dxf(small_poly=True)

    fo = FilterOptions(min_area=5.0, include_layers=("buildings",))
    extract_geometries(dxf_path, output_dir, filter_options=fo)

    poly_dir = output_dir / "polygon" / "polygon"
    if not (
        poly_dir.parent.exists()
    ):  # extractor may emit polygon rings as LINESTRINGs
        pytest.skip(
            "Polygon output not produced by extractor; area filter not applicable here"
        )

    gdf = _read_any_vector(poly_dir)
    # The 2x2 square (area 4) is filtered out; the 5x5 square remains.
    assert len(gdf) == 1
