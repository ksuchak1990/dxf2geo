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
def test_layer_regex_is_case_insensitive(make_dxf, output_dir, api):
    extract_geometries, FilterOptions, *_ = api
    dxf_path = make_dxf()

    # Include "roads" regardless of case
    fo = FilterOptions(include_layer_patterns=(r"(?i)^RoAdS$",))
    extract_geometries(dxf_path, output_dir, filter_options=fo)

    gdf = _read_any_vector(output_dir / "linestring" / "linestring")
    # Two road lines expected
    assert len(gdf) == 2
    assert set(gdf["LAYER"].str.lower()) == {"roads"}


@have_gdal_dxf
@pytest.mark.skipif(gpd is None, reason="geopandas not available")
def test_exclude_wins_when_both_include_and_exclude_set(make_dxf, output_dir, api):
    extract_geometries, FilterOptions, *_ = api
    dxf_path = make_dxf()

    # Explicitly include tmp, but also exclude it; exclusion should win
    fo = FilterOptions(include_layers=("tmp",), exclude_layers=("tmp",))
    extract_geometries(dxf_path, output_dir, filter_options=fo)

    # Either no linestring dir, or present but empty after filtering.
    ls_dir = output_dir / "linestring"
    if not ls_dir.exists():
        return
    base = ls_dir / "linestring"
    shp = base.with_suffix(".shp")
    gpkg = base.with_suffix(".gpkg")
    if shp.exists() or gpkg.exists():
        gdf = _read_any_vector(base)
        assert len(gdf) == 0
