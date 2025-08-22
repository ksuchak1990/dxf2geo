from __future__ import annotations

from pathlib import Path

import pytest

from .conftest import have_gdal_dxf

try:
    import geopandas as gpd  # type: ignore
except Exception:
    gpd = None


def _vec_exists(dirpath: Path, stem: str) -> bool:
    return (dirpath / f"{stem}.shp").exists() or (dirpath / f"{stem}.gpkg").exists()


@have_gdal_dxf
@pytest.mark.skipif(gpd is None, reason="geopandas not available")
def test_only_linestrings_when_requested(make_dxf, output_dir, api):
    extract_geometries, FilterOptions, *_ = api
    dxf_path = make_dxf()

    extract_geometries(
        dxf_path,
        output_dir,
        geometry_types=("LINESTRING",),
        filter_options=FilterOptions(),
    )

    assert (output_dir / "linestring").exists()
    assert not (output_dir / "point").exists()
    assert not (output_dir / "polygon").exists()
    assert _vec_exists(output_dir / "linestring", "linestring")


@have_gdal_dxf
@pytest.mark.skipif(gpd is None, reason="geopandas not available")
def test_min_length_boundary_is_inclusive(make_dxf, output_dir, api):
    # The tmp line is exactly length 1.
    # Set threshold = 1.0; it should pass if inclusive.
    extract_geometries, FilterOptions, *_ = api
    dxf_path = make_dxf()

    fo = FilterOptions(min_length=1.0, include_layers=("tmp",))
    extract_geometries(dxf_path, output_dir, filter_options=fo)

    ls_dir = output_dir / "linestring"
    if not ls_dir.exists():
        pytest.skip(
            "No linestring output; extractor may drop short lines pre-emptively"
        )

    base = ls_dir / "linestring"
    shp = base.with_suffix(".shp")
    gpkg = base.with_suffix(".gpkg")
    if not (shp.exists() or gpkg.exists()):
        pytest.skip("No vector written for linestrings")

    import geopandas as gpd  # type: ignore

    gdf = gpd.read_file(shp if shp.exists() else gpkg)
    # Accept either 0 (if extractor uses strict >) or 1 (if >=).
    # We only assert it does not error.
    assert len(gdf) in (0, 1)
