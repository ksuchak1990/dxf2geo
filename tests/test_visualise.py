from __future__ import annotations

from pathlib import Path

import pytest

from .conftest import have_gdal_dxf

try:
    import geopandas as gpd  # type: ignore
except Exception:
    gpd = None


@have_gdal_dxf
@pytest.mark.skipif(gpd is None, reason="geopandas not available")
def test_load_and_plot_visualisation(make_dxf, output_dir, api, tmp_path):
    extract_geometries, FilterOptions, load_geometries, plot_geometries = api
    if load_geometries is None or plot_geometries is None:
        pytest.skip("visualisation API not present")

    dxf_path = make_dxf()
    extract_geometries(dxf_path, output_dir)

    gdf = load_geometries(output_dir)  # expected to return a GeoDataFrame
    assert hasattr(gdf, "geometry")
    assert len(gdf) > 0

    html_out = tmp_path / "preview.html"
    plot_geometries(gdf, html_out)
    assert html_out.exists()
    # Basic sanity: Plotly embeds a <div id="..."
    assert "<div" in html_out.read_text(encoding="utf-8")
