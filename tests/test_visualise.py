from __future__ import annotations

import pytest

try:
    import geopandas as gpd  # type: ignore
except Exception:
    gpd = None


@pytest.mark.skipif(gpd is None, reason="geopandas not available")
def test_load_and_plot_visualisation(make_dxf, output_dir, api, tmp_path):
    extract_geometries, FilterOptions, load_geometries, plot_geometries = api
    if load_geometries is None or plot_geometries is None:
        pytest.skip("visualise API not present")

    dxf_path = make_dxf()
    extract_geometries(dxf_path, output_dir)

    gdf = load_geometries(output_dir)
    assert hasattr(gdf, "geometry")
    assert isinstance(gdf, gpd.GeoDataFrame)
    assert len(gdf) > 0

    html_out = tmp_path / "preview.html"
    plot_geometries(gdf, html_out)
    assert html_out.exists()
    text = html_out.read_text(encoding="utf-8", errors="ignore")
    assert "<html" in text.lower() or "<div" in text.lower()
