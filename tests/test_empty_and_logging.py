from __future__ import annotations

from pathlib import Path

from .conftest import have_gdal_dxf

try:
    import geopandas as gpd  # type: ignore
except Exception:
    gpd = None


def _vec_exists(dirpath: Path, stem: str) -> bool:
    return (dirpath / f"{stem}.shp").exists() or (dirpath / f"{stem}.gpkg").exists()


@have_gdal_dxf
def test_filters_can_yield_empty_but_not_error(make_dxf, output_dir, api):
    extract_geometries, FilterOptions, *_ = api
    dxf_path = make_dxf()

    # Use an include that matches nothing
    fo = FilterOptions(include_layers=("does_not_exist",))
    extract_geometries(dxf_path, output_dir, filter_options=fo)

    # No crash; either no subdirs, or present but with zero features.
    # Don’t assert presence of files
    # Just ensure the call succeeded and output root exists.
    assert output_dir.exists()


@have_gdal_dxf
def test_export_log_if_written(make_dxf, output_dir, api):
    extract_geometries, FilterOptions, *_ = api
    dxf_path = make_dxf()

    extract_geometries(dxf_path, output_dir)
    log = output_dir / "export.log"
    # Some environments may not emit a log; tolerate absence.
    # If present, it should be non-empty.
    if log.exists():
        text = log.read_text(encoding="utf-8", errors="ignore")
        assert len(text.strip()) > 0
