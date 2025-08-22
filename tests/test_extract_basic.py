from __future__ import annotations

from .conftest import have_gdal_dxf


@have_gdal_dxf
def test_extract_creates_expected_structure(make_dxf, output_dir, api):
    extract_geometries, FilterOptions, *_ = api
    dxf_path = make_dxf()

    extract_geometries(dxf_path, output_dir)

    # Must have linestring output; others are optional depending on DXF entity mapping.
    ls_dir = output_dir / "linestring"
    assert ls_dir.is_dir(), "Missing linestring output directory"
    shp = ls_dir / "linestring.shp"
    gpkg = ls_dir / "linestring.gpkg"
    assert shp.exists() or gpkg.exists(), "No vector written for linestrings"

    # If optional dirs exist, they must contain a vector dataset.
    for name in ("point", "polygon"):
        sub = output_dir / name
        if sub.is_dir():
            shp = sub / f"{name}.shp"
            gpkg = sub / f"{name}.gpkg"
            assert shp.exists() or gpkg.exists(), f"No vector written for {name}"


@have_gdal_dxf
def test_geometry_type_selection(make_dxf, output_dir, api):
    extract_geometries, FilterOptions, *_ = api
    dxf_path = make_dxf()

    # Request only LINESTRING to verify filtering behaviour.
    extract_geometries(
        dxf_path,
        output_dir,
        geometry_types=("LINESTRING",),
        filter_options=FilterOptions(),
    )

    assert (output_dir / "linestring").exists()
    # These should not be created when only LINESTRING is requested.
    assert not (output_dir / "point").exists()
    assert not (output_dir / "polygon").exists()
