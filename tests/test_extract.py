from pathlib import Path

import geopandas as gpd

from dxf2geo import extract


def write_minimal_dxf(path: Path) -> None:
    content = (
        "0\nSECTION\n2\nENTITIES\n"
        "0\nPOINT\n8\n0\n10\n0.0\n20\n0.0\n30\n0.0\n"
        "0\nLINE\n8\n0\n10\n0.0\n20\n0.0\n30\n0.0\n11\n1.0\n21\n1.0\n31\n0.0\n"
        "0\nENDSEC\n0\nEOF\n"
    )
    path.write_text(content, encoding="ascii")


def test_extract_writes_gpkg(tmp_path):
    dxf = tmp_path / "test.dxf"
    write_minimal_dxf(dxf)

    out_dir = tmp_path / "out"
    extract.extract_geometries(
        dxf,
        out_dir,
        flatten=True,
        output_format="GPKG",
        assume_crs=3857,  # set a CRS; DXF has none
    )

    gpkg = out_dir / "all_geometries.gpkg"
    assert gpkg.exists()

    gdf = gpd.read_file(gpkg, engine="pyogrio")
    assert not gdf.empty
    assert gdf.crs is not None
