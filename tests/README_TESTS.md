# dxf2geo test notes

- Tests create their own `.dxf` inputs with GDAL/OGR and write outputs to temporary folders.
- The suite expects the GDAL DXF driver to be available (`ogr.GetDriverByName("DXF")`), otherwise tests are skipped.
- If your package outputs GeoPackages instead of Shapefiles, the readers look for either `.shp` or `.gpkg`.
- If any public API names differ from the README examples, adjust the small import shims in `conftest.py`.
