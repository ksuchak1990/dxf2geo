# dxf2geo

[![PyPI - Version](https://img.shields.io/pypi/v/dxf2geo.svg)](https://pypi.org/project/dxf2geo)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/dxf2geo.svg)](https://pypi.org/project/dxf2geo)
[![Tests](https://github.com/ksuchak1990/dxf2geo/actions/workflows/test.yml/badge.svg)](https://github.com/ksuchak1990/dxf2geo/actions/workflows/test.yml)
[![Lint](https://github.com/ksuchak1990/dxf2geo/actions/workflows/clean_code.yml/badge.svg)](https://github.com/ksuchak1990/dxf2geo/actions/workflows/clean_code.yml)

> [!WARNING]  
> This package is in the early stages of development and should not be installed unless you are one of the developers.

**dxf2geo** is a small Python package for converting CAD `.dxf` files into
geospatial formats such as Shapefiles and GeoPackages, and for producing
interactive visualisations of the extracted geometry.

It is designed to automate the process of extracting geometry by type (point,
line, polygon, etc.), filtering or cleaning the results, and inspecting the
output spatially.

-----

## Table of contents

- [Installation](#installation)
- [Features](#features)
- [Example usage](#example-usage)
- [Filtering options](#layer-filtering)
- [License](#license)

## Installation

`dxf2geo` uses the GDAL Python bindings (`osgeo.*`). 
On supported platforms, `pip install dxf2geo` will pull a compatible `GDAL`
wheel from PyPI.

```bash
pip install dxf2geo
```

If your platform does not have a prebuilt GDAL wheel, install GDAL from your
system/package manager first (or via Conda), then install dxf2geo:

```
sudo apt install gdal-bin libgdal-dev
pip install dxf2geo
```

Before usage, it may be worth verifying your installation to ensure that GDAL is
installed:

```bash
python -c "from osgeo import gdal, ogr; print('GDAL', gdal.VersionInfo(), 'DXF driver:', bool(ogr.GetDriverByName('DXF')))"
```

If the installation has worked, you should see something like `GDAL ##### DXF
driver: True`.

## Features

- Converts DXF files to common vector formats (e.g. Shapefile, GeoPackage),
- Supports geometry filtering by type (e.g., LINESTRING, POLYGON),
- Skips invalid geometries,
- Visualises output geometries in an interactive Plotly-based HTML map,
- Filters out short, axis-aligned DXF gridding lines (optional cleanup step).

## Example usage

Below is an example of using the functionality of this package on a CAD file
`example.dxf`.
This creates a set of shapefiles for of the types of geometry in a new `output/`
directory.

```python
# Imports
from dxf2geo.extract import extract_geometries
from dxf2geo.visualise import (
    load_geometries,
    plot_geometries,
)
from pathlib import Path

# Define paths
input_dxf = Path("./example.dxf").expanduser()
output_dir = Path("output")

# Process CAD file
extract_geometries(input_dxf, output_dir)

# Produce `plotly` html figure
gdf = load_geometries(output_dir)
plot_geometries(gdf, output_dir / "geometry_preview.html")
```

Following this, we would have an output folder that looks like:

```
output/
├── point/
│   └── point.shp
├── linestring/
│   └── linestring.shp
├── polygon/
│   └── polygon.shp
...
└── export.log
```

## Layer filtering

At present we can filter CAD layers based on a number of different criteria.

### Layer name (exact, case-insensitive)

```python
FilterOptions(
    include_layers=("roads", "buildings"),
    exclude_layers=("defpoints", "tmp"),
)
```

### Layer name (regular expressions)

We can also filter layers using **regular expressions** (applied to the CAD
layer name).

```python
FilterOptions(
    # Include any "roads" or "road" layer (case-insensitive), and any layer starting with "bldg_"
    include_layer_patterns=(r"(?i)^roads?$", r"^bldg_.*"),
    # Exclude layers named "defpoints" (any case) or prefixed with "tmp_"
    exclude_layer_patterns=(r"(?i)^defpoints$", r"^tmp_"),
)
```

### Geometry size/structure

```python
# Drop empty geometries and zero-area/length features
FilterOptions(drop_empty=True, drop_zero_geom=True)

# Minimum polygon area
FilterOptions(min_area=5.0)

# Minimum line length
FilterOptions(min_length=10.0)
```

### Spatial bounding box

```python
# (minx, miny, maxx, maxy)
FilterOptions(bbox=(430000.0, 420000.0, 435000.0, 425000.0))
```

### Attribute-value exclusions (exact match)

```python
# Exclude features where fields have disallowed values
FilterOptions(exclude_field_values={
    "EntityType": {"TEXT", "MTEXT"},
    "Linetype": {"HIDDEN"},
})
```

### Geometry type selection (at extraction call)

```python
extract_geometries(
    dxf_path,
    output_root,
    geometry_types=("POINT", "LINESTRING", "POLYGON", "MULTILINESTRING", "MULTIPOLYGON"),
    filter_options=FilterOptions(...),
)
```

## License

`dxf2geo` is distributed under the terms of the
[MIT](https://spdx.org/licenses/MIT.html) license.
