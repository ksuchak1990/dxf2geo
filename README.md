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
- [License](#license)

## Installation

> **Requires GDAL installed on the system.**

Install GDAL **before** installing Python dependencies.

As an example, on Ubuntu/Debian, install GDAL using the following command:

```bash
sudo apt install gdal-bin libgdal-dev
```

Following this, we can install the package:

```bash
pip install dxf2geo
```

## Features

- Converts DXF files to Shapefile format using `ogr2ogr` (via subprocess),
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
from dxf2geo.extract import extract_geometries
from dxf2geo.visualise import (
    load_geometries,
    filter_modelspace_lines,
    remove_short_axis_aligned_lines,
    plot_geometries,
)
from pathlib import Path

input_dxf = Path("~/Downloads/example.dxf").expanduser()
output_dir = Path("output")

extract_geometries(input_dxf, output_dir)

gdf = load_geometries(output_dir)
gdf = filter_modelspace_lines(gdf)
gdf = remove_short_axis_aligned_lines(gdf)

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

## License

`dxf2geo` is distributed under the terms of the
[MIT](https://spdx.org/licenses/MIT.html) license.
