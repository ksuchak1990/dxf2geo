import geopandas as gpd
from shapely.geometry import LineString, Point

from dxf2geo.visualise import (
    filter_modelspace_lines,
    is_axis_aligned,
    is_short,
    remove_short_axis_aligned_lines,
)


def test_axis_aligned_detection():
    vertical = LineString([(0, 0), (0, 10)])
    horizontal = LineString([(0, 0), (10, 0)])
    diagonal = LineString([(0, 0), (10, 10)])

    assert is_axis_aligned(vertical)
    assert is_axis_aligned(horizontal)
    assert not is_axis_aligned(diagonal)


def test_short_line_detection():
    short = LineString([(0, 0), (0, 1)])
    long = LineString([(0, 0), (0, 10)])

    assert is_short(short)
    assert not is_short(long)


def test_filter_modelspace():
    gdf = gpd.GeoDataFrame(
        {"geometry": [Point(0, 0), Point(1, 1)], "PaperSpace": [1.0, 0.0]}
    )
    filtered = filter_modelspace_lines(gdf)
    assert len(filtered) == 1
    assert filtered.iloc[0]["PaperSpace"] == 0.0


def test_remove_short_axis_lines():
    gdf = gpd.GeoDataFrame(
        {
            "geometry": [
                LineString([(0, 0), (0, 1)]),  # short, axis-aligned
                LineString([(0, 0), (0, 10)]),  # long, axis-aligned
                LineString([(0, 0), (10, 10)]),  # not axis-aligned
            ],
            "geometry_type": ["LINESTRING"] * 3,
        }
    )

    result = remove_short_axis_aligned_lines(gdf)
    assert len(result) == 2  # one should be filtered
