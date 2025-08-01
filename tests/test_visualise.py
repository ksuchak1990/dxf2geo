import geopandas as gpd
from shapely.geometry import Point

from dxf2geo.visualise import filter_modelspace_lines


def test_filter_modelspace():
    gdf = gpd.GeoDataFrame(
        {"geometry": [Point(0, 0), Point(1, 1)], "PaperSpace": [1.0, 0.0]}
    )
    filtered = filter_modelspace_lines(gdf)
    assert len(filtered) == 1
    assert filtered.iloc[0]["PaperSpace"] == 0.0
