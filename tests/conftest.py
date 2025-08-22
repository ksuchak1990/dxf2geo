import warnings

# Shapely 2.x deprecation touched by some pyogrio versions
warnings.filterwarnings(
    "ignore",
    message="The 'shapely.geos' module is deprecated",
    category=DeprecationWarning,
    module=r"pyogrio(\.|$)",
)

# Optional: if any path still writes without a CRS, keep tests quiet
warnings.filterwarnings(
    "ignore",
    message="'crs' was not provided",
    category=UserWarning,
    module=r"pyogrio\.geopandas",
)
