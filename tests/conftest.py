import warnings

warnings.filterwarnings(
    "ignore",
    message="The 'shapely.geos' module is deprecated",
    category=DeprecationWarning,
    module=r"pyogrio(\.|$)",
)

warnings.filterwarnings(
    "ignore",
    message="'crs' was not provided",
    category=UserWarning,
    module=r"pyogrio\.geopandas",
)
