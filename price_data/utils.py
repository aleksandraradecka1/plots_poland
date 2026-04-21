import logging
from datetime import datetime
from pathlib import Path

import geopandas as gpd


def setup_file_logger(logger: logging.Logger, class_name: str, log_dir: Path = None) -> None:
    if logger.handlers:
        return
    if log_dir is None:
        log_dir = Path(__file__).resolve().parent.parent / "logs"
    log_dir = Path(log_dir)
    log_dir.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    handler = logging.FileHandler(log_dir / f"{class_name}_{timestamp}.log", encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s  %(levelname)s  %(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


def generate_grid_tiles(
    bbox: tuple[float, float, float, float],
    grid_size: int,
) -> list[tuple[float, float, float, float]]:
    """
    Split a bounding box into grid_size × grid_size non-overlapping tiles.

    Args:
        bbox: (lat_min, lon_min, lat_max, lon_max) in EPSG:4326
        grid_size: number of subdivisions along each axis

    Returns:
        Flat list of grid_size² tuples, each (lat_min, lon_min, lat_max, lon_max)
    """
    lat_min, lon_min, lat_max, lon_max = bbox
    lat_step = (lat_max - lat_min) / grid_size
    lon_step = (lon_max - lon_min) / grid_size
    tiles = []
    for i in range(grid_size):
        for j in range(grid_size):
            tile = (
                lat_min + i * lat_step,
                lon_min + j * lon_step,
                lat_min + (i + 1) * lat_step,
                lon_min + (j + 1) * lon_step,
            )
            tiles.append(tile)
    return tiles


def fetch_tile_remote(
    tile_bbox: tuple[float, float, float, float],
    wfs_url: str,
    layer: str,
    srsname: str,
    max_features: int | None,
) -> gpd.GeoDataFrame:
    """
    Fetch a single tile from the WFS and return a GeoDataFrame.

    Creates a fresh WebFeatureService connection per call (connections are not
    serialisable across Ray workers).

    Args:
        tile_bbox:    (lat_min, lon_min, lat_max, lon_max) in EPSG:4326
        wfs_url:      WFS endpoint URL
        layer:        WFS layer name (e.g. "ms:lokale")
        srsname:      SRS name for the request (e.g. "EPSG:2180")
        max_features: Optional feature limit per tile request

    Returns:
        GeoDataFrame with features for this tile
    """
    import io
    from owslib.wfs import WebFeatureService

    wfs = WebFeatureService(url=wfs_url, version="2.0.0")
    kwargs: dict = {
        "typename": layer,
        "srsname": srsname,
        "bbox": (*tile_bbox, "EPSG:4326"),
    }
    if max_features is not None:
        kwargs["maxfeatures"] = max_features
    response = wfs.getfeature(**kwargs)
    return gpd.read_file(io.BytesIO(response.read()))
