from dataclasses import dataclass, field
import io
import logging
import os
from typing import Literal, Optional

import geopandas as gpd
import pandas as pd
from owslib.wfs import WebFeatureService

from inflation_downloader import InflationDataDownloader
from utils import setup_file_logger

logger = logging.getLogger(__name__)

WFS_URL = "https://mapy.geoportal.gov.pl/wss/service/rcn"
PRICE_COL = "tran_cena_brutto"

# Available layers
LAYER_LOKALE = "ms:lokale"       # apartments / local units
LAYER_BUDYNKI = "ms:budynki"     # buildings
LAYER_DZIALKI = "ms:dzialki"     # plots / land parcels
LAYER_POWIATY = "ms:powiaty"     # counties (admin units)

SaveFormat = Literal["duckdb", "geoparquet", "both"]


@dataclass
class RCNTransactionPipeline:
    """
    Downloads RCN property transaction data from the Polish Geoportal WFS,
    cleans and preprocesses it, and saves to DuckDB and/or GeoParquet.

    Attributes:
        layer:          WFS layer to download (default: ms:lokale for apartments).
        bbox:           Bounding box as (lat_min, lon_min, lat_max, lon_max) in EPSG:4326. Defaults to Warsaw's Wilanów district.
        max_features:   Maximum number of features to fetch per request (None = no limit).
        target_crs:     CRS to reproject geometry to before saving.
        save_format:    Where to save: "duckdb", "geoparquet", or "both".
        db_path:        Path to the DuckDB file. Required when save_format is "duckdb" or "both".
        parquet_path:   Path to the GeoParquet file. Required when save_format is "geoparquet" or "both".
        process_data:   Whether to run clean() and preprocess(). Defaults to True.
    """

    layer: str = LAYER_LOKALE
    bbox: tuple = (52.155, 21.055, 52.200, 21.130)  # Warsaw, Wilanów district
    max_features: Optional[int] = None
    target_crs: str = "EPSG:2180"
    save_format: SaveFormat = "duckdb"
    db_path: Optional[str] = None
    parquet_path: Optional[str] = None
    process_data: bool = True
    inflation_csv_path: str = "price_data/inflation.csv"

    _wfs: WebFeatureService = field(default=None, init=False, repr=False)
    _hicp: pd.DataFrame = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        setup_file_logger(logger, self.__class__.__name__)
        if self.save_format in ("duckdb", "both") and not self.db_path:
            raise ValueError("db_path is required when save_format is 'duckdb' or 'both'.")
        if self.save_format in ("geoparquet", "both") and not self.parquet_path:
            raise ValueError("parquet_path is required when save_format is 'geoparquet' or 'both'.")
        logger.info(
            "RCNTransactionPipeline initialised\n"
            "  layer=%s\n  bbox=%s\n  max_features=%s\n"
            "  target_crs=%s\n  save_format=%s\n  db_path=%s\n  parquet_path=%s",
            self.layer, self.bbox, self.max_features,
            self.target_crs, self.save_format, self.db_path, self.parquet_path,
        )
        self._refresh_inflation()

    def _refresh_inflation(self) -> None:
        """Run InflationDataDownloader to ensure inflation.csv is up-to-date, then load it."""
        downloader = InflationDataDownloader(csv_path=self.inflation_csv_path)
        logger.info(
            "Inflation baseline: %04d-%02d",
            downloader.baseline_year,
            downloader.baseline_month,
        )
        try:
            downloader.run()
        except Exception as exc:
            logger.warning("Could not refresh inflation data: %s — using existing CSV", exc)
        self._hicp = pd.read_csv(self.inflation_csv_path)[["year", "month", "hicp_rebased"]]
        latest = self._hicp.dropna(subset=["hicp_rebased"]).sort_values(["year", "month"]).iloc[-1]
        logger.info(
            "HICP data loaded: %d rows, latest available: %04d-%02d (hicp_rebased=%.4f)",
            len(self._hicp), int(latest["year"]), int(latest["month"]), latest["hicp_rebased"],
        )

    def _get_hicp_value(self, year: int, month: int) -> float:
        """Return hicp_rebased for (year, month), falling back to the latest available value."""
        valid = self._hicp.dropna(subset=["hicp_rebased"])
        row = valid[(valid["year"] == year) & (valid["month"] == month)]
        if not row.empty:
            return float(row["hicp_rebased"].iloc[0])
        # transaction is newer than latest available — use the most recent value
        latest = valid.sort_values(["year", "month"]).iloc[-1]
        return float(latest["hicp_rebased"])

    def connect(self) -> None:
        logger.info("Connecting to RCN WFS at %s", WFS_URL)
        self._wfs = WebFeatureService(url=WFS_URL, version="2.0.0")

    def download(self) -> gpd.GeoDataFrame:
        if self._wfs is None:
            self.connect()

        kwargs: dict = {"typename": self.layer, "srsname": "EPSG:2180"}

        if self.bbox is not None:
            # WFS 2.0.0 + EPSG:4326 expects (lat_min, lon_min, lat_max, lon_max)
            kwargs["bbox"] = (*self.bbox, "EPSG:4326")

        if self.max_features is not None:
            kwargs["maxfeatures"] = self.max_features

        logger.info("Downloading layer '%s' (max_features=%s)", self.layer, self.max_features)
        response = self._wfs.getfeature(**kwargs)
        gdf = gpd.read_file(io.BytesIO(response.read()))
        logger.info("Downloaded %d features", len(gdf))
        return gdf

    def clean(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        n_start = len(gdf)

        gdf[PRICE_COL] = pd.to_numeric(gdf[PRICE_COL], errors="coerce")
        if "lok_pow_uzyt" in gdf.columns:
            gdf["lok_pow_uzyt"] = pd.to_numeric(gdf["lok_pow_uzyt"], errors="coerce")

        gdf = gdf.dropna(subset=["geometry", PRICE_COL])
        gdf = gdf[gdf[PRICE_COL] > 0]

        if "transaction_id" in gdf.columns:
            gdf = gdf.drop_duplicates(subset="transaction_id")

        q1 = gdf[PRICE_COL].quantile(0.25)
        q3 = gdf[PRICE_COL].quantile(0.75)
        iqr = q3 - q1
        gdf = gdf[
            (gdf[PRICE_COL] >= q1 - 3 * iqr) &
            (gdf[PRICE_COL] <= q3 + 3 * iqr)
        ]

        logger.info("Cleaned: %d → %d rows (removed %d)", n_start, len(gdf), n_start - len(gdf))
        return gdf.reset_index(drop=True)

    def preprocess(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        logger.info("Preprocessing %d features", len(gdf))

        if "tran_lokalny_id_iip" in gdf.columns and "teryt" in gdf.columns:
            gdf["transaction_id"] = gdf["teryt"].astype(str) + "_" + gdf["tran_lokalny_id_iip"].astype(str)
            logger.info("Created transaction_id from teryt + tran_lokalny_id_iip")

        if "dok_data" in gdf.columns:
            parsed = pd.to_datetime(gdf["dok_data"], utc=True)
            gdf["transaction_year"] = parsed.dt.year
            gdf["transaction_month"] = parsed.dt.month
            gdf["dok_data"] = parsed.dt.date
            logger.info("Parsed dok_data; year range: %s–%s", gdf["transaction_year"].min(), gdf["transaction_year"].max())

        if "lok_pow_uzyt" in gdf.columns:
            gdf["price_per_sqm"] = gdf[PRICE_COL] / gdf["lok_pow_uzyt"]

        # Inflation-normalised columns (requires transaction_year + transaction_month)
        if "transaction_year" in gdf.columns and "transaction_month" in gdf.columns:
            hicp_values = gdf.apply(
                lambda r: self._get_hicp_value(int(r["transaction_year"]), int(r["transaction_month"])),
                axis=1,
            )
            gdf[f"{PRICE_COL}_norm"] = gdf[PRICE_COL] * 100 / hicp_values
            logger.info("Computed %s_norm", PRICE_COL)
            if "price_per_sqm" in gdf.columns:
                gdf["price_per_sqm_norm"] = gdf["price_per_sqm"] * 100 / hicp_values
                logger.info("Computed price_per_sqm_norm")
        else:
            logger.warning(
                "transaction_year/transaction_month not available — skipping normalised columns"
            )

        wgs = gdf.geometry.to_crs("EPSG:4326")
        gdf["lon"] = wgs.x
        gdf["lat"] = wgs.y

        if gdf.crs is None or gdf.crs.to_epsg() != int(self.target_crs.split(":")[1]):
            gdf = gdf.to_crs(self.target_crs)
            logger.info("Reprojected geometry to %s", self.target_crs)

        logger.info("Preprocessing complete")
        return gdf

    def _save_duckdb(self, gdf: gpd.GeoDataFrame, table: str) -> None:
        import duckdb

        con = duckdb.connect(self.db_path)
        con.execute("INSTALL spatial; LOAD spatial;")

        df = gdf.copy()
        df["geometry"] = df["geometry"].apply(lambda g: g.wkt if g else None)
        con.register("_new_data", df)

        existing = con.execute(
            f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}'"
        ).fetchone()

        if existing is None:
            con.execute(f"CREATE TABLE {table} AS SELECT * FROM _new_data")
            logger.info("Created DuckDB table '%s' with %d rows", table, len(df))
        else:
            count_before = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            con.execute(f"""
                INSERT INTO {table}
                SELECT n.* FROM _new_data n
                LEFT JOIN {table} e ON n.transaction_id = e.transaction_id
                WHERE e.transaction_id IS NULL
            """)
            count_after = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            logger.info("Upserted %d new rows into DuckDB table '%s'", count_after - count_before, table)

        con.close()

    def _save_geoparquet(self, gdf: gpd.GeoDataFrame) -> None:
        import os

        if os.path.exists(self.parquet_path):
            existing = gpd.read_parquet(self.parquet_path)
            if "transaction_id" in existing.columns:
                known_ids = set(existing["transaction_id"])
                gdf = gdf[~gdf["transaction_id"].isin(known_ids)]
            combined = pd.concat([existing, gdf], ignore_index=True)
            combined = gpd.GeoDataFrame(combined, geometry="geometry", crs=gdf.crs)
            logger.info("Appended %d new rows to existing GeoParquet", len(gdf))
        else:
            combined = gdf
            logger.info("Created GeoParquet with %d rows", len(gdf))

        combined.to_parquet(self.parquet_path)

    def save(self, gdf: gpd.GeoDataFrame, table: str = "transactions") -> None:
        if self.save_format in ("duckdb", "both"):
            self._save_duckdb(gdf, table)
        if self.save_format in ("geoparquet", "both"):
            self._save_geoparquet(gdf)

    def run(self, table: str = "transactions") -> gpd.GeoDataFrame:
        gdf = self.download()
        if self.process_data:
            gdf = self.clean(gdf)
            gdf = self.preprocess(gdf)
        self.save(gdf, table=table)
        logger.info("Pipeline run finished.")
        return gdf


if __name__ == "__main__":
    pipeline = RCNTransactionPipeline(
        save_format="both",
        db_path="lokale_wilanow.duckdb",
        parquet_path="lokale_wilanow.parquet"
    )
    pipeline.run()
