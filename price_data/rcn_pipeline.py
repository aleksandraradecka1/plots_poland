from dataclasses import dataclass, field
import io
import logging
import os
from pathlib import Path
from typing import Literal, Optional

import geopandas as gpd
import pandas as pd
from owslib.wfs import WebFeatureService

from inflation_downloader import InflationDataDownloader
from utils import setup_file_logger, generate_grid_tiles, fetch_tile_remote

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
        bbox:           Bounding box as (lat_min, lon_min, lat_max, lon_max) in EPSG:4326. Defaults to Warsaw's Mokotów district.
                        Ignored if area_file is provided.
        area_file:      Path to geospatial file (GeoJSON, GeoParquet, Shapefile) defining the analysis area.
                        If provided, convex hull is used for download, then results are clipped to exact geometry.
        max_features:   Maximum number of features to fetch per request (None = no limit).
        target_crs:     CRS to reproject geometry to before saving.
        save_format:    Where to save: "duckdb", "geoparquet", or "both".
        db_path:        Path to the DuckDB file. Required when save_format is "duckdb" or "both".
        parquet_path:   Path to the GeoParquet file. Required when save_format is "geoparquet" or "both".
        process_data:   Whether to run clean() and preprocess(). Defaults to True.
    """

    layer: str = LAYER_LOKALE
    bbox: Optional[tuple] = (52.125, 20.950, 52.245, 21.070)  # Warsaw, Mokotów district
    area_file: Optional[str] = None
    max_features: Optional[int] = None
    target_crs: str = "EPSG:2180"
    save_format: SaveFormat = "duckdb"
    db_path: Optional[str] = None
    parquet_path: Optional[str] = None
    process_data: bool = True
    download_mode: Literal["sequential", "parallel"] = "sequential"
    grid_size: int = 4
    cpu_fraction: float = 0.5
    inflation_csv_path: str = str(Path(__file__).parent / "inflation.csv")

    _wfs: WebFeatureService = field(default=None, init=False, repr=False)
    _hicp: pd.DataFrame = field(default=None, init=False, repr=False)
    _area_geom: gpd.GeoDataFrame = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        setup_file_logger(logger, self.__class__.__name__, log_dir=Path(__file__).parent / "logs")
        if self.download_mode not in ("sequential", "parallel"):
            raise ValueError(
                f"download_mode must be 'sequential' or 'parallel', got {self.download_mode!r}"
            )
        if self.grid_size < 1:
            raise ValueError(f"grid_size must be >= 1, got {self.grid_size}")
        if self.cpu_fraction <= 0.0 or self.cpu_fraction > 1.0:
            raise ValueError(
                f"cpu_fraction must be in (0.0, 1.0], got {self.cpu_fraction}"
            )
        if self.save_format in ("duckdb", "both") and not self.db_path:
            raise ValueError("db_path is required when save_format is 'duckdb' or 'both'.")
        if self.save_format in ("geoparquet", "both") and not self.parquet_path:
            raise ValueError("parquet_path is required when save_format is 'geoparquet' or 'both'.")
        if self.area_file is not None:
            self._load_area_file()
        logger.info(
            "RCNTransactionPipeline initialised\n"
            "  layer=%s\n  bbox=%s\n  area_file=%s\n  max_features=%s\n"
            "  target_crs=%s\n  save_format=%s\n  db_path=%s\n  parquet_path=%s\n"
            "  download_mode=%s\n  grid_size=%s\n  cpu_fraction=%s",
            self.layer, self.bbox, self.area_file, self.max_features,
            self.target_crs, self.save_format, self.db_path, self.parquet_path,
            self.download_mode, self.grid_size, self.cpu_fraction,
        )
        self._refresh_inflation()

    def _load_area_file(self) -> None:
        """Load area geometry from a geospatial file and derive bbox in EPSG:4326 from its convex hull."""
        path = Path(self.area_file)
        if not path.exists():
            raise FileNotFoundError(f"area_file not found: {self.area_file}")

        suffix = path.suffix.lower()
        if suffix == ".parquet":
            area = gpd.read_parquet(self.area_file)
        else:
            # covers .geojson, .json, .shp, .gpkg, etc.
            area = gpd.read_file(self.area_file)

        if area.crs is None:
            raise ValueError(
                f"area_file '{self.area_file}' has no CRS defined. "
                "Please set the CRS before using it as an analysis area."
            )

        # Reproject to EPSG:4326 for WFS bbox and to target_crs for clipping
        area_4326 = area.to_crs("EPSG:4326")
        self._area_geom = area.to_crs(self.target_crs)

        # Derive bbox from convex hull bounds: (lat_min, lon_min, lat_max, lon_max)
        hull = area_4326.union_all().convex_hull
        minx, miny, maxx, maxy = hull.bounds  # (lon_min, lat_min, lon_max, lat_max)
        self.bbox = (miny, minx, maxy, maxx)
        logger.info(
            "Loaded area_file '%s' (%d features, CRS=%s) → bbox=%s",
            self.area_file, len(area), area.crs.to_string(), self.bbox,
        )

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

    def _parallel_download(self) -> gpd.GeoDataFrame:
        """Download tiles in parallel using Ray. Lazy-imports ray and tqdm."""
        try:
            import ray
            from tqdm import tqdm
        except ImportError as exc:
            raise ImportError(
                f"Parallel download requires 'ray' and 'tqdm'. "
                f"Install them with: pip install ray tqdm\n(original error: {exc})"
            ) from exc

        worker_count = max(1, int(os.cpu_count() * self.cpu_fraction))
        tiles = generate_grid_tiles(self.bbox, self.grid_size)
        logger.info(
            "Parallel download: grid=%dx%d, tiles=%d, bbox=%s, workers=%d",
            self.grid_size, self.grid_size, len(tiles), self.bbox, worker_count,
        )

        try:
            ray.init(num_cpus=worker_count, ignore_reinit_error=True)
        except Exception as exc:
            logger.error("Ray initialisation failed: %s", exc)
            raise

        # Apply @ray.remote decorator at runtime so module stays importable without ray
        remote_fn = ray.remote(fetch_tile_remote)

        futures = [
            remote_fn.remote(tile, WFS_URL, self.layer, "EPSG:2180", self.max_features)
            for tile in tiles
        ]

        successful_gdfs: list[gpd.GeoDataFrame] = []
        remaining = list(futures)

        with tqdm(total=len(tiles), desc="Downloading tiles") as pbar:
            while remaining:
                done, remaining = ray.wait(remaining, num_returns=1)
                ref = done[0]
                try:
                    gdf_tile = ray.get(ref)
                    logger.info(
                        "Tile fetched: %d features", len(gdf_tile)
                    )
                    successful_gdfs.append(gdf_tile)
                except Exception as exc:
                    logger.warning("Tile failed: %s", exc)
                pbar.update(1)

        if not successful_gdfs:
            logger.warning("All tiles failed — returning empty GeoDataFrame")
            return gpd.GeoDataFrame()

        merged = pd.concat(successful_gdfs, ignore_index=True)
        n_before = len(merged)
        # Deduplicate by the natural transaction key (teryt + tran_lokalny_id_iip) —
        # two transactions can share the same geometry (e.g. same building).
        if "tran_lokalny_id_iip" in merged.columns and "teryt" in merged.columns:
            merged["_dedup_key"] = merged["teryt"].astype(str) + "_" + merged["tran_lokalny_id_iip"].astype(str)
            merged = merged.drop_duplicates(subset="_dedup_key").drop(columns="_dedup_key")
        else:
            logger.warning("teryt/tran_lokalny_id_iip columns not found — skipping deduplication")
        merged = gpd.GeoDataFrame(merged, geometry="geometry", crs=successful_gdfs[0].crs)
        logger.info("Merged tiles: %d features before dedup, %d after", n_before, len(merged))

        # Reproject to target_crs if needed
        if merged.crs is not None and merged.crs.to_string() != self.target_crs:
            merged = merged.to_crs(self.target_crs)

        # Apply area clip if area_file was provided
        if self._area_geom is not None:
            n_before_clip = len(merged)
            if merged.crs != self._area_geom.crs:
                merged = merged.to_crs(self._area_geom.crs)
            clip_geom = self._area_geom.union_all()
            merged = merged[merged.intersects(clip_geom)]
            merged = gpd.GeoDataFrame(merged, geometry="geometry", crs=self._area_geom.crs)
            logger.info("Clipped to area_file geometry: %d → %d features", n_before_clip, len(merged))

        return merged

    def download(self) -> gpd.GeoDataFrame:
        if self.download_mode == "parallel":
            return self._parallel_download()
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

        # Clip to exact area geometry if area_file was provided
        if self._area_geom is not None:
            n_before = len(gdf)
            # Ensure same CRS for clipping
            if gdf.crs != self._area_geom.crs:
                gdf = gdf.to_crs(self._area_geom.crs)
            # Clip to the union of all area geometries
            clip_geom = self._area_geom.union_all()
            gdf = gdf[gdf.intersects(clip_geom)]
            logger.info("Clipped to area_file geometry: %d → %d features", n_before, len(gdf))

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

        if "dok_data" in gdf.columns:
            parsed = pd.to_datetime(gdf["dok_data"], errors="coerce", utc=True)
            today = pd.Timestamp.now(tz="UTC")
            earliest_before = parsed.min()
            mask = (
                parsed.notna() &
                parsed.dt.day.between(1, 31) &
                parsed.dt.month.between(1, 12) &
                (parsed.dt.year >= 1990) &
                (parsed <= today)
            )
            gdf = gdf[mask]
            earliest_after = parsed[mask].min()
            logger.info(
                "dok_data filter: earliest %s -> %s (removed %d rows)",
                earliest_before.date(), earliest_after.date(), (~mask).sum(),
            )

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
                lambda r: self._get_hicp_value(int(r["transaction_year"]), int(r["transaction_month"]))
                if pd.notna(r["transaction_year"]) and pd.notna(r["transaction_month"])
                else float("nan"),
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
            # Normalise object columns to avoid pyarrow type conflicts on concat
            for col in existing.select_dtypes(include="object").columns:
                if col == "geometry":
                    continue
                existing[col] = existing[col].apply(
                    lambda v: v.decode() if isinstance(v, bytes) else (str(v) if v is not None else None)
                )
            # Cast teryt to str in both frames if present to avoid int64/str conflicts
            for col in ("teryt",):
                if col in existing.columns:
                    existing[col] = existing[col].astype(str)
                if col in gdf.columns:
                    gdf = gdf.copy()
                    gdf[col] = gdf[col].astype(str)
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

    def plot_transactions(self, gdf: gpd.GeoDataFrame, output_path: str = "transactions_plot.png") -> None:
        """
        Create a 2x2 subplot summary of transaction data and save as PNG.

        Subplots:
          - Upper-left:  Map coloured by transaction date (continuous palette)
          - Upper-right: Scatter plot of dok_data vs price_per_sqm
          - Lower-left:  Map coloured by tran_rodzaj_rynku (market type)
          - Lower-right: Map coloured by price_per_sqm_norm, fallback to
                         tran_cena_brutto_norm, then tran_cena_brutto
        """
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates
        from matplotlib.colors import Normalize
        from matplotlib.cm import ScalarMappable

        BG = "#0d0d0d"
        CMAP_CONTINUOUS = "plasma"
        CMAP_DATE = "spring"

        gdf_wm = gdf.to_crs("EPSG:3857")

        fig, axes = plt.subplots(2, 2, figsize=(16, 14))
        fig.patch.set_facecolor(BG)
        for ax in axes.flat:
            ax.set_facecolor(BG)
            ax.tick_params(colors="white", labelsize=7)
            for spine in ax.spines.values():
                spine.set_edgecolor("#444444")

        # ── Upper-left: date of transaction ──────────────────────────────────
        ax_date = axes[0, 0]
        if "dok_data" in gdf_wm.columns:
            dates = pd.to_datetime(gdf_wm["dok_data"])
            date_num = mdates.date2num(dates)
            norm = Normalize(vmin=date_num.min(), vmax=date_num.max())
            gdf_wm.plot(
                ax=ax_date,
                column=date_num,
                cmap=CMAP_DATE,
                norm=norm,
                markersize=4,
                alpha=0.8,
                legend=False,
            )
            sm = ScalarMappable(cmap=CMAP_DATE, norm=norm)
            sm.set_array([])
            cbar = fig.colorbar(sm, ax=ax_date, fraction=0.03, pad=0.02)
            cbar.ax.yaxis.set_tick_params(color="white", labelsize=7)
            cbar.outline.set_edgecolor("#444444")
            tick_locs = norm.vmin + (norm.vmax - norm.vmin) * pd.Series([0, 0.25, 0.5, 0.75, 1.0])
            cbar.set_ticks(tick_locs.tolist())
            cbar.set_ticklabels(
                [mdates.num2date(t).strftime("%Y-%m") for t in tick_locs],
                color="white",
            )
        ax_date.set_title("Transaction date", color="white", fontsize=10)
        ax_date.set_axis_off()

        # ── Upper-right: scatter dok_data vs price_per_sqm ───────────────────
        ax_scatter = axes[0, 1]
        ax_scatter.set_facecolor(BG)
        if "dok_data" in gdf.columns and "price_per_sqm" in gdf.columns:
            scatter_df = gdf[["dok_data", "price_per_sqm"]].dropna()
            # clip to 2nd–98th percentile to remove extreme outliers from display
            p_low = scatter_df["price_per_sqm"].quantile(0.02)
            p_high = scatter_df["price_per_sqm"].quantile(0.98)
            scatter_df = scatter_df[
                scatter_df["price_per_sqm"].between(p_low, p_high)
            ]
            dates_s = pd.to_datetime(scatter_df["dok_data"])
            ax_scatter.scatter(
                dates_s,
                scatter_df["price_per_sqm"],
                s=6,
                alpha=0.5,
                color="#e040fb",
                linewidths=0,
            )
            # Trend line via numpy polyfit on numeric dates
            import numpy as np
            date_num = mdates.date2num(dates_s)
            z = np.polyfit(date_num, scatter_df["price_per_sqm"], 1)
            p = np.poly1d(z)
            x_line = np.linspace(date_num.min(), date_num.max(), 200)
            ax_scatter.plot(
                mdates.num2date(x_line), p(x_line),
                color="#ffffff", linewidth=1.5, alpha=0.8, label="trend",
            )
            ax_scatter.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
            ax_scatter.xaxis.set_major_locator(mdates.AutoDateLocator())
            plt.setp(ax_scatter.get_xticklabels(), rotation=30, ha="right")
            ax_scatter.set_xlabel("Transaction date", color="white", fontsize=8)
            ax_scatter.set_ylabel("Price per sqm (PLN)", color="white", fontsize=8)
        else:
            ax_scatter.text(
                0.5, 0.5, "date / price per sqm\nnot available",
                ha="center", va="center", color="white", transform=ax_scatter.transAxes,
            )
        ax_scatter.set_title("Date vs price per sqm", color="white", fontsize=10)
        ax_scatter.tick_params(colors="white", labelsize=7)
        for spine in ax_scatter.spines.values():
            spine.set_edgecolor("#444444")

        # ── Lower-left: tran_rodzaj_rynku ────────────────────────────────────
        ax_rynek = axes[1, 0]
        MARKET_LABEL_MAP = {"pierwotny": "primary", "wtorny": "secondary"}
        if "tran_rodzaj_rynku" in gdf_wm.columns:
            categories = gdf_wm["tran_rodzaj_rynku"].astype("category")
            cat_codes = categories.cat.codes
            unique_cats = categories.cat.categories.tolist()
            MARKET_COLORS = ["#00bfff", "#c8a882"]  # cyan-blue vs light brown
            cmap_cat = plt.get_cmap("Set2", len(unique_cats))
            color_list = [MARKET_COLORS[i] if i < len(MARKET_COLORS) else cmap_cat(i) for i in range(len(unique_cats))]
            point_colors = [color_list[code] for code in cat_codes]
            gdf_wm.plot(
                ax=ax_rynek,
                color=point_colors,
                markersize=4,
                alpha=0.8,
            )
            # Manual legend patches
            from matplotlib.patches import Patch
            patches = [
                Patch(color=color_list[i], label=MARKET_LABEL_MAP.get(str(cat), str(cat)))
                for i, cat in enumerate(unique_cats)
            ]
            ax_rynek.legend(
                handles=patches,
                loc="lower left",
                fontsize=7,
                facecolor=BG,
                edgecolor="#444444",
                labelcolor="white",
            )
        ax_rynek.set_title("Market type", color="white", fontsize=10)
        ax_rynek.set_axis_off()

        # ── Lower-right: price map (norm > brutto_norm > brutto) ─────────────
        ax_price = axes[1, 1]
        if "price_per_sqm_norm" in gdf_wm.columns:
            price_col = "price_per_sqm_norm"
            price_label = "Price/sqm normalised (PLN)"
        elif "tran_cena_brutto_norm" in gdf_wm.columns:
            price_col = "tran_cena_brutto_norm"
            price_label = "Gross price normalised (PLN)"
        else:
            price_col = PRICE_COL
            price_label = "Gross price (PLN)"

        plot_gdf = gdf_wm.dropna(subset=[price_col])
        if not plot_gdf.empty:
            from matplotlib.colors import Normalize as MNorm
            p_norm = MNorm(
                vmin=plot_gdf[price_col].quantile(0.02),
                vmax=plot_gdf[price_col].quantile(0.98),
            )
            sm2 = ScalarMappable(cmap=CMAP_CONTINUOUS, norm=p_norm)
            sm2.set_array([])
            plot_gdf.plot(
                ax=ax_price,
                column=price_col,
                cmap=CMAP_CONTINUOUS,
                norm=p_norm,
                markersize=4,
                alpha=0.8,
                legend=False,
            )
            cbar2 = fig.colorbar(sm2, ax=ax_price, fraction=0.03, pad=0.02)
            cbar2.ax.yaxis.set_tick_params(color="white", labelsize=7)
            cbar2.ax.tick_params(labelcolor="white")
            cbar2.outline.set_edgecolor("#444444")
            ax_price.set_title(f"{price_label}", color="white", fontsize=10)
        ax_price.set_title(price_label, color="white", fontsize=10)
        ax_price.set_axis_off()

        fig.suptitle("RCN Transaction Summary", color="white", fontsize=14, y=1.01)
        plt.tight_layout()
        fig.savefig(output_path, dpi=150, bbox_inches="tight", facecolor=BG)
        plt.close(fig)
        logger.info("Plot saved to %s", output_path)

    def run(self, table: str = "transactions") -> gpd.GeoDataFrame:
        gdf = self.download()
        if self.process_data:
            gdf = self.clean(gdf)
            gdf = self.preprocess(gdf)
        self.save(gdf, table=table)
        self.plot_transactions(gdf, output_path=str(Path(__file__).parent / "data_plot.png"))
        logger.info("Pipeline run finished.")
        return gdf


if __name__ == "__main__":
    _here = Path(__file__).parent
    pipeline = RCNTransactionPipeline(
        area_file=str(_here / "mokotow_district.geojson"),
        save_format="both",
        db_path=str(_here / "lokale_mokotow.duckdb"),
        parquet_path=str(_here / "lokale_mokotow.parquet"),
        download_mode="parallel",
    )
    pipeline.run()
