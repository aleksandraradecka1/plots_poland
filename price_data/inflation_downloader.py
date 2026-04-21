from dataclasses import dataclass, field
import logging
from datetime import datetime
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import requests

from utils import setup_file_logger

logger = logging.getLogger(__name__)

EUROSTAT_BASE = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data"
MANR_URL = f"{EUROSTAT_BASE}/prc_hicp_manr"
MIDX_URL = f"{EUROSTAT_BASE}/prc_hicp_midx"

CSV_COLUMNS = ["year", "month", "inflation_value", "hicp_index", "hicp_rebased"]


@dataclass
class InflationDataDownloader:
    csv_path: str = "price_data/inflation.csv"
    start_date: str = "1990-01"
    baseline_year: int = 2025
    baseline_month: int = 1

    def __post_init__(self) -> None:
        setup_file_logger(logger, self.__class__.__name__, log_dir=Path(__file__).parent / "logs")
        logger.info(
            "InflationDataDownloader initialised  csv_path=%s  start_date=%s  baseline=%04d-%02d",
            self.csv_path,
            self.start_date,
            self.baseline_year,
            self.baseline_month,
        )

    def _load_existing(self) -> pd.DataFrame:
        if Path(self.csv_path).exists():
            df = pd.read_csv(self.csv_path)
            # ensure new columns exist in older CSVs
            for col in CSV_COLUMNS:
                if col not in df.columns:
                    df[col] = float("nan")
            return df
        return pd.DataFrame(columns=CSV_COLUMNS)

    def _next_start_date(self, df: pd.DataFrame) -> str:
        if df.empty:
            return self.start_date
        max_year = int(df["year"].max())
        max_month = int(df.loc[df["year"] == max_year, "month"].max())
        if max_month == 12:
            next_year, next_month = max_year + 1, 1
        else:
            next_year, next_month = max_year, max_month + 1
        return f"{next_year}-{next_month:02d}"

    def _fetch(self, url: str, start: str, end: str) -> dict:
        params = {
            "geo": "PL",
            "coicop": "CP00",
            "format": "JSON",
            "startPeriod": start,
            "endPeriod": end,
        }
        logger.info("Fetching %s  start=%s  end=%s", url.split("/")[-1], start, end)
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
        except requests.exceptions.HTTPError as exc:
            logger.error("HTTP error %s fetching %s", exc.response.status_code, url)
            raise RuntimeError(f"Eurostat API returned HTTP {exc.response.status_code}") from exc
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as exc:
            logger.error("Network error fetching %s: %s", url, exc)
            raise RuntimeError(f"Network error fetching {url}: {exc}") from exc
        return response.json()

    def _parse(self, raw: dict, value_col: str) -> pd.DataFrame:
        """Parse a Eurostat JSON-stat response into a (year, month, value_col) DataFrame."""
        value_dict = raw.get("value", {})
        index_dict = (
            raw.get("dimension", {})
            .get("time", {})
            .get("category", {})
            .get("index", {})
        )
        if not value_dict or not index_dict:
            return pd.DataFrame(columns=["year", "month", value_col])
        inv_index = {v: k for k, v in index_dict.items()}
        records = []
        for str_pos, val in value_dict.items():
            period = inv_index.get(int(str_pos))
            if period is None:
                continue
            year_str, month_str = period.split("-")
            records.append({"year": int(year_str), "month": int(month_str), value_col: float(val)})
        if not records:
            return pd.DataFrame(columns=["year", "month", value_col])
        df = pd.DataFrame(records)
        df["year"] = df["year"].astype(int)
        df["month"] = df["month"].astype(int)
        df[value_col] = df[value_col].astype(float)
        return df

    def _compute_rebased(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add hicp_rebased = hicp_index / baseline_index * 100.

        The raw hicp_index from Eurostat uses 2015 as the reference year (= 100).
        We rebase to a more recent date (default: January 2025) so that deflated
        property prices are expressed in terms familiar to the user — i.e. in
        today's money rather than 2015 money.
        """
        baseline_rows = df[(df["year"] == self.baseline_year) & (df["month"] == self.baseline_month)]
        if baseline_rows.empty or pd.isna(baseline_rows["hicp_index"].iloc[0]):
            logger.warning(
                "Baseline %04d-%02d not found in HICP index data — hicp_rebased will be NaN",
                self.baseline_year,
                self.baseline_month,
            )
            df["hicp_rebased"] = float("nan")
        else:
            h_baseline = baseline_rows["hicp_index"].iloc[0]
            logger.info(
                "Baseline %04d-%02d  hicp_index=%.4f", self.baseline_year, self.baseline_month, h_baseline
            )
            df["hicp_rebased"] = df["hicp_index"] / h_baseline * 100
        return df

    def _save(self, existing: pd.DataFrame, new_manr: pd.DataFrame, new_midx: pd.DataFrame) -> None:
        # start from existing as the base, then upsert each new series independently
        combined = existing.set_index(["year", "month"])

        if not new_manr.empty:
            manr_indexed = new_manr.set_index(["year", "month"])["inflation_value"]
            combined["inflation_value"] = manr_indexed.combine_first(combined.get("inflation_value", pd.Series(dtype=float)))

        if not new_midx.empty:
            midx_indexed = new_midx.set_index(["year", "month"])["hicp_index"]
            # union index: bring in any new rows from MIDX not yet in existing
            combined = combined.reindex(combined.index.union(midx_indexed.index))
            combined["hicp_index"] = midx_indexed.combine_first(combined["hicp_index"])

        combined = combined.reset_index().sort_values(["year", "month"]).reset_index(drop=True)

        # recompute hicp_rebased across the full dataset so baseline is always consistent
        combined = self._compute_rebased(combined)

        Path(self.csv_path).parent.mkdir(parents=True, exist_ok=True)
        combined[CSV_COLUMNS].to_csv(self.csv_path, index=False)

    def plot_inflation_values(self, output_path: str = "price_data/inflation_values.png") -> None:
        """Plot year-on-year HICP inflation rate (MANR) and save to a PNG file."""
        df = self._load_existing()
        df = df.dropna(subset=["inflation_value"]).sort_values(["year", "month"])
        if df.empty:
            logger.warning("No inflation_value data available to plot")
            return

        df["date"] = pd.to_datetime(df[["year", "month"]].assign(day=1))

        fig, ax = plt.subplots(figsize=(12, 5))
        ax.plot(df["date"], df["inflation_value"], linewidth=1.5, color="steelblue")
        ax.axhline(0, color="black", linewidth=0.8, linestyle="--")
        ax.set_title("Poland HICP Inflation (year-on-year, %)")
        ax.set_xlabel("Date")
        ax.set_ylabel("Inflation rate (%)")
        ax.grid(True, alpha=0.3)
        fig.tight_layout()

        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(output_path, dpi=150)
        plt.close(fig)
        logger.info("Saved inflation plot to %s", output_path)

    def run(self) -> pd.DataFrame:
        existing = self._load_existing()
        start = self._next_start_date(existing)
        end = datetime.now().strftime("%Y-%m")
        logger.info("Starting download  csv_path=%s  start=%s  end=%s", self.csv_path, start, end)

        # --- MANR (year-on-year rate) ---
        raw_manr = self._fetch(MANR_URL, start, end)
        new_manr = self._parse(raw_manr, "inflation_value")
        if new_manr.empty:
            logger.info("No new MANR records available")
        else:
            earliest = new_manr.sort_values(["year", "month"]).iloc[0]
            logger.info(
                "Earliest MANR record: %04d-%02d", int(earliest["year"]), int(earliest["month"])
            )

        # --- MIDX (index, 2015=100) ---
        # fetch from the very beginning to ensure baseline is always present
        raw_midx = self._fetch(MIDX_URL, self.start_date, end)
        new_midx = self._parse(raw_midx, "hicp_index")
        if new_midx.empty:
            logger.info("No MIDX records available")
        else:
            earliest_midx = new_midx.sort_values(["year", "month"]).iloc[0]
            logger.info(
                "Earliest MIDX record: %04d-%02d", int(earliest_midx["year"]), int(earliest_midx["month"])
            )

        if new_manr.empty and new_midx.empty:
            return pd.DataFrame(columns=CSV_COLUMNS)

        self._save(existing, new_manr, new_midx)
        logger.info("Saved updated inflation data to %s", self.csv_path)
        return new_manr


if __name__ == "__main__":
    InflationDataDownloader().run()
