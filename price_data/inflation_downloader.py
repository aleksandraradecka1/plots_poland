from dataclasses import dataclass
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

from utils import setup_file_logger

logger = logging.getLogger(__name__)

EUROSTAT_URL = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/prc_hicp_manr"


@dataclass
class InflationDataDownloader:
    csv_path: str = "price_data/inflation.csv"
    start_date: str = "1990-01"

    def __post_init__(self) -> None:
        setup_file_logger(logger, self.__class__.__name__)
        logger.info(
            "InflationDataDownloader initialised  csv_path=%s  start_date=%s",
            self.csv_path,
            self.start_date,
        )

    def _load_existing(self) -> pd.DataFrame:
        if Path(self.csv_path).exists():
            return pd.read_csv(self.csv_path)
        return pd.DataFrame(columns=["year", "month", "inflation_value"])

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

    def _fetch(self, start: str, end: str) -> dict:
        params = {
            "geo": "PL",
            "coicop": "CP00",
            "format": "JSON",
            "startPeriod": start,
            "endPeriod": end,
        }
        logger.info("Fetching inflation data  start=%s  end=%s", start, end)
        try:
            response = requests.get(EUROSTAT_URL, params=params, timeout=30)
            response.raise_for_status()
        except requests.exceptions.HTTPError as exc:
            logger.error("HTTP error %s fetching inflation data", exc.response.status_code)
            raise RuntimeError(f"Eurostat API returned HTTP {exc.response.status_code}") from exc
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as exc:
            logger.error("Network error fetching inflation data: %s", exc)
            raise RuntimeError(f"Network error fetching inflation data: {exc}") from exc
        return response.json()

    def _parse(self, raw: dict) -> pd.DataFrame:
        value_dict = raw.get("value", {})
        index_dict = (
            raw.get("dimension", {})
            .get("time", {})
            .get("category", {})
            .get("index", {})
        )
        if not value_dict or not index_dict:
            return pd.DataFrame(columns=["year", "month", "inflation_value"])
        # invert index: {positional_int -> period_string}
        inv_index = {v: k for k, v in index_dict.items()}
        records = []
        for str_pos, val in value_dict.items():
            period = inv_index.get(int(str_pos))
            if period is None:
                continue
            year_str, month_str = period.split("-")
            records.append(
                {
                    "year": int(year_str),
                    "month": int(month_str),
                    "inflation_value": float(val),
                }
            )
        if not records:
            return pd.DataFrame(columns=["year", "month", "inflation_value"])
        df = pd.DataFrame(records)
        df["year"] = df["year"].astype(int)
        df["month"] = df["month"].astype(int)
        df["inflation_value"] = df["inflation_value"].astype(float)
        return df

    def _save(self, existing: pd.DataFrame, new: pd.DataFrame) -> None:
        combined = pd.concat([existing, new], ignore_index=True)
        combined = combined.drop_duplicates(subset=["year", "month"])
        Path(self.csv_path).parent.mkdir(parents=True, exist_ok=True)
        combined.to_csv(self.csv_path, index=False)

    def run(self) -> pd.DataFrame:
        existing = self._load_existing()
        start = self._next_start_date(existing)
        end = datetime.now().strftime("%Y-%m")
        logger.info(
            "Starting download  csv_path=%s  start=%s  end=%s",
            self.csv_path,
            start,
            end,
        )
        raw = self._fetch(start, end)
        new = self._parse(raw)
        if new.empty:
            logger.info("No new records available")
            return pd.DataFrame(columns=["year", "month", "inflation_value"])
        earliest = new.sort_values(["year", "month"]).iloc[0]
        logger.info(
            "Earliest record returned by API: %04d-%02d",
            int(earliest["year"]),
            int(earliest["month"]),
        )
        self._save(existing, new)
        logger.info("Wrote %d new records to %s", len(new), self.csv_path)
        return new


if __name__ == "__main__":
    InflationDataDownloader().run()
