# Downloading RCN transaction data

Scripts for downloading, cleaning, and storing Polish real estate transaction data from the [RCN WFS service](https://mapy.geoportal.gov.pl/wss/service/rcn).

## Setup

### 1. Create the conda environment

```bash
conda env create -f price_data/env.yml
```

This creates an environment named `dev` with Python 3.12 and all required dependencies including `geopandas`, `owslib`, and `duckdb`.

### 2. Activate the environment

```bash
conda activate dev
```

### 3. Run the pipeline

```bash
python price_data/rcn_pipeline.py
```

Or import and configure it in a notebook or script:

```python
from price_data.rcn_pipeline import RCNTransactionPipeline

pipeline = RCNTransactionPipeline(
    layer="ms:lokale",
    bbox=(52.155, 21.055, 52.200, 21.130),  # (lat_min, lon_min, lat_max, lon_max)
    save_format="duckdb",
    db_path="plots_poland.duckdb",
)
pipeline.run()
```

## Example output

`gdf.head().T` from `lokale_wilanow.parquet`:

| field | 0 | 1 | 2 |
|---|---|---|---|
| gml_id | lokale.6175521 | lokale.6170887 | lokale.6091368 |
| teryt | 1465 | 1465 | 1465 |
| tran_lokalny_id_iip | fc3c00bf-b93b-... | aaad1bb8-bef7-... | 62330fa4-25c3-... |
| tran_wersja_id | 2025-07-25T12:42:16 | 2023-10-24T10:09:33 | 2021-11-25T14:29:44 |
| tran_rodzaj_trans | wolnyRynek | wolnyRynek | wolnyRynek |
| tran_rodzaj_rynku | pierwotny | wtorny | pierwotny |
| tran_sprzedajacy | osobaFizyczna | osobaFizyczna | osobaPrawna |
| tran_kupujacy | osobaFizyczna | osobaFizyczna | osobaFizyczna |
| tran_cena_brutto | 1499598.0 | 775000.0 | 370000.0 |
| dok_data | 2025-06-10 | 2023-03-29 | 2021-04-11 |
| nier_rodzaj | nieruchomoscLokalowa | nieruchomoscLokalowa | nieruchomoscLokalowa |
| nier_prawo | wlasnoscLokaluWrazZPrawemZwiazanym | wlasnoscLokaluWrazZPrawemZwiazanym | wlasnoscLokaluWrazZPrawemZwiazanym |
| lok_id_lokalu | 146505_8.0517.1013_BUD.96_LOK | 146516_8.0620.18/3.6_BUD.8_LOK | 146516_8.1014.79/38.1_BUD.73_LOK |
| lok_funkcja | mieszkalna | mieszkalna | mieszkalna |
| lok_liczba_izb | 3 | 2 | 2 |
| lok_nr_kond | 9 | 2 | 4 |
| lok_pow_uzyt | 74.72 | 52.01 | 39.74 |
| lok_pow_przyn | NaN | 5.08 | 2.76 |
| lok_cena_brutto | 1499598 | 740000 | 370000 |
| lok_adres | MSC:Warszawa;UL:ulica Powsińska;NR_PORZ:27 | MSC:Warszawa;UL:ulica Bruzdowa;NR_PORZ:100E | MSC:Warszawa;UL:ulica Zdrowa;NR_PORZ:6 |
| geometry | POINT (640969.59 482108.13) | POINT (644716.65 480534.70) | POINT (641137.09 480011.37) |
| transaction_id | 1465_fc3c00bf-... | 1465_aaad1bb8-... | 1465_62330fa4-... |
| transaction_year | 2025 | 2023 | 2021 |
| transaction_month | 6 | 3 | 4 |
| price_per_sqm | 20069.57 | 14900.98 | 9310.52 |
| lon | 21.06277 | 21.11689 | 21.06435 |
| lat | 52.18701 | 52.17190 | 52.16812 |
