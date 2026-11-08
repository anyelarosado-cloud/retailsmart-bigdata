"""Crea erp_stock y erp_plazos en BigQuery (retailsmart_smart) desde erp_stock.sql.

    python cargar_erp.py                   # stock, plazos, stock operativo y demanda diaria
    python cargar_erp.py --solo-operativo  # solo la calibración (si erp_stock ya existe)
    python cargar_erp.py --landing         # además exporta Parquet a Landing (landing/erp/)
"""
import argparse
import pathlib

from google.cloud import bigquery

PROJECT = "retailsmart-bd"
BUCKET = f"gs://retailsmart-lake-{PROJECT}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", default=PROJECT)
    ap.add_argument("--landing", action="store_true")
    ap.add_argument("--solo-operativo", action="store_true", help="omite erp_stock.sql")
    a = ap.parse_args()
    bq = bigquery.Client(project=a.project)
    scripts = ["erp_stock_operativo.sql"] if a.solo_operativo else ["erp_stock.sql", "erp_stock_operativo.sql"]
    for nombre in scripts:
        sql = (pathlib.Path(__file__).parent / nombre).read_text(encoding="utf-8").replace("{PROJECT}", a.project)
        bq.query(sql).result()      # ejecuta las sentencias del archivo como un script
        print(f"ejecutado: {nombre}")
    for t in ("erp_stock", "erp_plazos", "erp_stock_operativo", "demanda_diaria_centro"):
        n = bq.get_table(f"{a.project}.retailsmart_smart.{t}").num_rows
        print(f"{t}: {n:,} filas")
        if a.landing:
            cfg = bigquery.ExtractJobConfig(destination_format="PARQUET")
            bq.extract_table(f"{a.project}.retailsmart_smart.{t}", f"{BUCKET}/landing/erp/{t}/part-*.parquet",
                             job_config=cfg).result()
            print(f"  exportado a {BUCKET}/landing/erp/{t}/")


if __name__ == "__main__":
    main()
