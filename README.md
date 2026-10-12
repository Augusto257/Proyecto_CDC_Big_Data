# Proyecto CDC Big Data Analítica 2026

Predicción distrital t+1 de dengue y enfermedad de Carrión + priorización regional (CDC Perú).

## Reproducibilidad desde cero
1. Crear proyecto GCP, activar facturación, APIs (Storage, Dataproc, BigQuery) y rol `Trabajador Dataproc` a la service account Compute.
2. Crear bucket `gs://cdc-epi-lake-2026` (southamerica-east1, Standard, no público) y carpetas `raw/dengue|carrión|ipress|boletines`, `processed/`, `code/`.
3. Subir CSV completos a `raw/` (ver informe 3.1: 1.09M filas, 117MB) y 11 PDF a `raw/boletines/` (99MB). `data_sample/` permite probar sin los archivos completos.
4. Crear clúster `epi-cluster` (2.3.37-debian12 Spark 3.5.3, n4-standard-4, single-node, southamerica-east1-b).
5. Subir `code/` a `gs://cdc-epi-lake-2026/code/` y lanzar en orden: `01_explore` → `02_clean` → `05_opt` → `06_scale` → `07_baseline` (PySpark). Vía B local: `pip install -r requirements.txt`, `python code/03_markitdown.py`, curaduría Tabla 1/2-A, `python code/04_parse_md.py`, subir `out/boletines_*` a `processed/`.
6. BigQuery: dataset `epi_analytics` + tablas `epi_clean` (Parquet `processed/epi_clean/*`), `ipress_dim`, `boletines_master|detalle` (CSV `;`, header 1, esquema del informe).
7. Verificación: `epi_clean` 98029, `master` 22, `detalle` 726, baseline RMSE 17.14 / MAE 6.53. EDA en `out/*.png`.
8. Apagar/borrar el clúster. Costo Hito 1 ≈ S/27 cubierto por crédito.

## Orden de `code/`
01 perfilado · 02 limpieza/agregación · 03 PDF→MD · 04 MD→master/detalle · 05 optimización · 06 escalabilidad · 07 baseline t+1.
