from pyspark.sql import SparkSession, functions as F
import time
spark = SparkSession.builder.appName("05-opt").getOrCreate()
B = "gs://cdc-epi-lake-2026"
epi = spark.read.parquet(f"{B}/processed/epi_clean/")
ipr = spark.read.parquet(f"{B}/processed/ipress_dim/")
print(f"epi={epi.count()} ipr={ipr.count()}")

def run(use_broadcast, use_cache, use_filter):
    e = epi
    if use_filter: e = e.filter(F.col("ano").between(2020, 2024))
    if use_cache:
        e = e.cache(); i = ipr.cache()
    else:
        i = ipr
    t0 = time.time()
    if use_broadcast:
        from pyspark.sql.functions import broadcast
        j = e.join(broadcast(i), "ubigeo", "left")
    else:
        j = e.join(i, "ubigeo", "left")
    n = j.count()
    t1 = time.time()
    n2 = j.count()
    t2 = time.time()
    return n, round(t1-t0, 2), round(t2-t1, 2)

for cfg in [(False, False, False), (True, True, True)]:
    n, t_first, t_second = run(*cfg)
    print(f"cfg broadcast/cache/filter={cfg} -> filas={n} t1={t_first}s t2(cache)={t_second}s")

print("FIN OPT")
