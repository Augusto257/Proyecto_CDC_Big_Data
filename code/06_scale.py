from pyspark.sql import SparkSession
import time
spark = SparkSession.builder.appName("06-scale").getOrCreate()
B = "gs://cdc-epi-lake-2026"
base = spark.read.parquet(f"{B}/processed/epi_clean/")
n1 = base.count()
print(f"1x filas={n1}")
for mult, name in [(10, "scale_x10"), (100, "scale_x100")]:
    t0 = time.time()
    from functools import reduce
    acc = reduce(lambda a, b: a.union(b), [base] * mult)
    from pyspark.sql.functions import broadcast
    ipr = spark.read.parquet(f"{B}/processed/ipress_dim/")
    j = acc.join(broadcast(ipr), "ubigeo", "left")
    n = j.count()
    t1 = time.time()
    print(f"{mult}x filas={n} tiempo_total={round(t1-t0,2)}s")
print("FIN SCALE")
