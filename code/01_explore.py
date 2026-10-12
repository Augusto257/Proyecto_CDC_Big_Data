from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("01-explore-epi").getOrCreate()
BUCKET="gs://cdc-epi-lake-2026"

def profile(path, delim, encoding="UTF-8"):
    print(f"\n===== {path} delim={delim} =====")
    df = spark.read.option("header",True).option("delimiter",delim).option("encoding",encoding).csv(path)
    df.printSchema()
    total = df.count()
    print(f"total filas (con datos): {total}")
    print("--- 5 primeras ---"); df.show(5, truncate=True)
    crit = [c for c in ["ubigeo","UBIGEO","ano","semana"] if c in df.columns]
    if crit:
        df.select([F.sum(F.col(c).isNull().cast("int")).alias("null_"+c) for c in crit]).show()
    if "ano" in df.columns: df.select(F.min(F.col("ano").cast("int")).alias("min_ano"),F.max(F.col("ano").cast("int")).alias("max_ano")).show()
    if "semana" in df.columns: df.select(F.min(F.col("semana").cast("int")).alias("min_semana"),F.max(F.col("semana").cast("int")).alias("max_semana")).show()
    dup = df.count() - df.dropDuplicates().count()
    print(f"filas duplicadas exactas: {dup}")

profile(f"{BUCKET}/raw/dengue/*",";")
profile(f"{BUCKET}/raw/carrion/*",",")
profile(f"{BUCKET}/raw/ipress/*",",","ISO-8859-1")
print("FIN EXPLORACION")
