from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("02-clean-epi").getOrCreate()
BUCKET = "gs://cdc-epi-lake-2026"

dengue = (spark.read.option("header", True).option("delimiter", ";")
          .option("encoding", "UTF-8").csv(f"{BUCKET}/raw/dengue/*"))
carrion = (spark.read.option("header", True).option("delimiter", ",")
           .option("quote", '"').option("encoding", "UTF-8").csv(f"{BUCKET}/raw/carrion/*"))
ipress = (spark.read.option("header", True).option("delimiter", ",")
          .option("encoding", "ISO-8859-1").csv(f"{BUCKET}/raw/ipress/*"))

tot_d, tot_c, tot_i = dengue.count(), carrion.count(), ipress.count()
print(f"RAW dengue={tot_d} carrion={tot_c} ipress={tot_i}")

def norm_epi(df, grupo):
    df = df.dropDuplicates()  # duplicados exactos fuera
    df = (df.withColumn("ubigeo", F.lpad(F.trim(F.col("ubigeo").cast("string")), 6, "0"))
            .withColumn("ano", F.col("ano").cast("int"))
            .withColumn("semana", F.col("semana").cast("int"))
            .withColumn("departamento", F.upper(F.trim(F.col("departamento"))))
            .withColumn("provincia", F.upper(F.trim(F.col("provincia"))))
            .withColumn("distrito", F.upper(F.trim(F.col("distrito"))))
            .withColumn("edad", F.col("edad").cast("int"))
            .withColumn("sexo", F.upper(F.trim(F.col("sexo"))))
            .withColumn("grupo", F.lit(grupo)))
    valid = df.filter(F.col("ubigeo").rlike("^[0-9]{6}$") &
                      F.col("ano").between(2000, 2024) &
                      F.col("semana").between(1, 53))
    rej = df.count() - valid.count()
    print(f"{grupo}: rechazados={rej} ({rej/df.count()*100:.2f}%)")
    return valid

dengue_n = norm_epi(dengue, "DENGUE")
carrion_n = norm_epi(carrion, "CARRION")

cols = ["ubigeo", "ano", "semana", "grupo"]
epi_agg = (dengue_n.select(cols).unionByName(carrion_n.select(cols))
           .groupBy("ubigeo", "ano", "semana", "grupo")
           .agg(F.count("*").alias("casos")))
print(f"epi_agg filas={epi_agg.count()}")
epi_agg.show(5)

ipress_n = (ipress.withColumnRenamed("UBIGEO", "ubigeo")
            .withColumn("ubigeo", F.lpad(F.trim(F.col("ubigeo").cast("string")), 6, "0"))
            .filter(F.col("ubigeo").rlike("^[0-9]{6}$"))
            .dropDuplicates())
ipress_agg = ipress_n.groupBy("ubigeo").agg(F.count("*").alias("n_ipress"))
print(f"ipress ubigeos distintos={ipress_agg.count()}")

from pyspark.sql.functions import broadcast
final = epi_agg.join(broadcast(ipress_agg), "ubigeo", "left").fillna({"n_ipress": 0})
sin_match = final.filter(F.col("n_ipress") == 0).count()
print(f"filas epi sin IPRESS (n_ipress=0): {sin_match}")

final.write.mode("overwrite").partitionBy("ano").parquet(f"{BUCKET}/processed/epi_clean/")
ipress_agg.write.mode("overwrite").parquet(f"{BUCKET}/processed/ipress_dim/")
print("FIN CLEAN: parquet en processed/epi_clean/ (particion ano) + processed/ipress_dim/")
