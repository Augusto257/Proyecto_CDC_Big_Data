from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

spark = SparkSession.builder.appName("08-baseline").getOrCreate()
B = "gs://cdc-epi-lake-2026"
df = spark.read.parquet(f"{B}/processed/epi_clean/")
df = df.withColumn("t", F.col("ano")*53 + F.col("semana"))
w = Window.partitionBy("ubigeo", "grupo").orderBy("t")
df = (df.withColumn("lag1", F.lag("casos", 1).over(w))
        .withColumn("lag2", F.lag("casos", 2).over(w))
        .withColumn("target", F.lead("casos", 1).over(w))
        .na.fill({"lag1": 0, "lag2": 0}))
df = df.filter(F.col("target").isNotNull())
df = df.withColumn("grupo_dengue", (F.col("grupo") == "DENGUE").cast("int"))
feat = ["semana", "n_ipress", "lag1", "lag2", "grupo_dengue"]
va = VectorAssembler(inputCols=feat, outputCol="features")
data = va.transform(df).select("features", F.col("target").alias("label"), "ano")
train = data.filter(F.col("ano") <= 2021)
test = data.filter(F.col("ano") >= 2022)
print(f"train={train.count()} test={test.count()}")
lr = LinearRegression(featuresCol="features", labelCol="label")
model = lr.fit(train)
pred = model.transform(test)
for m in ["rmse", "mae"]:
    v = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName=m).evaluate(pred)
    print(f"BASELINE {m.upper()}={round(v,3)}")
print(f"coef={model.coefficients} intercept={model.intercept}")
pred.select("label", "prediction").show(5)
print("FIN BASELINE: RMSE/MAE declarados como linea base tecnica")
