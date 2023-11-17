from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, split, explode, regexp_extract_all, expr, regexp_extract
from pyspark.sql.types import StringType, BooleanType

spark = SparkSession.builder.appName("[TEST] VINF - Shazam for lyrics") \
    .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.14.0") \
    .config("spark.executor.heartbeatInterval", "5000s") \
    .config("spark.network.timeout", "6000s") \
    .getOrCreate()

wiki_df = spark.read.format("xml").option("rowTag", "page").load(
    "file:///C:/Users/A/PycharmProjects/VINF/enwiki-20231020-pages-articles.xml")
wiki_df.printSchema()
wiki_df.show()

df = wiki_df.withColumn("value", col("revision.text._VALUE"))
df.show(n=20)

years_pattern = r'years_active\s*=(.*?)(\n|\|)'
origin_pattern = r'origin\s*=(.*?)(\n|\|)'
website_pattern = r'website\s*=(.*?)(\n)'
name_pattern = r'name\s*=(.*?)(\n|\|)'
pattern = r"(\{\{Infobox musical artist.*?)"
# pattern = r"\{\{Infobox musical artist[\s\S]*?\}\}(?=\n\n|\n'|[a-zA-Z])"

initial_extracted_df = df.withColumn('extracted', regexp_extract(col('value'), pattern, 1))

filtered_df = initial_extracted_df.filter(col('extracted') != '')

final_df = filtered_df.withColumn('name', regexp_extract(col('value'), name_pattern, 1)) \
    .withColumn('years', regexp_extract(col('value'), years_pattern, 1)) \
    .withColumn('origin', regexp_extract(col('value'), origin_pattern, 1)) \
    .withColumn('website', regexp_extract(col('value'), website_pattern, 1))

selected_df = final_df.select("title", "name", "years", "website", "origin")
selected_df.coalesce(1).write.mode("overwrite").option("header", "true").option("sep", "\t").option("encoding",
                                                                                                    "UTF-8").csv(
    "C:/Users/A/PycharmProjects/VINF/final_data")

spark.stop()
