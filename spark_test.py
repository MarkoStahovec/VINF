from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, split, explode
from pyspark.sql.types import StringType, BooleanType
import spacy


nlp = spacy.load("en_core_web_sm")

@udf(StringType())
def your_ner_function(text):
    text = text[:200]
    return text
    doc = nlp(text)
    entities = [entity.text for entity in doc.ents]
    return '|'.join(entities)

# print( "spark version=" ,SparkSession.builder.appName("test").getOrCreate().version)
spark = SparkSession.builder.appName("[TEST] VINF - Shazam for lyrics")\
    .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.14.0")\
    .config("spark.executor.heartbeatInterval", "500s") \
    .config("spark.network.timeout", "600s") \
    .getOrCreate()

wiki_df = spark.read.format("xml").option("rowTag", "page").load("file:///C:/Users/A/PycharmProjects/VINF/enwiki-latest-pages-articles1.xml")
# TODO: Parse Wikitext here to get plain text

wiki_df.printSchema()
wiki_df.show()
wiki_df_with_entities = wiki_df.withColumn("entities", your_ner_function(col("revision.text._VALUE")))
wiki_df_with_entities = wiki_df_with_entities.withColumn("wiki_title", col("title"))
wiki_df_with_entities = wiki_df_with_entities.withColumn("additional_redirects", col("redirect._title"))
wiki_df_with_entities.show(n=20)

# wiki_df_exploded = wiki_df_with_entities.withColumn("entity", explode(split(col("entities"), "\|")))
wiki_df_exploded = wiki_df_with_entities

tsv_df = spark.read.csv("file:///C:/Users/A/PycharmProjects/VINF/parsed_data.tsv", header=True, sep="\t")
"""
joined_df = tsv_df.alias("tsv").join(
    wiki_df_exploded.alias("wiki"),
    (col("tsv.Artist") == col("wiki.entity")) |
    (col("tsv.Song_Name") == col("wiki.entity")) |
    (col("tsv.Album_Name") == col("wiki.entity")),
    "left"
)
"""
joined_df = tsv_df.alias("tsv").join(
    wiki_df_exploded.alias("wiki"),
    (col("tsv.Artist") == col("wiki.wiki_title")) |
    (col("tsv.Artist") == col("wiki.additional_redirects")),
    "left"
)

result_df = joined_df.select("tsv.*", "wiki.entities", "wiki.wiki_title", "wiki.additional_redirects")
# result_df.write.csv("file:///C:/Users/A/PycharmProjects/VINF/final_data.tsv", sep='\t', header=True)
result_df.coalesce(1).write.mode("overwrite").option("header", "true").option("sep", "\t").option("encoding", "UTF-8").csv("C:/Users/A/PycharmProjects/VINF/final_data")

spark.stop()





"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, split, explode, regexp_extract_all, expr, regexp_extract
from pyspark.sql.types import StringType, BooleanType

spark = SparkSession.builder.appName("[TEST] VINF - Shazam for lyrics") \
    .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.14.0") \
    .config("spark.executor.heartbeatInterval", "500s") \
    .config("spark.network.timeout", "600s") \
    .getOrCreate()

wiki_df = spark.read.format("xml").option("rowTag", "page").load(
    "file:///C:/Users/A/PycharmProjects/VINF/enwiki-20231020-pages-articles24.xml")
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

extracted_df = df.withColumn('name', regexp_extract(col('value'), name_pattern, 1))\
    .withColumn('years', regexp_extract(col('value'), years_pattern, 1)) \
    .withColumn('origin', regexp_extract(col('value'), origin_pattern, 1)) \
    .withColumn('website', regexp_extract(col('value'), website_pattern, 1)) \
    .withColumn('extracted', regexp_extract(col('value'), pattern, 1))
extracted_df.show(truncate=40)

selected_df = extracted_df.select("title", "name", "years", "website", "origin", "extracted")
selected_df.coalesce(1).write.mode("overwrite").option("header", "true").option("sep", "\t").option("encoding",
                                                                                                    "UTF-8").csv(
    "C:/Users/A/PycharmProjects/VINF/final_data")

spark.stop()

"""
