from pyspark.sql import SparkSession
import pyspark.sql.functions as f
import pandas as pd

# Create a SparkSession with the Spark XML package
spark = SparkSession.builder \
    .appName("Wikipedia XML Parsing") \
    .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.14.0") \
    .getOrCreate()

# Read the parsed_data CSV file into a DataFrame
df_parsed_data = spark.read.csv("../data/parsed_data.csv", header=True, sep="\t")

# Extract the 'Nick' column values into a list
nicks_list = df_parsed_data.select("Nick").rdd.flatMap(lambda x: x).collect()

# Extract the 'Natioanlity' column values into a list
nationalities_list = sorted(df_parsed_data.select("Nationality").distinct().rdd.flatMap(lambda x: x).collect())

dump_df = spark.read.format('xml') \
    .option("rowTag", "page") \
    .load("../wikidumps/enwiki-latest-pages-articles1.xml-p1p41242.bz2")

dump_df.printSchema()

# Show titles
# dump_df.select("titles").show(n=20, truncate=False)

#####################

# Save the DataFrame locally
# dump_df.repartition(1).write.options(header="True", delimiter=",").mode("overwrite").csv("output")

filtered_df = (
    dump_df
    .filter(f.col("title").isin(nicks_list))
    # .filter(f.col("revision.text._VALUE").contains("team_history"))
    .withColumn("team_history", f.regexp_extract("revision.text._VALUE", r'team_history\s*=\s*(.*?)(\n|$)', 1))
    # .withColumn("revision_text", f.col("revision.text._VALUE"))
)

# Select and show the desired columns
result_df = filtered_df.select("title", "team_history")
result_df.show(n=result_df.count(), truncate=False)

# filtered_df.repartition(1).write.options(header="True", delimiter=",").mode("overwrite").csv("output") TODO

# Stop the SparkSession
spark.stop()
