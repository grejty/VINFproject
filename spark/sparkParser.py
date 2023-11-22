from pyspark.sql import SparkSession
import pyspark.sql.functions as f

# Create a SparkSession with the Spark XML package
spark = SparkSession.builder \
    .appName("Wikipedia XML Parsing") \
    .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.14.0") \
    .getOrCreate()

# Read the parsed_data CSV file into a DataFrame
df_parsed_data = spark.read.csv("../data/parsed_data.csv", header=True, sep="\t")

# Extract the 'Nick' column values into a list
nicks_list = df_parsed_data.select("Nick").rdd.flatMap(lambda x: x).collect()

# Extract the 'Nationality' column values into a list
nationalities_list = df_parsed_data.select("Nationality").distinct().rdd.flatMap(lambda x: x).collect()

dump_df = spark.read.format('xml') \
    .option("rowTag", "page") \
    .load("../wikidumps/enwiki-latest-pages-articles1.xml-p1p41242.bz2")

print("\nSchema of dump is as follows:")
dump_df.printSchema()

filtered_df = (
    dump_df
    .filter(f.col("title").isin(nationalities_list))
    .filter((f.col("revision.text._VALUE").contains("population_estimate")) |
            (f.col("revision.text._VALUE").contains("population_census")))
    .withColumn("population", f.regexp_extract("revision.text._VALUE", r'\|\spopulation_(?![^=]*_)\D*(\d+.*?)[&}<(\n]', 1))
    .withColumnRenamed("title", "country")
)

# Select and show the desired columns
result_df = filtered_df.select("country", "population").orderBy("country")

print("Results of parsing the dump:")
result_df.show(n=result_df.count(), truncate=False)

# Join DataFrames on the common columns "title" and "Nationality"
joined_df = df_parsed_data.join(result_df, df_parsed_data["Nationality"] == result_df["country"], "left_outer")

# Add the "population" column to the original DataFrame
df_parsed_data_enriched = joined_df.withColumn("Population", f.lit(result_df["population"])).drop("country")

# Save the updated DataFrame with the new column back to a specific CSV file
df_parsed_data_enriched.coalesce(1).write.csv("../spark/parsed_data_enriched", header=True, sep="\t", mode="overwrite", quote="")

# Stop the SparkSession
spark.stop()
