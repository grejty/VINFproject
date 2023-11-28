from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.window import Window

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
nationalities_list = sorted(df_parsed_data.select("Nationality").distinct().rdd.flatMap(lambda x: x).collect())
# 'Afghanistan', 'Albania', 'Algeria', 'Argentina', 'Australia', 'Austria', 'Azerbaijan', 'Belarus', 'Belgium',
# 'Bosnia and Herzegovina', 'Brazil', 'Bulgaria', 'Cambodia', 'Canada', 'Chile', 'China', 'Colombia', 'Croatia',
# 'Czechia', 'Denmark', 'Ecuador', 'Estonia', 'Finland', 'France', 'Germany', 'Guatemala', 'Hong Kong', 'Hungary',
# 'Iceland', 'India', 'Indonesia', 'Iran', 'Iraq', 'Ireland', 'Israel', 'Italy', 'Jordan', 'Kazakhstan', 'Kosovo',
# 'Kyrgyzstan', 'Latvia', 'Lebanon', 'Lithuania', 'Malaysia', 'Malta', 'Mexico', 'Mongolia', 'Montenegro', 'Morocco',
# 'Netherlands', 'New Zealand', 'North Macedonia', 'Norway', 'Pakistan', 'Peru', 'Philippines', 'Poland', 'Portugal',
# 'Romania', 'Russia', 'Saudi Arabia', 'Serbia', 'Slovakia', 'Slovenia', 'South Africa', 'South Korea', 'Spain', 'Sudan'
# , 'Sweden', 'Switzerland', 'Syria', 'Taiwan', 'Tunisia', 'Turkey', 'Ukraine', 'United Arab Emirates', 'United Kingdom'
# , 'United States', 'Uruguay', 'Uzbekistan', 'Venezuela', 'Vietnam']
# Amount - 82 countries in parsed data (not enriched)

print("\nAmount of nationalities in parsed data (not enriched):", len(nationalities_list))

print("\nLoading XML file...")
dump_df = spark.read.format('xml') \
    .option("rowTag", "page") \
    .load("../wikidumps/enwiki-latest-pages-articles.xml.bz2")

print("\nSchema of the dump is as follows:")
dump_df.printSchema()

print("Filtering desired data...")
filtered_df = (
    dump_df
    .filter(f.col("title").isin(nationalities_list))
    .filter((f.col("revision.text._VALUE").contains("population_estimate")) |
            (f.col("revision.text._VALUE").contains("population_census")))
    .withColumn("population",
                f.regexp_extract("revision.text._VALUE", r'\|\spopulation_(?![^=]*_)\D*(\d+.*?)[&}<(\n]', 1))
    .withColumnRenamed("title", "country")
)

# Select and show the desired columns
result_df = filtered_df.select("country", "population").orderBy("country")

number_of_nationalities = result_df.count()
print(f"\nFound {number_of_nationalities} nationalities in the dump. "
      f"(Success rate: {number_of_nationalities / len(nationalities_list) * 100:.2f}%)")

print("Results of parsing the dump:")
result_df.show(n=number_of_nationalities, truncate=False)

# Join DataFrames on the common columns "title" and "Nationality"
joined_df = df_parsed_data.join(result_df, df_parsed_data["Nationality"] == result_df["country"], "left_outer")

# Add the "population" column to the original DataFrame
df_parsed_data_enriched = joined_df.withColumn("Population",
                                               f.coalesce(result_df["population"],
                                                          f.lit(None))).drop("country")

# Define a window specification over the entire DataFrame to count occurrences of each Nationality
window_spec = Window.partitionBy("Nationality")

# Calculate the count of occurrences of each Nationality
nationality_count = f.count("Nationality").over(window_spec)

# Add a new column "Nationality_Count" calculated as the count of occurrences of each Nationality
df_parsed_data_enriched = df_parsed_data_enriched \
    .withColumn("Nationality_Count", nationality_count)

# Save the updated DataFrame with the new column back to a specific CSV file
df_parsed_data_enriched.coalesce(1).write.csv("../spark/parsed_data_enriched",
                                              header=True,
                                              sep="\t",
                                              mode="overwrite",
                                              quote="")

# Count the number of records with and without data in the "Population" column
populated_records_count = df_parsed_data_enriched.filter(f.col("Population").isNotNull()).count()
empty_records_count = df_parsed_data_enriched.filter(f.col("Population").isNull()).count()

# Calculate the success rate
total_records = populated_records_count + empty_records_count
success_rate = (populated_records_count / total_records) * 100

print(f"Players with Population data: {populated_records_count}")
print(f"Players without Population data: {empty_records_count}")
print(f"Success Rate: {success_rate:.2f}%")

# Stop the SparkSession
spark.stop()
