import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession

# Initialize GlueContext with SparkContext
sc = SparkContext()
glueContext = GlueContext(sc)

# Read data from S3 using Glue catalog
datasource0 = glueContext.create_dynamic_frame.from_catalog(database="trafficdata", table_name="your-table-name")

# Perform data transformation and cleaning as needed
# applying mapping/
transformed_data = ApplyMapping.apply(frame = datasource0, mappings = [("old_column", "string", "new_column", "int")])

# Convert DynamicFrame to DataFrame for further processing
transformed_df = transformed_data.toDF()

# Example transformation: Selecting specific columns
transformed_df = transformed_df.select("new_column1", "new_column2")

# Example transformation: Filtering data
transformed_df = transformed_df.filter(transformed_df["new_column1"] > 100)

# Write transformed data to S3 or another destination
# writing to Parquet format
transformed_df.write.format("parquet").save("s3://your-bucket/your-output-path")

