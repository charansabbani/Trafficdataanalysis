from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor 
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("TrafficDataPrediction") \
    .getOrCreate()

# Assuming 'df' is your DataFrame containing features and labels
# Define the input features and output label column names
input_features = ["feature1", "feature2", "feature3", "feature4"]  # Replace with actual column names
output_label = "label"

# Assemble features into a single vector column
assembler = VectorAssembler(inputCols=input_features, outputCol="features")
df_assembled = assembler.transform(df)

# Split the dataset into train and test sets
train_data, test_data = df_assembled.randomSplit([0.8, 0.2])

# Define RandomForestRegressor model
rf = RandomForestRegressor(featuresCol="features", labelCol=output_label)

# Define a grid of hyperparameters to search over
param_grid = ParamGridBuilder() \
    .addGrid(rf.numTrees, [10, 20, 30]) \
    .addGrid(rf.maxDepth, [5, 10, 15]) \
    .build()

# Define an evaluator for model evaluation
evaluator = RegressionEvaluator(labelCol=output_label, predictionCol="prediction", metricName="rmse")

# Create a CrossValidator with 3 folds
cv = CrossValidator(estimator=rf,
                    estimatorParamMaps=param_grid,
                    evaluator=evaluator,
                    numFolds=3)

# Train model with CrossValidator
cv_model = cv.fit(train_data)

# Make predictions on the test data
predictions = cv_model.transform(test_data)

# Evaluate the model performance on test data
rmse = evaluator.evaluate(predictions)
print("Root Mean Squared Error (RMSE) on test data: {:.4f}".format(rmse))

# Get the best model from CrossValidator
best_model = cv_model.bestModel

# Save the trained model for future use
# Ensure you have the correct path and permissions set up if using AWS S3 or local storage
best_model.save("s3://ourbucket/our-model-path")

# Stop SparkSession
spark.stop()
