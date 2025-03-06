#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# In[ ]:


# In[ ]:


# trunk-ignore(ruff/E402)
from pyspark.sql import SparkSession

# trunk-ignore(ruff/E402)
# trunk-ignore(ruff/F403)
from pyspark.sql.functions import *

# In[ ]:


spark = SparkSession.builder.appName("").getOrCreate()


# In[ ]:


df = spark.read.csv(
    "/home/danila/BigDataRetake/big-data-pipeline/data/accidents.csv",
    inferSchema=True,
    header=True,
).limit(1500000)


# In[ ]:


cols_to_drop = [
    "ID",
    "Start_Time",
    "End_Time",
    "Description",
    "Street",
    "Weather_Timestamp",
    "Zipcode",
    "County",
    "City",
    "Airport_Code",
    "Precipitation(in)",
]


# In[ ]:


df = df.drop(*cols_to_drop)


# In[ ]:


columns = df.dtypes
for cols, typ in columns:
    if typ != "boolean":
        # trunk-ignore(ruff/F405)
        df = df.withColumn(cols, when(isnan(col(cols)), None).otherwise(col(cols)))


# In[ ]:


label = "Severity"
string_cols = [cols[0] for cols in df.dtypes if cols[1] == "string"]
num_cols = [cols[0] for cols in df.dtypes if cols[1] == "int" or cols[1] == "double"]
num_cols.remove(label)
bool_cols = [cols[0] for cols in df.dtypes if cols[1] == "boolean"]


# In[ ]:


df = df.fillna("unknown", string_cols)
df = df.fillna(0, num_cols)


# In[ ]:


for c in bool_cols:
    # trunk-ignore(ruff/F405)
    df = df.withColumn(c, col(c).cast("integer"))


# In[ ]:


spark.stop()


# In[ ]:


spark = (
    SparkSession.builder.appName("RandomForestGridSearch")
    .config("spark.ui.port", "4040")
    .getOrCreate()
)


# In[ ]:


sc = spark.sparkContext


# In[ ]:


def row_to_labeled_point(row):
    label = row["Severity"]
    features = [row[col] for col in df.columns if col != "Severity"]
    return LabeledPoint(label, features)


labeled_points_rdd = df.rdd.map(row_to_labeled_point)

training_data, test_data = labeled_points_rdd.randomSplit([0.7, 0.3], seed=42)


# In[ ]:


# trunk-ignore(ruff/E402)
from pyspark.mllib.evaluation import MulticlassMetrics

# trunk-ignore(ruff/E402)
from pyspark.mllib.regression import LabeledPoint

# trunk-ignore(ruff/E402)
from pyspark.mllib.tree import GradientBoostedTrees, RandomForest

# In[ ]:


num_trees_grid = [10, 20, 30]
max_depth_grid = [5, 10, 15]

best_model = None
best_accuracy = 0.0
best_params = {}

for num_trees in num_trees_grid:
    for max_depth in max_depth_grid:
        model = RandomForest.trainClassifier(
            training_data,
            numClasses=4,
            categoricalFeaturesInfo={},
            numTrees=num_trees,
            featureSubsetStrategy="auto",
            impurity="gini",
            maxDepth=max_depth,
            maxBins=32,
            seed=42,
        )

        predictions = model.predict(test_data.map(lambda x: x.features))
        labels_and_predictions = test_data.map(lambda x: x.label).zip(predictions)

        metrics = MulticlassMetrics(labels_and_predictions)
        accuracy = metrics.accuracy

        print(f"numTrees: {num_trees}, maxDepth: {max_depth}, Accuracy: {accuracy}")

        if accuracy > best_accuracy:
            best_accuracy = accuracy
            best_model = model
            best_params = {"numTrees": num_trees, "maxDepth": max_depth}

print(f"Best Model Parameters: {best_params}")
print(f"Best Model Accuracy: {best_accuracy}")

spark.stop()


# In[ ]:


spark = SparkSession.builder.appName("GradientBoostingGridSearch").getOrCreate()

sc = spark.sparkContext


# In[ ]:


num_iterations_grid = [10, 20, 30]
max_depth_grid = [3, 5, 7]

# Grid search
best_model = None
best_accuracy = 0.0
best_params = {}

for num_iterations in num_iterations_grid:
    for max_depth in max_depth_grid:
        model = GradientBoostedTrees.trainClassifier(
            training_data,
            categoricalFeaturesInfo={},
            numIterations=num_iterations,
            maxDepth=max_depth,
            learningRate=0.1,
            maxBins=32,
        )

        # Evaluate the model on the validation set
        predictions = model.predict(test_data.map(lambda x: x.features))
        labels_and_predictions = test_data.map(lambda x: x.label).zip(predictions)

        # Calculate accuracy
        metrics = MulticlassMetrics(labels_and_predictions)
        accuracy = metrics.accuracy

        print(
            f"numIterations: {num_iterations}, maxDepth: {max_depth}, Accuracy: {accuracy}"
        )

        # Track the best model
        if accuracy > best_accuracy:
            best_accuracy = accuracy
            best_model = model
            best_params = {"numIterations": num_iterations, "maxDepth": max_depth}

# Print the best model and its parameters
print(f"Best Model Parameters: {best_params}")
print(f"Best Model Accuracy: {best_accuracy}")

# Stop the Spark session
spark.stop()


# In[ ]:
