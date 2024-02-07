from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import lead, col
from graphframes import GraphFrame

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("User Navigation Patterns") \
    .getOrCreate()

# Assuming df is your DataFrame containing user navigation data
# Load or create your DataFrame here

# Create Vertices DataFrame
vertices = df.selectExpr("screen_name as id").distinct()

# Define a window spec for ordering events within each user session
windowSpec = Window.partitionBy("customer_id").orderBy("hitsnumber")

# Create Edges DataFrame
edges = df.withColumn("dst", lead("screen_name").over(windowSpec)) \
    .select("customer_id", "screen_name", "dst") \
    .withColumnRenamed("customer_id", "user") \
    .withColumnRenamed("screen_name", "src") \
    .filter(col("dst").isNotNull())  # Remove rows where there is no subsequent screen

# Initialize GraphFrame
g = GraphFrame(vertices, edges)

# Find paths leading to 'InvestmentsPage'
motifs = g.find("(a)-[e1]->(b); (b)-[e2]->(c)")
motifs.filter("c.id = 'InvestmentsPage'").show()


# additional analysis

# Define a conversion screen for analysis
conversion_screen = 'PurchaseConfirmationPage'

# Find paths leading to the conversion screen
# This query finds all paths of length 2 leading to the conversion screen
conversion_motifs = g.find("(a)-[e1]->(b); (b)-[e2]->(c)") \
    .filter(f"c.id = '{conversion_screen}'")

# Group by the path (a.id, b.id) and count the occurrences to find the most common paths
common_paths = conversion_motifs.groupBy("a.id", "b.id") \
    .count() \
    .orderBy("count", ascending=False)

common_paths.show()


# variable path

from pyspark.sql import functions as F

# Initialize GraphFrame as done previously
# g = GraphFrame(vertices, edges)

# Define the target screen
target_screen = "SpecificScreen"

# Initialize an empty DataFrame to store paths
all_paths = None

# Iterate to find paths of increasing lengths
for length in range(1, 11):  # Up to 10 hops
    # Find motifs representing paths of the current length ending at the target screen
    pattern = "-".join([f"(v{i})-[e{i}]->" for i in range(length)]) + f"(v{length});"
    paths = g.find(pattern).filter(f"v{length}.id = '{target_screen}'")
    
    # Select and rename columns to standardize schema across iterations
    selected_columns = [F.col(f"v{i}.id").alias(f"screen_{i}") for i in range(length)]
    paths = paths.select(*selected_columns)
    
    # Aggregate results
    if all_paths is None:
        all_paths = paths
    else:
        all_paths = all_paths.unionByName(paths, allowMissingColumns=True)

# Show results
all_paths.show(truncate=False)

# more things

# Create Vertices DataFrame
vertices = df.selectExpr("screen_name as id").distinct()

# Define a window spec
windowSpec = Window.partitionBy("customer_id").orderBy("hitsnumber")

# Create Edges DataFrame with a lagged 'screen_name' to represent 'src' to 'dst' navigation
edges = df.withColumn("dst", lead("screen_name").over(windowSpec)) \
    .select("customer_id", "screen_name", "dst") \
    .withColumnRenamed("customer_id", "user") \
    .withColumnRenamed("screen_name", "src") \
    .filter(col("dst").isNotNull())  # Filter out the last navigation event for each user session

