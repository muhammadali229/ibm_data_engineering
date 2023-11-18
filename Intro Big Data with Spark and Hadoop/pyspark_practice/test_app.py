import findspark
findspark.init()
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, BooleanType, ArrayType, MapType
from pyspark.sql.functions import col, struct, when, lit, sum, expr, array_contains, udf, upper, explode, row_number, rank, dense_rank, lead, current_date, date_format, to_date, datediff, from_json, to_json, json_tuple, get_json_object, collect_set
from pyspark.sql.window import Window
from pyspark.sql.types import StringType
import json
import time
spark = SparkSession.builder.master('local[1]').appName('ali_spark_condv1').getOrCreate()
spark.udf.registerJavaFunction('encrypt_sal', 'bt.encryptsal.EncryptSalary', StringType())
data2 = [("James","","Smith","36636","M",3000),
    ("Michael","Rose","","40288","M",4000),
    ("Robert","","Williams","42114","M",4000),
    ("Maria","Anne","Jones","39192","F",4000),
    ("Jen","Mary","Brown","","F",-1)
  ]

schema = StructType([ \
    StructField("firstname",StringType(),True), \
    StructField("middlename",StringType(),True), \
    StructField("lastname",StringType(),True), \
    StructField("id", StringType(), True), \
    StructField("gender", StringType(), True), \
    StructField("salary", IntegerType(), True) \
  ])
 
df = spark.createDataFrame(data=data2,schema=schema)
df.printSchema()
df.withColumn("en", expr("encrypt_sal(firstname)")).show()