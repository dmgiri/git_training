from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, explode_outer
from pyspark.sql.types import ArrayType, StructType

appName = "PySpark Example - JSON file to Spark Data Frame"
master = "local"

spark = SparkSession.builder \
    .appName(appName) \
    .master(master) \
    .getOrCreate()


json_file_path = r'C:\Users\Sai\Desktop\pyspark_project\new.json'
df = spark.read.json(json_file_path, multiLine=True)
df_flat = df.withColumn('grants_mod', explode('grants')).drop('grants')

def flatten(df):
    complex_fields = dict([(field.name, field.dataType)
                           for field in df.schema.fields
                           if type(field.dataType) == ArrayType or type(field.dataType) == StructType])
    while len(complex_fields) != 0:
        col_name = list(complex_fields.keys())[0]

        if (type(complex_fields[col_name]) == StructType):
            expanded = [col(col_name + '.' + k).alias(col_name + '_' + k) for k in
                        [n.name for n in complex_fields[col_name]]]

            df = df.select("*", *expanded).drop(col_name)

        elif (type(complex_fields[col_name]) == ArrayType):
            df = df.withColumn(col_name, explode_outer(col_name))

        complex_fields = dict([(field.name, field.dataType)
                               for field in df.schema.fields
                               if type(field.dataType) == ArrayType or type(field.dataType) == StructType])
    return df


df_flat = flatten(df_flat)
print(df_flat.schema.names)
