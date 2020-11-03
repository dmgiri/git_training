import pandas as pd
import numpy as np
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark import SparkContext

from pyspark.sql.types import *

path = r'C:\Users\Sai\Desktop\pyspark_project\grantnav-20201030062709.csv'

df = pd.read_csv(path)
df = df.replace('[\n\r\t]','', regex=True)

def equivalent_type(f):
  if f == 'datetime64[ns]': return DateType()
  elif f == 'int64': return LongType()
  elif f == 'int32': return IntegerType()
  elif f == 'float64': return FloatType()
  else: return StringType()

def define_structure(string, format_type):
  try: typo = equivalent_type(format_type)
  except: typo = StringType()
  return StructField(string, typo)

#Given pandas dataframe, it will return a spark's dataframe
def pandas_to_spark(df_pandas):
  columns = list(df_pandas.columns)
  types = list(df_pandas.dtypes)
  struct_list = []
  for column, typo in zip(columns, types):
    struct_list.append(define_structure(column, typo))
  p_schema = StructType(struct_list)
  spark = SparkSession.builder.master("local").appName("aap").getOrCreate()

  return spark.createDataFrame(df_pandas, p_schema)



# pandas to spark
sp_df = pandas_to_spark(df)
sp_df.show(3000)

