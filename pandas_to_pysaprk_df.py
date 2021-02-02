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

#['Identifier', 'Title', 'Description', 'Currency', 'Amount Applied For', 'Amount Awarded', 'Amount Disbursed', 'Award Date', 'URL', 'Planned Dates:Start Date', 'Planned Dates:End Date', 'Planned Dates:Duration (months)', 'Actual Dates:Start Date', 'Actual Dates:End Date', 'Actual Dates:Duration (months)', 'Recipient Org:Identifier', 'Recipient Org:Name', 'Recipient Org:Charity Number', 'Recipient Org:Company Number', 'Recipient Org:Postal Code', 'Recipient Org:Location:0:Geographic Code Type', 'Recipient Org:Location:0:Geographic Code', 'Recipient Org:Location:0:Name', 'Recipient Org:Location:1:Geographic Code Type', 'Recipient Org:Location:1:Geographic Code', 'Recipient Org:Location:1:Name', 'Recipient Org:Location:2:Geographic Code Type', 'Recipient Org:Location:2:Geographic Code', 'Recipient Org:Location:2:Name', 'Funding Org:Identifier', 'Funding Org:Name', 'Funding Org:Postal Code', 'Grant Programme:Code', 'Grant Programme:Title', 'Grant Programme:URL', 'Beneficiary Location:0:Name', 'Beneficiary Location:0:Country Code', 'Beneficiary Location:0:Geographic Code', 'Beneficiary Location:0:Geographic Code Type', 'Beneficiary Location:1:Name', 'Beneficiary Location:1:Country Code', 'Beneficiary Location:1:Geographic Code', 'Beneficiary Location:1:Geographic Code Type', 'Beneficiary Location:2:Name', 'Beneficiary Location:2:Country Code', 'Beneficiary Location:2:Geographic Code', 'Beneficiary Location:2:Geographic Code Type', 'Beneficiary Location:3:Name', 'Beneficiary Location:3:Country Code', 'Beneficiary Location:3:Geographic Code', 'Beneficiary Location:3:Geographic Code Type', 'Beneficiary Location:4:Name', 'Beneficiary Location:4:Country Code', 'Beneficiary Location:4:Geographic Code', 'Beneficiary Location:4:Geographic Code Type', 'Beneficiary Location:5:Name', 'Beneficiary Location:5:Country Code', 'Beneficiary Location:5:Geographic Code', 'Beneficiary Location:5:Geographic Code Type', 'Beneficiary Location:6:Name', 'Beneficiary Location:6:Country Code', 'Beneficiary Location:6:Geographic Code', 'Beneficiary Location:6:Geographic Code Type', 'Beneficiary Location:7:Name', 'Beneficiary Location:7:Country Code', 'Beneficiary Location:7:Geographic Code', 'Beneficiary Location:7:Geographic Code Type', 'From An Open Call?', 'Data Source', 'Publisher:Name', 'Recipient Region', 'Recipient District', 'Recipient District Geographic Code', 'Recipient Ward', 'Recipient Ward Geographic Code', 'Retrieved for use in GrantNav', 'License (see note)']


