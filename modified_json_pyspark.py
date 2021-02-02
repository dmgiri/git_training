import sys
from functools import reduce

from pyspark.sql.functions import explode, col, explode_outer
from pyspark.sql.types import ArrayType, StructType

from util import get_spark_session

json_file_path = r'/user/ods/ods_external_ds/fullload/360_Giving/*'
spark = get_spark_session(sys.argv[1], 'GMS - Populate account_delta')

df = spark.read.json(json_file_path, multiLine=True)
giving_df = df.withColumn('grants_mod', explode('grants')).drop('grants')


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


giving_df = flatten(giving_df)

old_col = giving_df.schema.names
new_dict = {ord(x): '_' for x in '[ ,;{}()\n\t=]'}
new_col = [x.translate(new_dict) for x in old_col]

dim_organisation_account_delta = reduce(
    lambda giving_df, idx: giving_df.withColumnRenamed(old_col[idx], new_col[idx]),
    range(len(old_col)),
    giving_df)

spark.sql("use dm_gms_360")
spark.sql('DROP TABLE IF EXISTS 360_giving')
giving_df.write.option("header", "true").mode("overwrite").saveAsTable("360_giving")
