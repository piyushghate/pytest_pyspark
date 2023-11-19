import findspark
findspark.init()

import pytest
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Create a Spark session
spark = SparkSession.builder \
    .appName("example") \
    .getOrCreate()

current_script_directory = os.path.dirname(os.path.realpath(__file__))

file_name = 'dataset/organization.csv'
file_path = os.path.join(current_script_directory, file_name)

@pytest.fixture
def df():
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    return df


columList = [('Index', 'int'),
             ('Organization Id', 'string'),
             ('Name', 'string'),
             ('Website', 'string'),
             ('Country', 'string'),
             ('Description', 'string'),
             ('Founded', 'int'),
             ('Industry', 'string'),
             ('Number of employees', 'int')]



# function to return the data type of the column
def func1(df, colName):
    for item in df.dtypes:
        if item[0] == colName:
            return item[1]
    return 0

# check the length of the file
def test_file_length(df):
    assert len(columList) == len(df.columns)

# check the imp columns exits
def test_column_exits(df):
    for item in columList:
        assert item[0] in df.columns


# check nulls
def test_check_pk_null(df):
    assert df.filter(F.col('Organization Id').isNull()).count() == 0


# check unique values
def test_unique_values(df):
    test_df = df.groupBy('Organization Id').agg(
        F.count('Organization Id').alias('count')
    ).filter(F.col('count') > 1)
    assert test_df.count() == 0


##check data types
def test_check_datatypes(df):
    for item in columList:
        assert func1(df, item[0]) == item[1]
