# run docker build --tag=college-history

# import dependencies
from pyspark.sql import SparkSession, DataFrame, Window
import pyspark.sql.functions as F

# read all csv data in data/
def read_college_date(spark: SparkSession, data_location: str = 'data/*.csv') -> DataFrame:
    df = spark.read.csv(data_location, header='true')
    return df

# add the source file column to df
def add_source_file(df: DataFrame) -> DataFrame:
    df = df.withColumn('source_file', F.input_file_name())
    return df

# select only important columns
def select_and_filter(df: DataFrame) -> DataFrame:
    df = df.select('NPT4_PUB', 'NPT4_PRIV', 'STABBR', 'source_file')
    df = df.where(F.col('NPT4_PUB') != 'NULL') # if we don't have cost information, throw it out.
    return df

# extract year from the newly added column 'source_file'
def pull_year_from_file_name(df: DataFrame) -> DataFrame:
    # pull out the year that can be found in the file name between "MERGED" and "_"
    df = df.withColumn('year',
                       F.substring(F.regexp_extract(F.col('source_file'),
                        'MERGED(.*)_', 1), 1, 4))
    return df

# get cost per year on public instituition
def pull_cost_by_year_metrics_public(df: DataFrame) -> DataFrame:
    results = df.groupby('year').agg(F.avg('NPT4_PUB')).alias('avg_cost_public_instituiion')
    results.sort(F.col('year').desc()).show()


# get cost per year on private instituition
def pull_cost_by_year_metrics_private(df: DataFrame) -> DataFrame:
    results = df.groupby('year').agg(F.avg('NPT4_PRIV')).alias('avg_cost_private_instituiion')
    results.sort(F.col('year').desc()).show()

# Which States cost the most when it comes to attending college?
def pull_most_expensive_states_for_college(df: DataFrame):
    df = df.filter(F.col('year') == 2019)
    win = Window.partitionBy('STABBR').orderBy(F.col('NPT4_PUB').desc())
    df = df.withColumn('rowNum', F.row_number().over(win))
    df = df.filter(F.col('rowNum') <= 10)
    df = df.groupby('STABBR').agg(F.avg(F.col('NPT4_PUB')).alias('avg_cost'))
    print('Which States cost the most when it comes to attending college?')
    df.sort(F.col('avg_cost').desc()).limit(10).show()

# Which States has the highest number of student?
def pull_number_of_student_per_states_public(df: DataFrame):
    win = Window.partitionBy('STABBR').orderBy(F.col('NUM4_PUB').desc())
    df = df.withColumn('rowNum', F.row_number().over(win))
    df = df.groupby('STABBR').agg(F.sum(F.col('NUM4_PUB')).alias('total_number_of_students'))
    print("Top 10 States with Higher Number of Student in Public Instituition")
    df.sort(F.col('total_number_of_students').desc()).limit(20).show()

# Which States has the highest number of student?
def pull_number_of_student_per_states_private(df: DataFrame):
    win = Window.partitionBy('STABBR').orderBy(F.col('NUM4_PRIV').desc())
    df = df.withColumn('rowNum', F.row_number().over(win))
    df = df.groupby('STABBR').agg(F.sum(F.col('NUM4_PRIV')).alias('total_number_of_students'))
    print("Top 10 States with Higher Number of Student in Private Instituition")
    df.sort(F.col('total_number_of_students').desc()).limit(20).show()
    


def main():
    spark = SparkSession.builder.appName('HistoricCollegeData') \
        .getOrCreate()

    df = read_college_date(spark=spark)
    df = add_source_file(df)
    df = select_and_filter(df)
    df = pull_year_from_file_name(df)
    pull_cost_by_year_metrics_public(df)
    pull_cost_by_year_metrics_private(df)
    pull_most_expensive_states_for_college(df)
    pull_number_of_student_per_states_public(df)
    pull_number_of_student_per_states_private(df)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()