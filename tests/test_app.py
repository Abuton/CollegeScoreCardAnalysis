# run docker-compose up test

import os, sys
import pytest
import app
from pyspark.sql import Row

sys.path.append(os.path.join('..'))


@pytest.mark.usefixtures("spark_session")
def test_read_college_date(spark_session):
    test_df = app.read_college_date(spark_session)
    assert test_df


@pytest.mark.usefixtures("spark_session")
def test_add_source_file(spark_session):
    test_df = app.read_college_date(spark_session)
    test_df = app.add_source_file(test_df)
    assert 'source_file' in test_df.columns


@pytest.mark.usefixtures("spark_session")
def test_select_and_filter(spark_session):
    test_df = spark_session.createDataFrame(
        [('10000', 'IA', 'test.file'), ('NULL', 'IA', 'test.file')],
        ['NPT4_PUB', 'STABBR', 'source_file']
    )
    test_df = app.select_and_filter(test_df)
    assert test_df.count() == 1


@pytest.mark.usefixtures("spark_session")
def test_pull_year_from_file_name(spark_session):
    test_df = spark_session.createDataFrame(
        [('10000', 'IA', 'MERGED2019_')],
        ['NPT4_PUB', 'STABBR', 'source_file']
    )

    test_df = app.pull_year_from_file_name(test_df)
    assert test_df.select('year').collect() == [Row(year='2019')]