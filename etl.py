import configparser
import pandas as pd
from datetime import datetime
from pyspark.sql.functions import dayofweek
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, lit
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, to_date, upper
import logging
from pyspark.sql.types import DateType
from pyspark.sql.functions import monotonically_increasing_id


# CONFIG
config = configparser.ConfigParser()
config.read('dl.cfg')

KEY = config.get('AWS', 'AWS_ACCESS_KEY_ID')
SECRET = config.get('AWS', 'AWS_SECRET_ACCESS_KEY')
output_data = config.get('S3', 'DEST_S3_BUCKET')


os.environ['AWS_ACCESS_KEY_ID']=KEY
os.environ['AWS_SECRET_ACCESS_KEY']=SECRET

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.repositories", "https://repos.spark-packages.org/")\
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0,saurfang:spark-sas7bdat:2.0.0-s_2.11")\
        .enableHiveSupport().getOrCreate()
    return spark


def rename_columns(table, new_columns):
    for original, new in zip(table.columns, new_columns):
        table = table.withColumnRenamed(original, new)
    return table


def SAS_to_date(date):
    if date is not None:
        return pd.to_timedelta(date, unit='D') + pd.Timestamp('1960-1-1')

SAS_to_date_udf = udf(SAS_to_date, DateType())


def process_immigration_data(spark, output_data):
    """Process immigration data to get f_immigration, d_immi_citzen and d_immi_airline tables
        Arguments:
            spark {object}: SparkSession object
            input_data {object}: Source S3 endpoint
            output_data {object}: Target S3 endpoint
        Returns:
            None
    """
    logging.info("Start processing immigration")
    
    # read immigration data file
    df = spark.read.format("com.github.saurfang.sas.spark").load("../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat", forceLowercaseNames=True, inferLong=True)
    
    logging.info("Start processing f_immigration")
    
    # extract columns to create fact_immigration table
    f_immigration = df.select('cicid', 'i94yr', 'i94mon', 'i94port', 'i94addr', 'arrdate', 'depdate', 'i94mode', 'i94visa')
    f_immigration = f_immigration.distinct()
    f_immigration = f_immigration.withColumn("immigration_id", monotonically_increasing_id())
    
    # data wrangling to match data model
    new_columns = ['cic_id', 'year', 'month', 'city_code', 'state_code', 'arrive_date', 'departure_date', 'mode', 'visa']
    
    # renaming columns using the function rename_columns()
    f_immigration = rename_columns(f_immigration, new_columns)
    
    #  add a new column to f_immigration by assigning a literal or constant value = United States
    f_immigration = f_immigration.withColumn('country', lit('United States'))
    
    # convert column arrive_date to date type format
    f_immigration = f_immigration.withColumn('arrive_date', SAS_to_date_udf(col('arrive_date')))
    
    # convert column departure_date to date type format
    f_immigration = f_immigration.withColumn('departure_date', SAS_to_date_udf(col('departure_date')))
    
    logging.info("Start loading f_immigration parquet files partitioned by state_code")
    
    # write f_immigration table to parquet files partitioned by state_code
    f_immigration.write.mode("overwrite").partitionBy('state_code').parquet(path=output_data + 'f_immigration')   
    
    
    
    logging.info("Start processing d_citizen table")
    
    # extract columns from immigration data file to create d_citizen table
    d_citizen = df.select('cicid', 'i94cit', 'i94res', 'biryear', 'gender', 'insnum').distinct().withColumn("immi_citizen_id", monotonically_increasing_id())
    
    # data wrangling to match data model
    new_columns = ['cic_id', 'citizen_country', 'residence_country', 'birth_year', 'gender', 'ins_num']
    d_citizen = rename_columns(d_citizen, new_columns)

    # write d_citizen table to parquet files
    d_citizen.write.mode("overwrite").parquet(path=output_data + 'd_citizen')
    
    
    
    logging.info("Start processing d_airline")
    
    # extract columns from immigration data file to create d_airline table
    d_airline = df.select('cicid', 'airline', 'admnum', 'fltno', 'visatype').distinct().withColumn("immi_airline_id", monotonically_increasing_id())
    
    # data wrangling to match data model
    new_columns = ['cic_id', 'airline', 'admin_num', 'flight_number', 'visa_type']
    d_airline = rename_columns(d_airline, new_columns)

    # write d_airline table to parquet files
    d_airline.write.mode("overwrite").parquet(path=output_data + 'd_airline')
    
    
    
def process_label_descriptions(spark, input_data, output_data):
    """ Parsing label desctiption file to get codes of country, city, state
        Arguments:
            spark {object}: SparkSession object
            input_data {object}: Source S3 endpoint
            output_data {object}: Target S3 endpoint
        Returns:
            None
    """

    logging.info("Start processing label descriptions")
    label_file = os.path.join(input_data + "I94_SAS_Labels_Descriptions.SAS")
    with open(label_file) as f:
        contents = f.readlines()

    country_code = {}
    for countries in contents[10:245]:
        pair = countries.split('=')
        code, country = pair[0].strip(), pair[1].strip().strip("'")
        country_code[code] = country
        
    spark.createDataFrame(country_code.items(), ['code', 'country'])\
         .write.mode("overwrite")\
         .parquet(path=output_data + 'country_code')

    city_code = {}
    for cities in contents[302:962]:
        pair = cities.split('=')
        code, city = pair[0].strip("\t").strip().strip("'"),\
                     pair[1].strip('\t').strip().strip("''")
        city_code[code] = city
    spark.createDataFrame(city_code.items(), ['code', 'city'])\
         .write.mode("overwrite")\
         .parquet(path=output_data + 'city_code')

    state_code = {}
    for states in contents[981:1036]:
        pair = states.split('=')
        code, state = pair[0].strip('\t').strip("'"), pair[1].strip().strip("'")
        state_code[code] = state
    spark.createDataFrame(state_code.items(), ['code', 'state'])\
         .write.mode("overwrite")\
         .parquet(path=output_data + 'state_code')
    
def process_temperature_data(spark, output_data):
    """ Process temperature data to get dim_temperature table
        Arguments:
            spark {object}: SparkSession object
            input_data {object}: Source S3 endpoint
            output_data {object}: Target S3 endpoint
        Returns:
            None
    """

    logging.info("Start processing d_temperature")
    # read temperature data file
    tempe_data = os.path.join('../../data2/GlobalLandTemperaturesByCity.csv')
    df = spark.read.csv(tempe_data, header=True)

    df = df.where(df['Country'] == 'United States')
    d_temperature = df.select(['dt', 'AverageTemperature', 'AverageTemperatureUncertainty',\
                         'City', 'Country']).distinct()

    new_columns = ['dt', 'avg_temp', 'avg_temp_uncertnty', 'city', 'country']
    d_temperature = rename_columns(d_temperature, new_columns)

    d_temperature = d_temperature.withColumn('dt', to_date(col('dt')))
    d_temperature = d_temperature.withColumn('year', year(d_temperature['dt']))
    d_temperature = d_temperature.withColumn('month', month(d_temperature['dt']))
 
    # write dim_temperature table to parquet files
    d_temperature.write.mode("overwrite")\
                   .parquet(path=output_data + 'd_temperature')
    
def process_demography_data(spark, input_data, output_data):
    """ Process demograpy data to get dim_demog_population 
     and d_demog_statistics table
        Arguments:
            spark {object}: SparkSession object
            input_data {object}: Source S3 endpoint
            output_data {object}: Target S3 endpoint
        Returns:
            None
    """

    logging.info("Start processing d_demog_statistics")
    # read demography data file
    demog_data = os.path.join(input_data + 'us-cities-demographics.csv')
    df = spark.read.format('csv').options(header=True, delimiter=';').load(demog_data)


    d_demog_statistics = df.select(['City', 'State', 'Male Population', 'Female Population', \
                              'Number of Veterans', 'Foreign-born', 'Race']).distinct() \
                              .withColumn("demog_pop_id", monotonically_increasing_id())


    new_columns = ['city', 'state', 'male_population', 'female_population', \
                   'num_vetarans', 'foreign_born', 'race']
    d_demog_statistics = rename_columns(d_demog_statistics, new_columns)

    # write dim_demog_population table to parquet files
    d_demog_statistics.write.mode("overwrite")\
                        .parquet(path=output_data + 'd_demog_statistics')

    
    logging.info("Start processing d_demog_statistics")
    d_demog_statistics = df.select(['City', 'State', 'Median Age', 'Average Household Size'])\
                             .distinct()\
                             .withColumn("d_demog_statistics", monotonically_increasing_id())

    new_columns = ['city', 'state', 'median_age', 'avg_household_size']
    d_demog_statistics = rename_columns(d_demog_statistics, new_columns)
    d_demog_statistics = d_demog_statistics.withColumn('city', upper(col('city')))
    d_demog_statistics = d_demog_statistics.withColumn('state', upper(col('state')))

    # write dim_demog_statistics table to parquet files
    d_demog_statistics.write.mode("overwrite")\
                        .parquet(path=output_data + 'd_demog_statistics')
    
    
'''Paths for local testing'''
input_data = "./" # if runs on S3 bucket, please replace by SOURCE_S3_BUCKET
output_data = "s3a://gfp-udacity/" # if runs on S3 bucket, please replace by DEST_S3_BUCKET = s3a://gfp-udacity/

spark = create_spark_session()

process_immigration_data(spark, output_data)
process_label_descriptions(spark, input_data, output_data)
process_temperature_data(spark, output_data)
process_demography_data(spark, input_data, output_data)
