import pandas as pd
import pyspark
from pyspark.sql import types
from pyspark.sql import functions as F

def rename_columns_rides_data(data_rides:pd.DataFrame, year:int):
    if year == 2016:
        data_rides.columns = ['trip_id', 
                            'trip_start_time', 
                            'trip_stop_time',
                            'trip_duration_seconds', 
                            'from_station_name', 
                            'to_station_name', 
                            'user_type']
    elif year == 2017:
        data_rides.columns = ['trip_id', 
                            'trip_start_time', 
                            'trip_stop_time',
                            'trip_duration_seconds', 
                            'from_station_id',
                            'from_station_name',
                            'to_station_id', 
                            'to_station_name', 
                            'user_type']
    elif year == 2018:
        data_rides.columns = ['trip_id', 
                            'trip_duration_seconds', 
                            'from_station_id',
                            'trip_start_time', 
                            'from_station_name', 
                            'trip_stop_time',
                            'to_station_id', 
                            'to_station_name', 
                            'user_type']
    elif year in [2019,2020,2021,2022]:
        data_rides.columns = ['trip_id', 
                            'trip_duration_seconds', 
                            'from_station_id', 
                            'trip_start_time', 
                            'from_station_name',
                            'to_station_id', 
                            'trip_stop_time',  
                            'to_station_name',
                            'bike_id', 
                            'user_type']
    return data_rides

def get_time_of_day(hour:int):
    if hour in [6,7,8,9,10]:
        return 'morning'
    elif hour in [11,12,13,14,15,16,17]:
        return 'day'
    elif hour in [18,19,20,21,22]:
        return 'evening'
    else:
        return 'night'
get_time_of_day_udf = F.udf(get_time_of_day, returnType=types.StringType())

    
def add_time_variables(data_rides:pd.DataFrame):
    data_rides['year'] = data_rides.trip_start_time.apply(lambda x: x.year)
    data_rides['month'] = data_rides.trip_start_time.apply(lambda x: x.month)
    data_rides['day'] = data_rides.trip_start_time.apply(lambda x: x.day)
    data_rides['weekday'] = data_rides.trip_start_time.apply(lambda x: x.dayofweek)
    data_rides['weekend'] = data_rides['weekday'] > 4
    
    data_rides['hour'] = data_rides.trip_start_time.apply(lambda x: x.hour)
    # 0:= first quarter hour, 1:= second quarter hour 2:= third quarter hour, 3:= forth quarter hour
    data_rides['quarter_hour'] = data_rides.trip_start_time.apply(lambda x: x.round('15min').minute//15)
    # morning:= 6-10 o'clock, day:= 11-17 o'clock, evening:= 18-22 o'clock, night:= 23-5 o'clock
    data_rides['time_of_day'] = data_rides.trip_start_time.apply(lambda x: get_time_of_day(x.hour))

    return data_rides

def add_time_variables_pyspark(data_rides):
    data_rides.withColumn('year', F.year('trip_start_time'))
    data_rides.withColumn('month', F.month('trip_start_time'))
    data_rides.withColumn('day', F.dayofmonth('trip_start_time'))
    data_rides.withColumn('weekday', F.dayofweek('trip_start_time'))
    data_rides.withColumn('weekend', F.dayofweek('trip_start_time')>4)
    
    data_rides.withColumn('hour', F.hour('trip_start_time'))
    data_rides.withColumn('quarter_hour', F.minute('trip_start_time')//15)
    data_rides.withColumn('time_of_day', get_time_of_day_udf(F.hour('trip_start_time')))

    return data_rides

# 2016

def add_information_from_keys_2016(data_rides:pd.DataFrame, key_stationName_stationId_placeId:pd.DataFrame):
    
    data_rides['from_station_id'] = None
    data_rides['from_place_id'] = ''
    data_rides['from_lat'] = None
    data_rides['from_lng'] = None
    data_rides['to_station_id'] = None
    data_rides['to_place_id'] = ''
    data_rides['to_lat'] = None
    data_rides['to_lng'] = None
    
    for i in range(len(data_rides)):
        #df_tmp_1 = key_stationName_stationId_placeId[key_stationName_stationId_placeId.KEY.apply(lambda x: x[0]) == data_rides.from_station_name[i]]
        df_tmp_1 = key_stationName_stationId_placeId[key_stationName_stationId_placeId.station_name == data_rides.from_station_name[i]]
        #df_tmp_2 = key_stationName_stationId_placeId[key_stationName_stationId_placeId.KEY.apply(lambda x: x[0]) == data_rides.to_station_name[i]]
        df_tmp_2 = key_stationName_stationId_placeId[key_stationName_stationId_placeId.station_name == data_rides.to_station_name[i]]
        if len(df_tmp_1):
            data_rides.loc[i, 'from_station_id'] = int(df_tmp_1.station_id)
            data_rides.loc[i, 'from_place_id'] = df_tmp_1.place_id.values[0]
            data_rides.loc[i, 'from_lat'] = df_tmp_1.location_lat.values[0]
            data_rides.loc[i, 'from_lng'] = df_tmp_1.location_lng.values[0]
        if len(df_tmp_2):
            data_rides.loc[i, 'to_station_id'] = int(df_tmp_2.station_id)
            data_rides.loc[i, 'to_place_id'] = df_tmp_2.place_id.values[0]
            data_rides.loc[i, 'to_lat'] = df_tmp_2.location_lat.values[0]
            data_rides.loc[i, 'to_lng'] = df_tmp_2.location_lng.values[0]
    
    return data_rides

def clean_rides_data_2016(df_2016:pd.DataFrame, key_stationName_stationId_placeId:pd.DataFrame, spark:pyspark.sql.session.SparkSession):
    # Rename columns uniformly
    df_2016 = rename_columns_rides_data(df_2016, 2016)
    # Get correct data types
    df_2016.trip_start_time = pd.to_datetime(df_2016.trip_start_time) 
    df_2016.trip_stop_time = pd.to_datetime(df_2016.trip_stop_time)    
    # Delete rides with no start station
    df_2016 = df_2016[~df_2016.from_station_name.isna()].reset_index(drop=True)
    # Delete rides with no end station
    df_2016 = df_2016[~df_2016.to_station_name.isna()].reset_index(drop=True)
    
    sparkDF_2016 = spark.createDataFrame(df_2016) 
    
    # Add columns from bikestation_location
    df_2016 = add_information_from_keys_2016(df_2016, key_stationName_stationId_placeId)
    
    # Add columns 'year', 'month', 'day', 'weekday', 'weekend'
    df_2016 = add_time_variables(df_2016)
    
    df_2016 = df_2016.dropna()
    
    return df_2016

# 2017


def clean_bikestation_location_data():
    pass