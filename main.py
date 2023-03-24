import logging
logging.basicConfig(filename='app.log', 
                    filemode='w', 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', 
                    level=logging.INFO)


from load_data_opentoronto import *
from generate_additional_information_tables import *
from data_opentoronto_transformation import *

from get_bikestation_location import get_bikestation_location

import findspark
findspark.init()

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import types
from pyspark.sql import functions as F

logging.info('Create Spark Session')
spark = SparkSession.builder \
    .master("local[*]") \
    .appName('toronto_rides') \
    .getOrCreate()

import os

if __name__ == "__main__":
    
    logging.info('Get Web Data Structure from JSON file')
    structure_toronto_bikeshare = gather_structure_toronto_bikeshare()
    
    logging.info('Load Data for years 2016 - 2022')
    
    df_2016 = fetch_2016(structure_toronto_bikeshare["2016"]["url"])
    logging.info('Year 2016 loaded')
    
    df_2017 = fetch_2017_2021(structure_toronto_bikeshare["2017"]["url"], year=2017)
    logging.info('Year 2017 loaded')
    
    df_2018 = fetch_2017_2021(structure_toronto_bikeshare["2018"]["url"], year=2018)
    logging.info('Year 2018 loaded')
    
    df_2019 = fetch_2017_2021(structure_toronto_bikeshare["2019"]["url"], year=2019)
    logging.info('Year 2019 loaded')
    
    df_2020 = fetch_2017_2021(structure_toronto_bikeshare["2020"]["url"], year=2020)
    logging.info('Year 2020 loaded')
    
    df_2021 = fetch_2017_2021(structure_toronto_bikeshare["2021"]["url"], year=2021)
    logging.info('Year 2021 loaded')
    
    df_2022 = fetch_2022(structure_toronto_bikeshare["2022"]["url"])
    logging.info('Year 2022 loaded')
    
    logging.info('Generate Table with unique station names')
    data_station_unique_name = generate_all_unique_station_names(data_list=[df_2016, df_2017, df_2018, df_2019, df_2020, df_2021, df_2022],
                                                                 year_list=[2016, 2017, 2018, 2019, 2020, 2021, 2022])
    logging.info('Table with unique staion names is generated')
    
    logging.info('Generate/Load bikestation_location from Google Maps API')
    if os.path.isfile('./data/bikestation_location.csv'):
        bikestation_location = pd.read_csv('./data/bikestation_location.csv')
    else:
        bikestation_location = get_bikestation_location(data_station_unique_name)
    logging.info('bikestation_location is generated/loaded')
        
    logging.info('Generate Table Station Name <-> Station Id')
    data_stationName_stationId = generate_table_StationName_StationID_from_data(data_list=[df_2017, df_2018, df_2019, df_2020, df_2021, df_2022],
                                                                                year_list=[2017, 2018, 2019, 2020, 2021, 2022])
    logging.info('Table StationName <-> StationId is generated')
    
    logging.info('Generate Key StationName <-> StationId <-> Place Id (Google Maps API)')
    # Fill missing values for place_id with 0 as default value for missing get request
    bikestation_location = bikestation_location.fillna(value={'place_id':0})
    
    # Merge tables
    key_stationName_stationId_placeId = bikestation_location.merge(data_stationName_stationId, how='left', on='station_name')
    
    # Clean Data for 2016
    logging.info('Clean/Transform Rides Data 2016')
    df_2016 = clean_rides_data_2016(df_2016, key_stationName_stationId_placeId)
    
    # Clean Data for 2017
    logging.info('Clean/Transform Rides Data 2017')
    
    # Clean Data for 2018
    logging.info('Clean/Transform Rides Data 2018')
    
    # Clean Data for 2019
    logging.info('Clean/Transform Rides Data 2019')
    
    # Clean Data for 2020
    logging.info('Clean/Transform Rides Data 2020')
    
    # Clean Data for 2021
    logging.info('Clean/Transform Rides Data 2021')  
    
    # Clean Data for 2022
    logging.info('Clean/Transform Rides Data 2022')
    
#    key_stationName_stationId_placeId = generate_key_StationName_StationID_PlaceId(bikestation_location, data_stationName_stationId)
    
#    logging.info('Generate/Load bikestation_distance from Google Maps API')
#    if os.path.isfile('./data/bikestation_distance.csv'):
#        bikestation_distance = pd.read_csv('./data/bikestation_distance.csv')
#    else:
#        bikestation_distance = get_bikestation_distance(bikestation_location)
#    logging.info('bikestation_distance is generated/laoded')
    

    

    

    
  

    