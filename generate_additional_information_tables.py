from data_opentoronto_transformation import *

import os
import pandas as pd
from thefuzz import process

def generate_all_unique_station_names(data_list:list[pd.DataFrame],
                                      year_list:list[int],
                                      save_table:bool = True):
    data = pd.concat([rename_columns_rides_data(elm, year_list[i])[['from_station_name','to_station_name','user_type']] for i, elm in enumerate(data_list)])
    # Delete rows with false structure (last column missing due to error in data (missing comma))
    data = data.dropna()
    # Strip all strings
    data['from_station_name'] = data.from_station_name.apply(lambda x: x.strip())
    data['to_station_name'] = data.to_station_name.apply(lambda x: x.strip())
    
    # Rename columns to concat start station and end station into one dataset
    df1 = data[['from_station_name']].rename(columns={'from_station_name':'station_name'})
    df2 = data[['to_station_name']].rename(columns={'to_station_name':'station_name'})
    # Concatinate tables
    df = pd.concat([df1, df2]).reset_index(drop=True)
    # Select unique station names
    unique_station_names = df.station_name.sort_values().unique() 
    # Save and return unique station names
    final_df = pd.DataFrame(unique_station_names,columns=['station_name'])
    
    # Save table if not existing
    if not os.path.isfile('./data/station_name_unique.csv') & save_table:
        final_df.to_csv('./data/station_name_unique.csv', index=False)
    
    return final_df

def generate_table_StationName_StationID_from_data(data_list:list[pd.DataFrame], 
                                         year_list:list[int],
                                         save_table:bool = True):

    # Combine all data frames that have the following columns: 'from_station_name','from_station_id','to_station_name','to_station_id'
    data = pd.concat([rename_columns_rides_data(elm, year_list[i])[['from_station_name','from_station_id','to_station_name','to_station_id','user_type']] for i, elm in enumerate(data_list)])
    # Delete all rows with missing values
    data = data.dropna()
    # Strip all strings
    data['from_station_name'] = data.from_station_name.apply(lambda x: x.strip())
    data['to_station_name'] = data.to_station_name.apply(lambda x: x.strip())
        
    # Rename columns to concat start station and end station into one dataset
    df1 = data[['from_station_name','from_station_id']].rename(columns={'from_station_name':'station_name', 'from_station_id':'station_id'})
    df2 = data[['to_station_name', 'to_station_id']].rename(columns={'to_station_name':'station_name', 'to_station_id':'station_id'})     
    
    # Transform type of station id
    df1.station_id = df1.station_id.astype(float)
    df2.station_id = df2.station_id.astype(float)
    
    df = pd.concat([df1, df2]).reset_index(drop=True)
    # Group By combination of station name and station i
    df = df.groupby(['station_name','station_id']).size().reset_index().rename(columns={0:'count'}) 
    # Save table if not existing
    if not os.path.isfile('./data/stationName_stationId.csv') & save_table:
        df[['station_name','station_id']].to_csv('./data/stationName_stationId.csv', index=False)
    
    #dic = dict(zip(df.station_name, df.station_id))
    df_final = df[['station_name','station_id']]
    return df_final
    
def generate_key_StationName_StationID_PlaceId(data_bikestation_location: pd.DataFrame,
                                               data_stationName_stationId: pd.DataFrame):
    # Fill missing values for place_id with 0 as default value for missing get request
    data_bikestation_location = data_bikestation_location.fillna(value={'place_id':0})
    
    # Merge tables
    data = data_bikestation_location.merge(data_stationName_stationId, how='left', on='station_name')
    
    # Generate dictionary station_name:formatted_address
    dic_station_adress = dict()
    for elm in data_stationName_stationId.station_name:
        dic_station_adress[elm] = data_bikestation_location[data_bikestation_location.station_name == elm]['formatted_address'].values
    
    dic_station_adress = data.groupby('station_name')['formatted_address'].apply(list).reset_index(name='formatted_address')
    
    # Generate unique key
    keys = data.groupby(['station_name','place_id','station_id']).size().reset_index().rename(columns={0:'count_val'})
    keys = pd.DataFrame(keys.apply(lambda x: (x['station_name'],x['place_id'],x['station_id']), axis=1), columns=['KEY'])
    
    
    keys['location_lat'] = None
    keys['location_lng'] = None
    keys['business_status'] = ''
    keys['formatted_address'] = ''
    
    for i,key in enumerate(keys.KEY):
        df_tmp = data[(data.station_name == key[0]) & (data.place_id == key[1]) & (data.station_id == key[2])][['location_lat','location_lng','business_status','formatted_address']].reset_index(drop=True)
        keys.loc[i, 'location_lat'] = df_tmp.location_lat[0]
        keys.loc[i, 'location_lng'] = df_tmp.location_lng[0]
        keys.loc[i, 'business_status'] = df_tmp.business_status[0]
        keys.loc[i, 'formatted_address'] = df_tmp.formatted_address[0]
        
    if not os.path.isfile('./data/stationName_stationId_placeId.csv'):
        keys.to_csv('./data/stationName_stationId_placeId.csv', index=False)    
    
    return keys
