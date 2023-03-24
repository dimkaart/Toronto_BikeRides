import json
import requests
from zipfile import ZipFile
from io import BytesIO
import os

import pandas as pd

from data_opentoronto_transformation import rename_columns_rides_data

def gather_structure_toronto_bikeshare(file_path:str = './structure_opentoronto.json'):
    with open(file_path, 'r') as f:
        structure_opentoronto = json.load(f)
    return structure_opentoronto
    
def fetch_2016(dataset_url:str):
    df_2016 = pd.read_excel(dataset_url)
    df_2016 = rename_columns_rides_data(df_2016, 2016)
    
    return df_2016


def fetch_2017_2021(dataset_url:str, year:int):
    # Fetch data from webpage url through GET request
    r = requests.get(dataset_url, allow_redirects=True)
    # Read the zip file
    zip_file = ZipFile(BytesIO(r.content))    
    # Get data from the .csv files in the zipped file
    try:
        dfs = {text_file.filename: pd.read_csv(zip_file.open(text_file.filename)) 
                                        for text_file in zip_file.infolist() 
                                        if text_file.filename.endswith('.csv')} 
        
        df = pd.concat([data for key,data in dfs.items()])
    except UnicodeDecodeError as e:
        dfs = {text_file.filename: pd.read_csv(zip_file.open(text_file.filename), encoding='latin1') 
                                    for text_file in zip_file.infolist() 
                                    if text_file.filename.endswith('.csv')} 
        
        df = pd.concat([data for key,data in dfs.items()])  
        df.iloc[:,0] = df.iloc[:,0].fillna(df.iloc[:,-1])
        df = df.iloc[:,:-1]
        
        df['Start Station Name'] = df['Start Station Name'].apply(lambda x: x.replace('\x96','-') if isinstance(x, str) else None)
        df['End Station Name'] = df['End Station Name'].apply(lambda x: x.replace('\x96','-') if isinstance(x, str) else None)

    df = rename_columns_rides_data(df, year)
    return df

def fetch_2022(webpage_url:str):
    # Fetch data from webpage url through GET request
    r = requests.get(webpage_url, allow_redirects=True)
    
    zip_file = ZipFile(BytesIO(r.content))

    dfs = {text_file.filename: pd.read_csv(zip_file.open(text_file.filename)) 
                                for text_file in zip_file.infolist() 
                                if text_file.filename.endswith('.csv')} 
    df1 = pd.concat([data for key,data in dfs.items()])
    
    # Zip file inside zip file
    zip_2 = [text_file.filename for text_file in zip_file.infolist() if text_file.filename.endswith('.zip')]
    
    zip_file_2 =  ZipFile(zip_file.open(zip_2[0])) 
    dfs2 = {text_file.filename: pd.read_csv(zip_file_2.open(text_file.filename)) 
                                for text_file in zip_file_2.infolist() 
                                if text_file.filename.endswith('.csv')} 
    df2 = pd.concat([data for key,data in dfs2.items()])
    
    df = pd.concat([df1,df2])
    
    df = rename_columns_rides_data(df, 2022)
    return df
   

def save_locally(data:pd.DataFrame, save_path:str = './data/2016/', year:int = 2016):
    if not os.path.exists(save_path):
        os.makedirs(save_path)
        
    data.to_csv(f'{save_path}toronto_bikeshare_rides_{year}.xls', index=False)
###############################################################################
#structure_opentoronto = gather_structure_toronto_bikeshare()
#
#df_2016 = fetch_2016(structure_opentoronto["2016"]["url"])
#df_2017 = fetch_2017_2021(structure_opentoronto["2017"]["url"])
#df_2018 = fetch_2017_2021(structure_opentoronto["2018"]["url"])
#df_2019 = fetch_2017_2021(structure_opentoronto["2019"]["url"])
#df_2020 = fetch_2017_2021(structure_opentoronto["2020"]["url"])
#df_2021 = fetch_2017_2021(structure_opentoronto["2021"]["url"])
#df_2022 = fetch_2022(structure_opentoronto["2022"]["url"])
#
#save_locally(df_2016)
#save_locally(df_2017, './data/2017/', 2017)
#save_locally(df_2018, './data/2018/', 2018)
#save_locally(df_2019, './data/2019/', 2019)
#save_locally(df_2020, './data/2020/', 2020)
#save_locally(df_2021, './data/2021/', 2021)
#save_locally(df_2022, './data/2022/', 2022)