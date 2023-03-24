from api import GOOGLE_MAPS_API_KEY

import logging 
log = logging.getLogger(__name__)

### Find Place API
import requests
import json
import os
import pandas as pd

from thefuzz import fuzz
import geopy.distance

from generate_additional_information_tables import generate_all_unique_station_names

def save_selected_candidate(data:pd.DataFrame, row:int, information:dict, num_candidates:int = 1, which_candidate:int = 0):
    data.loc[row, 'Status'] = information['status']
    data.loc[row, 'formatted_address'] = information['candidates'][which_candidate]['formatted_address']
    data.loc[row, 'location_lat'] = information['candidates'][which_candidate]['geometry']['location']['lat']
    data.loc[row, 'location_lng'] = information['candidates'][which_candidate]['geometry']['location']['lng']
    data.loc[row, 'place_id'] = information['candidates'][which_candidate]['place_id']
    data.loc[row, 'business_status'] = information['candidates'][which_candidate]['business_status']
    data.loc[row, 'num_candidates'] = num_candidates
    
    return data

def get_bikestation_location(data_station_unique_name:pd.DataFrame,
                             save_table:bool = True):
    # Default location if request got through but was not correctly interpreted (domain knowledge) --> rows with this values will be deleted afterwards as it is not correct
    FORMATED_ADDRESS = '600 Queens Quay W, Toronto, ON M5V 3M3, Kanada'
    PLACE_ID = 'ChIJc4Q9rCY1K4gR3ugLYeivBR4'
    
    
    station_name_lis = ['Riverdale Park North (Broadview Ave)', 'Riverdale Park South (Broadview Ave)']
    
    
    # fields selection light
    fields = ['business_status', 'formatted_address', 'geometry', 'name', 'place_id']

    fields_url = "%2C".join(fields)
    # Add information that we search bike share station 
    station_type = "+Bike+Share+Toronto"
    
    # Create copy for return
    final_df = data_station_unique_name.copy()
    # Add additional columns for information from GET request
    final_df['Status'] = ''
    final_df['formatted_address'] = ''
    final_df['location_lat'] = None
    final_df['location_lng'] = None
    final_df['place_id'] = ''
    final_df['business_status'] = ''
    final_df['num_candidates'] = None
    
    for i,station_name in enumerate(station_name_lis):#data_station_unique_name.station_name):
        if i % 100 == 0:
            print(f'Station {i} is loaded')
            log.info(f'Station {i} is loaded')
        
        station_name = 'Runnymede Rd / Annette St, Toronto, ON M6S 2C3, Kanada'
        
        # Replace spaces with + sign for url
        station_name = "+".join(station_name.split())
        # Define URL with all inputs and API Key
        url = f"https://maps.googleapis.com/maps/api/place/findplacefromtext/json?input={station_name + station_type}&inputtype=textquery&fields={fields_url}&key={GOOGLE_MAPS_API_KEY}"
        
        payload={}
        headers = {}
        # GET request of url
        response = requests.request("GET", url, headers=headers, data=payload)
        # Transform response text to JASON dictionary
        js_dic = json.loads(response.text)
        
        if js_dic['status'] == "OK":
            # Case: More than one potential candidate
            if len(js_dic['candidates']) > 1:
                # Check for DEFAULT responst of Google Maps API
                first_false = js_dic['candidates'][0]['place_id'] == PLACE_ID
                second_false = js_dic['candidates'][1]['place_id'] == PLACE_ID
                
                if first_false:
                    final_df = save_selected_candidate(data=final_df, row=i, information=js_dic, num_candidates=1, which_candidate=1)
                elif second_false:
                    final_df = save_selected_candidate(data=final_df, row=i, information=js_dic, num_candidates=1, which_candidate=0)
                else:
                    # Calculate similarity ratio between both candidates
                    ratio = fuzz.ratio(js_dic['candidates'][0]['formatted_address'],js_dic['candidates'][1]['formatted_address'] )
                    # Calculate geodesic distance between both candidates
                    p1 = (js_dic['candidates'][0]['geometry']['location']['lat'], js_dic['candidates'][0]['geometry']['location']['lng'])
                    p2 = (js_dic['candidates'][1]['geometry']['location']['lat'], js_dic['candidates'][1]['geometry']['location']['lng'])
                    dist = geopy.distance.geodesic(p1, p2).km

                    # Case: Distance between both candidates larger 50 Meter
                    if (dist >= 0.05):
                        #Clean Data: If one name contain SMART and the other one does not delete ' - SMART'
                        name_station = " ".join(station_name.split('+')).replace(' - SMART','')
                        name_option_1 = js_dic["candidates"][0]["formatted_address"].split(',')[0].replace(' - SMART','')
                        name_option_2 = js_dic["candidates"][1]["formatted_address"].split(',')[0].replace(' - SMART','')
                        # Check if name of first candidate equals station name
                        if name_station == name_option_1:
                            final_df = save_selected_candidate(data=final_df, row=i, information=js_dic, num_candidates=1, which_candidate=0)
                        # Check if name of second candidate equals station name
                        elif name_station == name_option_2:
                            final_df = save_selected_candidate(data=final_df, row=i, information=js_dic, num_candidates=1, which_candidate=1)
                        # If both stations do not have same name --> take first candidate by DEFAULT
                        else:
                            log.error(f'''Two different locations (WRatio:{ratio}) & (geodesic distance: {round(dist,2)} km) for {" ".join(station_name.split("+"))}:  
                                    Loc1 - Id1: {js_dic["candidates"][0]["formatted_address"]} - {js_dic["candidates"][0]["place_id"]}  
                                    Loc2 - Id2: {js_dic["candidates"][1]["formatted_address"]} - {js_dic["candidates"][1]["place_id"]}
                                    --> First Location is saved not equal {FORMATED_ADDRESS} - {PLACE_ID}''')
                            final_df = save_selected_candidate(data=final_df, row=i, information=js_dic, num_candidates=len(js_dic['candidates']), which_candidate=0)
                    # Case distance between both candidates smaller 50 Meter --> take first Candidate as DEFAULT
                    else:
                        log.info(f'''Two different locations (WRatio:{ratio}) & (geodesic distance: {round(dist,2)} km) for {" ".join(station_name.split("+"))}:  
                                  Loc1 - Id1: {js_dic["candidates"][0]["formatted_address"]} - {js_dic["candidates"][0]["place_id"]}  
                                  Loc2 - Id2: {js_dic["candidates"][1]["formatted_address"]} - {js_dic["candidates"][1]["place_id"]}
                                  --> First Location is saved''')
                        final_df = save_selected_candidate(data=final_df, row=i, information=js_dic, num_candidates=1, which_candidate=0)
            # Case: Only one potential candidate
            else:
                # Check if candidate equals DEFAULT place --> If yes keep empty else save candidate
                if (js_dic['candidates'][0]['place_id'] == PLACE_ID) & (" ".join(station_name.split('+')) != FORMATED_ADDRESS):
                    log.error(f'''station name: {i} - {" ".join(station_name.split('+'))} but GET request got DEFAULT Value {FORMATED_ADDRESS} - {PLACE_ID}''')
                    final_df.loc[i, 'Status'] = f'{js_dic["status"]} with station name {" ".join(station_name.split("+"))} but result was DEFAULT Value {FORMATED_ADDRESS} - {PLACE_ID}'
                else:
                    final_df = save_selected_candidate(data=final_df, row=i, information=js_dic, num_candidates=len(js_dic['candidates']), which_candidate=0)
        # Case: No potential candidate
        else:
            final_df.loc[i, 'Status'] = js_dic['status']
         
    if not os.path.isfile('./data/bikestation_location.csv') & save_table:
        final_df.to_csv('./data/bikestation_location.csv', index=False)
    
    return final_df
        
 
 
    
#  Output Example:
#{
#   "candidates" : [
#      {
#         "formatted_address" : "Wellington St W / York St, Toronto, ON, Kanada",
#         "geometry" : {
#            "location" : {
#               "lat" : 43.6467334,
#               "lng" : -79.3830164
#            },
#            "viewport" : {
#               "northeast" : {
#                  "lat" : 43.64808322989271,
#                  "lng" : -79.38166657010728
#               },
#               "southwest" : {
#                  "lat" : 43.64538357010727,
#                  "lng" : -79.38436622989273
#               }
#            }
#         },
#         "name" : "Bike Share Toronto",
#         "place_id" : "ChIJL99WANM0K4gRY37HiX-3hCo"
#      }
#   ],
#   "status" : "OK"
#}

#'1 Market St'
#{
#    'candidates': [
#        {
#            'business_status': 'CLOSED_PERMANENTLY', 
#            'formatted_address': '600 Queens Quay W, Toronto, ON M5V 3M3, Kanada', 
#            'geometry': {
#                'location': {
#                    'lat': 43.6368477, 
#                    'lng': -79.3971792
#                    }, 
#                'viewport': {
#                    'northeast': {
#                        'lat': 43.63819752989272, 
#                        'lng': -79.39582937010728
#                        }, 
#                    'southwest': {
#                        'lat': 43.63549787010728, 
#                        'lng': -79.39852902989271
#                        }
#                    }
#                }, 
#            'icon': 'https://maps.gstatic.com/mapfiles/place_api/icons/v1/png_71/generic_business-71.png', 
#            'icon_background_color': '#7B9EB0', 
#            'icon_mask_base_uri': 'https://maps.gstatic.com/mapfiles/place_api/icons/v2/generic_pinlet', 
#            'name': 'Bike Share Toronto', 
#            'permanently_closed': True, 
#            'place_id': 'ChIJc4Q9rCY1K4gR3ugLYeivBR4', 
#            'plus_code': {
#                'compound_code': 'JJP3+P4 Toronto, Ontario, Kanada', 
#                'global_code': '87M2JJP3+P4'
#                }, 
#            'rating': 0, 
#            'types': ['point_of_interest', 'establishment'], 
#            'user_ratings_total': 0
#            }
#        ], 
#    'status': 'OK'
#}
#
#'1 Market St - SMART'
#{
#    'candidates': [ 
#        {
#            'business_status': 'OPERATIONAL', 
#            'formatted_address': '3 Market St. #1, Toronto, ON M5E 0A2, Kanada', 
#            'geometry': {
#                'location': {
#                    'lat': 43.6469044, 
#                    'lng': -79.37109029999999
#                    }, 
#                'viewport': {
#                    'northeast': {
#                        'lat': 43.64821862989272, 
#                        'lng': -79.36972377010727
#                        }, 
#                    'southwest': {
#                        'lat': 43.64551897010728, 
#                        'lng': -79.37242342989272
#                        }
#                    }
#                }, 
#            'icon': 'https://maps.gstatic.com/mapfiles/place_api/icons/v1/png_71/generic_business-71.png', 
#            'icon_background_color': '#7B9EB0', 
#            'icon_mask_base_uri': 'https://maps.gstatic.com/mapfiles/place_api/icons/v2/generic_pinlet', 
#            'name': 'Market Wharf', 
#            'place_id': 'ChIJuxZpuC_L1IkRevhhOmK2C8k', 
#            'plus_code': {
#                'compound_code': 'JJWH+PH Toronto, Ontario, Kanada', 
#                'global_code': '87M2JJWH+PH'
#                }, 
#            'rating': 0, 
#            'types': ['point_of_interest', 'establishment'], 
#            'user_ratings_total': 0
#            }, 
#        {
#            'business_status': 'OPERATIONAL', 
#            'formatted_address': '1 Market St., Toronto, ON M5E 0A3, Kanada', 
#            'geometry': {
#                'location': {
#                    'lat': 43.6468812, 
#                    'lng': -79.3710785
#                    }, 
#                'viewport': {
#                    'northeast': {
#                        'lat': 43.64843782989271, 
#                        'lng': -79.36965587010728
#                        }, 
#                    'southwest': {
#                        'lat': 43.64573817010727, 
#                        'lng': -79.37235552989273
#                        }
#                    }
#                }, 
#            'icon': 'https://maps.gstatic.com/mapfiles/place_api/icons/v1/png_71/parking-71.png', 
#            'icon_background_color': '#7B9EB0', 
#            'icon_mask_base_uri': 'https://maps.gstatic.com/mapfiles/place_api/icons/v2/parking_pinlet', 
#            'name': '1 Market St Parking', 
#            'photos': [{
#                'height': 4032, 
#                'html_attributions': ['<a href="https://maps.google.com/maps/contrib/101680034576412901835">Edith Hernandez</a>'], 
#                'photo_reference': 'AfLeUgNknB401CaIHy9WHWMt8H3NPs3rhUPTPhay8DtoHwNjIDy5Vz9ravFSTuK6qSlyvwywOMJrD0lbN9z9KwYjAK8V3BCf-H51WdSYoGTUQvszP4oZ2zonVFkAPDnuAAXLoslWCZ-SH4VuQse-St0BJqVOG3b9-wnc1cNQcxuZWCzYfuE3', 
#                'width': 3024
#                }], 
#            'place_id': 'ChIJL_HKxS_L1IkRAY-3KV3Fk3c', 
#            'plus_code': {
#                'compound_code': 'JJWH+XM Toronto, Ontario, Kanada', 'global_code': '87M2JJWH+XM'
#            }, 
#            'rating': 4, 
#            'types': ['parking', 'point_of_interest', 'establishment'], 
#            'user_ratings_total': 33
#        }
#    ], 
#    'status': 'OK'
#}