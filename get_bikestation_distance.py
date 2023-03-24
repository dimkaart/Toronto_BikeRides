from api import GOOGLE_MAPS_API_KEY

import requests

import pandas as pd

def get_bikestation_distance(bikestation_location_data:pd.DataFrame):
    pass

placeid1 = 'ChIJL99WANM0K4gRY37HiX-3hCo'
placeid2 = 'ChIJ8b0SFs40K4gRtP6z4PTjG9c'
placeid3 = 'ChIJsea3Nwk1K4gRMA-29p_mVYE'

mode='bicycling'

origins = f'place_id:{placeid1}|place_id:{placeid2}|place_id:{placeid3}'
destinations = f'place_id:{placeid1}|place_id:{placeid2}|place_id:{placeid3}'
units='metric'

url = f"https://maps.googleapis.com/maps/api/distancematrix/json?origins={origins}&destinations={destinations}&units={units}&mode={mode}&key={GOOGLE_MAPS_API_KEY}"
print(url)

payload={}
headers = {}

response = requests.request("GET", url, headers=headers, data=payload)

print(response.text)
# Output Example:
#{
#   "destination_addresses" : [
#      "Wellington St W / York St, Toronto, ON M5H 3T9, Canada",
#      "Simcoe St / Queen St W, Toronto, ON M5H 4G1, Canada",
#      "Lake Shore Blvd W / Ontario Dr, Toronto, ON M6K 3C3, Canada"
#   ],
#   "origin_addresses" : [
#      "Wellington St W / York St, Toronto, ON M5H 3T9, Canada",
#      "Simcoe St / Queen St W, Toronto, ON M5H 4G1, Canada",
#      "Lake Shore Blvd W / Ontario Dr, Toronto, ON M6K 3C3, Canada"
#   ],
#   "rows" : [
#      {
#         "elements" : [
#            {
#               "distance" : {
#                  "text" : "1 m",
#                  "value" : 0
#               },
#               "duration" : {
#                  "text" : "1 min",
#                  "value" : 0
#               },
#               "status" : "OK"
#            },
#            {
#               "distance" : {
#                  "text" : "0.7 km",
#                  "value" : 689
#               },
#               "duration" : {
#                  "text" : "4 mins",
#                  "value" : 220
#               },
#               "status" : "OK"
#            },
#            {
#               "distance" : {
#                  "text" : "4.7 km",
#                  "value" : 4672
#               },
#               "duration" : {
#                  "text" : "17 mins",
#                  "value" : 1021
#               },
#               "status" : "OK"
#            }
#         ]
#      },
#      {
#         "elements" : [
#            {
#               "distance" : {
#                  "text" : "0.8 km",
#                  "value" : 832
#               },
#               "duration" : {
#                  "text" : "6 mins",
#                  "value" : 334
#               },
#               "status" : "OK"
#            },
#            {
#               "distance" : {
#                  "text" : "1 m",
#                  "value" : 0
#               },
#               "duration" : {
#                  "text" : "1 min",
#                  "value" : 0
#               },
#               "status" : "OK"
#            },
#            {
#               "distance" : {
#                  "text" : "4.6 km",
#                  "value" : 4568
#               },
#               "duration" : {
#                  "text" : "17 mins",
#                  "value" : 1039
#               },
#               "status" : "OK"
#            }
#         ]
#      },
#      {
#         "elements" : [
#            {
#               "distance" : {
#                  "text" : "5.1 km",
#                  "value" : 5089
#               },
#               "duration" : {
#                  "text" : "19 mins",
#                  "value" : 1110
#               },
#               "status" : "OK"
#            },
#            {
#               "distance" : {
#                  "text" : "4.6 km",
#                  "value" : 4564
#               },
#               "duration" : {
#                  "text" : "18 mins",
#                  "value" : 1097
#               },
#               "status" : "OK"
#            },
#            {
#               "distance" : {
#                  "text" : "1 m",
#                  "value" : 0
#               },
#               "duration" : {
#                  "text" : "1 min",
#                  "value" : 0
#               },
#               "status" : "OK"
#            }
#         ]
#      }
#   ],
#   "status" : "OK"
#}