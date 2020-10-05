# Get PurpleAir sensor data from PurpleAir and Thingspeak API's for bounding box and year-month
# James S. Lucas 20201004
# Todo:  prepare as function accepting input for bbox and date range
#        convert sensor_ids tuple to dictionary
#        rename variables consistently with names that are more appropriate to function
#        allow partial month date range
#        exception handling
#        directories and paths from config file vs hardcoded. Prompt for path on first run.

import requests
import json
import os
import config
import pandas as pd
import calendar
from datetime import datetime


#user_directory = r' '
matrix5 = r'd:\Users\James\OneDrive\Documents\House\PurpleAir'
virtualbox = r'/media/sf_PurpleAir'
servitor = r'c:\Users\Jim\OneDrive\Documents\House\PurpleAir'
wsl_ubuntu_matrix5 = r'/mnt/d/Users/James/OneDrive/Documents/House/PurpleAir'
wsl_ubuntu_servitor = r'/mnt/c/Users/Jim/OneDrive/Documents/House/PurpleAir'

# Change this variable to point to the desired directory above. 
data_directory = matrix5


def get_sensor_indexes():
   root_url = "https://api.purpleair.com/v1/sensors"
   #           SE lon / lat            NW lon / lat
   bbox = ['-117.5298', '33.7180', '-117.4166', '33.8188']
   #sensor_index is returned automatically and doesn't need to be included in params fields
   params = {
      'fields': "name,latitude,longitude",
      'nwlng': bbox[0],
      'selat': bbox[1],
      'selng': bbox[2],
      'nwlat': bbox[3]
      }
   url_template = root_url + "?fields={fields}&nwlng={nwlng}&nwlat={nwlat}&selng={selng}&selat={selat}"
   url = url_template.format(**params)
   try:
      list_of_sensor_indexes = []
      header = {"X-API-Key":config.READ_KEY}
      response = requests.get(url, headers=header)
      if response.status_code == 200:
         #json_response = response.json()
         #print(json.dumps(json_response, indent=4, sort_keys=True))
         sensors_data = json.loads(response.text)
         for sensor_list in sensors_data['data']:
            list_of_sensor_indexes.append(sensor_list[0])
         print(" ")
         print (list_of_sensor_indexes)
         print(" ")
         return list_of_sensor_indexes
      else:
         print("error no 200 response.")
   except Exception as e:
      print(e)


def get_sensor_ids(list_of_sensor_indexes):
   sensor_ids = []
   root_url = "https://api.purpleair.com/v1/sensors/{sensor_index}"
   header = {"X-API-Key":config.READ_KEY}
   for sensor_index in list_of_sensor_indexes:
      params = {'sensor_index': sensor_index}
      url = root_url.format(**params)
      response = requests.get(url, headers=header)
      sensor_data = json.loads(response.text)
      sensor_ids.append((
         sensor_data['sensor']['name'],
         sensor_data['sensor']['latitude'],
         sensor_data['sensor']['longitude'],
         sensor_data['sensor']['sensor_index'], 
         sensor_data['sensor']['primary_id_a'], 
         sensor_data['sensor']['primary_key_a']
         ))
   return sensor_ids


def date_range(start, end, intv):
   diff = (end  - start ) / intv
   for i in range(intv):
      yield (start + diff * i).strftime("%Y%m%d")
   yield end.strftime("%Y%m%d")


def get_ts_data(sensor_ids, data_directory, yr, mnth):
   for sensor in sensor_ids:
      sensor_name = sensor[0]
      lat = sensor[1]
      lon = sensor[2]
      # returns a tuple (first_day_of_month, last_day_of_month)
      mnth_range = calendar.monthrange(yr, mnth)
      start_time_str = str(yr) + "-" + str(mnth) + "-" + str(mnth_range[0])
      start_time = datetime.strptime(start_time_str, "%Y-%m-%d")
      end_time_str = str(yr) + "-" + str(mnth) + "-" + str(mnth_range[1])
      end_time = datetime.strptime(end_time_str, "%Y-%m-%d")
      data_range = list(date_range(start_time, end_time, 4)) 
      filename_template = '{sensor_name} ({lat} {lon}) Primary {mnth}_{first_day}_{yr} {mnth}_{last_day}_{yr}.csv'
      params = {
         'sensor_name': sensor_name,
         'lat': str(lat),
         'lon': str(lon),
         'mnth': str(mnth),
         'first_day': str(mnth_range[0]),
         'last_day': str(mnth_range[1]),
         'yr': str(yr)
         }
      filename = filename_template.format(**params)
      output_folder = start_time.strftime('%Y-%m')
      output_path = data_directory + os.path.sep + output_folder
      if not os.path.isdir(output_path):
         os.mkdir(output_path)
      output_pathname = output_path + os.path.sep + filename
      for t in range(0, 4):
         root_url = 'https://api.thingspeak.com/channels/{channel}/feeds.csv?api_key={api_key}&start={start}%2000:00:00&end={end}%2023:59:59'
         channel = sensor[4]
         api_key = sensor[5]
         start_time = data_range[t]
         end_time = data_range[t+1]
         params = {
            'channel': channel,
            'api_key': api_key,
            'start': start_time,
            'end': end_time
            }
         url = root_url.format(**params)
         print(url)
         if t == 0:
            df = pd.read_csv(url)
         else:
            df = pd.concat([df, pd.read_csv(url)])
      mapping = {
         'created_at': 'created_at',
         'entry_id': 'entry_id',
         'field1': 'PM1.0_CF1_ug/m3',
         'field2': 'PM2.5_CF1_ug/m3',
         'field3': 'PM10.0_CF1_ug/m3',
         'field4': 'UptimeMinutes',
         'field5': 'RSSI_dbm',
         'field6': 'Temperature_F',
         'field7': 'Humidity_%',
         'field8': 'PM2.5_ATM_ug/m3'
         }
      df = df.rename(columns=mapping)
      print(" ")
      print(df)
      df.to_csv(output_pathname, index=False, header=True)


#Main
yr = 2020
mnth = 9
list_of_sensor_indexes = get_sensor_indexes()
sensor_ids = get_sensor_ids(list_of_sensor_indexes)
get_ts_data(sensor_ids, data_directory, yr, mnth)