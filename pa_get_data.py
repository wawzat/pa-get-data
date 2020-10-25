# Get PurpleAir sensor data from PurpleAir and Thingspeak API's for year-month and store as csv files
# similar to downloaded CSV's from PurpleAir map list.
# Rename config_template py to config.py and edit to include PurpleAir keys, directory path and bounding box variables.
# James S. Lucas 20201025

import requests
import json
import os
import config
import argparse
import pandas as pd
import calendar
from datetime import datetime
import pytz


# Change this variable to point to the desired directory variable in config.py. 
data_directory = config.matrix5


def get_arguments():
    parser = argparse.ArgumentParser(
    description='Dowload PurpleAir csv files for sensors in bounding box during year and month provided.',
    prog='pa_get_data',
    usage='%(prog)s [-y <year>], [-m <month>]',
    formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    g=parser.add_argument_group(title='arguments',
          description='''    -y, --year   year to get data for.
    -m  --month  month to get data for.                                        ''')
    g.add_argument('-y', '--year',
                    type=int,
                    dest='yr',
                    help=argparse.SUPPRESS)
    g.add_argument('-m', '--month',
                    type=int,
                    dest='mnth',
                    help=argparse.SUPPRESS)
    args = parser.parse_args()
    return(args)

args = get_arguments()


def get_sensor_indexes():
   root_url = "https://api.purpleair.com/v1/sensors"
   bbox = config.bbox
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
   tz_offset = int(pytz.timezone('US/Pacific').localize(datetime(yr,mnth,1)).strftime('%z')) * -1 // 100
   time_offset = '0' + str(tz_offset) + ':00:00'
   for sensor in sensor_ids:
      sensor_name = sensor[0]
      lat = sensor[1]
      lon = sensor[2]
      # returns a tuple (first_day_of_month, last_day_of_month)
      mnth_range = calendar.monthrange(yr, mnth)
      start_date_str = str(yr) + "-" + str(mnth) + "-" + str(mnth_range[0])
      start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
      end_date_str = str(yr) + "-" + str(mnth+1) + "-" + '1'
      end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
      data_range = list(date_range(start_date, end_date, 4)) 
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
      output_folder = start_date.strftime('%Y-%m')
      output_path = data_directory + os.path.sep + output_folder
      if not os.path.isdir(output_path):
         os.mkdir(output_path)
      output_pathname = output_path + os.path.sep + filename
      for t in range(0, 4):
         root_url = 'https://api.thingspeak.com/channels/{channel}/feeds.csv?api_key={api_key}&start={start}%20{offset}&end={end}%20{offset}'
         channel = sensor[4]
         api_key = sensor[5]
         start_date = data_range[t]
         end_date = data_range[t+1]
         params = {
            'channel': channel,
            'api_key': api_key,
            'start': start_date,
            'end': end_date,
            'offset': time_offset
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
list_of_sensor_indexes = get_sensor_indexes()
sensor_ids = get_sensor_ids(list_of_sensor_indexes)
get_ts_data(sensor_ids, data_directory, args.yr, args.mnth)