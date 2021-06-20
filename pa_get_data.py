# Get PurpleAir sensor data from PurpleAir and Thingspeak API's for year-month and store as csv files
# similar to downloaded CSV's from PurpleAir map list.
# Rename config_template py to config.py and edit to include PurpleAir keys, directory path and bounding box variables.
# James S. Lucas 20210120
# Todo: Handle daylight savings offset in transition months

import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import json
import os
import config
import argparse
import pandas as pd
from time import sleep
import calendar
from datetime import datetime
import pytz
from timezonefinder import TimezoneFinder
from dateutil.relativedelta import relativedelta
import winsound


# Change this variable to point to the desired directory variable in config.py. 
data_directory = config.matrix5


def get_arguments():
   parser = argparse.ArgumentParser(
   description='Dowload PurpleAir csv files for sensors in bounding box during year and month provided.',
   prog='pa_get_data',
   usage='%(prog)s [-b <bbox>], [-y <year>], [-m <month>], [-c <channel>], [-g <group>], [-a], [r]',
   formatter_class=argparse.RawDescriptionHelpFormatter,
   )
   g=parser.add_argument_group(title='arguments',
         description='''    -b, --bbox  bbox format: Lower_Left_lon Lower_Left_lat Upper_Right_lon Upper_Right_lat
   -y, --year     year to get data for.
   -m  --month    month to get data for.
   -c  --channel  channel to get.  
   -g  --group    group to get (primary or secondary).
   -a  --all      get all channels and groups.
   -r  --regional get data for regions defined in config.py bbox_dict.
   -f  --folder   folder prefex to save data in                                                ''')
   g.add_argument('-b', '--bbox',
                  type=float,
                  nargs=4,
                  dest='bbox',
                  default=False,
                  help=argparse.SUPPRESS)
   g.add_argument('-y', '--year',
                  type=int,
                  dest='yr',
                  help=argparse.SUPPRESS)
   g.add_argument('-m', '--month',
                  type=int,
                  dest='mnth',
                  help=argparse.SUPPRESS)
   g.add_argument('-c', '--channel',
                  type=str,
                  dest='channel',
                  default='a',
                  choices=['a', 'b'],
                  help=argparse.SUPPRESS)
   g.add_argument('-g', '--group',
                  type=str,
                  dest='group',
                  default='p',
                  choices=['p', 's'],
                  help=argparse.SUPPRESS)
   g.add_argument('-a', '--all',
                  action='store_true',
                  dest='all',
                  help=argparse.SUPPRESS)
   g.add_argument('-r', '--regional',
                  action='store_true',
                  dest='regional',
                  help=argparse.SUPPRESS),
   g.add_argument('-f', '--folder',
                  type=str,
                  dest='folder',
                  default=False,
                  help=argparse.SUPPRESS)
   args = parser.parse_args()
   return(args)


def get_sensor_indexes(bbox):
   session = requests.Session()
   retry = Retry(connect=3, backoff_factor=0.5)
   adapter = HTTPAdapter(max_retries=retry)
   session.mount('http://', adapter)
   session.mount('https://', adapter)

   root_url = "https://api.purpleair.com/v1/sensors"
   #bbox = config.bbox
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
      header = {"X-API-Key":config.PURPLEAIR_READ_KEY}
      response = session.get(url, headers=header)
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
   header = {"X-API-Key":config.PURPLEAIR_READ_KEY}
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
         sensor_data['sensor']['primary_key_a'],
         sensor_data['sensor']['primary_id_b'], 
         sensor_data['sensor']['primary_key_b'],
         sensor_data['sensor']['secondary_id_a'], 
         sensor_data['sensor']['secondary_key_a'],
         sensor_data['sensor']['secondary_id_b'], 
         sensor_data['sensor']['secondary_key_b']
        ))
      #print(sensor_data['sensor']['latitude'])
      #print(sensor_data['sensor']['longitude'])
   return sensor_ids


def date_range(start, end, intv):
   diff = (end  - start ) / intv
   for i in range(intv):
      yield (start + diff * i).strftime("%Y%m%d")
   yield end.strftime("%Y%m%d")


def get_ts_data(sensor_ids, data_directory, yr, mnth, channel, group, suffix, folder):
   num_sensors = len(sensor_ids)
   request_num = 0
   for sensor in sensor_ids:
      sensor_name = sensor[0]
      latitude = sensor[1]
      longitude = sensor[2]
      tf = TimezoneFinder()
      data_tz = tf.timezone_at(lng=longitude, lat=latitude)
      #print(data_tz)
      tz_offset = int(pytz.timezone(data_tz).localize(datetime(yr,mnth,1)).strftime('%z')) * -1 // 100
      time_offset = '0' + str(tz_offset) + ':00:00'
      # returns a tuple (first_day_of_month, last_day_of_month)
      mnth_range = calendar.monthrange(yr, mnth)
      start_date_str = str(yr) + "-" + str(mnth) + "-" + "01"
      start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
      #end_date_str = str(yr) + "-" + str(mnth+1) + "-" + '1'
      #end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
      end_date = start_date + relativedelta(months=+1) 
      data_range = list(date_range(start_date, end_date, 4)) 
      if group == 'p':
         group_str = "Primary"
      elif group == 's':
         group_str = "Secondary"
      if channel == 'a':
         channel_str = 'a'
      elif channel == 'b':
         channel_str = 'b'
      filename_template = '{sensor_name} ({lat} {lon}) {group} {mnth}_{first_day}_{yr} {mnth}_{last_day}_{yr}_{channel}.csv'
      params = {
         'sensor_name': sensor_name,
         'lat': str(latitude),
         'lon': str(longitude),
         'group': group_str,
         'mnth': str(mnth),
         'first_day': "01",
         'last_day': str(mnth_range[1]),
         'yr': str(yr),
         'channel': channel_str
         }
      filename = filename_template.format(**params)
      #output_folder = start_date.strftime('%Y-%m')
      if folder:
         output_folder = folder + " " + start_date.strftime('%Y-%m') + suffix
      else:
         output_folder = start_date.strftime('%Y-%m') + suffix
      output_path = data_directory + os.path.sep + output_folder
      if not os.path.isdir(output_path):
         os.mkdir(output_path)
      output_pathname = output_path + os.path.sep + filename
      for t in range(0, 4):
         request_num += 1
         root_url = 'https://api.thingspeak.com/channels/{channel}/feeds.csv?api_key={api_key}&start={start}%20{offset}&end={end}%20{offset}'
         if channel == 'a' and group == 'p':
            channel_id = sensor[4] # primary channel A
            api_key = sensor[5] # primary channel A
         elif channel == 'b' and group == 'p':
            channel_id = sensor[6] # primary channel B
            api_key = sensor[7] # primary channel B
         start_date = data_range[t]
         if channel == 'a' and group == 's':
            channel_id = sensor[8] # secondary channel A
            api_key = sensor[9] # secondary channel A
         elif channel == 'b' and group == 's':
            channel_id = sensor[10] # secondary channel B
            api_key = sensor[11] # secondary channel B

         end_date = data_range[t+1]
         params = {
            'channel': channel_id,
            'api_key': api_key,
            'start': start_date,
            'end': end_date,
            'offset': time_offset
            }
         url = root_url.format(**params)
         print(f"{yr}-{mnth} {request_num} of {num_sensors * 4} : {url}")
         if t == 0:
            df = pd.read_csv(url)
         else:
            df = pd.concat([df, pd.read_csv(url)])
      if channel == 'a' and group == 'p':
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
      elif channel == 'b' and group == 'p':
         mapping = {
            'created_at': 'created_at',
            'entry_id': 'entry_id',
            'field1': 'PM1.0_CF1_ug/m3',
            'field2': 'PM2.5_CF1_ug/m3',
            'field3': 'PM10.0_CF1_ug/m3',
            'field4': 'Free_Mem',
            'field5': 'ADC',
            'field6': 'Pressure_hpa',
            'field7': 'IAQ',
            'field8': 'PM2.5_ATM_ug/m3'
            }
      if channel == 'a' and group == 's':
         mapping = {
            'created_at': 'created_at',
            'entry_id': 'entry_id',
            'field1': '>=0.3um/dl',
            'field2': '>=0.5um/dl',
            'field3': '>=1.0um/dl',
            'field4': '>=2.5um/dl',
            'field5': '>=5.0um/dl',
            'field6': '>=10.0um/dl',
            'field7': 'PM1.0_ATM_ug/m3',
            'field8': 'PM10_ATM_ug/m3'
            }
      if channel == 'b' and group == 's':
         mapping = {
            'created_at': 'created_at',
            'entry_id': 'entry_id',
            'field1': '>=0.3um/dl',
            'field2': '>=0.5um/dl',
            'field3': '>=1.0um/dl',
            'field4': '>=2.5um/dl',
            'field5': '>=5.0um/dl',
            'field6': '>=10.0um/dl',
            'field7': 'PM1.0_ATM_ug/m3',
            'field8': 'PM10_ATM_ug/m3'
            }

      df = df.rename(columns=mapping)
      #print(" ")
      #print(df)
      if not df.empty:
         df.to_csv(output_pathname, index=False, header=True)


#Main
args = get_arguments()

#yrs = [2018, 2019, 2020]
#args.channel = 'b'
#args.group = 's'
#for yr in yrs:
   #mnth = 1
   #while mnth <= 12:
      #get_ts_data(sensor_ids, data_directory, yr, mnth, args.channel, args.group)
      #mnth += 1
if args.regional:
   for key, value in config.bbox_dict.items():
      #print(value[0])
      list_of_sensor_indexes = get_sensor_indexes(value[0])
      sensor_ids = get_sensor_ids(list_of_sensor_indexes)
      if args.all:
         channels = ['a', 'b']
         groups = ['p', 's']
         for g in groups:
            for c in channels:
               get_ts_data(sensor_ids, data_directory, args.yr, args.mnth, c, g, suffix=value[1], folder=args.folder)
      else:
         get_ts_data(sensor_ids, data_directory, args.yr, args.mnth, args.channel, args.group, suffix=value[1], folder=args.folder)
else:
   if args.bbox:
      bbox = args.bbox
   else:
      bbox = config.bbox
   list_of_sensor_indexes = get_sensor_indexes(bbox)
   sensor_ids = get_sensor_ids(list_of_sensor_indexes)
   if args.all:
      channels = ['a', 'b']
      groups = ['p', 's']
      for g in groups:
         for c in channels:
            get_ts_data(sensor_ids, data_directory, args.yr, args.mnth, c, g, suffix = '', folder=args.folder)
   else:
      get_ts_data(sensor_ids, data_directory, args.yr, args.mnth, args.channel, args.group, suffix = '', folder=args.folder)

duration = 500 # milliseconds
freq = 660 # Hz
winsound.Beep(freq, duration)