# Get PurpleAir sensor data from PurpleAir url into dataframe
# James S. Lucas 20210318

import pandas as pd
import json

def get_sensor_data(sensor_id):
   root_url = "https://www.purpleair.com/json?show="

   params = {
      'sensor_id': sensor_id
      }
   url_template = root_url + "{sensor_id}"
   url = url_template.format(**params)
   print(url)
   try:
      df = pd.read_json(url)
      
   except Exception as e:
      print(e)
   return df

sensor_id = 9208
df = get_sensor_data(sensor_id)

print(df['results'][0]['Stats'])
stats_dict = json.loads(df['results'][0]['Stats'])
pm25_24_hr_avg =stats_dict['v5']
output_string = f"24 Hr Average PM 2.5: {pm25_24_hr_avg}"
print(output_string)