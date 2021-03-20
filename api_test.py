# Get PurpleAir sensor data from PurpleAir API into dataframe
# James S. Lucas 20210319

import pandas as pd
import requests

def get_sensor_data(PA_READ_KEY, sensor_id):
   root_url = "https://api.purpleair.com/v1/sensors/"
   params = {
      'sensor_id': sensor_id
      }
   url_template = root_url + "{sensor_id}"
   url = url_template.format(**params)
   header = {"X-API-Key":PA_READ_KEY}
   response = requests.get(url, headers=header)
   if response.status_code == 200:
      #print(response.text)
      df = pd.read_json(response.text)
   else:
      raise requests.exceptions.RequestException
   return df

PA_READ_KEY = 'INSERT PURPLEAIR API READ KEY HERE'
sensor_id = '9208'

df = get_sensor_data(PA_READ_KEY, sensor_id)
pm25_24_hr_avg = df['sensor']['stats_a']['pm2.5_24hour']
output_string = f"24 Hr Average PM 2.5: {pm25_24_hr_avg}"
print(output_string)