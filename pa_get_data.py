# Get PurpleAir sensor data from API
import requests
import json
#from os import path
import config


#user_directory = r' '
matrix5 = r'd:\Users\James\OneDrive\Documents\House\PurpleAir'
virtualbox = r'/media/sf_PurpleAir'
servitor = r'c:\Users\Jim\OneDrive\Documents\House\PurpleAir'
wsl_ubuntu_matrix5 = r'/mnt/d/Users/James/OneDrive/Documents/House/PurpleAir'
wsl_ubuntu_servitor = r'/mnt/c/Users/Jim/OneDrive/Documents/House/PurpleAir'

# Change this variable to point to the desired directory above. 
data_directory = matrix5

sensor_id = "9208"

connection_url = "https://api.purpleair.com/v1/sensors"
#                  SE lon / lat               NW lon / lat
#bbox = [-117.5298-.004, 33.7180-.004, -117.4166+.004, 33.8188+.004]

params = {
   'fields': "name,latitude,longitude,pm2.5",
   'nwlng': "-117.5298",
   'selat': "33.7180",
   'selng': "-117.4166",
   'nwlat': "33.8188"
}

url_template = connection_url + "?fields={fields}&nwlng={nwlng}&nwlat={nwlat}&selng={selng}&selat={selat}"
url = url_template.format(**params)

def get_sensor_indexes(url):
   try:
      list_of_sensor_indexes = []
      print(url)
      header = {"X-API-Key":config.X_API_Key}
      response = requests.get(url, headers=header)
      #print(response.text)
      json_response = response.json()
      if response.status_code == 200:
         print(json.dumps(json_response, indent=4, sort_keys=True))
         sensors_data = json.loads(response.text)
         #pm25_reading = sensors_data['fields'][4]
         pm25_reading = sensors_data['data']
         for sensor_list in sensors_data['data']:
            list_of_sensor_indexes.append(sensor_list[0])
         print(pm25_reading)
         print(" ")
         print (list_of_sensor_indexes)
         return list_of_sensor_indexes
      else:
         print("error no 200 response.")
   except Exception as e:
      print(e)


def create_group(list_of_sensor_indexes):
   create_group_id_flag = False
   if create_group_id_flag:
      header = {"X-API-Key":config.WRITE_KEY}
      group_create_url = "https://api.purpleair.com/v1/groups"
      payload = {'name': "SCTV"}
      response_group = requests.post(group_create_url, params = payload, headers=header)
      group_data = json.loads(response_group.text)
      group_id = group_data['group_id']
      print("group_id: " + str(group_id))
      print(" ")
      print(response_group.text)
   member_create_url = "https://api.purpleair.com/v1/groups/{group_id}/members"
   group_id = 458
   params = {'group_id': group_id}
   url = member_create_url.format(**params)
   print(" ")
   print(url)
   for sensor_index in list_of_sensor_indexes:
      payload = {'sensor_index': sensor_index}
      header = {"X-API-Key":config.WRITE_KEY, 'Content-Type': 'application/json'}
      response_members = requests.post(url, json=payload, headers=header)
      print(response_members.text)
      print(" ")


def get_sensor_ids(list_of_sensor_indexes):
   sensor_ids = []
   root_url = "https://api.purpleair.com/v1/sensors/{sensor_index}"
   header = {"X-API-Key":config.X_API_Key}
   for sensor_index in list_of_sensor_indexes:
      params = {'sensor_index': sensor_index}
      url = root_url.format(**params)
      response = requests.get(url, headers=header)
      #payload = {'sensor_index': sensor_index}
      json_response = response.json()
      print(json.dumps(json_response, indent=4, sort_keys=True))
      print(" ")
      sensor_data = json.loads(response.text)
      sensor_ids.append((sensor_data['sensor']['sensor_index'], sensor_data['sensor']['primary_id_a'], sensor_data['sensor']['primary_key_a']))
   for sensor_id in sensor_ids:
      print(sensor_id)


create_group_flag = False
list_of_sensor_indexes = get_sensor_indexes(url)
if create_group_flag:
   create_group(list_of_sensor_indexes)
sensor_ids = get_sensor_ids(list_of_sensor_indexes)