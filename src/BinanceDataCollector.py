import requests, json
import os

api_url = 'https://api.binance.com/api/v3/ticker/24hr'
time_url = 'https://api.binance.com/api/v3/time'

api_response = requests.get(api_url)
time_response = requests.get(time_url)

sys_time = time_response.json()

path = os.getcwd()

if api_response.status_code == 200:
    # Parse the JSON data
    data = api_response.json()

    # Specify the file path where you want to save the JSON data
    file_path = "{path}/data/{time}_binance_data.json".format(path=path, time=sys_time)

    # Write the JSON data to the file
    with open(file_path, 'w') as json_file:
        json.dump(data, json_file)

    print(f"JSON data has been saved to {file_path}")
else:
    print(f"Failed to retrieve data. Status code: {api_response.status_code}")
