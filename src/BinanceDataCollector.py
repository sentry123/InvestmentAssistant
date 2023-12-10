import os
import json
import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime


def convert_timestamp_to_parts(timestamp_ms):
    timestamp_seconds = timestamp_ms['serverTime'] / 1000.0
    dt_utc = datetime.utcfromtimestamp(timestamp_seconds)

    year = dt_utc.strftime('%Y')
    month = dt_utc.strftime('%m')
    day = dt_utc.strftime('%d')
    hour_24_format = dt_utc.strftime('%H')
    minute = dt_utc.strftime('%M')

    return str(year), str(month), str(day), str(hour_24_format), str(minute)


if __name__ == '__main__':
    api_url = 'https://api.binance.com/api/v3/ticker/24hr'
    time_url = 'https://api.binance.com/api/v3/time'

    apikey = 'mO88LXsFYg845NdH3rFp9uKFaEISXJhsqoAJkhraAnq913LjrgIGuvRWBgMT6zRY'
    apisecret = 'wXRUvAjOa2ygGkTH3jlU8vIIxaepKSPFpniOaJlY1NWcAZgRLMDYALPnoEis5SWQ'

    headers = {'Content-Type': 'application/json'}
    auth = HTTPBasicAuth(apikey, apisecret)

    api_response = requests.get(api_url, auth=auth)
    time_response = requests.get(time_url, auth=auth)

    sys_time = time_response.json()

    if api_response.status_code == 200:
        # Parse the JSON data
        data = api_response.json()

        year, month, day, hour_24_format, minute = convert_timestamp_to_parts(sys_time)

        # Define the directory structure
        base_dir = os.getcwd()
        dynamic_path = f'{base_dir}/data-lake/year={year}/month={month}/day={day}/hour={hour_24_format}/{minute}_binance_data.json'

        # Create the directories if they don't exist
        os.makedirs(os.path.dirname(dynamic_path), exist_ok=True)

        # Write the JSON data to the file
        with open(dynamic_path, 'w') as json_file:
            json.dump(data, json_file)

        print(f"JSON data has been saved to {dynamic_path}")
    else:
        print(f"Failed to retrieve data. Status code: {api_response.status_code}")
