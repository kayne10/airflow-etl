import requests
import configparser
from datetime import datetime
import json
import os

config = configparser.ConfigParser()
config.read('config.ini')

def get_weather():

    params = {'q':'Denver, USA','appid':config['OWM']['API_KEY']}

    result = requests.get('https://api.openweathermap.org/data/2.5/weather?',params)

    if result.status_code == 200:
        json_data = result.json()
        file_name = str(datetime.now().date()) + '.json'
        tot_name = os.path.join(os.path.dirname(__file__), './data', file_name)

        with open(tot_name, 'w') as output_file:
            json.dump(json_data, output_file)

    else:
        print("Error in API call")


if __name__ == '__main__':
    get_weather()
