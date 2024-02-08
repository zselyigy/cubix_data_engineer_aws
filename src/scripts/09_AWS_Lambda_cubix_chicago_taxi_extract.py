# some exploratory data analysis
from datetime import datetime
from dateutil.relativedelta import relativedelta
import os
from typing import Dict

import boto3
import json
import requests

# 1. DONE get T-2 month taxi data
# 2. DONE get T-2 month weather data
# 3. DONE upload to s3 (raw_data/to_processed/taxi_data and raw_data/to_processed/taxi_data)
# 4. DONE create functions - organize the code
# 5. DONE creating a trigger


def get_taxi_data(formatted_datetime: str) -> Dict:
    """
    Retrieves taxi data for the given date.
    
    Parameters:
        formatted_datetime (str): The date in 'YYY-MM-DD' format.
    
    Returns:
        Dict: A dictionary containing the taxi_data as a JSON
    """
    
    url = f'https://data.cityofchicago.org/resource/wrvz-psew.json?$where=trip_start_timestamp >= "{formatted_datetime}T00:00:00" AND trip_start_timestamp <= "{formatted_datetime}T23:59:59"&$limit=30000'
    
    # the os.environ.get looks for the specified variable in the .env file of the root
    # folder of the project
    headers = {'X-App-Token': os.environ.get("CHICHAGO_API_TOKEN")}
    
    # response = requests.get(url, headers)    # in case an error comes leave the headers parameter
    response = requests.get(url)
    
    taxi_data = response.json()
    # print(taxi_data)
    
    return taxi_data


def get_weather_data(formatted_datetime: str) -> Dict:
    """
    Retrieve weather data from the open-meteo.com site for a given day.

    Parameters
    ----------
    formatted_datetime : str
        formatted_datetime (str): The date in 'YYY-MM-DD' format.

    Returns
    -------
    Dict
        A dictionary containing weather data retrieved from open-meteo.com, including
        temperature at 2 meters, wind speed at 10 meters, rainfall, and precipitation.

    Notes
    -----
    The weather data is obtained using the open-meteo.com API (https://archive-api.open-meteo.com/v1/era5).
    The latitude and longitude are set to 41.85 and -87.65, respectively, for the location of Chicago, USA.
    """

    # get weather data from the open-meteo.com site for a given day
    url = 'https://archive-api.open-meteo.com/v1/era5'
    params = {
        'latitude': 41.85,
        'longitude': -87.65,
        'start_date': formatted_datetime,
        'end_date': formatted_datetime,
        'hourly': 'temperature_2m,wind_speed_10m,rain,precipitation'
    }

    response = requests.get(url, params=params)
    weather_data = response.json()
    # print(weather_data)
    
    return weather_data


def upload_to_s3(data: Dict, folder_name: str, filename: str) -> None:
    """
    Uploads data to an Amazon S3 bucket.

    Parameters
    ----------
    data (Dict): The data to be uploaded, typically a dictionary or JSON-serializable object.
    folder_name (str): The name of the folder in the S3 bucket where the data will be stored.
    filename (str): The name of the file under the specified folder in the S3 bucket.

    Returns
    -------
    None: This function does not return any value.
    """
    
    client = boto3.client('s3')
    client.put_object(
        Bucket = 'cubix-chicago-taxi-zsigy',
        Key = f'raw_data/to_processed/{folder_name}/{filename}',
        Body = json.dumps(data)
        )

def lambda_handler(event, context):
    # get the taxi data of the last full month (T-1 months') data
    current_datetime = datetime.now()    # current date
    # get the data two months before as a formatted string
    formatted_datetime = (current_datetime - relativedelta(months=2)).strftime('%Y-%m-%d')
    
    taxi_data = get_taxi_data(formatted_datetime)

    weather_data = get_weather_data(formatted_datetime)

    taxi_filename = f'taxi_raw_{formatted_datetime}.json'
    upload_to_s3(data=taxi_data, folder_name='taxi_data', filename=taxi_filename)
    
    weather_filename = f'weather_raw_{formatted_datetime}.json'
    upload_to_s3(data=weather_data, folder_name='weather_data', filename=weather_filename)
