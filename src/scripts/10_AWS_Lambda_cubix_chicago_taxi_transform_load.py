from io import StringIO
import json

import boto3
import pandas as pd

s3 = boto3.client('s3')

def taxi_trips_transformations(taxi_trips: pd.DataFrame) -> pd.DataFrame:
    """ Perform transformation with the taxi data

    Args:
        taxi_trips (pd.DataFrame): 
            The DataFrame holding the daily taxi trips.

    Returns:
        pd.DataFrame:
            The cleaned, DataFrame holding the daily taxi trips.
    """
    
    # error handling - can be extended with some other ideas
    if not isinstance(taxi_trips, pd.DataFrame):
        raise TypeError('taxi_trips is a not a valid Pandas DataFrame.')
    
    # the order is important, because these two columns contain lots of NaN values
    # first drop them, then the rows having NaN in the other columns only
    taxi_trips.drop(['pickup_census_tract','dropoff_census_tract',
                     'pickup_centroid_location', 'dropoff_centroid_location'],
                    axis=1, inplace=True)
    taxi_trips.dropna(inplace=True)
    taxi_trips.rename(columns={"pickup_community_area": "pickup_community_area_id",
                            "dropoff_community_area": "dropoff_community_area_id"},
                            inplace=True)
    taxi_trips['trip_start_timestamp'] = pd.to_datetime(taxi_trips['trip_start_timestamp']).dt.floor('H')
    taxi_trips['datetime_for_weather'] = taxi_trips['trip_start_timestamp'].dt.floor('H')
    
    return taxi_trips


def update_taxi_trips_with_master_data(taxi_trips: pd.DataFrame,
                                   payment_type_master: pd.DataFrame,
                                   company_master: pd.DataFrame) -> pd.DataFrame:
    """ Removes the payment_type and the company columns, and adds their ids

    Args:
        taxi_trips (pd.DataFrame):
            DataFrame holding the daily taxi trips.
        payment_type_master (pd.Dataframe): Payment type master table.
        company_master (pd.Dataframe): Company master table.

    Returns:
        pd.DataFrame: The updated DataFrame having ids, not payment type and company names.
    """
    taxi_trips_id = taxi_trips.merge(payment_type_master, on='payment_type')
    taxi_trips_id = taxi_trips_id.merge(company_master, on='company')
    taxi_trips_id.drop(['payment_type', 'company'], axis=1, inplace=True)
    
    return taxi_trips_id
    
    
def update_master(taxi_trips: pd.DataFrame, master: pd.DataFrame, id_column: str,
                  value_column: str) -> pd.DataFrame :
    """ Extend the master with new types if there are any of them.

    Args:
        taxi_trips (pd.DataFrame):
            DataFrame holding the daily taxi trips.
        master (pd.DataFrame):
            DataFrame holding the master data.
        id_column (str):
            The id column of the master DataFrame.
        value_column (str): 
            The value column of the master and taxi_trips DataFrame.

    Returns:
        pd.DataFrame:
            The updated master data. If no new type appeared returns the original one.
    """
    max_id = master[id_column].max()
    list_new_values = [value for value in taxi_trips[value_column].values
                                  if value not in master[value_column].values]
    new_values_df = pd.DataFrame({
        id_column: range(max_id + 1, max_id + len(list_new_values) + 1),
        value_column: list_new_values
        })
    # print(f'original master: {master}')
    # print(master[id_column].dtype)
    updated_master = pd.concat([master, new_values_df], ignore_index=True)
    # print(f'updated master: {updated_master}')
    # print(master[id_column].dtype)
    updated_master[id_column] = updated_master[id_column].astype('int32')
    # print(f'updated master: {updated_master}')
    # print(updated_master[id_column].dtype)
    return updated_master


def transform_weather_data(weather_data: json) -> pd.DataFrame:
    """ Make transformations on the daily weather api response.

    Args:
        weather_data (json):
            The daily weather data from the Open Metao API.
        
    Returns:
        pd.DataFrame:
            A DataFrame representation of the weather data.
    """
    weather_data_filtered = {
        'datetime': weather_data['hourly']['time'],
        'temperature': weather_data['hourly']['temperature_2m'],
        'wind_speed': weather_data['hourly']['wind_speed_10m'],
        'rain': weather_data['hourly']['rain'],
        'precipitation': weather_data['hourly']['precipitation']
        }
    weather_df = pd.DataFrame(weather_data_filtered)
    weather_df['datetime'] = pd.to_datetime(weather_df['datetime'])
    
    return weather_df

def read_csv_from_s3(bucket: str, path: str, filename:str) -> pd.DataFrame:
    """ Downloads a csv file from an s3 bucket.

    Args:
        bucket (str):
            The bucket where the file is.
        path (str):
            The folder of the file.
        filename (str):
            The name of the file.
        
    Returns:
        pd.DataFrame:
            The DataFrame of the downloaded file.
    """

    full_path = f'{path}{filename}'
    object = s3.get_object(Bucket=bucket, Key=full_path)
    object = object['Body'].read().decode('utf-8')
    # with StringIO the Body of the object behaves as a file
    df = pd.read_csv(StringIO(object))
    
    return df

def upload_dataframe_to_s3(dataframe: pd.DataFrame, bucket: str, path: str) -> None:
    """ Uploads a dataframe to the specified S3 path.

    Args:
        dataframe (pd.DataFrame):
            The DataFrame to be uploaded.
        bucket (str):
            The bucket where we want to store the file.
        path (str):
            The folder of the file we want to place.

    Returns:
        None
    """

    buffer = StringIO()
    dataframe.to_csv(buffer, index=False)
    df_content = buffer.getvalue()
    s3.put_object(Bucket=bucket, Key= path, Body=df_content)
    
    
def upload_master_data_to_s3(bucket: str, path: str, file_type: str,
                            dataframe: pd.DataFrame) -> None:
    """ Archives the previous master DataFrame and uploads a new one as csv file
        to s3.

    Args:
        bucket (str):
            The bucket where we want to store the file.
        path (str):
            The folder of the file we want to place.
        file_type (str):
            Either 'company' or 'payment_type'.
        dataframe (pd.DataFrame):
            The DataFrame to be uploaded.
        
    Returns:
        None
    """
    master_file_path = f'{path}{file_type}_master.csv'
    previous_master_file_path = f'transformed_data/master_table_previous_version/{file_type}_master_previous_version.csv'
    
    s3.copy_object(Bucket=bucket,
                    CopySource={'Bucket': bucket, 'Key': master_file_path},
                    Key=previous_master_file_path
                   )
    
    upload_dataframe_to_s3(bucket= bucket, dataframe= dataframe, path= master_file_path)


def upload_and_move_file_on_s3(dataframe: pd.DataFrame,
                               datetime_col: str,
                               bucket: str,
                               file_type: str,
                               filename: str,
                               source_path: str,
                               target_path_raw: str,
                               target_path_transformed: str) -> None:

    """ Uploads a dataframe to S3, then moves a file from the base folder to another.

    Args:
        dataframe (pd.DataFrame):
            The DataFrame to be uploaded.
        datetime_col (str):
            Name of the datetime column used to derive the date for the file name.
        bucket (str):
            The name of the S3 bucket.
        file_type (str):
            Either 'weather' or 'trips'.
        filename (str):
            Name of the file to be uploaded or moved.
        source_path (str):
            The folder of the file we want move or delete.
        target_path_raw (str):
            Folder where the raw data should be moved.
        target_path_transformed (str):
            Folder where the transformed data will go.

    Returns:
        None
    """

                                   
    
    # upload the csv from a DataFrame to s3
    
    # contruct a filename like this:
    # 'transformed_data/taxi_trips/taxi_trips_2024-01-31.csv'
    formatted_date = dataframe[datetime_col].iloc[0].strftime('%Y-%m-%d')
    new_path_with_filename = f'{target_path_transformed}{file_type}_{formatted_date}.csv'

    print(f'Target file in transformed folder: {target_path_transformed}{file_type}_{formatted_date}.csv')
    upload_dataframe_to_s3(bucket= bucket, dataframe= dataframe, path= new_path_with_filename)
    
    print(f'Source file in raw folder: {source_path}{filename}')
    print(f'Targer file in raw folder: {target_path_raw}{filename}')
    s3.copy_object(Bucket=bucket,
                   CopySource={'Bucket': bucket, 'Key': f'{source_path}{filename}'},
                   Key= f'{target_path_raw}{filename}'
                   )
    print('copy done')
    s3.delete_object(Bucket=bucket,
                     Key= f'{source_path}{filename}')
    print('delete done')
#
#
# MAIN FUNCTION
#
#

def lambda_handler(event, context):
    bucket = 'cubix-chicago-taxi-zsigy'
    raw_taxi_trips_folder = f'raw_data/to_processed/taxi_data/'
    raw_weather_folder = f'raw_data/to_processed/weather_data/'
    target_taxi_trips_folder = f'raw_data/processed/taxi_data/'
    target_weather_folder = f'raw_data/processed/weather_data/'
    
    transformed_taxi_trips_folder = f'transformed_data/taxi_trips/'
    transformed_weather_folder = f'transformed_data/weather/'
    
    payment_type_master_folder = f'transformed_data/payment_types/'
    company_master_folder = f'transformed_data/company/'
    
    payment_type_master_file_name = 'payment_type_master.csv'
    company_master_file_name = 'company_master.csv'
    
    payment_type_master = read_csv_from_s3(bucket=bucket,
                                           path=payment_type_master_folder,
                                           filename=payment_type_master_file_name)

    company_master = read_csv_from_s3(bucket=bucket,
                                           path=company_master_folder,
                                           filename=company_master_file_name)

    # taxi data transformation transformation and loading
    
    # i = 0
    for file in s3.list_objects(Bucket = bucket, Prefix = raw_taxi_trips_folder)['Contents']:
        taxi_trip_file_key = file['Key']
        if file['Key'] != raw_taxi_trips_folder:
            if taxi_trip_file_key[-5:] == '.json':
                
                filename = taxi_trip_file_key.split('/')[-1]
                
                response = s3.get_object(Bucket = bucket, Key = taxi_trip_file_key)
                content = response['Body']
                taxi_trips_data_json = json.loads(content.read())
                
                taxi_trips_data_raw = pd.DataFrame(taxi_trips_data_json)
                taxi_trips_transformed = taxi_trips_transformations(taxi_trips_data_raw)
                
                # update the master tables
                company_master_updated = update_master(taxi_trips_transformed,
                                                       company_master,
                                                       'company_id',
                                                       'company')
                

                payment_type_master_updated = update_master(taxi_trips_transformed,
                                                            payment_type_master,
                                                            'payment_type_id',
                                                            'payment_type')

                # update the transformed taxi trips DataFrame with the new master tables
                taxi_trips = update_taxi_trips_with_master_data(taxi_trips= taxi_trips_transformed,
                                payment_type_master= payment_type_master_updated,
                                company_master= company_master_updated)
                
                upload_and_move_file_on_s3(dataframe= taxi_trips,
                                           datetime_col= 'datetime_for_weather',
                                           bucket= bucket,
                                           file_type= 'taxi',
                                           filename= filename,
                                           source_path= raw_taxi_trips_folder,
                                           target_path_raw= target_taxi_trips_folder,
                                           target_path_transformed= transformed_taxi_trips_folder)
                print('taxi_trips is uploaded and moved')
                
                # upload to s3
                upload_master_data_to_s3(bucket= bucket,
                                        path= payment_type_master_folder,
                                        file_type= 'payment_type',
                                        dataframe= payment_type_master_updated)
                print('Payment type master has been updated.')

                upload_master_data_to_s3(bucket= bucket,
                                        path= company_master_folder,
                                        file_type= 'company',
                                        dataframe= company_master_updated)
                print('Company master has been updated.')

        #         i += 1
                
        # # terminate the for loop after one file only to enhance test speed
        # if i == 1:
        #     break


    # weather data transformation and loading
    # i = 0
    for file in s3.list_objects(Bucket = bucket, Prefix = raw_weather_folder)['Contents']:
        weather_file_key = file['Key']
        if file['Key'] != raw_weather_folder:
            if weather_file_key[-5:] == '.json':
                
                filename = weather_file_key.split('/')[-1]
                
                response = s3.get_object(Bucket = bucket, Key = weather_file_key)
                content = response['Body']
                weather_data_json = json.loads(content.read())
                weather_data = transform_weather_data(weather_data_json)

                # upload to s3                
                upload_and_move_file_on_s3(dataframe= weather_data,
                                           datetime_col= 'datetime',
                                           bucket= bucket,
                                           file_type= 'weather',
                                           filename= filename,
                                           source_path= raw_weather_folder,
                                           target_path_raw= target_weather_folder,
                                           target_path_transformed= transformed_weather_folder)
                print('weather file is uploaded and moved')

        #         i += 1
                
        # # terminate the for loop after one file only to enhance test speed
        # if i == 1:
        #     break    