from io import BytesIO
from sqlalchemy import create_engine, text
from time import sleep
import requests as re
import pandas as pd
import zipfile


def db1_process(db_str, bb_data, rain_data):
    try:
        engine = create_engine(db_str)
        with engine.connect() as connection:
            print("Database is ready!")
            bb_data.to_sql('blue_bike_data', con=connection,
                           if_exists='replace', index=False)
            rain_data.to_sql('rain_data', con=connection,
                             if_exists='replace', index=False)
    except Exception as e:
        print(e)


def db2_process(db_str, data):
    try:
        engine = create_engine(db_str)
        with engine.connect() as connection:
            print("Database is ready!")
            data.to_sql('blue_bike_rain', con=connection,
                        if_exists='replace', index=False)
    except Exception as e:
        print(e)


print("Script started...")


url = "https://s3.amazonaws.com/hubway-data/201905-bluebikes-tripdata.zip"
req = re.get(url)
rainfall = pd.read_csv('/app/Rainfalldata.csv')
if req.status_code == 200:
    bitfile = BytesIO(req.content)
    with zipfile.ZipFile(bitfile, 'r') as new_zip:
        file_list = new_zip.namelist()
        with new_zip.open("201905-bluebikes-tripdata.csv") as data_file:
            # I used pandas to read the csv file and then save it as a new csv file. The file would become corrupted if I didn't do this
            data = pd.read_csv(data_file)
            # Now we attempt a connection

            db1_info = {"username": "root", "host": "trans_db",
                        "password": "password", "database": "bbdb", "port": "5432"}
            db2_info = {"username": "root", "host": "db2", "password": "password",
                        "port": "5432", "database": "bbdb2"}
#
            db1_string = f"postgreqsql://{db1_info['username']}:{db1_info['password']}@{db1_info['host']}:5432/{db1_info['database']}"
            db2_string = f"postgreqsql://{db2_info['username']}:{db2_info['password']}@{db2_info['host']}:5432/{db2_info['database']}"

            # Loading raw data into first postgres instance
            db1_process(db1_string, data, rainfall)

            # Now we clean the data and then send it to the 3rd postgres instance
            data.drop(columns=["start station longitude", "start station latitude", "start station id",
                               "end station id", "birth year", "gender", "usertype", "bikeid", "end station longitude", "end station latitude"], inplace=True)
            data.rename(columns={"start station name": "start_station_name",
                        "end station name": "end_station_name", "starttime": "start_time", "stoptime": "stop_time"}, inplace=True)

            data['Date'] = data['start_time'].str[5:10]
            summed_df = data.groupby(
                'Date')['tripduration'].sum().reset_index()
            summed_df = summed_df.rename(
                columns={'tripduration': 'totalduration'})
            summed_df['Date'] = summed_df['Date'].apply(
                lambda x: '{}/{}'.format(int(x.split('-')[0]), int(x.split('-')[1])))
            print(summed_df.head())
            rainfall['Date'] = rainfall['Date'].apply(
                lambda x: '{}/{}'.format(x.split('/')[0], x.split('/')[1]))
            print(rainfall.head())
            merged_df = pd.merge(summed_df, rainfall,
                                 on='Date', how='inner')

            # With the new dataframe we can now send it to the 3rd postgres instance
            db2_process(db2_string, merged_df)
