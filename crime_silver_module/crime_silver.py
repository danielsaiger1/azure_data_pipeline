import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
import datetime
from datetime import timedelta

class CrimeDataProcessor:
    def __init__(self, file_path_input, file_path_output):
        self.file_path_input = file_path_input
        self.file_path_output = file_path_output
        self.df_crime = None

    def load_data(self):
        self.df_raw = spark.read.csv(self.file_path_input, header=False)

        if self.df_raw.rdd.isEmpty():
            raise ValueError(f"No data found in the file at path: {self.file_path_input}")

        self.df_crime = self.df_raw.toPandas()
        self.df_crime.columns = self.df_crime.iloc[0]  
        self.df_crime = self.df_crime[1:]  
        self.df_crime.reset_index(drop=True, inplace=True)

    def add_crime_category(self):
        self.df_crime['Crime_Category'] = np.where(
            self.df_crime['Crm Cd Desc'].str.contains(r'\b(BURGLARY|BURGLARY, ATTEMPTED)\b', case=False, na=False), 'Burglary',
            np.where(
                self.df_crime['Crm Cd Desc'].str.contains(r'\b(PICKPOCKET|PICKPOCKET, ATTEMPT|PURSE SNATCHING)\b', case=False, na=False), 'Pickpocket',
                self.df_crime['Crm Cd Desc']
            )
                )    

    def combine_datetime(self, row):
        date = pd.to_datetime(row['DATE OCC'], format='%m/%d/%Y %I:%M:%S %p')

        hours = int(row['TIME OCC'] )// 100
        minutes = int(row['TIME OCC']) % 100
        
        combined_datetime = date + timedelta(hours=hours, minutes=minutes)
        
        rounded_datetime = combined_datetime.replace(minute=0, second=0, microsecond=0)

        return rounded_datetime


    def process_timestamps(self):
        self.df_crime['timestamp'] = self.df_crime.apply(self.combine_datetime, axis=1)
        self.df_crime['timestamp_unix'] = (self.df_crime['timestamp'] - pd.Timestamp("1970-01-01")) // pd.Timedelta('1s')
        self.df_crime = self.df_crime.drop(['Date Rptd', 'DATE OCC', 'TIME OCC'], axis=1)

    def save_data(self):
        self.df_crime.to_csv(self.file_path_output)

def main():
    global spark
    spark = SparkSession.builder.appName("CrimeDataProcessor").getOrCreate()

    file_path_input = 'abfss://crimeweatherbronze@storagecrimeweather.dfs.core.windows.net/crime_bronze/LA_crime_data.csv'
    file_path_output = 'abfss://crimeweathersilver@storagecrimeweather.dfs.core.windows.net/crime_silver/crime_silver.csv'

    processor = CrimeDataProcessor(file_path_input, file_path_output)
    processor.load_data()
    processor.add_crime_category()
    processor.process_timestamps()
    processor.save_data()

if __name__ == "__main__":
    main()