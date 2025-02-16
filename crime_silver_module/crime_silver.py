import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from datetime import timedelta

class CrimeDataProcessor:
    def __init__(self, spark, file_path_input, file_path_output):
        self.spark = spark
        self.file_path_input = file_path_input
        self.file_path_output = file_path_output
        self.df_crime = None

    def load_data(self):
        self.df_raw = self.spark.read.csv(self.file_path_input, header=True, inferSchema=True)

        if self.df_raw.rdd.isEmpty():
            raise ValueError(f"No data found in the file at path: {self.file_path_input}")

        self.df_crime = self.df_raw.toPandas()

    def add_crime_category(self):
        category_mapping = {
            "BURGLARY": "Burglary",
            "BURGLARY, ATTEMPTED": "Burglary",
            "PICKPOCKET": "Pickpocket",
            "PICKPOCKET, ATTEMPT": "Pickpocket",
            "PURSE SNATCHING": "Pickpocket"
        }

        self.df_crime['Crime_Category'] = self.df_crime['Crm Cd Desc'].map(
            lambda x: next((v for k, v in category_mapping.items() if pd.notna(x) and k in x.upper()), x)
        )

    def combine_datetime(self, row):
        try:
            date = pd.to_datetime(row['DATE OCC'], format='%m/%d/%Y %I:%M:%S %p', errors='coerce')
            time_occ = str(int(row['TIME OCC'])).zfill(4)  # Ensure it's a 4-digit string

            hours = int(time_occ[:2])
            minutes = int(time_occ[2:])

            combined_datetime = date + timedelta(hours=hours, minutes=minutes)
            return combined_datetime.replace(minute=0, second=0, microsecond=0)

        except Exception as e:
            print(f"Error processing timestamp for row {row}: {e}")
            return pd.NaT

    def process_timestamps(self):
        self.df_crime['timestamp'] = self.df_crime.apply(self.combine_datetime, axis=1)
        self.df_crime['timestamp_unix'] = (self.df_crime['timestamp'] - pd.Timestamp("1970-01-01")) // pd.Timedelta('1s')

        cols_to_drop = ['Date Rptd', 'DATE OCC', 'TIME OCC']
        self.df_crime.drop(columns=[col for col in cols_to_drop if col in self.df_crime.columns], inplace=True)

    def save_data(self):
        df_spark = self.spark.createDataFrame(self.df_crime)
        df_spark.write.mode("overwrite").csv(self.file_path_output, header=True)

def main():
    spark = SparkSession.builder.appName("CrimeDataProcessor").getOrCreate()

    file_path_input = 'abfss://crimeweatherbronze@storagecrimeweather.dfs.core.windows.net/crime_bronze/LA_crime_data.csv'
    file_path_output = 'abfss://crimeweathersilver@storagecrimeweather.dfs.core.windows.net/crime_silver/crime_silver.csv'

    processor = CrimeDataProcessor(spark, file_path_input, file_path_output)
    processor.load_data()
    processor.add_crime_category()
    processor.process_timestamps()
    processor.save_data()

if __name__ == "__main__":
    main()
