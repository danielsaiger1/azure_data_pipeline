import pandas as pd
import numpy as np
from pyspark.sql.functions import col

class WeatherDataProcessor:
    def __init__(self, file_path_input, file_path_output):
        self.file_path_input = file_path_input
        self.file_path_output = file_path_output
        self.df_weather = None

    def load_data(self):
        df_raw = spark.read.json(self.file_path_input)

        if df_raw.rdd.isEmpty():
            raise ValueError(f"No data found in the file at path: {self.file_path_input}")

        df_transformed = df_raw.select(
            col("dt").alias("timestamp_unix"),
            col("main.temp").alias("temperature"),
            col("clouds.all").alias("cloudiness"),
            col("weather.main").getItem(0).alias("weather_main"),
            col("weather.description").getItem(0).alias("weather_description")
        )

        self.df_weather = df_transformed.toPandas()

    # Method for parsing the dates from UNIX to datetime format
    @staticmethod
    def parse_date(value):
        try:
            return pd.to_datetime(value, unit='s')
        except (ValueError, TypeError):
            return pd.NaT

    # Method to extract Year, Month, Day, Hour from timestamp
    @staticmethod
    def split_date(dataframe, formats):
        for fmt in formats:
            dataframe[fmt] = getattr(dataframe['timestamp'].dt, fmt)
        return dataframe

    # Method to convert temperatures from Kelvin to Celsius
    @staticmethod
    def parse_temp(kelvin):
        return round((kelvin - 273.15), 2)

    # Method to categorize hours in time of day 
    @staticmethod
    def categorize_time(hour):
        if 6 <= hour < 12:
            return "morning"
        elif 12 <= hour < 18:
            return "afternoon"
        elif 18 <= hour < 24:
            return "evening"
        else:
            return "night"

    # Method to categorizes temperature in types
    @staticmethod
    def temp_type(temp):
        if temp < 10.01:
            return 'cold'
        elif temp > 10 and temp < 20.01:
            return 'mild'
        else:
            return 'hot'

    # Method to categorize weather in weather types
    @staticmethod
    def weather_type(dataframe):
        good_weather_conditions = ['clear sky', 'few clouds']
        dataframe['weather_type'] = np.where(dataframe['weather_description'].isin(good_weather_conditions), "good", "bad")
        return dataframe

    # Method to categorize months in seasons
    @staticmethod
    def get_season(month):
        if month in [12, 1, 2]:
            return "winter"
        elif month in [3, 4, 5]:
            return "spring"
        elif month in [6, 7, 8]:
            return "summer"
        else:
            return "fall"

    def process_data(self):
        if self.df_weather is None:
            raise ValueError("DataFrame is empty. Please load the data first.")

        # Parse timestamps
        self.df_weather['timestamp'] = self.df_weather['timestamp_unix'].apply(self.parse_date)

        # Parse temperatures to Celsius
        self.df_weather['temperature'] = self.df_weather['temperature'].apply(self.parse_temp)

        # Split dates into separate columns
        formats = ['year', 'month', 'day', 'hour']
        self.df_weather = self.split_date(self.df_weather, formats)

        # Extract time of day
        self.df_weather['time_of_day'] = self.df_weather['hour'].apply(self.categorize_time)

        # Parse weather types
        self.df_weather = self.weather_type(self.df_weather)

        # Temperature types
        self.df_weather['temp_type'] = self.df_weather['temperature'].apply(self.temp_type)

        # Seasons
        self.df_weather['season'] = self.df_weather['month'].apply(self.get_season)

    def get_processed_data(self):
        return self.df_weather
    
    def save_data(self):
        self.df_weather.to_csv(self.file_path_output)

def main():
    file_path_input = 'abfss://crimeweatherbronze@storagecrimeweather.dfs.core.windows.net/weather_bronze/openweather_raw.json'
    file_path_output = 'abfss://crimeweathersilver@storagecrimeweather.dfs.core.windows.net/weather_silver/openweather_silver.csv'

    processor = WeatherDataProcessor(file_path_input, file_path_output)
    processor.load_data()
    processor.process_data()
    processor.save_data()

    #df_weather = processor.get_processed_data()
    

if __name__ == "__main__":
    main()
