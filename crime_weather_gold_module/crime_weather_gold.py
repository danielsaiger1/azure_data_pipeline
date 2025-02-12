import pandas as pd

class DataMerger:
    def __init__(self, file_path_input_weather, file_path_input_crime, file_path_output):
        self.file_path_input_weather = file_path_input_weather
        self.file_path_input_crime = file_path_input_crime
        self.file_path_output = file_path_output
    
    def load_df(self):
        self.df_weather_raw = spark.read.csv(self.file_path_input_weather, header=True)
        self.df_weather.reset_index(drop=True, inplace=True)
        
        self.df_crime_raw = spark.read.csv(self.file_path_input_crime, header=True)
        self.df_crime.reset_index(drop=True, inplace=True)
        
        return self.df_weather_raw, self.df_crime_raw

    def merge_df(self):
        self.df_crime_weather = pd.merge(self.df_weather_raw, self.df_crime_raw, on='timestamp_unix', how='inner')
        return self.df_crime_weather
    
    def process_df(self):
        self.df_crime_weather = self.df_crime_weather.iloc[3:]
        self.df_crime_weather = self.df_crime_weather.rename(columns = {"timestamp_x": "timestamp"})
        self.df_crime_weather = self.df_crime_weather.drop(['_c0_x', '_c0_y', 'timestamp_y', 'Crm Cd', 'Crm Cd 1', 'Crm Cd 2', 'Crm Cd 3', 'Crm Cd 4'], axis=1)
        
        return self.df_crime_weather
    
    def save_df(self):
        self.df_crime_weather.to_csv(self.file_path_output)
        print(f"Data saved to {self.file_path_output}") 
        
def main():
    file_path_input_weather = 'abfss://crimeweathersilver@storagecrimeweather.dfs.core.windows.net/weather_silver/openweather_silver.csv'
    file_path_input_crime = 'abfss://crimeweathersilver@storagecrimeweather.dfs.core.windows.net/crime_silver/crime_silver.csv'
    file_path_output = 'abfss://crimeweathergold@storagecrimeweather.dfs.core.windows.net/gold_layer/crime_weather_gold.csv'
    
    merger = DataMerger(file_path_input_weather, file_path_input_crime, file_path_output)
    merger.load_df()
    merger.merge_df()
    merger.process_df()
    merger.save_df()