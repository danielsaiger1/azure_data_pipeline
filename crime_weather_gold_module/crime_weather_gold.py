import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from azure.storage.filedatalake import DataLakeServiceClient
from azure.identity import DefaultAzureCredential

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataMerger:
    def __init__(self, file_path_input_weather, file_path_input_crime, file_path_output):
        self.file_path_input_weather = file_path_input_weather
        self.file_path_input_crime = file_path_input_crime
        self.file_path_output = file_path_output
        self.spark = SparkSession.builder.appName("DataMerger").getOrCreate()
    
    def load_df(self):
        try:
            self.df_weather_raw = self.spark.read.csv(self.file_path_input_weather, header=True, inferSchema=True)
            self.df_crime_raw = self.spark.read.csv(self.file_path_input_crime, header=True, inferSchema=True)
            logger.info("DataFrames loaded successfully.")
        except Exception as e:
            logger.error(f"Error loading DataFrames: {e}")
            raise
        return self.df_weather_raw, self.df_crime_raw

    def merge_df(self):
        try:
            self.df_crime_weather = self.df_weather_raw.join(
                self.df_crime_raw, on="timestamp_unix", how="inner"
            )
            logger.info("DataFrames merged successfully.")
        except Exception as e:
            logger.error(f"Error merging DataFrames: {e}")
            raise
        return self.df_crime_weather
    
    def process_df(self):
        try:
            drop_columns = ['_c0_x', '_c0_y', 'timestamp_y', 'Crm Cd', 'Crm Cd 1', 'Crm Cd 2', 'Crm Cd 3', 'Crm Cd 4']
            drop_columns = [col for col in drop_columns if col in self.df_crime_weather.columns]  # Ensure columns exist

            self.df_crime_weather = self.df_crime_weather.drop(*drop_columns)
            self.df_crime_weather = self.df_crime_weather.withColumnRenamed("timestamp_x", "timestamp")
            logger.info("DataFrame processed successfully.")
        except Exception as e:
            logger.error(f"Error processing DataFrame: {e}")
            raise
        return self.df_crime_weather
    
    def save_df(self):
        try:
            self.df_crime_weather.write.mode("overwrite").csv(self.file_path_output, header=True)
            logger.info(f"Data saved to {self.file_path_output}")
        except Exception as e:
            logger.error(f"Error saving DataFrame: {e}")
            raise
        
def main():
    try:
        file_path_input_weather = 'abfss://crimeweathersilver@storagecrimeweather.dfs.core.windows.net/weather_silver/openweather_silver.csv'
        file_path_input_crime = 'abfss://crimeweathersilver@storagecrimeweather.dfs.core.windows.net/crime_silver/crime_silver.csv'
        file_path_output = 'abfss://crimeweathergold@storagecrimeweather.dfs.core.windows.net/gold_layer/crime_weather_gold.csv'
        
        merger = DataMerger(file_path_input_weather, file_path_input_crime, file_path_output)
        merger.load_df()
        merger.merge_df()
        merger.process_df()
        merger.save_df()
    except Exception as e:
        logger.error(f"Error in main function: {e}")

if __name__ == "__main__":
    main()