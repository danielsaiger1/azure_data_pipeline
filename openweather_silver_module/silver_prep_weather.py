from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, from_unixtime, year, month, dayofmonth, hour, when

class WeatherDataProcessor:
    def __init__(self, spark, file_path_input, file_path_output):
        self.spark = spark
        self.file_path_input = file_path_input
        self.file_path_output = file_path_output
        self.df_weather = None

    def load_data(self):
        df_raw = self.spark.read.json(self.file_path_input)
        
        if df_raw.rdd.isEmpty():
            raise ValueError(f"No data found in the file at path: {self.file_path_input}")
        
        self.df_weather = df_raw.select(
            col("dt").alias("timestamp_unix"),
            col("main.temp").alias("temperature"),
            col("clouds.all").alias("cloudiness"),
            col("weather.main")[0].alias("weather_main"),
            col("weather.description")[0].alias("weather_description")
        )

    def process_data(self):
        if self.df_weather is None:
            raise ValueError("DataFrame is empty. Please load the data first.")

        self.df_weather = self.df_weather.withColumn("timestamp", from_unixtime(col("timestamp_unix")))
        self.df_weather = self.df_weather.withColumn("year", year(col("timestamp")))
        self.df_weather = self.df_weather.withColumn("month", month(col("timestamp")))
        self.df_weather = self.df_weather.withColumn("day", dayofmonth(col("timestamp")))
        self.df_weather = self.df_weather.withColumn("hour", hour(col("timestamp")))
        
        # Convert temperature from Kelvin to Celsius
        self.df_weather = self.df_weather.withColumn("temperature_celsius", col("temperature") - 273.15)
        
        # Categorize time of day
        self.df_weather = self.df_weather.withColumn("time_of_day", when((col("hour") >= 6) & (col("hour") < 12), "morning")
                                                     .when((col("hour") >= 12) & (col("hour") < 18), "afternoon")
                                                     .when((col("hour") >= 18) & (col("hour") < 24), "evening")
                                                     .otherwise("night"))
        
        # Categorize temperature
        self.df_weather = self.df_weather.withColumn("temp_type", when(col("temperature_celsius") < 10.01, "cold")
                                                     .when((col("temperature_celsius") >= 10.01) & (col("temperature_celsius") <= 20.00), "mild")
                                                     .otherwise("hot"))
        
        # Categorize weather
        good_weather_conditions = ["clear sky", "few clouds"]
        self.df_weather = self.df_weather.withColumn("weather_type", when(col("weather_description").isin(good_weather_conditions), "good").otherwise("bad"))
        
        # Categorize seasons
        self.df_weather = self.df_weather.withColumn("season", when(col("month").isin([12, 1, 2]), "winter")
                                                     .when(col("month").isin([3, 4, 5]), "spring")
                                                     .when(col("month").isin([6, 7, 8]), "summer")
                                                     .otherwise("fall"))

    def save_data(self):
        self.df_weather.write.mode("overwrite").csv(self.file_path_output, header=True)


def main():
    spark = SparkSession.builder.appName("WeatherDataProcessor").getOrCreate()
    
    file_path_input = 'abfss://crimeweatherbronze@storagecrimeweather.dfs.core.windows.net/weather_bronze/openweather_raw.json'
    file_path_output = 'abfss://crimeweathersilver@storagecrimeweather.dfs.core.windows.net/weather_silver/openweather_silver.csv'
    
    processor = WeatherDataProcessor(spark, file_path_input, file_path_output)
    processor.load_data()
    processor.process_data()
    processor.save_data()

if __name__ == "__main__":
    main()
