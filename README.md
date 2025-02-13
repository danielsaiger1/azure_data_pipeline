# Azure Data Pipeline

This project is an example of a simple Azure Data Pipeline.

It uses Los Angeles crime data and weather data from the OpenWeather API

## Datasets

**Weather Data:**  https://openweathermap.org/ (API)
**LA crime data:** https://data.lacity.org/Public-Safety/Crime-Data-from-2020-to-Present/2nrs-mtv8/about_data (.CSV)

## Architecture
Raw data gets loaded into the bronze layer. After that, gets transformed and moved to silver and finally joined in gold using Azure Synapse (PySpark). Final visualisation can be made in a analytics tool like PowerBI

Secrets are handled with Azure Key Vault

![image](https://github.com/user-attachments/assets/61a1eab1-ae72-476d-b4fe-b45bd2aa5130)



