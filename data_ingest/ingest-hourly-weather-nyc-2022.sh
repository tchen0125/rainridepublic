wget -O hourly-weather-nyc-2022.csv https://www.ncei.noaa.gov/data/local-climatological-data/access/2022/72505394728.csv
hdfs dfs -copyFromLocal hourly-weather-nyc-2022.csv /user/bj2351_nyu_edu/final/data/hourly-weather-nyc-2022.csv

