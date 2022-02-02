from pyspark.sql import SparkSession
from geopy.geocoders import Nominatim

spark = SparkSession.builder.master("local").appName("MLData").getOrCreate()
raw = spark.read.options(header='True', inferSchema='True', delimiter=',').csv("ml/raw_fraud_test.csv")
stripped = raw.select("trans_date_trans_time", "cc_num", "amt", "merch_lat", "merch_long", "is_fraud")




def getCountry(lat, long):
  geo = Nominatim(user_agent="MLDataProcessor")
  return geo.reverse(str(lat)+","+str(long)).raw['address'].get("country_code")

def strippedToCountry(row):
  print("test2")
  return (row[0], row[1], row[2], getCountry(row[3], row[4]),row[5])
print("test")
country = stripped.rdd.map(strippedToCountry)

country.take(5).foreach(print)

#stripped.show(10)
#stripped.toPandas().to_csv("ml/stripped_fraud_test.csv")
