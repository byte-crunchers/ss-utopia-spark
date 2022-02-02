from pyspark.sql import SparkSession
from countries import Point, CountryChecker


spark = SparkSession.builder.master("local").appName("MLData").getOrCreate()
raw = spark.read.options(header='True', inferSchema='True', delimiter=',').csv("raw_fraud_test.csv")
stripped = raw.select("trans_date_trans_time", "cc_num", "amt", "merch_lat", "merch_long", "is_fraud")





cc = CountryChecker('TM_WORLD_BORDERS-0.3/TM_WORLD_BORDERS-0.3.shp')

def getCountry(lat, long, cc):
  country = cc.getCountry(Point(lat, long))
  if country: return country.iso
  else: return "ocean"
def strippedToCountry(row,cc):
  return (row[0], row[1], row[2], getCountry(row[3], row[4], cc),row[5])

country = stripped.rdd.map(lambda x: strippedToCountry(x,cc))

country.take(5).foreach(print)

#stripped.show(10)
#stripped.toPandas().to_csv("ml/stripped_fraud_test.csv")
