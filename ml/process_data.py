from countries import Point, CountryChecker
import csv
from multiprocessing import Pool, Process
import tqdm



#this is ugly but it sets up an initializer function that gives each process a seperate cc
cc = None
def getcc():
  global cc
  cc = CountryChecker('TM_WORLD_BORDERS-0.3/TM_WORLD_BORDERS-0.3.shp')

def getCountry(lat, long, cc):
  country = cc.getCountry(Point(lat, long))
  if country: return country.iso
  else: return "ocean"

def procloc (row):
  return (row[0], row[1], row[2], getCountry(row[3], row[4], cc), row[5])



if __name__ == "__main__":
  csvfile = open("raw_fraud_train.csv", newline='')
  rdr = csv.reader(csvfile)
  next(rdr) #skip header
  stripped = [(row[1], row[2], float(row[5]), float(row[20]), float(row[21]), int(row[22])) for row in rdr]
  for i in range (0, 5):
    print(stripped[i])
  csvfile.close()


  locs = []
  cut = stripped[0:50]
  with Pool(processes=10, initializer=getcc, initargs=()) as p:
    for row in tqdm.tqdm(p.imap(procloc, stripped, 500), total=len(stripped)):
      locs.append(row)

    #locs = [(row[0], row[1], row[2], getCountry(row[3], row[4], cc),row[5]) for row in stripped]
    for i in range (0, 5):
        print(locs[i])

    with open("loc_fraud_train.csv", 'w', newline = '') as output:
      wtr = csv.writer(output)
      for row in locs:
        wtr.writerow(row)
      print("Done!!\n\n")
    





'''
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
'''