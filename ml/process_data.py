import csv
import datetime
import tqdm #progress bar
from dateutil import parser

if __name__ == "__main__":
  csvfile = open("loc_fraud_test.csv", newline='')
  num_lines = sum(1 for line in csvfile)
  csvfile.seek(0) #go back to the beginning of the file
  
  rdr = csv.reader(csvfile)
  cards = {} #dictionary entry for each card containing all transaction that use that card
  
  for row in tqdm.tqdm(rdr, "Parsing CSV", total=num_lines):
    try:
      cards[row[1]].append((parser.parse(row[0]), float(row[2]), row[3], int(row[4])),)
    except:
      cards[row[1]] = [(parser.parse(row[0]), float(row[2]), row[3], int(row[4])),]
  print (cards["2291163933867244"]) #debug
  
  csvfile.seek(0) #go back to the beginning of the file

  
  with open("vel_fraud_test.csv", 'w', newline = '') as output:
    wtr = csv.writer(output)
    for row in tqdm.tqdm(rdr, "Analyzing Velocity", num_lines):
      history = cards[row[1]] #other transactions on this card
      trans_date = parser.parse(row[0]) #this is wasteful, but is simplifies things
      amount = float(row[2])
      country = row[3]
      isfraud = int(row[4])

      #We want to compare transactions in the past three days with the proceding 30 days
      window = trans_date - datetime.timedelta(days = 3)
      comparison_date = trans_date - datetime.timedelta(days = 33)

      recent = 0
      past = 0
      for trans in history:
        #if trans[0] > trans_date:
        #  print ("greater")
        #print (trans[0], trans_date, window)
        if trans[0] < trans_date and trans[0] > window:
          recent += trans[1] #add amount
        elif trans[0] < window and trans [0] > comparison_date:
          past += trans[1] #add amount
      #print(f"recent = {recent:.2f}, history = {past:.2f}")
      #skip velocity check if user has not much history or hasn't used their card recently
      if recent == None or recent < 10 or past == None or past < 100:
        velocity = 0
      else:
        velocity = (10 * recent / past - 1)
      wtr.writerow((row[0], row[1], velocity, row[2], row[3], row[4]),)
    
    
    print("Done!!\n\n")
      
    

  '''
  curs = this.conn.cursor()
        current_date = datetime.datetime.now()
        window = current_date - datetime.timedelta(days = 3)
        comparison_date = current_date - datetime.timedelta(days = 33)
        curs.execute("SELECT SUM(transfer_value) FROM card_transactions \
                      WHERE card_num = " + str(this.trans.card) + " AND time_stamp > '" + str(window) +  "'")
        recent = curs.fetchone()[0]
        #if the user has no recent activity, skip this check
        if recent == None or recent < 10:
            return
        #print("recent: " + str(recent))
        curs.execute("SELECT sum(transfer_value) FROM card_transactions \
                      WHERE card_num = " + str(this.trans.card) + " \
                      AND time_stamp BETWEEN '" + str(comparison_date) +  "' AND '" + str(window) + "'")
        history = curs.fetchone()[0]
        #print ("history: " + str(history))
        #if the user has no history, add half the velocity weighting
        if history == None or history < 100:
            this.fraud_value += this.weighting_velocity * 0.5
            return
        this.fraud_value += (10 * recent / history - 1) * this.weighting_velocity'''

   
  
  
  csvfile.close()




'''
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
